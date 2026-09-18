//! Overlaps table decoding with frame receipt.
//!
//! A q table arrives as a name list followed by one column payload after another, so every
//! column whose bytes have fully arrived can be decoded while the peer is still delivering the
//! rest. The caller keeps reading the socket on its own thread and publishes how many bytes are
//! in place; a scanner task on the thread pool walks the columns as far as the received prefix
//! allows and hands each complete column to a decode task. Both sides address one buffer: the
//! reader writes only beyond the published length and the decoders read only below it.

use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::mpsc;
use std::sync::{Condvar, Mutex};

use polars::prelude::{DataFrame, Series};

use crate::connector::QStream;
use crate::errors::XqdbError;
use crate::serde6::{deserialize_series, ListScan, ScanOutcome};
use crate::types::{SymbolEncoding, K};

/// Bytes after the table type byte: attribute, dictionary type (99) and the name list's type (11).
const TABLE_PREAMBLE: usize = 4;
/// Type (0), attribute and count of the column list that follows the names.
const COLUMN_LIST_HEADER: usize = 6;
pub(crate) const TABLE_TYPE: u8 = 98;

struct BodyPointer(*mut u8);

// SAFETY: the pointer is only dereferenced through `SharedBody`, whose reader and writers touch
// disjoint byte ranges separated by the published length.
unsafe impl Send for BodyPointer {}
unsafe impl Sync for BodyPointer {}

struct Progress {
    received: usize,
    failed: bool,
}

/// One frame body in flight: a buffer the reader fills front to back and decoders read behind it.
struct SharedBody {
    bytes: BodyPointer,
    total: usize,
    progress: Mutex<Progress>,
    changed: Condvar,
}

impl SharedBody {
    /// # Safety
    ///
    /// `bytes` must stay valid for `total` bytes for the lifetime of the value, `received` bytes
    /// must already be initialized, and nothing but `write_region` may write through it.
    unsafe fn new(bytes: *mut u8, total: usize, received: usize) -> Self {
        Self {
            bytes: BodyPointer(bytes),
            total,
            progress: Mutex::new(Progress {
                received,
                failed: false,
            }),
            changed: Condvar::new(),
        }
    }

    fn snapshot(&self) -> Result<usize, XqdbError> {
        let progress = self
            .progress
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if progress.failed {
            return Err(receipt_failed());
        }
        Ok(progress.received)
    }

    /// Bytes delivered so far, valid to read because the reader never rewrites them.
    fn received(&self, count: usize) -> &[u8] {
        // SAFETY: `count` came from `progress.received`, which only ever names initialized bytes
        // the reader has finished writing, and the pointer stays valid for the whole body.
        unsafe { std::slice::from_raw_parts(self.bytes.0, count) }
    }

    /// Blocks until at least `needed` bytes are in place and returns everything received.
    fn wait_for(&self, needed: usize) -> Result<&[u8], XqdbError> {
        if needed > self.total {
            return Err(XqdbError::DeserializationErr(
                "q table structure extends beyond its frame".to_string(),
            ));
        }
        let mut progress = self
            .progress
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while progress.received < needed && !progress.failed {
            progress = self
                .changed
                .wait(progress)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        if progress.failed {
            return Err(receipt_failed());
        }
        let count = progress.received;
        drop(progress);
        Ok(self.received(count))
    }

    fn publish(&self, received: usize) {
        let mut progress = self
            .progress
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        progress.received = received;
        drop(progress);
        self.changed.notify_all();
    }

    fn fail(&self) {
        let mut progress = self
            .progress
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        progress.failed = true;
        drop(progress);
        self.changed.notify_all();
    }
}

fn receipt_failed() -> XqdbError {
    XqdbError::IOError(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "IPC message body ended before its declared length",
    ))
}

/// Reads the rest of a table frame into `buffer`, decoding columns as they complete.
///
/// `buffer` already holds the first `buffer.len()` body bytes and has capacity for the whole
/// body. The I/O outcome is returned separately from the decode outcome: a failed read leaves the
/// connection out of sync and the caller disconnects, while a malformed table is an ordinary
/// error after a fully consumed frame.
pub(crate) fn receive_table(
    stream: &mut (dyn QStream + Send + Sync),
    buffer: &mut Vec<u8>,
    body_length: usize,
    encoding: SymbolEncoding,
) -> Result<Result<K, XqdbError>, XqdbError> {
    debug_assert!(buffer.capacity() >= body_length);
    debug_assert!(buffer.first() == Some(&TABLE_TYPE));
    let received = buffer.len();
    // SAFETY: the reservation covers `body_length` bytes, the first `received` are initialized,
    // and the buffer is neither touched nor reallocated until the scope below has finished.
    let body = unsafe { SharedBody::new(buffer.as_mut_ptr(), body_length, received) };
    let (sender, results) = mpsc::channel();
    let names: Mutex<Option<Result<Vec<String>, XqdbError>>> = Mutex::new(None);
    let io_result: Mutex<Option<Result<(), XqdbError>>> = Mutex::new(None);

    // A decode task that panics on malformed input surfaces here only after the scope has joined
    // every task, including the reader, so the frame is consumed either way and the I/O outcome
    // recorded inside the scope decides whether the connection survived.
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        rayon::scope(|scope| {
            let (names, body, io_result) = (&names, &body, &io_result);
            scope.spawn(move |scope| {
                let outcome = scan_columns(scope, body, encoding, sender);
                *names
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(outcome);
            });
            let result = read_remaining(stream, body, received);
            *io_result
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(result);
        });
    }));
    match io_result
        .into_inner()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
    {
        Some(Ok(())) => (),
        Some(Err(error)) => return Err(error),
        None => return Err(receipt_failed()),
    }
    if outcome.is_err() {
        return Ok(Err(parser_panic()));
    }
    let names = match names
        .into_inner()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
    {
        Some(Ok(names)) => names,
        Some(Err(error)) => return Ok(Err(error)),
        None => return Ok(Err(parser_panic())),
    };

    let mut columns: Vec<Option<Series>> = (0..names.len()).map(|_| None).collect();
    for (index, column) in results.try_iter() {
        match column {
            Ok(column) => columns[index] = Some(column),
            Err(error) => return Ok(Err(error)),
        }
    }
    let mut frame_columns = Vec::with_capacity(names.len());
    for (column, name) in columns.into_iter().zip(names) {
        let Some(mut column) = column else {
            return Ok(Err(parser_panic()));
        };
        column.rename(name.into());
        frame_columns.push(column.into());
    }
    Ok(DataFrame::new_infer_height(frame_columns)
        .map(K::DataFrame)
        .map_err(|error| XqdbError::DeserializationErr(error.to_string())))
}

pub(crate) fn parser_panic() -> XqdbError {
    XqdbError::DeserializationErr("malformed q value caused an internal parser panic".to_string())
}

fn read_remaining(
    stream: &mut (dyn QStream + Send + Sync),
    body: &SharedBody,
    mut received: usize,
) -> Result<(), XqdbError> {
    while received < body.total {
        // SAFETY: only this thread writes, always past the count it last published, so the region
        // starts beyond every byte a decoder may be reading and ends at the allocation, whose
        // capacity the caller initialized.
        let region = unsafe {
            std::slice::from_raw_parts_mut(body.bytes.0.add(received), body.total - received)
        };
        match stream.read(region) {
            Ok(0) => {
                body.fail();
                return Err(receipt_failed());
            }
            Ok(count) => {
                received += count;
                body.publish(received);
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => {
                body.fail();
                return Err(XqdbError::IOError(error));
            }
        }
    }
    Ok(())
}

/// Walks the table structure as bytes arrive and spawns one decode task per complete column,
/// returning the column names once the whole body has been accounted for.
fn scan_columns<'scope>(
    scope: &rayon::Scope<'scope>,
    body: &'scope SharedBody,
    encoding: SymbolEncoding,
    sender: mpsc::Sender<(usize, Result<Series, XqdbError>)>,
) -> Result<Vec<String>, XqdbError> {
    let mut pos = TABLE_PREAMBLE;
    let names_end = scan_to_end(&mut ListScan::new(11, pos)?, body)?;
    let received = body.wait_for(names_end)?;
    let names = match deserialize_series(&received[pos..names_end], 11, false, encoding)? {
        K::Series(series) => series,
        other => {
            return Err(XqdbError::DeserializationErr(format!(
                "Expecting array, but got {other:?}"
            )))
        }
    };
    let names: Vec<String> = names
        .str()
        .map_err(|error| XqdbError::DeserializationErr(error.to_string()))?
        .iter()
        .map(|name| name.unwrap_or("").to_owned())
        .collect();
    pos = names_end.checked_add(COLUMN_LIST_HEADER).ok_or_else(|| {
        XqdbError::DeserializationErr("q table column list overflowed".to_string())
    })?;

    for index in 0..names.len() {
        let received = body.wait_for(pos.saturating_add(1))?;
        let k_type = received[pos];
        pos += 1;
        let end = scan_to_end(&mut ListScan::new(k_type, pos)?, body)?;
        let column: &'scope [u8] = &body.wait_for(end)?[pos..end];
        let sender = sender.clone();
        scope.spawn(move |_| {
            let decoded =
                deserialize_series(column, k_type, true, encoding).and_then(Series::try_from);
            // The receiver only disappears once the scope has ended, after this task.
            let _ = sender.send((index, decoded));
        });
        pos = end;
    }
    body.wait_for(body.total)?;
    if pos != body.total {
        return Err(XqdbError::DeserializationErr(format!(
            "q value has {} trailing byte(s)",
            body.total - pos
        )));
    }
    Ok(names)
}

fn scan_to_end(scan: &mut ListScan, body: &SharedBody) -> Result<usize, XqdbError> {
    let mut count = body.snapshot()?;
    loop {
        match scan.advance(body.received(count), body.total)? {
            ScanOutcome::Complete(end) => return Ok(end),
            ScanOutcome::Incomplete => {
                count = body.wait_for(count.saturating_add(1))?.len();
            }
        }
    }
}
