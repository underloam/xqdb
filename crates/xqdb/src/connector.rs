use crate::errors::XqdbError;
use crate::pipeline;
use crate::qvalue::{QValue, ValueMode};
use crate::serde6::{compress, decompress, deserialize, serialize_into};
use crate::types::{MsgType, SymbolEncoding, K};
use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, StreamOwned};
use rustls_platform_verifier::Verifier;
use std::borrow::Cow;
use std::collections::VecDeque;
use std::io::{self, Read as IoRead, Write as IoWrite};
use std::net::{Shutdown, SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub(crate) trait QStream: IoRead + IoWrite {}

impl<S: IoRead + IoWrite> QStream for S {}

/// Sized `Read` adapter over the boxed stream so `Read` combinators requiring `Self: Sized`
/// (e.g. `take`) apply to it.
struct StreamReader<'a>(&'a mut (dyn QStream + Send + Sync));

impl IoRead for StreamReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.0.read(buffer)
    }
}

#[derive(Debug)]
struct SharedTcpStream(Arc<TcpStream>);

impl IoRead for SharedTcpStream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.0.as_ref().read(buffer)
    }
}

impl IoWrite for SharedTcpStream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0.as_ref().write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.as_ref().flush()
    }
}

#[derive(Debug, Default)]
struct AbortState {
    active_operation: Option<AbortOperation>,
    next_operation_id: u64,
}

#[derive(Debug)]
struct AbortOperation {
    id: u64,
    stream: Option<Arc<TcpStream>>,
}

#[derive(Clone, Debug, Default)]
pub struct ConnectorAbortHandle {
    state: Arc<Mutex<AbortState>>,
}

struct ActiveIoGuard {
    state: Arc<Mutex<AbortState>>,
    id: u64,
}

impl ActiveIoGuard {
    fn attach_stream(&self, stream: Arc<TcpStream>) -> bool {
        let attached = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match state.active_operation.as_mut() {
                Some(operation) if operation.id == self.id => {
                    operation.stream = Some(Arc::clone(&stream));
                    true
                }
                _ => false,
            }
        };
        if !attached {
            let _ = stream.shutdown(Shutdown::Both);
        }
        attached
    }

    fn finish(self) -> bool {
        let aborted = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match state.active_operation.as_ref() {
                Some(operation) if operation.id == self.id => {
                    state.active_operation = None;
                    false
                }
                _ => true,
            }
        };
        aborted
    }
}

impl Drop for ActiveIoGuard {
    fn drop(&mut self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state
            .active_operation
            .as_ref()
            .is_some_and(|operation| operation.id == self.id)
        {
            state.active_operation = None;
        }
    }
}

impl ConnectorAbortHandle {
    pub fn abort(&self) -> Result<(), XqdbError> {
        let operation = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .active_operation
            .take();
        match operation.and_then(|operation| operation.stream) {
            Some(stream) => {
                match stream.shutdown(Shutdown::Both) {
                    Ok(()) => (),
                    Err(error) if error.kind() == io::ErrorKind::NotConnected => (),
                    Err(error) => return Err(XqdbError::IOError(error)),
                }
                #[cfg(windows)]
                {
                    use std::os::windows::io::AsRawSocket;
                    use windows_sys::Win32::Foundation::{ERROR_NOT_FOUND, HANDLE};
                    use windows_sys::Win32::System::IO::CancelIoEx;

                    // Winsock shutdown prevents new I/O but does not wake an already blocked
                    // recv while the peer stays open. Cancel pending I/O after shutdown so a
                    // read_exact retry cannot start another blocking read.
                    // SAFETY: the Arc keeps this socket handle alive throughout the call.
                    // A null OVERLAPPED cancels all pending operations on this socket only.
                    if unsafe { CancelIoEx(stream.as_raw_socket() as HANDLE, std::ptr::null()) }
                        == 0
                    {
                        let error = io::Error::last_os_error();
                        if error.raw_os_error() != Some(ERROR_NOT_FOUND as i32) {
                            return Err(XqdbError::IOError(error));
                        }
                    }
                }
                Ok(())
            }
            None => Ok(()),
        }
    }

    fn begin_operation(&self) -> ActiveIoGuard {
        let id = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let id = state.next_operation_id;
            state.next_operation_id = state.next_operation_id.wrapping_add(1);
            state.active_operation = Some(AbortOperation { id, stream: None });
            id
        };
        ActiveIoGuard {
            state: Arc::clone(&self.state),
            id,
        }
    }
}
/// Controls whether outgoing IPC frames are considered for q's wire compression.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CompressionMode {
    /// Preserve the historical policy: compress non-local frames at or above the threshold.
    #[default]
    Auto,
    /// Attempt compression for every frame (q's compressor keeps frames that do not benefit raw).
    On,
    /// Never compress outgoing frames.
    Off,
}

/// Connection policy applied by [`Connector::configure`] before the socket is opened.
///
/// A missing granular timeout inherits [`Connector::timeout`]; an explicit zero disables it.
/// TLS byte fields contain PEM data. Client certificate and key must be supplied together.
#[derive(Clone)]
pub struct ConnectorSettings {
    pub value_mode: ValueMode,
    pub compression: CompressionMode,
    pub compression_threshold: usize,
    pub connect_timeout: Option<Duration>,
    pub read_timeout: Option<Duration>,
    pub write_timeout: Option<Duration>,
    pub max_message_bytes: Option<usize>,
    pub max_pending_notifications: usize,
    pub tls_ca: Option<Vec<u8>>,
    pub tls_cert: Option<Vec<u8>>,
    pub tls_key: Option<Vec<u8>>,
    pub tls_server_name: Option<String>,
}

impl Default for ConnectorSettings {
    fn default() -> Self {
        Self {
            value_mode: ValueMode::Native,
            compression: CompressionMode::Auto,
            compression_threshold: 10_000_000,
            connect_timeout: None,
            read_timeout: None,
            write_timeout: None,
            max_message_bytes: None,
            max_pending_notifications: 1024,
            tls_ca: None,
            tls_cert: None,
            tls_key: None,
            tls_server_name: None,
        }
    }
}

pub struct Connector {
    pub enable_tls: bool,
    pub is_local: bool,
    pub port: u16,
    pub version: u8,
    pub host: String,
    pub user: String,
    pub password: String,
    pub timeout: Duration,
    pub symbol_encoding: SymbolEncoding,
    stream: Option<Box<dyn QStream + Send + Sync>>,
    tcp_stream: Option<Arc<TcpStream>>,
    abort_handle: ConnectorAbortHandle,
    settings: ConnectorSettings,
    pending_notifications: VecDeque<Result<K, XqdbError>>,
    receive_buffer: Vec<u8>,
    send_buffer: Vec<u8>,
}

const IPC_HEADER_LENGTH: usize = 8;
const MIN_SERIALIZED_VALUE_LENGTH: usize = 2;
/// Largest length the q IPC header can express: the byte at index 3 carries bits 32..40 and the
/// little-endian u32 at index 4..8 carries the low bits. This is the wire format's own limit, not
/// a policy ceiling, so nothing below it is refused.
const MAX_IPC_LENGTH_FIELD: usize = (1 << 40) - 1;
/// Upper bound on how far the q IPC decompressor can expand its input.
///
/// A group spends one control byte plus up to eight units; a back-reference unit spends two input
/// bytes and emits at most 257, so the worst case is 2056 output bytes per 17 input bytes (< 121x).
/// Output declared above this is unreachable from the payload already held in memory, so it is
/// rejected before the destination buffer is allocated and zeroed. Measured against this client's
/// own compressor on maximally compressible payloads: 120.935x at 10 MB, always below the bound.
const MAX_IPC_DECOMPRESSION_RATIO: u64 = 121;
/// First reservation for a message body, before the peer has delivered any of it, and the factor by
/// which it grows once full.
///
/// The floor is what a declared length alone can reserve, and the cost of gating is a chunked read
/// rather than the growth copies: measured against kola, a 64 KiB floor lost roughly a quarter of
/// the throughput on a 51 MiB table and cutting copy volume sevenfold did not recover it. A floor
/// above ordinary table payloads keeps those reads in a single pass, so only frames large enough to
/// be worth gating pay for a growth step.
const INITIAL_BODY_RESERVATION: usize = 32 * 1024 * 1024;
const BODY_RESERVATION_FACTOR: usize = 8;
/// Frame buffers are kept between requests so a steady stream of similar-sized messages reuses
/// committed pages instead of faulting in a fresh allocation each time: on Windows, first-touch of
/// a 51 MB buffer measured 3.9 ms, about a tenth of the whole read of a table that size. Buffers
/// that grew beyond this many bytes are released after use.
const MAX_RETAINED_BUFFER_BYTES: usize = 256 * 1024 * 1024;
/// Table bodies at least this large are decoded column by column while the rest of the frame is
/// still arriving; below it the decode is too short to be worth a thread-pool hand-off.
const PIPELINE_MIN_BODY_BYTES: usize = 256 * 1024;

/// Splits an IPC message length into the 40-bit form the q header carries, returning the high byte
/// written at index 3 and the little-endian u32 written at index 4..8.
pub(crate) fn ipc_length_header(total_length: usize) -> Result<(u8, u32), XqdbError> {
    if total_length > MAX_IPC_LENGTH_FIELD {
        return Err(XqdbError::Err(format!(
            "IPC message length {total_length} exceeds the {MAX_IPC_LENGTH_FIELD}-byte q header length field"
        )));
    }
    Ok(((total_length >> 32) as u8, total_length as u32))
}

fn checked_outgoing_message_length(
    body_length: usize,
    description: &str,
) -> Result<(usize, u8, u32), XqdbError> {
    let total_length = body_length
        .checked_add(IPC_HEADER_LENGTH)
        .ok_or_else(|| XqdbError::Err(format!("{description} length overflowed")))?;
    let (high_byte, low_length) = ipc_length_header(total_length)?;
    Ok((total_length, high_byte, low_length))
}

fn allocate_buffer(length: usize, description: &str) -> Result<Vec<u8>, XqdbError> {
    let mut buffer = Vec::new();
    reserve_buffer(&mut buffer, length, description)?;
    Ok(buffer)
}

/// Ensures an empty buffer can hold `length` bytes, keeping any larger capacity it already has.
/// Newly reserved capacity is zero-filled once, so the whole capacity is initialized memory that
/// raw-pointer readers and writers in the pipelined receive path may address.
fn reserve_buffer(buffer: &mut Vec<u8>, length: usize, description: &str) -> Result<(), XqdbError> {
    debug_assert!(buffer.is_empty());
    if buffer.capacity() >= length {
        return Ok(());
    }
    buffer.try_reserve_exact(length).map_err(|error| {
        XqdbError::Err(format!(
            "Unable to allocate {description} of {length} bytes: {error}"
        ))
    })?;
    let capacity = buffer.capacity();
    buffer.resize(capacity, 0);
    buffer.clear();
    Ok(())
}

/// Drops a retained frame buffer whose capacity exceeds `MAX_RETAINED_BUFFER_BYTES`, so one
/// oversized message does not pin its memory to the connection for good.
fn release_oversized_buffer(buffer: &mut Vec<u8>) {
    if buffer.capacity() > MAX_RETAINED_BUFFER_BYTES {
        *buffer = Vec::new();
    }
}

fn checked_body_length(total_length: u64, description: &str) -> Result<usize, XqdbError> {
    let total_length = usize::try_from(total_length).map_err(|_| {
        XqdbError::Err(format!(
            "{description} length cannot be represented on this platform"
        ))
    })?;
    let body_length = total_length.checked_sub(IPC_HEADER_LENGTH).ok_or_else(|| {
        XqdbError::Err(format!(
            "{description} length {total_length} is shorter than the {IPC_HEADER_LENGTH}-byte header"
        ))
    })?;
    if body_length < MIN_SERIALIZED_VALUE_LENGTH {
        return Err(XqdbError::Err(format!(
            "{description} body length {body_length} is too short to contain a serialized q value"
        )));
    }
    Ok(body_length)
}

/// Reads a declared message body, keeping the reservation within `BODY_RESERVATION_FACTOR` times
/// the bytes the peer has actually delivered.
///
/// A declared length alone must never turn into an allocation: on Windows, the platform this client
/// ships prebuilt binaries for, a large `HeapAlloc` is forwarded to `VirtualAlloc(MEM_COMMIT)` and
/// charged against the system commit limit immediately, so an 8-byte header claiming a terabyte
/// would starve every other allocation in the process rather than merely reserving address space.
/// The caller therefore reserves at most `INITIAL_BODY_RESERVATION` up front and this loop grows the
/// buffer only once the previous reservation is full. Frames at or below the floor are reserved
/// exactly and read in one pass.
///
/// Each `take` limit is clamped to the bytes still owed, which preserves the frame boundary and
/// keeps `read_to_end` off the infallible `small_probe_read` growth path it takes when full.
fn read_message_body(
    stream: &mut (dyn QStream + Send + Sync),
    body_length: usize,
    body: &mut Vec<u8>,
) -> Result<(), XqdbError> {
    while body.len() < body_length {
        if body.len() == body.capacity() {
            let target = body_length.min(body.capacity().saturating_mul(BODY_RESERVATION_FACTOR));
            body.try_reserve_exact(target - body.len())
                .map_err(|error| {
                    XqdbError::Err(format!(
                        "Unable to grow the IPC message body to {target} bytes: {error}"
                    ))
                })?;
        }
        let owed = body_length - body.len();
        let spare = (body.capacity() - body.len()).min(owed) as u64;
        match StreamReader(&mut *stream).take(spare).read_to_end(body) {
            Ok(0) => {
                return Err(XqdbError::IOError(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "IPC message body ended before its declared length",
                )))
            }
            Ok(_) => (),
            Err(error) => return Err(XqdbError::IOError(error)),
        }
    }
    Ok(())
}

/// Validates a declared decompressed length against the payload that must produce it, so a corrupt
/// or hostile prefix cannot demand an allocation those compressed bytes could never fill.
fn checked_decompressed_body_length(
    declared_total_length: u64,
    compressed_payload_length: usize,
) -> Result<usize, XqdbError> {
    let body_length = checked_body_length(declared_total_length, "Decompressed IPC message")?;
    let maximum_body_length =
        (compressed_payload_length as u64).saturating_mul(MAX_IPC_DECOMPRESSION_RATIO);
    if body_length as u64 > maximum_body_length {
        return Err(XqdbError::DeserializationErr(format!(
            "Decompressed IPC message length {declared_total_length} is unreachable from {compressed_payload_length} compressed bytes, which expand to at most {maximum_body_length} bytes"
        )));
    }
    Ok(body_length)
}

fn allocate_zeroed_buffer(length: usize, description: &str) -> Result<Vec<u8>, XqdbError> {
    let mut buffer = allocate_buffer(length, description)?;
    buffer.resize(length, 0);
    Ok(buffer)
}

fn compressed_message_length(body: &[u8], mode: u8) -> Result<(u64, usize), XqdbError> {
    match mode {
        1 => {
            let prefix: [u8; 4] = body
                .get(..4)
                .ok_or_else(|| {
                    XqdbError::Err(format!(
                        "Compressed IPC body is {} bytes; compression mode 1 requires a 4-byte decompressed-length prefix",
                        body.len()
                    ))
                })?
                .try_into()
                .map_err(|_| {
                    XqdbError::Err("Invalid compression mode 1 length prefix".to_owned())
                })?;
            Ok((u64::from(u32::from_le_bytes(prefix)), 4))
        }
        2 => {
            let prefix: [u8; 8] = body
                .get(..8)
                .ok_or_else(|| {
                    XqdbError::Err(format!(
                        "Compressed IPC body is {} bytes; compression mode 2 requires an 8-byte decompressed-length prefix",
                        body.len()
                    ))
                })?
                .try_into()
                .map_err(|_| {
                    XqdbError::Err("Invalid compression mode 2 length prefix".to_owned())
                })?;
            Ok((u64::from_le_bytes(prefix), 8))
        }
        _ => Err(XqdbError::Err(format!(
            "Unsupported IPC compression mode {mode}"
        ))),
    }
}
struct ParsedIpcHeader {
    message_kind: u8,
    compression_mode: u8,
    total_length: u64,
}

fn message_type(kind: u8) -> Result<MsgType, XqdbError> {
    match kind {
        0 => Ok(MsgType::Async),
        1 => Ok(MsgType::Sync),
        2 => Ok(MsgType::Response),
        _ => Err(XqdbError::DeserializationErr(format!(
            "Unsupported IPC message type {kind}"
        ))),
    }
}

fn parse_ipc_header(header: &[u8]) -> Result<ParsedIpcHeader, XqdbError> {
    if header.len() != IPC_HEADER_LENGTH {
        return Err(XqdbError::DeserializationErr(format!(
            "IPC header must be exactly {IPC_HEADER_LENGTH} bytes, got {}",
            header.len()
        )));
    }
    match header[0] {
        1 => (),
        0 => return Err(XqdbError::NotSupportedBigEndianErr()),
        byte_order => {
            return Err(XqdbError::DeserializationErr(format!(
                "Unsupported IPC byte-order marker {byte_order}"
            )))
        }
    }
    message_type(header[1])?;
    if header[2] > 2 {
        return Err(XqdbError::DeserializationErr(format!(
            "Unsupported IPC compression mode {}",
            header[2]
        )));
    }
    let low_length = u64::from(u32::from_le_bytes(header[4..8].try_into().map_err(
        |_| XqdbError::DeserializationErr("Invalid IPC length field".to_owned()),
    )?));
    let high_length = u64::from(header[3])
        .checked_shl(32)
        .ok_or_else(|| XqdbError::DeserializationErr("IPC length overflowed".to_owned()))?;
    let total_length = high_length
        .checked_add(low_length)
        .ok_or_else(|| XqdbError::DeserializationErr("IPC length overflowed".to_owned()))?;
    checked_body_length(total_length, "IPC message")?;
    Ok(ParsedIpcHeader {
        message_kind: header[1],
        compression_mode: header[2],
        total_length,
    })
}

fn enforce_message_bound(total_length: u64, maximum: Option<usize>) -> Result<(), XqdbError> {
    if let Some(maximum) = maximum {
        if total_length > maximum as u64 {
            return Err(XqdbError::Err(format!(
                "IPC message's uncompressed length {total_length} exceeds configured maximum {maximum}"
            )));
        }
    }
    Ok(())
}

fn enforce_compressed_physical_bound(
    physical_total_length: u64,
    logical_total_length: u64,
    prefix_length: usize,
) -> Result<(), XqdbError> {
    let decoded_body_length =
        checked_body_length(logical_total_length, "Decompressed IPC message")? as u64;
    let flag_bytes = decoded_body_length.checked_add(7).ok_or_else(|| {
        XqdbError::DeserializationErr(
            "Compressed IPC structural length bound overflowed".to_owned(),
        )
    })? / 8;
    let maximum_total_length = (IPC_HEADER_LENGTH as u64)
        .checked_add(prefix_length as u64)
        .and_then(|length| length.checked_add(decoded_body_length))
        .and_then(|length| length.checked_add(flag_bytes))
        .ok_or_else(|| {
            XqdbError::DeserializationErr(
                "Compressed IPC structural length bound overflowed".to_owned(),
            )
        })?;
    if physical_total_length > maximum_total_length {
        return Err(XqdbError::DeserializationErr(format!(
            "Compressed IPC frame length {physical_total_length} exceeds maximum structurally valid length {maximum_total_length} for declared uncompressed length {logical_total_length}"
        )));
    }
    Ok(())
}

fn decode_value_body(
    body: Cow<'_, [u8]>,
    encoding: SymbolEncoding,
    mode: ValueMode,
) -> Result<K, XqdbError> {
    match mode {
        ValueMode::Native => deserialize(body.as_ref(), &mut 0, encoding),
        ValueMode::Lossless => match body {
            Cow::Borrowed(body) => QValue::from_bytes(body).map(K::QValue),
            Cow::Owned(body) => QValue::from_owned_bytes(body).map(K::QValue),
        },
    }
}

fn prepare_ipc_body<'a>(
    header: ParsedIpcHeader,
    body: Cow<'a, [u8]>,
    max_message_bytes: Option<usize>,
) -> Result<(MsgType, Cow<'a, [u8]>), XqdbError> {
    let expected_body_length = checked_body_length(header.total_length, "IPC message")?;
    if body.len() != expected_body_length {
        return Err(XqdbError::DeserializationErr(format!(
            "IPC frame length mismatch: header declares {} body bytes, got {}",
            expected_body_length,
            body.len()
        )));
    }

    let body = match header.compression_mode {
        0 => {
            enforce_message_bound(header.total_length, max_message_bytes)?;
            body
        }
        compression_mode => {
            let (decompressed_length, prefix_length) =
                compressed_message_length(body.as_ref(), compression_mode)?;
            enforce_message_bound(decompressed_length, max_message_bytes)?;
            enforce_compressed_physical_bound(
                header.total_length,
                decompressed_length,
                prefix_length,
            )?;
            let decompressed_body_length = checked_decompressed_body_length(
                decompressed_length,
                body.len().saturating_sub(prefix_length),
            )?;
            let mut decompressed =
                allocate_zeroed_buffer(decompressed_body_length, "decompressed IPC body")?;
            decompress(body.as_ref(), &mut decompressed, prefix_length)?;
            Cow::Owned(decompressed)
        }
    };
    Ok((message_type(header.message_kind)?, body))
}

pub(crate) fn deserialize_ipc_frame(
    frame: &[u8],
    encoding: SymbolEncoding,
    mode: ValueMode,
) -> Result<(MsgType, K), XqdbError> {
    let header_bytes = frame.get(..IPC_HEADER_LENGTH).ok_or_else(|| {
        XqdbError::DeserializationErr(format!(
            "IPC frame is shorter than its {IPC_HEADER_LENGTH}-byte header"
        ))
    })?;
    let header = parse_ipc_header(header_bytes)?;
    let declared_length = usize::try_from(header.total_length).map_err(|_| {
        XqdbError::DeserializationErr(
            "IPC message length cannot be represented on this platform".to_owned(),
        )
    })?;
    if frame.len() != declared_length {
        return Err(XqdbError::DeserializationErr(format!(
            "IPC frame length mismatch: header declares {declared_length} bytes, got {}",
            frame.len()
        )));
    }
    let (message_type, body) =
        prepare_ipc_body(header, Cow::Borrowed(&frame[IPC_HEADER_LENGTH..]), None)?;
    Ok((message_type, decode_value_body(body, encoding, mode)?))
}

/// Where a received frame body lives after framing: uncompressed bodies stay in the connector's
/// retained buffer, decompressed ones own a fresh allocation sized by the declared length, and
/// tables large enough to pipeline arrive already decoded.
enum IpcBody {
    Retained,
    Decompressed(Vec<u8>),
    Decoded(Result<K, XqdbError>),
}

fn read_ipc_body(
    stream: &mut (dyn QStream + Send + Sync),
    max_message_bytes: Option<usize>,
    buffer: &mut Vec<u8>,
    pipeline: Option<SymbolEncoding>,
) -> Result<(MsgType, IpcBody), XqdbError> {
    let mut header_bytes = [0u8; IPC_HEADER_LENGTH];
    stream.read_exact(&mut header_bytes)?;
    let header = parse_ipc_header(&header_bytes)?;
    let body_length = checked_body_length(header.total_length, "IPC message")?;
    let mut prefix = [0u8; 8];
    let prefix_length = match header.compression_mode {
        0 => {
            enforce_message_bound(header.total_length, max_message_bytes)?;
            0
        }
        1 => 4,
        2 => 8,
        _ => unreachable!("compression mode was validated with the IPC header"),
    };
    if prefix_length > body_length {
        return Err(XqdbError::DeserializationErr(format!(
            "Compressed IPC body is {body_length} bytes; compression mode {} requires a {prefix_length}-byte decompressed-length prefix",
            header.compression_mode
        )));
    }
    if prefix_length > 0 {
        stream.read_exact(&mut prefix[..prefix_length])?;
        let (decompressed_length, _) =
            compressed_message_length(&prefix[..prefix_length], header.compression_mode)?;
        enforce_message_bound(decompressed_length, max_message_bytes)?;
        enforce_compressed_physical_bound(header.total_length, decompressed_length, prefix_length)?;
    }
    buffer.clear();
    reserve_buffer(
        buffer,
        body_length.min(INITIAL_BODY_RESERVATION).max(prefix_length),
        "IPC message body",
    )?;
    buffer.extend_from_slice(&prefix[..prefix_length]);
    let pipeline = pipeline.filter(|_| {
        prefix_length == 0
            && body_length >= PIPELINE_MIN_BODY_BYTES
            && buffer.capacity() >= body_length
    });
    if let Some(encoding) = pipeline {
        read_some(stream, buffer, body_length)?;
        if buffer[0] == pipeline::TABLE_TYPE && buffer.len() < body_length {
            let message_type = message_type(header.message_kind)?;
            let decoded = pipeline::receive_table(stream, buffer, body_length, encoding)?;
            return Ok((message_type, IpcBody::Decoded(decoded)));
        }
    }
    read_message_body(stream, body_length, buffer)?;
    let (message_type, body) = prepare_ipc_body(header, Cow::Borrowed(buffer), max_message_bytes)?;
    let body = match body {
        Cow::Borrowed(_) => IpcBody::Retained,
        Cow::Owned(decompressed) => IpcBody::Decompressed(decompressed),
    };
    Ok((message_type, body))
}

/// Performs one read into the buffer's initialized spare capacity, appending whatever arrived.
fn read_some(
    stream: &mut (dyn QStream + Send + Sync),
    buffer: &mut Vec<u8>,
    body_length: usize,
) -> Result<(), XqdbError> {
    let filled = buffer.len();
    let want = (buffer.capacity() - filled).min(body_length - filled);
    loop {
        // SAFETY: `reserve_buffer` initializes every byte of the capacity it hands out, and
        // `filled + want` stays within that capacity.
        let region =
            unsafe { std::slice::from_raw_parts_mut(buffer.as_mut_ptr().add(filled), want) };
        match stream.read(region) {
            Ok(0) => {
                return Err(XqdbError::IOError(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "IPC message body ended before its declared length",
                )))
            }
            Ok(count) => {
                // SAFETY: the read initialized `count` more bytes directly after `filled`.
                unsafe { buffer.set_len(filled + count) };
                return Ok(());
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(XqdbError::IOError(error)),
        }
    }
}

fn connect_to_addresses(
    addresses: impl IntoIterator<Item = SocketAddr>,
    timeout: Duration,
) -> io::Result<TcpStream> {
    let started = Instant::now();
    let mut last_error = None;
    for address in addresses {
        let result = if timeout.is_zero() {
            TcpStream::connect(address)
        } else {
            let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "TCP connection attempts timed out",
                ));
            };
            if remaining.is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "TCP connection attempts timed out",
                ));
            }
            TcpStream::connect_timeout(&address, remaining)
        };
        match result {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            "host resolved to no socket addresses",
        )
    }))
}

fn parse_certificates(
    pem: &[u8],
    description: &str,
) -> Result<Vec<CertificateDer<'static>>, XqdbError> {
    let mut certificates = Vec::new();
    for certificate in CertificateDer::pem_slice_iter(pem) {
        let certificate = certificate
            .map_err(|error| XqdbError::Err(format!("Invalid {description} PEM: {error}")))?;
        certificates.try_reserve(1).map_err(|error| {
            XqdbError::Err(format!(
                "Unable to allocate {description} certificate list: {error}"
            ))
        })?;
        certificates.push(certificate);
    }
    if certificates.is_empty() {
        return Err(XqdbError::Err(format!(
            "{description} PEM contains no certificates"
        )));
    }
    Ok(certificates)
}

fn parse_private_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>, XqdbError> {
    let mut keys = PrivateKeyDer::pem_slice_iter(pem);
    let key = keys
        .next()
        .ok_or_else(|| XqdbError::Err("Client key PEM contains no private key".to_owned()))?
        .map_err(|error| XqdbError::Err(format!("Invalid client key PEM: {error}")))?;
    if keys.next().is_some() {
        return Err(XqdbError::Err(
            "Client key PEM must contain exactly one private key".to_owned(),
        ));
    }
    Ok(key)
}

fn tls_client_config(settings: &ConnectorSettings) -> Result<ClientConfig, XqdbError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let verifier = match settings.tls_ca.as_deref() {
        Some(pem) => {
            let roots = parse_certificates(pem, "custom CA")?;
            #[cfg(not(target_os = "android"))]
            {
                Verifier::new_with_extra_roots(roots, Arc::clone(&provider))
            }
            #[cfg(target_os = "android")]
            {
                let _ = roots;
                return Err(XqdbError::Err(
                    "Custom TLS CA certificates are not supported on Android".to_owned(),
                ));
            }
        }
        None => Verifier::new(Arc::clone(&provider)),
    }
    .map_err(|error| XqdbError::Err(format!("Unable to configure TLS verification: {error}")))?;

    // `dangerous()` is rustls's entry point for any custom verifier. This verifier still performs
    // normal platform trust and hostname checks, augmented only by the caller's parsed CA roots.
    let builder = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| XqdbError::Err(format!("Unable to configure TLS versions: {error}")))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier));
    match (settings.tls_cert.as_deref(), settings.tls_key.as_deref()) {
        (Some(cert), Some(key)) => builder
            .with_client_auth_cert(
                parse_certificates(cert, "client certificate")?,
                parse_private_key(key)?,
            )
            .map_err(|error| XqdbError::Err(format!("Invalid TLS client identity: {error}"))),
        (None, None) => Ok(builder.with_no_client_auth()),
        _ => Err(XqdbError::Err(
            "TLS client certificate and key must be supplied together".to_owned(),
        )),
    }
}

fn effective_socket_timeout(configured: Option<Duration>, fallback: Duration) -> Option<Duration> {
    let timeout = configured.unwrap_or(fallback);
    (!timeout.is_zero()).then_some(timeout)
}

/// Socket receive buffer requested on Windows, where the default is 64 KiB and the receive-window
/// autotuning does not cover the per-socket AFD buffer. Measured on the 100k-row fixture over a
/// container-forwarded loopback: raw receive time fell 28% for an 11 MB table and 29% for a 51 MB
/// table between the default and 1 MiB, then flattened. Linux and macOS autotune their receive
/// buffers, and an explicit `SO_RCVBUF` would disable that and be clamped to `rmem_max`, so they
/// keep the platform default.
#[cfg(windows)]
const RECEIVE_BUFFER_BYTES: usize = 4 * 1024 * 1024;

#[cfg(windows)]
fn configure_receive_buffer(stream: &TcpStream) -> Result<(), XqdbError> {
    socket2::SockRef::from(stream)
        .set_recv_buffer_size(RECEIVE_BUFFER_BYTES)
        .map_err(XqdbError::IOError)
}

#[cfg(not(windows))]
fn configure_receive_buffer(_stream: &TcpStream) -> Result<(), XqdbError> {
    Ok(())
}

fn operation_aborted_error() -> XqdbError {
    XqdbError::IOError(io::Error::new(
        io::ErrorKind::Interrupted,
        "Connector operation was aborted",
    ))
}

fn tls_server_name(host: &str) -> Result<ServerName<'static>, XqdbError> {
    ServerName::try_from(host.to_owned())
        .map_err(|error| XqdbError::Err(format!("Invalid TLS server name: {error}")))
}

impl Connector {
    pub fn new(
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        enable_tls: bool,
        timeout: u64,
        version: u8,
    ) -> Self {
        let host = if host.is_empty() { "127.0.0.1" } else { host };
        let is_local = host == "127.0.0.1" || host == "localhost";
        Connector {
            host: host.to_string(),
            port,
            user: user.to_string(),
            password: password.to_string(),
            enable_tls,
            stream: None,
            tcp_stream: None,
            abort_handle: ConnectorAbortHandle::default(),
            is_local,
            timeout: Duration::new(timeout, 0),
            version,
            symbol_encoding: SymbolEncoding::Strict,
            settings: ConnectorSettings::default(),
            pending_notifications: VecDeque::new(),
            receive_buffer: Vec::new(),
            send_buffer: Vec::new(),
        }
    }

    pub fn abort_handle(&self) -> ConnectorAbortHandle {
        self.abort_handle.clone()
    }
    /// Validates and installs settings while the connector is disconnected.
    pub fn configure(&mut self, settings: ConnectorSettings) -> Result<(), XqdbError> {
        if self.stream.is_some() {
            return Err(XqdbError::Err(
                "Connector settings cannot be changed while connected".to_owned(),
            ));
        }
        if settings.max_pending_notifications == 0 {
            return Err(XqdbError::Err(
                "max_pending_notifications must be positive".to_owned(),
            ));
        }
        let has_tls_settings = settings.tls_ca.is_some()
            || settings.tls_cert.is_some()
            || settings.tls_key.is_some()
            || settings.tls_server_name.is_some();
        if has_tls_settings && !self.enable_tls {
            return Err(XqdbError::Err(
                "TLS settings require enable_tls=true".to_owned(),
            ));
        }
        if settings.tls_cert.is_some() != settings.tls_key.is_some() {
            return Err(XqdbError::Err(
                "TLS client certificate and key must be supplied together".to_owned(),
            ));
        }
        if self.enable_tls {
            tls_server_name(
                settings
                    .tls_server_name
                    .as_deref()
                    .unwrap_or(self.host.as_str()),
            )?;
            tls_client_config(&settings)?;
        }
        self.settings = settings;
        Ok(())
    }

    fn auth(&self, q_stream: &mut impl QStream) -> Result<(), XqdbError> {
        let credential_length = self
            .user
            .len()
            .checked_add(self.password.len())
            .and_then(|length| length.checked_add(3))
            .ok_or_else(|| {
                XqdbError::Err("Authentication credential length overflowed".to_owned())
            })?;
        let mut credential = allocate_buffer(credential_length, "authentication credential")?;
        credential.extend_from_slice(self.user.as_bytes());
        credential.push(b':');
        credential.extend_from_slice(self.password.as_bytes());
        credential.push(self.version);
        credential.push(0);
        q_stream.write_all(&credential)?;
        let mut support_version = [0u8];
        match q_stream.read(&mut support_version) {
            Ok(read_length) => {
                if read_length == 1 {
                    if support_version[0] >= 1 {
                        Ok(())
                    } else {
                        Err(XqdbError::VersionErr())
                    }
                } else {
                    Err(XqdbError::AuthErr())
                }
            }
            Err(e) => Err(XqdbError::IOError(e)),
        }
    }

    fn finish_operation<T>(
        &mut self,
        active_io: ActiveIoGuard,
        result: Result<T, XqdbError>,
    ) -> Result<T, XqdbError> {
        if active_io.finish() {
            self.disconnect_after_protocol_error();
            Err(operation_aborted_error())
        } else {
            result
        }
    }

    pub fn send(&mut self, msg_type: MsgType, expr: &str, args: &[K]) -> Result<(), XqdbError> {
        let active_io = self.abort_handle.begin_operation();
        if let Some(stream) = self.tcp_stream.as_ref() {
            active_io.attach_stream(Arc::clone(stream));
        }
        let result = self.send_inner(msg_type, expr, args);
        self.finish_operation(active_io, result)
    }

    fn send_inner(&mut self, msg_type: MsgType, expr: &str, args: &[K]) -> Result<(), XqdbError> {
        if self.version > 6 || self.stream.is_none() {
            return Err(XqdbError::NotConnectedErr());
        }
        if args.len() > 8 {
            return Err(XqdbError::TooManyArgumentErr());
        }

        let expr = expr.trim();
        let expression_length =
            i32::try_from(expr.len()).map_err(|_| XqdbError::OverLengthErr())?;
        let (body_length, is_lambda) = if args.is_empty() {
            (
                6usize
                    .checked_add(expr.len())
                    .ok_or_else(|| XqdbError::Err("IPC request length overflowed".to_owned()))?,
                false,
            )
        } else {
            let is_lambda = expr.starts_with('{') && expr.ends_with('}');
            let mut length = 12usize
                .checked_add(if is_lambda { 2 } else { 0 })
                .and_then(|length| length.checked_add(expr.len()))
                .ok_or_else(|| XqdbError::Err("IPC request length overflowed".to_owned()))?;
            for value in args {
                length = length
                    .checked_add(value.j6_len()?)
                    .ok_or_else(|| XqdbError::Err("IPC request length overflowed".to_owned()))?;
            }
            (length, is_lambda)
        };

        let (total_length, high_byte, low_length) =
            checked_outgoing_message_length(body_length, "IPC request")?;
        let mut frame = std::mem::take(&mut self.send_buffer);
        frame.clear();
        reserve_buffer(&mut frame, total_length, "IPC request")?;
        frame.extend_from_slice(&[1, msg_type as u8, 0, high_byte]);
        frame.extend_from_slice(&low_length.to_le_bytes());
        if args.is_empty() {
            frame.extend_from_slice(&[10, 0]);
            frame.extend_from_slice(&expression_length.to_le_bytes());
            frame.extend_from_slice(expr.as_bytes());
        } else {
            let argument_count =
                i32::try_from(args.len() + 1).map_err(|_| XqdbError::OverLengthErr())?;
            frame.extend_from_slice(&[0, 0]);
            frame.extend_from_slice(&argument_count.to_le_bytes());
            if is_lambda {
                frame.extend_from_slice(&[100, 0]);
            }
            frame.extend_from_slice(&[10, 0]);
            frame.extend_from_slice(&expression_length.to_le_bytes());
            frame.extend_from_slice(expr.as_bytes());
            for value in args {
                serialize_into(value, &mut frame)?;
            }
        }
        if frame.len() != total_length {
            return Err(XqdbError::Err(
                "Serialized request length differs from its declared q length".to_owned(),
            ));
        }

        let should_compress = match self.settings.compression {
            CompressionMode::On => true,
            CompressionMode::Off => false,
            CompressionMode::Auto => {
                !self.is_local && total_length >= self.settings.compression_threshold
            }
        };
        let payload = if should_compress {
            compress(frame)?
        } else {
            frame
        };
        let result = self
            .stream
            .as_mut()
            .ok_or_else(XqdbError::NotConnectedErr)?
            .write_all(&payload);
        self.send_buffer = payload;
        release_oversized_buffer(&mut self.send_buffer);
        if let Err(error) = result {
            self.disconnect_after_protocol_error();
            return Err(XqdbError::IOError(error));
        }
        Ok(())
    }

    pub fn receive(&mut self) -> Result<K, XqdbError> {
        if let Some(notification) = self.pending_notifications.pop_front() {
            return notification;
        }
        let active_io = self.abort_handle.begin_operation();
        if let Some(stream) = self.tcp_stream.as_ref() {
            active_io.attach_stream(Arc::clone(stream));
        }
        let result = self.receive_inner();
        self.finish_operation(active_io, result)
    }

    fn receive_inner(&mut self) -> Result<K, XqdbError> {
        let (_, body) = self.read_next_frame()?;
        self.decode_frame_body(body)
    }

    fn read_next_frame(&mut self) -> Result<(MsgType, IpcBody), XqdbError> {
        if self.version > 6 {
            return Err(XqdbError::NotConnectedErr());
        }
        let framing_result = {
            let stream = self
                .stream
                .as_mut()
                .ok_or_else(XqdbError::NotConnectedErr)?;
            read_ipc_body(
                stream.as_mut(),
                self.settings.max_message_bytes,
                &mut self.receive_buffer,
                matches!(self.settings.value_mode, ValueMode::Native)
                    .then_some(self.symbol_encoding),
            )
        };
        let (message_type, body) = match framing_result {
            Ok(message) => message,
            Err(error) => {
                self.disconnect_after_protocol_error();
                return Err(error);
            }
        };
        if matches!(&message_type, MsgType::Sync) {
            self.disconnect_after_protocol_error();
            return Err(XqdbError::Err(
                "Received unsupported synchronous request from q".to_owned(),
            ));
        }
        Ok((message_type, body))
    }

    /// Decodes a frame body, borrowing the retained receive buffer for native decoding and
    /// handing it over for lossless values, which keep the wire bytes.
    fn decode_frame_body(&mut self, body: IpcBody) -> Result<K, XqdbError> {
        let (encoding, mode) = (self.symbol_encoding, self.settings.value_mode);
        let value = match (body, mode) {
            (IpcBody::Retained, ValueMode::Lossless) => {
                let body = std::mem::take(&mut self.receive_buffer);
                decode_value_body(Cow::Owned(body), encoding, mode)
            }
            (IpcBody::Retained, ValueMode::Native) => {
                decode_value_body(Cow::Borrowed(&self.receive_buffer), encoding, mode)
            }
            (IpcBody::Decompressed(body), _) => decode_value_body(Cow::Owned(body), encoding, mode),
            (IpcBody::Decoded(value), _) => value,
        };
        release_oversized_buffer(&mut self.receive_buffer);
        value
    }

    /// Drops the socket and the retained receive buffer: after a failed or truncated read the
    /// buffer's spare capacity is no longer guaranteed initialized, which the pipelined receive
    /// path relies on, so it is rebuilt from scratch by the next connection.
    fn disconnect_after_protocol_error(&mut self) {
        self.receive_buffer = Vec::new();
        if let Some(stream) = self.tcp_stream.take() {
            let _ = stream.shutdown(Shutdown::Both);
        }
        self.stream = None;
    }

    pub fn connect(&mut self) -> Result<(), XqdbError> {
        let active_io = self.abort_handle.begin_operation();
        let result = self.connect_inner(&active_io);
        self.finish_operation(active_io, result)
    }

    fn connect_inner(&mut self, active_io: &ActiveIoGuard) -> Result<(), XqdbError> {
        if self.stream.is_some() {
            if let Some(stream) = self.tcp_stream.as_ref() {
                active_io.attach_stream(Arc::clone(stream));
            }
            return Ok(());
        }

        let tls = if self.enable_tls {
            let server_name = self
                .settings
                .tls_server_name
                .as_deref()
                .unwrap_or(self.host.as_str());
            Some((
                tls_server_name(server_name)?,
                tls_client_config(&self.settings)?,
            ))
        } else {
            None
        };
        let mut addresses = (self.host.as_str(), self.port)
            .to_socket_addrs()
            .map_err(XqdbError::IOError)?
            .peekable();
        if addresses.peek().is_none() {
            return Err(XqdbError::FailedToConnectErr(
                "host resolved to no socket addresses".to_owned(),
            ));
        }
        let connect_timeout = self.settings.connect_timeout.unwrap_or(self.timeout);
        let tcp_stream =
            connect_to_addresses(addresses, connect_timeout).map_err(XqdbError::IOError)?;
        tcp_stream.set_nodelay(true)?;
        configure_receive_buffer(&tcp_stream)?;
        tcp_stream
            .set_read_timeout(effective_socket_timeout(
                self.settings.read_timeout,
                self.timeout,
            ))
            .map_err(XqdbError::IOError)?;
        tcp_stream
            .set_write_timeout(effective_socket_timeout(
                self.settings.write_timeout,
                self.timeout,
            ))
            .map_err(XqdbError::IOError)?;

        let tcp_stream = Arc::new(tcp_stream);
        if !active_io.attach_stream(Arc::clone(&tcp_stream)) {
            return Err(operation_aborted_error());
        }
        let shared_stream = SharedTcpStream(Arc::clone(&tcp_stream));
        let result = match tls {
            Some((server_name, config)) => {
                rustls::ClientConnection::new(Arc::new(config), server_name)
                    .map_err(|error| XqdbError::Err(error.to_string()))
                    .and_then(|connection| {
                        self.install_authenticated_stream(StreamOwned::new(
                            connection,
                            shared_stream,
                        ))
                    })
            }
            None => self.install_authenticated_stream(shared_stream),
        };

        if result.is_err() {
            let _ = tcp_stream.shutdown(Shutdown::Both);
            self.stream = None;
        } else {
            self.tcp_stream = Some(tcp_stream);
        }
        result
    }

    pub fn shutdown(&mut self) -> Result<(), XqdbError> {
        if self.stream.is_none() {
            return Err(XqdbError::NotConnectedErr());
        }
        let result = match self.tcp_stream.take() {
            Some(stream) => match stream.shutdown(Shutdown::Both) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == io::ErrorKind::NotConnected => Ok(()),
                Err(error) => Err(XqdbError::IOError(error)),
            },
            None => Ok(()),
        };
        self.stream = None;
        result
    }

    fn install_authenticated_stream<S>(&mut self, mut stream: S) -> Result<(), XqdbError>
    where
        S: QStream + Send + Sync + 'static,
    {
        self.auth(&mut stream)?;
        self.stream = Some(Box::new(stream));
        Ok(())
    }

    pub fn execute(&mut self, expr: &str, args: &[K]) -> Result<K, XqdbError> {
        let active_io = self.abort_handle.begin_operation();
        if let Some(stream) = self.tcp_stream.as_ref() {
            active_io.attach_stream(Arc::clone(stream));
        }
        let result = self.execute_inner(&active_io, expr, args);
        self.finish_operation(active_io, result)
    }

    fn execute_inner(
        &mut self,
        active_io: &ActiveIoGuard,
        expr: &str,
        args: &[K],
    ) -> Result<K, XqdbError> {
        if self.stream.is_none() {
            self.connect_inner(active_io)?;
        }
        self.send_inner(MsgType::Sync, expr, args)?;
        loop {
            let (message_type, body) = self.read_next_frame()?;
            match message_type {
                MsgType::Response => return self.decode_frame_body(body),
                MsgType::Async => {
                    if self.pending_notifications.len() >= self.settings.max_pending_notifications {
                        self.disconnect_after_protocol_error();
                        return Err(XqdbError::Err(format!(
                            "Pending notification limit {} exceeded",
                            self.settings.max_pending_notifications
                        )));
                    }
                    if let Err(error) = self.pending_notifications.try_reserve(1) {
                        self.disconnect_after_protocol_error();
                        return Err(XqdbError::Err(format!(
                            "Unable to allocate pending notification slot: {error}"
                        )));
                    }
                    let notification = self.decode_frame_body(body);
                    self.pending_notifications.push_back(notification);
                }
                MsgType::Sync => unreachable!("read_next_frame rejects synchronous requests"),
            }
        }
    }

    pub fn execute_async(&mut self, expr: &str, args: &[K]) -> Result<(), XqdbError> {
        let active_io = self.abort_handle.begin_operation();
        if let Some(stream) = self.tcp_stream.as_ref() {
            active_io.attach_stream(Arc::clone(stream));
        }
        let result = if self.stream.is_none() {
            self.connect_inner(&active_io)
                .and_then(|()| self.send_inner(MsgType::Async, expr, args))
        } else {
            self.send_inner(MsgType::Async, expr, args)
        };
        self.finish_operation(active_io, result)
    }
}

impl Drop for Connector {
    fn drop(&mut self) {
        self.disconnect_after_protocol_error();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::generate_j6_ipc_msg;
    use crate::serde6::serialize;
    use polars::datatypes::{DataType as PolarsDataType, TimeUnit};
    use polars::prelude::{Categories, DataFrame, NamedFrom, Series};
    use std::io::{Cursor, Read, Write};
    use std::net::TcpListener;
    use std::thread;

    const TEST_CERT_PEM: &[u8] = br#"-----BEGIN CERTIFICATE-----
MIIDCTCCAfGgAwIBAgIUVSD5hARFLX22zr4k5G49vsqU6AEwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDkwNTAxMTAzM1oXDTM2MDkw
MjAxMTAzM1owFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAlyUNmO0C0SmL8T1Y7VWoFDrzT1ka326pko+WCCsyahEF
LPAf1Rr1T6t7IRjFm5PKbM4HhvUMkIyL+/C7iOxtu3XGLhKNSuyfRha5z//pdgwL
LPX5xQERRk5A6cAFyULXt2P7y9B9P8EgGAGLjpQUxCwuq/HEKIUon8e89OWlZX6G
JqhmE0FyFQtAibvcHFQs6SJusoOtXW/P9bBQalF4ziWneMKfOdpjOO3rINGthwDl
hk6pvN9bSwqYAeF2Nai8r0Oh2W9TwwhjHSxapA3kxwKA05Xlvc548mEOabAuGu/P
u/wMYStqfuEgC6QvkMA4bu0j16G7MLxkUB90WYAJiwIDAQABo1MwUTAdBgNVHQ4E
FgQUqsk9WgmwhiSKcMwpTTspa+Ol9qMwHwYDVR0jBBgwFoAUqsk9WgmwhiSKcMwp
TTspa+Ol9qMwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAbvlh
nBgndo6ZG+9CfwQUfcHLhfXmL9f7YRsPJmUiqpOnYnHSCzJdTsrQV8NmmzkH11rW
xv6d1EA2eXhA3Je9nbvo39GpLqRHBZsgGAY9hJijiBa+NF6Pu5+TnS7Wlq5nvxPp
FMfGyRFvoLtvOesfaB7BO3Vs8rB0dJ1n6QAvL2HRhGK2qx2MfeVkQUBrNFH0I2eG
GKwH2i7yKqABiZRuqA3UMVRmIA+L8ZrF0ClPxYqVcMNtsVKwX+7HBQEKl0JREs8o
RAWeTqxXJMvcV8GUAwKvBjC9K0IUmxrjhiMQLGlizjhcy949PkgAKxUXAH7Pp0KV
dlXS+6dC8b74KdzhVA==
-----END CERTIFICATE-----"#;

    const TEST_KEY_PEM: &[u8] = br#"-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCXJQ2Y7QLRKYvx
PVjtVagUOvNPWRrfbqmSj5YIKzJqEQUs8B/VGvVPq3shGMWbk8pszgeG9QyQjIv7
8LuI7G27dcYuEo1K7J9GFrnP/+l2DAss9fnFARFGTkDpwAXJQte3Y/vL0H0/wSAY
AYuOlBTELC6r8cQohSifx7z05aVlfoYmqGYTQXIVC0CJu9wcVCzpIm6yg61db8/1
sFBqUXjOJad4wp852mM47esg0a2HAOWGTqm831tLCpgB4XY1qLyvQ6HZb1PDCGMd
LFqkDeTHAoDTleW9znjyYQ5psC4a78+7/AxhK2p+4SALpC+QwDhu7SPXobswvGRQ
H3RZgAmLAgMBAAECggEALk3CMCbwFX8KadH3A+KZdvUmZBkys9+VoJpPhtog/ECR
mtZHkkRLgehRdO6/Xy20NogZ4z0AZ+o+2mTCNyzjYyouGCBD2//IvbKpozDN54XC
CLikl7d/gM/pcBMNUr6hEoRVd/e730O9ns0eYYQ5Jm44sCGFjDpbGzQYTdhqxsA3
EZcB2dWLYNePcLEybJGVWDUyqVJeCVI6WXRrl6IGNKkqTNY6x+yCEtDDbvav6F1+
ERZtBh8XhRquZkTzhkiMLzBd6a11RIUn47Fz9wJTFS/aXfXoTqlEC4mE2j72A1hn
M5QWLZFbYqVK870E1HO42TsQEYiDQlK5BrLAlHvZaQKBgQDOt1vs0hVeHb0BaLv/
wtvjh+jtR3CH+47iJAgYrT5T1qv7gZrgquqEU5jZpohCIrF9Mm0oCs+pMQYgDzac
l39n4dO/FFSr4AJO1GeEZTy9CFr5NwBhoIKZKcZw7qUTN+U7BKmUf2IcLbxjInu/
QH6oO30Ngja8jgjk6ofawnY4OQKBgQC7Lfa2w/TSEoqspzxmKS7rvFigAbW8rA1E
WTltX43YbCubU25Pnq/JR4oYRVajGtlTPlhhA+MCa+YxNVEOgRh2b38VTmxbHaOi
EP5SJ/bZ6JrMLiqzcZo0bK8jfgrKEdmN5rF7gE0fRGqIjnNcPpOl05fAIc8To0tX
CS3A9fen4wKBgQCVr3CBLB8M57vVKWH48cEIIYIpT3HNBfuRBUZXmBtp2ijvFgpw
ZVMsPtyPvmcsfLLJVZp1RF7axQUKcfm5qno3Xu9VjgNB8hO5wVS0KhqxRzuY+prs
Fq56+iUX4bbnE7KJ6fZh8Vu5y+R+ZJn3A1yztV/4SDIalz8ZhDqbzfSNAQKBgEDt
b6/0Bx87eUjsdcIGNRVmbuOJ1E2O7Mcxn/71b1GMLBAj/5a0t8s8+oTywFuxe4Mp
lCSK4Zq8bMvS77v1QdQLVuzAGEv+2vzjoiRDYpgx3EhJF1zJYjEfJh1Mold3m5xi
UlxBo/7dj4qwxwlPV43k+LWXxKnOMdsN/wX5DB/7AoGBAIt/Vyuu+4MqdjaWzg7+
hJSyKxZGcSEAvoTvNeCqCpKo/QPYkEP5t/S7xOHI27fU7cFu0sQGaP3nHMCdiQEL
IOLH4W91cG+dGNYdUb8dceyjtwcbL4A1XYPkf9xIZHt09QtY5BkLCUK3FZT4L/SX
/hT/7q39+51TU3/BJVuVTq6u
-----END PRIVATE KEY-----"#;

    struct MemoryStream {
        input: Cursor<Vec<u8>>,
        output: Arc<Mutex<Vec<u8>>>,
    }

    impl IoRead for MemoryStream {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            self.input.read(buffer)
        }
    }

    impl IoWrite for MemoryStream {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.output
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn install_memory_stream(connector: &mut Connector, input: Vec<u8>) -> Arc<Mutex<Vec<u8>>> {
        let output = Arc::new(Mutex::new(Vec::new()));
        connector.stream = Some(Box::new(MemoryStream {
            input: Cursor::new(input),
            output: Arc::clone(&output),
        }));
        output
    }

    fn connector_with_response(response: Vec<u8>) -> Connector {
        let mut connector = Connector::new("", 0, "", "", false, 0, 6);
        connector.stream = Some(Box::new(Cursor::new(response)));
        connector
    }

    fn response_header(compression_mode: u8, total_length: u64) -> Vec<u8> {
        let mut response = vec![
            1,
            2,
            compression_mode,
            (total_length >> 32) as u8,
            0,
            0,
            0,
            0,
        ];
        response[4..8].copy_from_slice(&(total_length as u32).to_le_bytes());
        response
    }
    fn server_authenticate(stream: &mut TcpStream) {
        let mut credential = [0u8; 3];
        stream
            .read_exact(&mut credential)
            .expect("server should read authentication");
        stream
            .write_all(&[6])
            .expect("server should acknowledge authentication");
    }

    fn server_read_frame(stream: &mut TcpStream) {
        let mut header = [0u8; IPC_HEADER_LENGTH];
        stream
            .read_exact(&mut header)
            .expect("server should read request header");
        let total_length = usize::try_from(
            (u64::from(header[3]) << 32)
                | u64::from(u32::from_le_bytes(header[4..8].try_into().unwrap())),
        )
        .unwrap();
        let mut body = vec![0u8; total_length - IPC_HEADER_LENGTH];
        stream
            .read_exact(&mut body)
            .expect("server should read request body");
    }

    fn wait_for_active_io(handle: &ConnectorAbortHandle) {
        let started = Instant::now();
        loop {
            if handle
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .active_operation
                .as_ref()
                .and_then(|operation| operation.stream.as_ref())
                .is_some()
            {
                return;
            }
            assert!(
                started.elapsed() < Duration::from_secs(1),
                "connector operation did not become abortable"
            );
            thread::yield_now();
        }
    }

    #[test]
    fn receive_rejects_short_header() {
        let error = connector_with_response(vec![1, 2, 0])
            .receive()
            .expect_err("short IPC header should fail");
        assert!(matches!(error, XqdbError::IOError(_)));
    }

    #[test]
    fn receive_rejects_length_shorter_than_header() {
        let mut connector = connector_with_response(response_header(0, 7));
        let error = connector
            .receive()
            .expect_err("invalid IPC length should fail");
        assert!(error.to_string().contains("shorter than the 8-byte header"));
        assert!(
            connector.stream.is_none(),
            "malformed frame should disconnect"
        );
    }

    #[test]
    fn receive_rejects_unknown_compression_mode_and_disconnects() {
        let mut connector = connector_with_response(response_header(3, 10));
        let error = connector
            .receive()
            .expect_err("unknown compression mode should fail");
        assert!(error
            .to_string()
            .contains("Unsupported IPC compression mode 3"));
        assert!(
            connector.stream.is_none(),
            "malformed frame should disconnect"
        );
    }

    #[test]
    fn receive_rejects_body_too_short_for_a_q_value() {
        for total_length in [8, 9] {
            let error = connector_with_response(response_header(0, total_length))
                .receive()
                .expect_err("empty or one-byte IPC body should fail");
            assert!(error
                .to_string()
                .contains("too short to contain a serialized q value"));
        }
    }

    #[test]
    fn receive_rejects_short_compression_prefixes() {
        for (compression_mode, prefix_length) in [(1, 4usize), (2, 8usize)] {
            let body = vec![0; prefix_length - 1];
            let error = compressed_message_length(&body, compression_mode)
                .expect_err("short compression prefix should fail");
            let message = error.to_string();
            assert!(
                message.contains("requires") && message.contains(&format!("{prefix_length}-byte")),
                "unexpected error: {message}"
            );
        }
    }

    #[test]
    fn receive_rejects_a_compressed_prefix_without_payload() {
        let mut response = response_header(1, 12);
        response.extend_from_slice(&10u32.to_le_bytes());
        assert!(matches!(
            connector_with_response(response).receive(),
            Err(XqdbError::DeserializationErr(_))
        ));
    }

    #[test]
    fn outgoing_frame_length_is_bounded_only_by_the_q_length_field() {
        let maximum_body = MAX_IPC_LENGTH_FIELD - IPC_HEADER_LENGTH;
        let (total_length, high_byte, low_length) =
            checked_outgoing_message_length(maximum_body, "test frame").expect("limit-sized frame");
        assert_eq!(total_length, MAX_IPC_LENGTH_FIELD);
        assert_eq!(high_byte, 0xff);
        assert_eq!(low_length, u32::MAX);
        // A frame above 4 GiB is emitted through the 40-bit form rather than refused.
        let (_, high_byte, low_length) =
            checked_outgoing_message_length(4 * 1024 * 1024 * 1024, "test frame")
                .expect("frame above u32::MAX");
        assert_eq!(high_byte, 1);
        assert_eq!(low_length, 8);

        let message = checked_outgoing_message_length(maximum_body + 1, "test frame")
            .expect_err("frame beyond the length field must fail")
            .to_string();
        assert!(
            message.contains("q header length field") || message.contains("length overflowed"),
            "unexpected error: {message}"
        );
        assert!(checked_outgoing_message_length(usize::MAX, "test frame").is_err());
    }

    #[test]
    fn allocation_helpers_reserve_fallibly_and_zero_on_request() {
        let buffer = allocate_buffer(32, "test buffer").expect("allocation should succeed");
        assert!(buffer.is_empty());
        assert!(buffer.capacity() >= 32);
        assert_eq!(
            allocate_zeroed_buffer(4, "test buffer").expect("allocation should succeed"),
            vec![0; 4]
        );
        allocate_buffer(usize::MAX, "oversized test buffer")
            .expect_err("capacity overflow should be reported");
    }

    #[test]
    fn receive_rejects_trailing_frame_bytes() {
        let mut body = serialize(&K::I32(42)).expect("test value should serialize");
        body.push(0);
        let total_length =
            u64::try_from(IPC_HEADER_LENGTH + body.len()).expect("test frame length fits u64");
        let mut response = response_header(0, total_length);
        response.extend_from_slice(&body);
        connector_with_response(response)
            .receive()
            .expect_err("trailing frame bytes should fail");
    }

    #[test]
    fn receive_rejects_decompressed_length_unreachable_from_its_payload() {
        // An 8-byte prefix plus 8 compressed bytes expands to at most 968 bytes.
        let mut response = response_header(2, 24);
        response.extend_from_slice(&(64 * 1024 * 1024u64).to_le_bytes());
        response.extend_from_slice(&[0u8; 8]);
        connector_with_response(response)
            .receive()
            .expect_err("unreachable decompressed length should fail");
    }

    #[test]
    fn receive_rejects_an_absurd_decompressed_length_without_an_absolute_ceiling() {
        // An impossible advertised output must fail before allocation.
        let mut response = response_header(2, 16);
        response.extend_from_slice(&u64::MAX.to_le_bytes());
        connector_with_response(response)
            .receive()
            .expect_err("absurd decompressed length should fail");
    }

    #[test]
    fn no_absolute_ceiling_rejects_a_large_frame() {
        // Both lengths from the report that the retired 512 MiB ceiling rejected: the wire frame
        // and, for a compressed response, the declared decompressed length.
        let total_length = 706_440_911u64;
        let expected_body_length =
            usize::try_from(total_length).expect("length fits usize") - IPC_HEADER_LENGTH;
        assert_eq!(
            checked_body_length(total_length, "IPC message").expect("large frame length"),
            expected_body_length
        );
        // 6 MB of compressed payload expands 118x here, inside the decompressor's 121x reach.
        assert_eq!(
            checked_decompressed_body_length(total_length, 6_000_000)
                .expect("large decompressed length"),
            expected_body_length
        );
        // Nothing below the q length field is refused, including lengths far above 4 GiB.
        let terabyte = 1024 * 1024 * 1024 * 1024u64;
        assert_eq!(
            checked_body_length(terabyte, "IPC message").expect("terabyte frame length"),
            usize::try_from(terabyte).expect("length fits usize") - IPC_HEADER_LENGTH
        );
    }

    #[test]
    fn receive_stops_at_the_declared_length_and_reports_a_short_body() {
        // A body that ends early fails as a short read instead of deserializing a partial frame.
        let mut response = response_header(0, 4096);
        response.extend_from_slice(&[0u8; 64]);
        let error = connector_with_response(response)
            .receive()
            .expect_err("a truncated frame must fail");
        assert!(
            matches!(&error, XqdbError::IOError(io_error)
                if io_error.kind() == io::ErrorKind::UnexpectedEof),
            "unexpected error: {error}"
        );

        let mut body = serialize(&K::CharVector(vec![b'z'; 32])).expect("value should serialize");
        let declared = u64::try_from(IPC_HEADER_LENGTH + body.len()).expect("length fits u64");
        let mut response = response_header(0, declared);
        response.append(&mut body);
        // A trailing byte of a following frame must survive: the read stops at the declared length.
        response.push(0xaa);
        let mut connector = connector_with_response(response);
        assert_eq!(
            connector.receive().expect("small frame should deserialize"),
            K::CharVector(vec![b'z'; 32])
        );
    }

    #[test]
    fn receive_decompresses_a_genuine_compressed_frame() {
        let body =
            serialize(&K::CharVector(vec![b'a'; 4096])).expect("test value should serialize");
        let total_length =
            u64::try_from(IPC_HEADER_LENGTH + body.len()).expect("test frame length fits u64");
        let mut frame = response_header(0, total_length);
        frame.extend_from_slice(&body);
        let compressed = compress(frame).expect("test frame should compress");
        assert_eq!(compressed[2], 1, "the test frame must arrive compressed");

        let value = connector_with_response(compressed)
            .receive()
            .expect("compressed frame should decompress");
        assert_eq!(value, K::CharVector(vec![b'a'; 4096]));
    }

    #[test]
    fn receive_applies_the_connector_symbol_encoding() {
        let body = [245, b'c', b'a', b'f', 0xe9, 0];
        let total_length =
            u64::try_from(IPC_HEADER_LENGTH + body.len()).expect("test frame length fits u64");
        let mut frame = response_header(0, total_length);
        frame.extend_from_slice(&body);

        let mut strict = connector_with_response(frame.clone());
        assert_eq!(strict.symbol_encoding, SymbolEncoding::Strict);
        assert!(matches!(
            strict.receive(),
            Err(XqdbError::DeserializationErr(_))
        ));

        let mut lossy = connector_with_response(frame);
        lossy.symbol_encoding = SymbolEncoding::Lossy;
        assert_eq!(
            lossy.receive().expect("lossy symbol atom should decode"),
            K::Symbol("caf\u{FFFD}".to_string())
        );
    }

    #[test]
    fn tls_client_config_uses_platform_verification() {
        tls_client_config(&ConnectorSettings::default())
            .expect("platform verifier configuration should build");
    }

    #[test]
    fn invalid_tls_server_name_returns_an_error_before_networking() {
        let mut connector = Connector::new("not a valid server name", 0, "", "", true, 0, 6);
        let error = connector
            .connect()
            .expect_err("invalid TLS server name should fail");
        assert!(matches!(
            error,
            XqdbError::Err(message) if message.contains("Invalid TLS server name")
        ));
    }

    #[test]
    fn connection_attempts_each_resolved_address() {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .expect("test listener should bind");
        let available = listener
            .local_addr()
            .expect("listener should have an address");
        let unavailable = SocketAddr::from(([127, 0, 0, 1], 0));

        let stream = connect_to_addresses([unavailable, available], Duration::from_secs(1))
            .expect("second address should connect");
        let (accepted, _) = listener
            .accept()
            .expect("listener should accept connection");
        drop((stream, accepted));
    }

    #[test]
    fn racing_abort_interrupts_receive_and_the_handle_reuses_on_reconnect() {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .expect("test listener should bind");
        let address = listener
            .local_addr()
            .expect("listener should have an address");
        let server = thread::spawn(move || {
            let (mut first, _) = listener.accept().expect("server should accept connection");
            server_authenticate(&mut first);
            let mut byte = [0u8];
            let _ = first.read(&mut byte);

            let (mut second, _) = listener.accept().expect("server should accept reconnect");
            server_authenticate(&mut second);
            server_read_frame(&mut second);
            let response = generate_j6_ipc_msg(MsgType::Response, false, K::I64(42)).unwrap();
            second.write_all(&response).unwrap();
        });

        let mut connector = Connector::new("127.0.0.1", address.port(), "", "", false, 2, 6);
        let abort_handle = connector.abort_handle();
        connector.connect().expect("connector should authenticate");
        let worker = thread::spawn(move || {
            let result = connector.receive();
            (connector, result)
        });

        wait_for_active_io(&abort_handle);
        let started = Instant::now();
        abort_handle.abort().expect("first abort should succeed");
        abort_handle
            .abort()
            .expect("repeated abort should be idempotent");
        let (mut connector, result) = worker.join().expect("connector worker should not panic");
        assert!(matches!(result, Err(XqdbError::IOError(_))));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "abort should interrupt receive before the configured socket timeout"
        );

        connector.connect().expect("connector should reconnect");
        assert_eq!(connector.execute("1+1", &[]).unwrap(), K::I64(42));
        server.join().expect("server should not panic");
    }
    #[test]
    fn abort_spans_execute_from_request_write_through_response_read() {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let address = listener.local_addr().unwrap();
        let (request_read, request_observed) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            let (mut first, _) = listener.accept().unwrap();
            server_authenticate(&mut first);
            server_read_frame(&mut first);
            request_read.send(()).unwrap();
            let mut byte = [0u8];
            let _ = first.read(&mut byte);

            let (mut second, _) = listener.accept().unwrap();
            server_authenticate(&mut second);
            server_read_frame(&mut second);
            let response = generate_j6_ipc_msg(MsgType::Response, false, K::I64(42)).unwrap();
            second.write_all(&response).unwrap();
        });

        let mut connector = Connector::new("127.0.0.1", address.port(), "", "", false, 2, 6);
        let abort_handle = connector.abort_handle();
        connector.connect().unwrap();
        let worker = thread::spawn(move || {
            let result = connector.execute("1+1", &[]);
            (connector, result)
        });
        request_observed
            .recv_timeout(Duration::from_secs(1))
            .expect("server should observe request before cancellation");
        abort_handle.abort().unwrap();

        let (mut connector, result) = worker.join().unwrap();
        assert!(matches!(result, Err(XqdbError::IOError(_))));
        connector.connect().expect("connector should reconnect");
        assert_eq!(connector.execute("1+1", &[]).unwrap(), K::I64(42));
        server.join().unwrap();
    }

    #[test]
    fn idle_abort_is_a_noop_for_an_authenticated_connection() {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            server_authenticate(&mut stream);
            server_read_frame(&mut stream);
            let response = generate_j6_ipc_msg(MsgType::Response, false, K::I64(2)).unwrap();
            stream.write_all(&response).unwrap();
        });

        let mut connector = Connector::new("127.0.0.1", address.port(), "", "", false, 2, 6);
        let abort_handle = connector.abort_handle();
        connector.connect().unwrap();
        abort_handle.abort().expect("idle cancel should be a no-op");
        connector
            .connect()
            .expect("existing stream remains connected");
        assert_eq!(connector.execute("1+1", &[]).unwrap(), K::I64(2));
        server.join().unwrap();
    }

    #[test]
    fn connector_settings_defaults_match_the_public_contract() {
        let settings = ConnectorSettings::default();
        assert_eq!(settings.value_mode, ValueMode::Native);
        assert_eq!(settings.compression, CompressionMode::Auto);
        assert_eq!(settings.compression_threshold, 10_000_000);
        assert_eq!(settings.connect_timeout, None);
        assert_eq!(settings.read_timeout, None);
        assert_eq!(settings.write_timeout, None);
        assert_eq!(settings.max_message_bytes, None);
        assert_eq!(settings.max_pending_notifications, 1024);
        assert!(settings.tls_ca.is_none());
        assert!(settings.tls_cert.is_none());
        assert!(settings.tls_key.is_none());
        assert!(settings.tls_server_name.is_none());
    }

    #[test]
    fn configure_rejects_invalid_notification_and_tls_combinations() {
        let mut connector = Connector::new("", 0, "", "", false, 0, 6);
        let settings = ConnectorSettings {
            max_pending_notifications: 0,
            ..ConnectorSettings::default()
        };
        assert!(connector.configure(settings).is_err());

        let settings = ConnectorSettings {
            tls_ca: Some(TEST_CERT_PEM.to_vec()),
            ..ConnectorSettings::default()
        };
        assert!(connector.configure(settings).is_err());

        let mut tls_connector = Connector::new("", 0, "", "", true, 0, 6);
        let settings = ConnectorSettings {
            tls_cert: Some(TEST_CERT_PEM.to_vec()),
            ..ConnectorSettings::default()
        };
        assert!(tls_connector.configure(settings).is_err());
    }

    #[test]
    fn tls_configuration_accepts_extra_ca_and_matching_client_identity() {
        let mut connector = Connector::new("", 0, "", "", true, 0, 6);
        let settings = ConnectorSettings {
            tls_ca: Some(TEST_CERT_PEM.to_vec()),
            tls_cert: Some(TEST_CERT_PEM.to_vec()),
            tls_key: Some(TEST_KEY_PEM.to_vec()),
            tls_server_name: Some("localhost".to_owned()),
            ..ConnectorSettings::default()
        };
        connector
            .configure(settings)
            .expect("valid PEM CA and client identity should configure");

        let invalid = ConnectorSettings {
            tls_ca: Some(b"not PEM".to_vec()),
            ..ConnectorSettings::default()
        };
        assert!(tls_client_config(&invalid).is_err());
    }

    #[test]
    fn explicit_zero_socket_timeout_disables_only_that_timeout() {
        let fallback = Duration::from_secs(30);
        assert_eq!(effective_socket_timeout(None, fallback), Some(fallback));
        assert_eq!(
            effective_socket_timeout(Some(Duration::from_millis(125)), fallback),
            Some(Duration::from_millis(125))
        );
        assert_eq!(
            effective_socket_timeout(Some(Duration::ZERO), fallback),
            None
        );
    }

    #[test]
    fn execute_preserves_async_notifications_while_waiting_for_responses() {
        let mut input =
            generate_j6_ipc_msg(MsgType::Async, false, K::I64(99)).expect("notification frame");
        input.extend(
            generate_j6_ipc_msg(MsgType::Response, false, K::I64(42))
                .expect("first response frame"),
        );
        input.extend(
            generate_j6_ipc_msg(MsgType::Response, false, K::I64(2))
                .expect("second response frame"),
        );
        let mut connector = Connector::new("", 0, "", "", false, 0, 6);
        install_memory_stream(&mut connector, input);

        assert_eq!(connector.execute("ignored", &[]).unwrap(), K::I64(42));
        assert_eq!(connector.execute("ignored", &[]).unwrap(), K::I64(2));
        assert_eq!(connector.receive().unwrap(), K::I64(99));
    }
    #[test]
    fn async_decode_error_is_queued_without_desynchronizing_responses() {
        let body = [128u8, b'n', b'o', b't', b'e', 0];
        let mut input = response_header(0, (IPC_HEADER_LENGTH + body.len()) as u64);
        input[1] = 0;
        input.extend_from_slice(&body);
        input.extend(
            generate_j6_ipc_msg(MsgType::Response, false, K::I64(42))
                .expect("first response frame"),
        );
        input.extend(
            generate_j6_ipc_msg(MsgType::Response, false, K::I64(2))
                .expect("second response frame"),
        );
        let mut connector = Connector::new("", 0, "", "", false, 0, 6);
        install_memory_stream(&mut connector, input);

        assert_eq!(connector.execute("ignored", &[]).unwrap(), K::I64(42));
        assert_eq!(connector.execute("ignored", &[]).unwrap(), K::I64(2));
        assert!(matches!(
            connector.receive(),
            Err(XqdbError::ServerErr(message)) if message == "note"
        ));
    }

    #[test]
    fn valid_server_error_response_does_not_disconnect_the_transport() {
        let body = [128u8, b'b', b'a', b'd', 0];
        let mut input = response_header(0, (IPC_HEADER_LENGTH + body.len()) as u64);
        input.extend_from_slice(&body);
        input.extend(
            generate_j6_ipc_msg(MsgType::Response, false, K::I64(42))
                .expect("following response frame"),
        );
        let mut connector = Connector::new("", 0, "", "", false, 0, 6);
        install_memory_stream(&mut connector, input);

        assert!(matches!(
            connector.execute("ignored", &[]),
            Err(XqdbError::ServerErr(message)) if message == "bad"
        ));
        assert!(connector.stream.is_some());
        assert_eq!(connector.execute("ignored", &[]).unwrap(), K::I64(42));
    }

    #[test]
    fn notification_overflow_errors_closes_and_keeps_already_queued_values() {
        let mut input =
            generate_j6_ipc_msg(MsgType::Async, false, K::I64(99)).expect("notification frame");
        input.extend(
            generate_j6_ipc_msg(MsgType::Async, false, K::I64(98))
                .expect("overflowing notification frame"),
        );
        input.extend(
            generate_j6_ipc_msg(MsgType::Response, false, K::I64(42)).expect("response frame"),
        );
        let mut connector = Connector::new("", 0, "", "", false, 0, 6);
        connector
            .configure(ConnectorSettings {
                max_pending_notifications: 1,
                ..ConnectorSettings::default()
            })
            .unwrap();
        install_memory_stream(&mut connector, input);

        let error = connector
            .execute("ignored", &[])
            .expect_err("second queued notification must overflow");
        assert!(error.to_string().contains("Pending notification limit 1"));
        assert!(connector.stream.is_none());
        assert_eq!(connector.receive().unwrap(), K::I64(99));
    }

    #[test]
    fn incoming_sync_requests_are_rejected_and_disconnect() {
        let frame =
            generate_j6_ipc_msg(MsgType::Sync, false, K::I64(42)).expect("sync request frame");
        let mut connector = connector_with_response(frame);
        let error = connector
            .receive()
            .expect_err("client cannot service incoming sync requests");
        assert!(error
            .to_string()
            .contains("unsupported synchronous request"));
        assert!(connector.stream.is_none());
    }

    #[test]
    fn message_bound_is_applied_to_uncompressed_and_compressed_lengths() {
        let uncompressed =
            generate_j6_ipc_msg(MsgType::Response, false, K::I32(42)).expect("response frame");
        let mut connector = Connector::new("", 0, "", "", false, 0, 6);
        connector
            .configure(ConnectorSettings {
                max_message_bytes: Some(uncompressed.len() - 1),
                ..ConnectorSettings::default()
            })
            .unwrap();
        install_memory_stream(&mut connector, uncompressed);
        assert!(connector.receive().is_err());
        assert!(connector.stream.is_none());

        let compressed =
            generate_j6_ipc_msg(MsgType::Response, true, K::CharVector(vec![b'a'; 4096]))
                .expect("compressed response frame");
        assert_ne!(compressed[2], 0);
        let mut connector = Connector::new("", 0, "", "", false, 0, 6);
        connector
            .configure(ConnectorSettings {
                max_message_bytes: Some(1024),
                ..ConnectorSettings::default()
            })
            .unwrap();
        install_memory_stream(&mut connector, compressed);
        assert!(connector.receive().is_err());
        assert!(connector.stream.is_none());

        let mut prefix_only = response_header(1, 10_000);
        prefix_only.extend_from_slice(&4096u32.to_le_bytes());
        let mut connector = Connector::new("", 0, "", "", false, 0, 6);
        connector
            .configure(ConnectorSettings {
                max_message_bytes: Some(1024),
                ..ConnectorSettings::default()
            })
            .unwrap();
        install_memory_stream(&mut connector, prefix_only);
        let error = connector
            .receive()
            .expect_err("logical bound must be checked from the prefix before body allocation");
        assert!(error.to_string().contains("configured maximum 1024"));
    }
    #[test]
    fn compressed_physical_length_must_fit_the_structural_encoding_bound() {
        let mut prefix_only = response_header(1, 10_000);
        prefix_only.extend_from_slice(&4096u32.to_le_bytes());
        let mut connector = Connector::new("", 0, "", "", false, 0, 6);
        connector
            .configure(ConnectorSettings {
                max_message_bytes: Some(20_000),
                ..ConnectorSettings::default()
            })
            .unwrap();
        install_memory_stream(&mut connector, prefix_only);

        let error = connector
            .receive()
            .expect_err("encoded body larger than the structural maximum must fail early");
        assert!(error
            .to_string()
            .contains("exceeds maximum structurally valid length"));
    }

    #[test]
    fn non_beneficial_compressed_null_within_structural_bound_is_accepted() {
        let frame = vec![1, 2, 1, 0, 15, 0, 0, 0, 10, 0, 0, 0, 0, 101, 0];
        assert_eq!(connector_with_response(frame).receive().unwrap(), K::Null);
    }

    #[test]
    fn lossless_receive_adopts_the_validated_value_body() {
        let body = serialize(&K::I32(42)).expect("value body");
        let frame =
            generate_j6_ipc_msg(MsgType::Response, false, K::I32(42)).expect("response frame");
        let mut connector = Connector::new("", 0, "", "", false, 0, 6);
        connector
            .configure(ConnectorSettings {
                value_mode: ValueMode::Lossless,
                ..ConnectorSettings::default()
            })
            .unwrap();
        install_memory_stream(&mut connector, frame);
        match connector.receive().expect("lossless value") {
            K::QValue(value) => assert_eq!(value.as_bytes(), body),
            value => panic!("expected lossless q value, got {value:?}"),
        }
    }

    /// Delivers its bytes in fixed-size pieces so column boundaries straddle reads.
    struct ChunkedStream {
        input: Cursor<Vec<u8>>,
        chunk: usize,
    }

    impl IoRead for ChunkedStream {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            let limit = buffer.len().min(self.chunk);
            self.input.read(&mut buffer[..limit])
        }
    }

    impl IoWrite for ChunkedStream {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// A table wide enough to cross `PIPELINE_MIN_BODY_BYTES`, mixing symbol, string, nested,
    /// timestamp and numeric columns so every scanner path runs.
    fn pipelined_table() -> DataFrame {
        let rows = 20_000usize;
        let symbols: Vec<&str> = (0..rows)
            .map(|row| ["AAPL", "MSFT", "GOOG", "a-symbol-longer-than-eight-bytes"][row % 4])
            .collect();
        let symbol = Series::new("sym".into(), symbols)
            .cast(&PolarsDataType::Categorical(
                Categories::global(),
                Categories::global().mapping(),
            ))
            .expect("categorical column");
        let text: Vec<String> = (0..rows).map(|row| format!("row {row}")).collect();
        let nested: Vec<Series> = (0..rows)
            .map(|row| Series::new("".into(), [row as f64, 0.5, -1.0].as_ref()))
            .collect();
        let time: Vec<i64> = (0..rows)
            .map(|row| 1_704_164_645_000_000_000 + row as i64)
            .collect();
        DataFrame::new_infer_height(vec![
            symbol.into(),
            Series::new("text".into(), text).into(),
            Series::new("nested".into(), nested).into(),
            Series::new("time".into(), time)
                .cast(&PolarsDataType::Datetime(TimeUnit::Nanoseconds, None))
                .expect("timestamp column")
                .into(),
            Series::new("volume".into(), (0..rows as i64).collect::<Vec<_>>()).into(),
            Series::new(
                "price".into(),
                (0..rows).map(|row| row as f64).collect::<Vec<_>>(),
            )
            .into(),
        ])
        .expect("test table should build")
    }

    fn chunked_connector(input: Vec<u8>, chunk: usize) -> Connector {
        let mut connector = Connector::new("", 0, "", "", false, 0, 6);
        connector.stream = Some(Box::new(ChunkedStream {
            input: Cursor::new(input),
            chunk,
        }));
        connector
    }

    #[test]
    fn pipelined_table_receive_matches_whole_frame_decoding() {
        let table = pipelined_table();
        let frame = generate_j6_ipc_msg(MsgType::Response, false, K::DataFrame(table.clone()))
            .expect("response frame");
        assert!(frame.len() - IPC_HEADER_LENGTH >= PIPELINE_MIN_BODY_BYTES);
        let (_, expected) =
            deserialize_ipc_frame(&frame, SymbolEncoding::Strict, ValueMode::Native)
                .expect("whole-frame decode");
        let K::DataFrame(expected) = expected else {
            panic!("expected a table");
        };

        for chunk in [997usize, 64 * 1024] {
            let mut connector = chunked_connector(frame.clone(), chunk);
            let K::DataFrame(received) = connector.receive().expect("pipelined table") else {
                panic!("expected a table");
            };
            assert!(
                received.equals_missing(&expected),
                "chunk {chunk}: pipelined decode differs from whole-frame decode"
            );
            assert!(
                connector.stream.is_some(),
                "a clean receive keeps the connection"
            );
        }
    }

    #[test]
    fn pipelined_table_receive_reports_truncation_as_a_short_read() {
        let frame = generate_j6_ipc_msg(MsgType::Response, false, K::DataFrame(pipelined_table()))
            .expect("response frame");
        let truncated = frame[..frame.len() / 2].to_vec();
        let mut connector = chunked_connector(truncated, 4096);
        let error = connector
            .receive()
            .expect_err("a truncated table must fail");
        assert!(
            matches!(&error, XqdbError::IOError(io_error)
                if io_error.kind() == io::ErrorKind::UnexpectedEof),
            "unexpected error: {error}"
        );
        assert!(connector.stream.is_none(), "a short read disconnects");
    }

    #[test]
    fn pipelined_table_receive_reports_a_bad_column_and_stays_in_sync() {
        let mut frame =
            generate_j6_ipc_msg(MsgType::Response, false, K::DataFrame(pipelined_table()))
                .expect("response frame");
        // Body layout: 98, attr, 99, 11, then the name list; corrupt the first column's type byte,
        // which follows the six-byte column-list header.
        let names_end =
            IPC_HEADER_LENGTH + 4 + calculate_names_end(&frame[IPC_HEADER_LENGTH + 4..]);
        frame[names_end + 6] = 200;
        let follow_up =
            generate_j6_ipc_msg(MsgType::Response, false, K::I64(7)).expect("second frame");
        frame.extend_from_slice(&follow_up);

        let mut connector = chunked_connector(frame, 4096);
        let error = connector
            .receive()
            .expect_err("an unsupported column type must fail");
        assert!(
            matches!(error, XqdbError::NotSupportedKListErr(200)),
            "unexpected error: {error}"
        );
        assert_eq!(
            connector
                .receive()
                .expect("the next frame is still readable"),
            K::I64(7)
        );
    }

    fn calculate_names_end(list: &[u8]) -> usize {
        let count = i32::from_le_bytes(list[1..5].try_into().expect("count")) as usize;
        let mut pos = 5;
        for _ in 0..count {
            pos += memchr::memchr(0, &list[pos..]).expect("terminated name") + 1;
        }
        pos
    }

    #[test]
    fn compression_setting_controls_outgoing_attempts() {
        let expression = "a".repeat(4096);
        let mut connector = Connector::new("", 0, "", "", false, 0, 6);
        connector
            .configure(ConnectorSettings {
                compression: CompressionMode::On,
                ..ConnectorSettings::default()
            })
            .unwrap();
        let output = install_memory_stream(&mut connector, Vec::new());
        connector.send(MsgType::Async, &expression, &[]).unwrap();
        assert_ne!(
            output
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())[2],
            0
        );

        let mut connector = Connector::new("", 0, "", "", false, 0, 6);
        connector
            .configure(ConnectorSettings {
                compression: CompressionMode::Off,
                ..ConnectorSettings::default()
            })
            .unwrap();
        let output = install_memory_stream(&mut connector, Vec::new());
        connector.send(MsgType::Async, &expression, &[]).unwrap();
        assert_eq!(
            output
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())[2],
            0
        );
    }
    #[test]
    fn abort_before_socket_attachment_is_observed_when_connect_completes() {
        let handle = ConnectorAbortHandle::default();
        let active_io = handle.begin_operation();
        handle
            .abort()
            .expect("operation without a socket should cancel");

        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let client = Arc::new(TcpStream::connect(listener.local_addr().unwrap()).unwrap());
        let (mut server, _) = listener.accept().unwrap();
        assert!(!active_io.attach_stream(client));
        assert!(active_io.finish());
        let mut byte = [0u8];
        assert_eq!(server.read(&mut byte).unwrap_or(0), 0);
    }

    #[test]
    fn abort_handle_can_be_reused_for_successive_streams() {
        let handle = ConnectorAbortHandle::default();
        for _ in 0..2 {
            let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
            let address = listener.local_addr().unwrap();
            let client = Arc::new(TcpStream::connect(address).unwrap());
            let (mut server, _) = listener.accept().unwrap();
            let active_io = handle.begin_operation();
            assert!(active_io.attach_stream(client));
            handle.abort().expect("active stream should abort");
            assert!(active_io.finish(), "active operation must observe abort");
            let mut byte = [0u8];
            assert_eq!(server.read(&mut byte).unwrap_or(0), 0);
        }
        handle.abort().expect("idle abort should remain idempotent");
    }
}
