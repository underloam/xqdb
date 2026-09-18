use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveTime, Timelike};
use indexmap::{map::Entry, IndexMap};
use polars::datatypes::{DataType as PolarsDataType, TimeUnit as PolarTimeUnit};
use polars::prelude::{Categories, DataFrame};
use polars::series::Series;
use polars_arrow::array::{
    Array, BinaryViewArray, BooleanArray, FixedSizeBinaryArray, FixedSizeListArray, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, ListArray, PrimitiveArray, UInt8Array,
    Utf8ViewArray, View,
};
use polars_arrow::bitmap::{Bitmap, MutableBitmap};
use polars_arrow::datatypes::{ArrowDataType, Field, TimeUnit};
use polars_arrow::{array::Utf8Array, offset::OffsetsBuffer};
use polars_buffer::Buffer;
use rayon::prelude::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::borrow::Cow;
use std::cmp::min;
use std::panic::{catch_unwind, AssertUnwindSafe};
use uuid::Uuid;
// time difference between chrono and q types
pub const NANOS_DIFF: i64 = 946684800000000000;
const NANOS_PER_DAY: i64 = 86400000000000;
const MS_PER_DAY: f64 = 86400000.0;
pub const DAY_DIFF: i32 = 730120;
const K_TYPE_NAME: [&str; 20] = [
    "",
    "boolean",
    "guid",
    "",
    "byte",
    "short",
    "int",
    "long",
    "real",
    "float",
    "char",
    "symbol",
    "timestamp",
    "",
    "date",
    "datetime",
    "timespan",
    "minute",
    "second",
    "time",
];
use crate::{
    errors::XqdbError,
    types::{
        for_each_category, try_own_str, validate_guid_series, validate_q_symbol,
        validate_q_time_series, QLambda, QOperator, SymbolEncoding, SymbolLookup, K, K_TYPE_SIZE,
        MAX_VALUE_DEPTH,
    },
};

/// U+FFFD REPLACEMENT CHARACTER, substituted for invalid UTF-8 under `SymbolEncoding::Lossy`.
const REPLACEMENT_CHARACTER: &str = "\u{FFFD}";

fn downcast_array<'a, T: 'static>(
    array: &'a dyn Array,
    expected: &str,
) -> Result<&'a T, XqdbError> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| XqdbError::NotAbleToSerializeErr(format!("expected Arrow {expected} array")))
}

trait WireNumber: Copy + bytemuck::Pod {
    const WIDTH: usize;

    #[cfg_attr(target_endian = "little", allow(dead_code))]
    fn from_le_chunk(chunk: &[u8]) -> Self;
}

macro_rules! impl_wire_number {
    ($number:ty, $width:expr) => {
        impl WireNumber for $number {
            const WIDTH: usize = $width;

            fn from_le_chunk(chunk: &[u8]) -> Self {
                <$number>::from_le_bytes(chunk.try_into().expect("validated chunk width"))
            }
        }
    };
}

impl_wire_number!(i16, 2);
impl_wire_number!(i32, 4);
impl_wire_number!(i64, 8);
impl_wire_number!(f32, 4);
impl_wire_number!(f64, 8);

/// Reserves exactly `count` elements up front, reporting allocation failure as a deserialization
/// error instead of aborting the process. Every caller sizes its allocation from a wire-declared
/// count multiplied by an element width, so the request can exceed the payload backing it.
fn try_vec_with_capacity<T>(count: usize, context: &str) -> Result<Vec<T>, XqdbError> {
    let mut values = Vec::new();
    values.try_reserve_exact(count).map_err(|error| {
        XqdbError::DeserializationErr(format!(
            "unable to allocate {context} for {count} element(s): {error}"
        ))
    })?;
    Ok(values)
}

/// Collects a fallible iterator of known length into a fallibly reserved vector. `collect` would
/// size its allocation from the same wire-declared count through the aborting allocator.
fn try_collect<T, I>(iterator: I, context: &str) -> Result<Vec<T>, XqdbError>
where
    I: ExactSizeIterator<Item = Result<T, XqdbError>>,
{
    let mut values = try_vec_with_capacity::<T>(iterator.len(), context)?;
    for value in iterator {
        values.push(value?);
    }
    Ok(values)
}

/// Collects an infallible iterator of known length into a fallibly reserved vector.
fn try_collect_infallible<T, I>(iterator: I, context: &str) -> Result<Vec<T>, XqdbError>
where
    I: ExactSizeIterator<Item = T>,
{
    let mut values = try_vec_with_capacity::<T>(iterator.len(), context)?;
    values.extend(iterator);
    Ok(values)
}

/// Copies a wire payload into an owned buffer with a fallible reservation, so a large frame reports
/// an allocation failure instead of aborting the process the way `to_vec` does.
fn try_copy_payload(payload: &[u8], context: &str) -> Result<Vec<u8>, XqdbError> {
    let mut owned = try_vec_with_capacity::<u8>(payload.len(), context)?;
    owned.extend_from_slice(payload);
    Ok(owned)
}

/// Appends to a buffer whose final size is not known up front, reserving fallibly so growth cannot
/// abort. `try_reserve` keeps the amortized geometric growth `extend_from_slice` would have used.
fn try_extend(buffer: &mut Vec<u8>, payload: &[u8], context: &str) -> Result<(), XqdbError> {
    if buffer.capacity() - buffer.len() < payload.len() {
        buffer.try_reserve(payload.len()).map_err(|error| {
            XqdbError::DeserializationErr(format!(
                "unable to grow {context} by {} byte(s): {error}",
                payload.len()
            ))
        })?;
    }
    buffer.extend_from_slice(payload);
    Ok(())
}

/// Pushes one element, reserving fallibly when the buffer is full, for the same reason as
/// `try_extend`: the element count comes from the wire and its total is not known up front.
fn try_push<T>(values: &mut Vec<T>, value: T, context: &str) -> Result<(), XqdbError> {
    if values.len() == values.capacity() {
        values.try_reserve(1).map_err(|error| {
            XqdbError::DeserializationErr(format!("unable to grow {context}: {error}"))
        })?;
    }
    values.push(value);
    Ok(())
}

fn decode_wire_numbers<T: WireNumber>(bytes: &[u8], context: &str) -> Result<Vec<T>, XqdbError> {
    if !bytes.len().is_multiple_of(T::WIDTH) {
        return Err(XqdbError::DeserializationErr(format!(
            "{context} payload has {} trailing byte(s)",
            bytes.len() % T::WIDTH
        )));
    }
    let value_count = bytes.len() / T::WIDTH;
    let mut values: Vec<T> = Vec::new();
    values.try_reserve_exact(value_count).map_err(|error| {
        XqdbError::DeserializationErr(format!(
            "unable to allocate {context} with {value_count} values: {error}"
        ))
    })?;
    #[cfg(target_endian = "little")]
    // SAFETY: the allocation holds exactly `value_count * T::WIDTH` bytes, the source slice
    // has the same byte length, and every WireNumber type is plain-old-data whose wire
    // encoding on little-endian targets matches its in-memory layout.
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), values.as_mut_ptr() as *mut u8, bytes.len());
        values.set_len(value_count);
    }
    #[cfg(target_endian = "big")]
    values.extend(bytes.chunks_exact(T::WIDTH).map(T::from_le_chunk));
    Ok(values)
}

/// Reuses the flat nested-list payload as Arrow storage instead of keeping both byte and
/// typed copies alive. Polars checks alignment and retains the original allocation layout;
/// unaligned storage and big-endian targets use the fallible copying decoder.
fn decode_owned_wire_numbers<T: WireNumber>(
    bytes: Vec<u8>,
    context: &str,
) -> Result<Buffer<T>, XqdbError> {
    #[cfg(target_endian = "little")]
    {
        match Buffer::from(bytes).try_transmute::<T>() {
            Ok(values) => Ok(values),
            Err(bytes) => decode_wire_numbers::<T>(&bytes, context).map(Buffer::from),
        }
    }
    #[cfg(target_endian = "big")]
    {
        decode_wire_numbers::<T>(&bytes, context).map(Buffer::from)
    }
}

fn write_wire_number<T: WireNumber>(output: &mut Vec<u8>, value: T) {
    #[cfg(target_endian = "little")]
    output.extend_from_slice(bytemuck::bytes_of(&value));
    #[cfg(target_endian = "big")]
    {
        let mut raw = [0u8; 16];
        let raw = &mut raw[..T::WIDTH];
        raw.copy_from_slice(&bytemuck::bytes_of(&value)[..T::WIDTH]);
        raw.reverse();
        output.extend_from_slice(raw);
    }
}

fn write_wire_numbers<T: WireNumber>(output: &mut Vec<u8>, values: &[T]) {
    #[cfg(target_endian = "little")]
    output.extend_from_slice(bytemuck::cast_slice(values));
    #[cfg(target_endian = "big")]
    for &value in values {
        write_wire_number(output, value);
    }
}

fn write_wire_numbers_from_iter<T, I>(output: &mut Vec<u8>, values: I)
where
    T: WireNumber,
    I: IntoIterator<Item = T>,
{
    for value in values {
        write_wire_number(output, value);
    }
}

fn try_write_wire_numbers_from_iter<T, I>(output: &mut Vec<u8>, values: I) -> Result<(), XqdbError>
where
    T: WireNumber,
    I: IntoIterator<Item = Result<T, XqdbError>>,
{
    for value in values {
        write_wire_number(output, value?);
    }
    Ok(())
}

/// Packs a bool iterator into an Arrow bitmap over a fallibly reserved byte buffer.
///
/// `Bitmap::from_trusted_len_iter` allocates through the infallible allocator, so a column too
/// large for the machine aborts the host process. Reserving the byte buffer here and handing it to
/// `MutableBitmap` keeps that allocation fallible while leaving the packing to polars: its aligned
/// extend calls `Vec::reserve` for exactly `bits.div_ceil(8)` bytes, which this reservation already
/// satisfies, so it never reaches its own allocator and its word-at-a-time loop is untouched. A
/// hand-rolled per-bit packer measured 1.5x to 3x slower, which is why the work is delegated.
fn try_bitmap<I>(bits: I, context: &str) -> Result<Bitmap, XqdbError>
where
    I: polars_arrow::trusted_len::TrustedLen<Item = bool>,
{
    let (lower, upper) = bits.size_hint();
    let length = upper.unwrap_or(lower);
    let bytes = try_vec_with_capacity::<u8>(length.div_ceil(8), context)?;
    let mut bitmap = MutableBitmap::try_new(bytes, 0).map_err(|error| {
        XqdbError::DeserializationErr(format!("unable to build {context}: {error}"))
    })?;
    bitmap.extend_from_trusted_len_iter(bits);
    Ok(bitmap.into())
}

/// Packs a validity iterator into a bitmap, returning `None` when every value is valid so
/// downstream kernels can skip null handling entirely.
fn optional_validity<I>(validity: I, context: &str) -> Result<Option<Bitmap>, XqdbError>
where
    I: polars_arrow::trusted_len::TrustedLen<Item = bool>,
{
    let bitmap = try_bitmap(validity, context)?;
    Ok((bitmap.unset_bits() != 0).then_some(bitmap))
}

fn q_list_length<T>(length: T) -> Result<[u8; 4], XqdbError>
where
    i32: TryFrom<T>,
{
    i32::try_from(length)
        .map(i32::to_le_bytes)
        .map_err(|_| XqdbError::OverLengthErr())
}

fn q_timestamp_nanoseconds(unix_nanoseconds: i64) -> Result<i64, XqdbError> {
    unix_nanoseconds
        .checked_sub(NANOS_DIFF)
        .filter(|value| *value > i64::MIN + 1 && *value < i64::MAX)
        .ok_or_else(|| {
            XqdbError::NotAbleToSerializeErr(
                "timestamp is outside q's representable nanosecond range".to_string(),
            )
        })
}
fn unix_timestamp_nanoseconds(q_nanoseconds: i64) -> Result<i64, XqdbError> {
    if q_nanoseconds == i64::MIN + 1 || q_nanoseconds == i64::MAX {
        return Err(XqdbError::DeserializationErr(
            "q timestamp infinity cannot be represented as a native timestamp".to_string(),
        ));
    }
    q_nanoseconds.checked_add(NANOS_DIFF).ok_or_else(|| {
        XqdbError::DeserializationErr(
            "finite q timestamp is outside the Unix nanosecond range".to_string(),
        )
    })
}

fn take_bytes<const N: usize>(
    bytes: &[u8],
    pos: &mut usize,
    context: &str,
) -> Result<[u8; N], XqdbError> {
    let end = pos
        .checked_add(N)
        .ok_or_else(|| XqdbError::DeserializationErr(format!("{context} byte range overflowed")))?;
    let value = bytes
        .get(*pos..end)
        .ok_or_else(|| XqdbError::DeserializationErr(format!("{context} omitted {N} byte(s)")))?;
    *pos = end;
    Ok(value
        .try_into()
        .expect("slice length was checked against the array width"))
}

/// Decodes wire text under `encoding`: valid UTF-8 is borrowed as is, anything else is an error
/// under `Strict` and transcoded under `Lossy`.
fn decode_utf8<'a>(
    bytes: &'a [u8],
    encoding: SymbolEncoding,
    context: &str,
) -> Result<Cow<'a, str>, XqdbError> {
    match std::str::from_utf8(bytes) {
        Ok(text) => Ok(Cow::Borrowed(text)),
        Err(error) => match encoding {
            SymbolEncoding::Strict => Err(XqdbError::DeserializationErr(format!(
                "{context} is not valid UTF-8: {error}"
            ))),
            SymbolEncoding::Lossy => transcode_utf8_lossy(bytes, context).map(Cow::Owned),
        },
    }
}

/// Replaces each maximal invalid subsequence of `bytes` with U+FFFD, producing the same text as
/// `String::from_utf8_lossy`. The output is reserved fallibly because the input length comes from
/// the wire; sizing it exactly costs a second validation pass, which only runs on invalid input.
fn transcode_utf8_lossy(bytes: &[u8], context: &str) -> Result<String, XqdbError> {
    let transcoded_length = bytes.utf8_chunks().fold(0usize, |length, chunk| {
        let replacement = if chunk.invalid().is_empty() {
            0
        } else {
            REPLACEMENT_CHARACTER.len()
        };
        length
            .saturating_add(chunk.valid().len())
            .saturating_add(replacement)
    });
    let mut text = String::new();
    text.try_reserve_exact(transcoded_length).map_err(|error| {
        XqdbError::DeserializationErr(format!(
            "unable to allocate {context} of {transcoded_length} transcoded byte(s): {error}"
        ))
    })?;
    for chunk in bytes.utf8_chunks() {
        text.push_str(chunk.valid());
        if !chunk.invalid().is_empty() {
            text.push_str(REPLACEMENT_CHARACTER);
        }
    }
    Ok(text)
}

/// Owns decoded text, keeping the buffer `Lossy` transcoding already produced.
fn try_own_decoded(text: Cow<'_, str>, context: &str) -> Result<String, XqdbError> {
    match text {
        Cow::Borrowed(text) => try_own_str(text, context),
        Cow::Owned(text) => Ok(text),
    }
}

/// Advances past one NUL-terminated wire string and returns its bytes without decoding them, for
/// passes that only measure a payload and leave text decoding to the pass that materializes it.
fn take_until_nul<'a>(
    bytes: &'a [u8],
    pos: &mut usize,
    context: &str,
) -> Result<&'a [u8], XqdbError> {
    let tail = bytes.get(*pos..).ok_or_else(|| {
        XqdbError::DeserializationErr(format!("{context} starts beyond the available payload"))
    })?;
    let length = memchr::memchr(0, tail)
        .ok_or_else(|| XqdbError::DeserializationErr(format!("{context} is not NUL-terminated")))?;
    let end = pos
        .checked_add(length)
        .ok_or_else(|| XqdbError::DeserializationErr(format!("{context} length overflowed")))?;
    let value = &bytes[*pos..end];
    *pos = end.checked_add(1).ok_or_else(|| {
        XqdbError::DeserializationErr(format!("{context} terminator offset overflowed"))
    })?;
    Ok(value)
}

fn take_utf8_until_nul<'a>(
    bytes: &'a [u8],
    pos: &mut usize,
    encoding: SymbolEncoding,
    context: &str,
) -> Result<Cow<'a, str>, XqdbError> {
    decode_utf8(take_until_nul(bytes, pos, context)?, encoding, context)
}

fn take_list_length(bytes: &[u8], pos: &mut usize, context: &str) -> Result<usize, XqdbError> {
    // One bounds check for the attribute byte plus the 4-byte length; error paths only
    // allocate on failure so per-row callers stay allocation-free.
    let end = pos.checked_add(5).ok_or_else(|| {
        XqdbError::DeserializationErr(format!("{context} header offset overflowed"))
    })?;
    let header = bytes.get(*pos..end).ok_or_else(|| {
        XqdbError::DeserializationErr(format!("{context} header omitted its 5 byte(s)"))
    })?;
    let raw_length = i32::from_le_bytes(
        header[1..5]
            .try_into()
            .expect("list header slice is exactly five bytes"),
    );
    *pos = end;
    usize::try_from(raw_length)
        .map_err(|_| XqdbError::DeserializationErr(format!("{context} length cannot be negative")))
}

fn check_symbol_list_consumed(payload: &[u8], cursor: usize) -> Result<(), XqdbError> {
    if cursor == payload.len() {
        Ok(())
    } else {
        Err(XqdbError::DeserializationErr(format!(
            "q symbol list has {} trailing byte(s)",
            payload.len() - cursor
        )))
    }
}

/// Hashes one already well-mixed word: a symbol of at most eight bytes packed little-endian.
/// Symbols never contain NUL, so zero padding cannot collide with a shorter symbol.
#[derive(Default)]
struct WordHasher(u64);

impl std::hash::Hasher for WordHasher {
    fn finish(&self) -> u64 {
        let mut hash = self.0.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        hash ^= hash >> 32;
        hash = hash.wrapping_mul(0xD6E8_FEB8_6659_FD93);
        hash ^ (hash >> 32)
    }

    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.0 = (self.0 << 8) | u64::from(byte);
        }
    }

    fn write_u64(&mut self, word: u64) {
        self.0 = word;
    }
}

/// Hashes byte-slice keys with one-shot XXH3, which stays cheap for the short strings q symbols
/// are; the length prefix `[u8]`'s `Hash` impl writes is folded into the seed.
#[derive(Default)]
struct SymbolHasher(u64);

impl std::hash::Hasher for SymbolHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        self.0 = xxhash_rust::xxh3::xxh3_64_with_seed(bytes, self.0);
    }

    fn write_usize(&mut self, length: usize) {
        self.0 = length as u64;
    }
}

/// Resolves symbol bytes to global category ids while decoding one column.
///
/// q symbol columns repeat few distinct values over many rows, so each distinct symbol enters the
/// shared categorical mapping once and every later occurrence is resolved locally: consecutive
/// repeats by comparison with the previous symbol, symbols of at most eight bytes through a
/// word-keyed map, and longer ones through a byte-slice map. This replaces a concurrent-map
/// lookup per row with a private one.
struct SymbolInterner<'a> {
    mapping: std::sync::Arc<polars::prelude::CategoricalMapping>,
    last: Option<(&'a [u8], u32)>,
    short: std::collections::HashMap<u64, u32, std::hash::BuildHasherDefault<WordHasher>>,
    long: std::collections::HashMap<&'a [u8], u32, std::hash::BuildHasherDefault<SymbolHasher>>,
}

impl<'a> SymbolInterner<'a> {
    fn new() -> Self {
        Self {
            mapping: Categories::global().mapping(),
            last: None,
            short: std::collections::HashMap::default(),
            long: std::collections::HashMap::default(),
        }
    }

    /// `symbol` must be valid UTF-8; callers validate the whole payload before scanning.
    fn category(&mut self, symbol: &'a [u8]) -> Result<u32, XqdbError> {
        if let Some((last, cat)) = self.last {
            if last == symbol {
                return Ok(cat);
            }
        }
        let cat = if symbol.len() <= 8 {
            let mut word = [0u8; 8];
            word[..symbol.len()].copy_from_slice(symbol);
            match self.short.entry(u64::from_le_bytes(word)) {
                std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    *entry.insert(insert_global_category(&self.mapping, symbol)?)
                }
            }
        } else {
            match self.long.entry(symbol) {
                std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    *entry.insert(insert_global_category(&self.mapping, symbol)?)
                }
            }
        };
        self.last = Some((symbol, cat));
        Ok(cat)
    }
}

fn insert_global_category(
    mapping: &polars::prelude::CategoricalMapping,
    symbol: &[u8],
) -> Result<u32, XqdbError> {
    // SAFETY: the symbol-list payload was validated as UTF-8 before it was split at NUL bytes,
    // and NUL is a one-byte code point, so every split piece is valid UTF-8.
    let symbol = unsafe { std::str::from_utf8_unchecked(symbol) };
    mapping
        .insert_cat(symbol)
        .map_err(|error| XqdbError::DeserializationErr(error.to_string()))
}

/// Decodes a validated symbol-list payload straight into a global categorical column, skipping
/// the intermediate string column and the per-row concurrent-map cast it needed.
fn decode_symbol_column(payload: &[u8], length: usize, name: &str) -> Result<K, XqdbError> {
    use polars::prelude::{Categorical32Type, CategoricalChunked, IntoSeries, UInt32Chunked};

    let mut interner = SymbolInterner::new();
    let mut categories = try_vec_with_capacity::<u32>(length, "q symbol-list categories")?;
    let mut cursor = 0usize;
    for _ in 0..length {
        let terminator = memchr::memchr(0, &payload[cursor..]).ok_or_else(|| {
            XqdbError::DeserializationErr("q symbol-list value is not NUL-terminated".to_string())
        })?;
        categories.push(interner.category(&payload[cursor..cursor + terminator])?);
        cursor += terminator + 1;
    }
    check_symbol_list_consumed(payload, cursor)?;
    let dtype = PolarsDataType::Categorical(Categories::global(), interner.mapping.clone());
    let physical = UInt32Chunked::from_vec(name.into(), categories);
    // SAFETY: every category id was returned by `dtype`'s own mapping, and the global categories
    // use a u32 physical representation, which `cat32()` in the serializer already relies on.
    let series = unsafe {
        CategoricalChunked::<Categorical32Type>::from_cats_and_dtype_unchecked(physical, dtype)
    };
    Ok(K::Series(series.into_series()))
}

fn finite_f64_to_i64(value: f64, context: &str) -> Result<i64, XqdbError> {
    const I64_UPPER_EXCLUSIVE: f64 = 9_223_372_036_854_775_808.0;
    const I64_LOWER_INCLUSIVE: f64 = -9_223_372_036_854_775_808.0;
    if !value.is_finite() || !(I64_LOWER_INCLUSIVE..I64_UPPER_EXCLUSIVE).contains(&value) {
        return Err(XqdbError::DeserializationErr(format!(
            "{context} is outside the i64 range"
        )));
    }
    Ok(value as i64)
}

fn q_datetime_nanoseconds(q_days: f64) -> Result<i64, XqdbError> {
    let q_milliseconds = finite_f64_to_i64((q_days * MS_PER_DAY).round(), "finite q datetime")?;
    q_milliseconds
        .checked_mul(1_000_000)
        .and_then(|value| value.checked_add(NANOS_DIFF))
        .ok_or_else(|| {
            XqdbError::DeserializationErr(
                "finite q datetime is outside the Unix nanosecond range".to_string(),
            )
        })
}
fn deserialize_lambda(
    vec: &[u8],
    pos: &mut usize,
    start_pos: usize,
    encoding: SymbolEncoding,
) -> Result<K, XqdbError> {
    let context_tail = vec
        .get(start_pos..)
        .ok_or_else(|| XqdbError::DeserializationErr("q lambda omitted its context".to_string()))?;
    let context_length = memchr::memchr(0, context_tail).ok_or_else(|| {
        XqdbError::DeserializationErr("q lambda context is not NUL-terminated".to_string())
    })?;
    let context_end = start_pos
        .checked_add(context_length)
        .ok_or_else(|| XqdbError::DeserializationErr("q lambda context overflowed".to_string()))?;
    let context = decode_utf8(&vec[start_pos..context_end], encoding, "q lambda context")?;
    let vector_start = context_end.checked_add(1).ok_or_else(|| {
        XqdbError::DeserializationErr("q lambda source offset overflowed".to_string())
    })?;
    let vector_end = vector_start.checked_add(6).ok_or_else(|| {
        XqdbError::DeserializationErr("q lambda source header overflowed".to_string())
    })?;
    let header = vec.get(vector_start..vector_end).ok_or_else(|| {
        XqdbError::DeserializationErr(
            "q lambda omitted its type-10 character-vector source".to_string(),
        )
    })?;
    if header[0] != 10 {
        return Err(XqdbError::DeserializationErr(format!(
            "q lambda source must be a type-10 character vector, got type {}",
            header[0]
        )));
    }
    if header[1] != 0 {
        return Err(XqdbError::DeserializationErr(format!(
            "q lambda source has unsupported attribute {}",
            header[1]
        )));
    }
    let source_length = i32::from_le_bytes(header[2..6].try_into().unwrap());
    let source_length = usize::try_from(source_length).map_err(|_| {
        XqdbError::DeserializationErr("q lambda source length cannot be negative".to_string())
    })?;
    let source_end = vector_end.checked_add(source_length).ok_or_else(|| {
        XqdbError::DeserializationErr("q lambda source length overflowed".to_string())
    })?;
    let source = decode_utf8(
        vec.get(vector_end..source_end).ok_or_else(|| {
            XqdbError::DeserializationErr(format!(
                "q lambda source length {source_length} exceeds the available payload"
            ))
        })?,
        encoding,
        "q lambda source",
    )?;
    let lambda = QLambda::with_context(source, context)
        .map_err(|error| XqdbError::DeserializationErr(error.to_string()))?;
    *pos = source_end;
    Ok(K::Lambda(lambda))
}

pub fn deserialize(vec: &[u8], pos: &mut usize, encoding: SymbolEncoding) -> Result<K, XqdbError> {
    match catch_unwind(AssertUnwindSafe(|| {
        deserialize_unchecked(vec, pos, false, encoding, 0)
    })) {
        Ok(Ok(value)) if *pos == vec.len() => Ok(value),
        Ok(Ok(_)) => Err(XqdbError::DeserializationErr(format!(
            "q value has {} trailing byte(s)",
            vec.len().saturating_sub(*pos)
        ))),
        Ok(Err(error)) => Err(error),
        Err(_) => Err(XqdbError::DeserializationErr(
            "malformed q value caused an internal parser panic".to_string(),
        )),
    }
}

fn deserialize_unchecked(
    vec: &[u8],
    pos: &mut usize,
    is_column: bool,
    encoding: SymbolEncoding,
    depth: usize,
) -> Result<K, XqdbError> {
    if depth > MAX_VALUE_DEPTH {
        return Err(XqdbError::DeserializationErr(format!(
            "q value nesting exceeds {MAX_VALUE_DEPTH} levels"
        )));
    }
    let k_type = *vec.get(*pos).ok_or_else(|| {
        XqdbError::DeserializationErr("q value omitted its type byte".to_string())
    })?;
    *pos += 1;
    let start_pos = *pos;
    match k_type {
        237..=255 => match k_type {
            255 => {
                let value = take_bytes::<1>(vec, pos, "q boolean atom")?[0];
                match value {
                    0 => Ok(K::Boolean(false)),
                    1 => Ok(K::Boolean(true)),
                    _ => Err(XqdbError::DeserializationErr(format!(
                        "q boolean atom must be 0 or 1, got {value}"
                    ))),
                }
            }
            254 => Ok(K::Guid(Uuid::from_bytes(take_bytes::<16>(
                vec,
                pos,
                "q GUID atom",
            )?))),
            252 => Ok(K::U8(take_bytes::<1>(vec, pos, "q byte atom")?[0])),
            251 => Ok(K::I16(i16::from_le_bytes(take_bytes::<2>(
                vec,
                pos,
                "q short atom",
            )?))),
            250 => Ok(K::I32(i32::from_le_bytes(take_bytes::<4>(
                vec,
                pos,
                "q int atom",
            )?))),
            249 => Ok(K::I64(i64::from_le_bytes(take_bytes::<8>(
                vec,
                pos,
                "q long atom",
            )?))),
            248 => Ok(K::F32(f32::from_le_bytes(take_bytes::<4>(
                vec,
                pos,
                "q real atom",
            )?))),
            247 => Ok(K::F64(f64::from_le_bytes(take_bytes::<8>(
                vec,
                pos,
                "q float atom",
            )?))),
            246 => Ok(K::Char(take_bytes::<1>(vec, pos, "q char atom")?[0])),
            245 => Ok(K::Symbol(try_own_decoded(
                take_utf8_until_nul(vec, pos, encoding, "q symbol atom")?,
                "q symbol atom",
            )?)),
            // timestamp
            244 => {
                let q_ns = i64::from_le_bytes(take_bytes::<8>(vec, pos, "q timestamp atom")?);
                if q_ns == i64::MIN {
                    Ok(K::Null)
                } else {
                    let ns = unix_timestamp_nanoseconds(q_ns)?;
                    Ok(K::DateTime(DateTime::from_timestamp_nanos(ns)))
                }
            }
            // month
            243 => {
                let unit = i32::from_le_bytes(take_bytes::<4>(vec, pos, "q month atom")?);
                let year = 2000i32.checked_add(unit.div_euclid(12)).ok_or_else(|| {
                    XqdbError::DeserializationErr(
                        "q month is outside chrono's representable range".to_string(),
                    )
                })?;
                let month = unit.rem_euclid(12) as u32 + 1;
                NaiveDate::from_ymd_opt(year, month, 1)
                    .map(K::Date)
                    .ok_or_else(|| {
                        XqdbError::DeserializationErr(
                            "q month is outside chrono's representable range".to_string(),
                        )
                    })
            }
            // date
            242 => {
                let q_days = i32::from_le_bytes(take_bytes::<4>(vec, pos, "q date atom")?);
                let days = q_days.checked_add(DAY_DIFF).ok_or_else(|| {
                    XqdbError::DeserializationErr(
                        "q date is outside chrono's representable range".to_string(),
                    )
                })?;
                NaiveDate::from_num_days_from_ce_opt(days)
                    .map(K::Date)
                    .ok_or_else(|| {
                        XqdbError::DeserializationErr(
                            "q date is outside chrono's representable range".to_string(),
                        )
                    })
            }
            // datetime
            241 => {
                let unit = f64::from_le_bytes(take_bytes::<8>(vec, pos, "q datetime atom")?);
                if unit.is_nan() {
                    Ok(K::Null)
                } else if !unit.is_finite() {
                    Err(XqdbError::DeserializationErr(
                        "q datetime infinity cannot be represented as a native timestamp"
                            .to_string(),
                    ))
                } else {
                    Ok(K::DateTime(DateTime::from_timestamp_nanos(
                        q_datetime_nanoseconds(unit)?,
                    )))
                }
            }
            // timespan
            240 => {
                let ns = i64::from_le_bytes(take_bytes::<8>(vec, pos, "q timespan atom")?);
                Ok(K::Duration(Duration::nanoseconds(ns)))
            }
            // time, second, minute
            237..=239 => {
                let unit = i32::from_le_bytes(take_bytes::<4>(vec, pos, "q time atom")?);
                if unit < 0 {
                    return Err(XqdbError::NotSupportedMinusTimeErr(k_type));
                }
                let multiplier = match k_type {
                    237 => 1_000_000i64,
                    238 => 1_000_000_000i64,
                    239 => 60_000_000_000i64,
                    _ => unreachable!(),
                };
                let nanoseconds = i64::from(unit)
                    .checked_mul(multiplier)
                    .filter(|value| *value < NANOS_PER_DAY)
                    .ok_or_else(|| {
                        XqdbError::DeserializationErr("q time atom exceeds one day".to_string())
                    })?;
                NaiveTime::from_num_seconds_from_midnight_opt(
                    (nanoseconds / 1_000_000_000) as u32,
                    (nanoseconds % 1_000_000_000) as u32,
                )
                .map(K::Time)
                .ok_or_else(|| {
                    XqdbError::DeserializationErr("q time atom exceeds one day".to_string())
                })
            }
            _ => Err(XqdbError::NotSupportedKTypeErr(k_type)),
        },
        // string, list(i16, i32, i64, f32, f64)
        0..=19 => {
            let end_pos = match calculate_array_end_index(vec, *pos, k_type) {
                Ok(end_pos) => end_pos,
                Err(e) => {
                    if !is_column && k_type == 0 {
                        take_bytes::<1>(vec, pos, "q mixed-list attribute")?;
                        let raw_length =
                            i32::from_le_bytes(take_bytes::<4>(vec, pos, "q mixed-list length")?);
                        let length = usize::try_from(raw_length).map_err(|_| {
                            XqdbError::DeserializationErr(
                                "q mixed-list length cannot be negative".to_string(),
                            )
                        })?;
                        let remaining_bytes = vec.len().saturating_sub(*pos);
                        if length > remaining_bytes / 2 {
                            return Err(XqdbError::DeserializationErr(format!(
                                "mixed-list count {length} exceeds the available {remaining_bytes}-byte payload"
                            )));
                        }
                        let mut res = Vec::new();
                        res.try_reserve_exact(length).map_err(|error| {
                            XqdbError::DeserializationErr(format!(
                                "unable to allocate mixed list with {length} values: {error}"
                            ))
                        })?;
                        for _ in 0..length {
                            res.push(deserialize_unchecked(vec, pos, false, encoding, depth + 1)?);
                        }
                        return Ok(K::MixedList(res));
                    } else {
                        return Err(e);
                    }
                }
            };
            let start_pos = *pos;
            *pos = end_pos;
            deserialize_series(&vec[start_pos..end_pos], k_type, k_type != 10, encoding)
        }
        99 => {
            let key_type = *vec.get(*pos).ok_or_else(|| {
                XqdbError::DeserializationErr("q dictionary omitted its key type".to_string())
            })?;
            if key_type == 98 {
                let mut key_df: DataFrame =
                    deserialize_unchecked(vec, pos, true, encoding, depth + 1)?.try_into()?;
                let value_df: DataFrame =
                    deserialize_unchecked(vec, pos, true, encoding, depth + 1)?.try_into()?;
                key_df = key_df
                    .hstack(value_df.columns())
                    .map_err(|e| XqdbError::Err(e.to_string()))?;
                Ok(K::DataFrame(key_df))
            } else if key_type == 11 || key_type == 0 {
                *pos += 1;
                let keys: Series = if key_type == 11 {
                    let end_pos = calculate_array_end_index(vec, *pos, 11)?;
                    let keys =
                        deserialize_series(&vec[*pos..end_pos], 11, true, encoding)?.try_into()?;
                    *pos = end_pos;
                    keys
                } else if take_list_length(vec, pos, "q dictionary keys")? == 0 {
                    // ()!() keys q's empty dictionary literal with an empty general list
                    // rather than an empty symbol list; a general list can only stand in for
                    // symbol keys while it holds none.
                    new_empty_series(11)?.try_into()?
                } else {
                    return Err(XqdbError::Err(
                        "Only support symbol keys dictionary or keyed table, got k type 0"
                            .to_string(),
                    ));
                };
                let value_type = *vec.get(*pos).ok_or_else(|| {
                    XqdbError::DeserializationErr("q dictionary omitted its values".to_string())
                })?;
                if value_type > 19 {
                    return Err(XqdbError::Err(format!(
                        "Not support k type {value_type:?} values in dictionary"
                    )));
                }
                let values = deserialize_unchecked(vec, pos, is_column, encoding, depth + 1)?;
                let keys = keys
                    .cat32()
                    .map_err(|error| XqdbError::DeserializationErr(error.to_string()))?;
                let value_length = match &values {
                    K::Series(series) => series.len(),
                    K::MixedList(values) => values.len(),
                    K::CharVector(values) => values.len(),
                    value => {
                        return Err(XqdbError::DeserializationErr(format!(
                            "dictionary values must be a list, got {value:?}"
                        )))
                    }
                };
                if keys.len() != value_length {
                    return Err(XqdbError::DeserializationErr(format!(
                        "dictionary key/value length mismatch: {} keys and {value_length} values",
                        keys.len()
                    )));
                }
                let keys = keys.iter_str();
                let values = match values {
                    K::Series(series) => {
                        let mut decoded =
                            try_vec_with_capacity::<K>(series.len(), "q dictionary values")?;
                        for value in series.iter() {
                            decoded.push(K::try_from_any_value(value)?);
                        }
                        decoded
                    }
                    K::MixedList(values) => values,
                    K::CharVector(values) => try_collect_infallible(
                        values.into_iter().map(K::Char),
                        "q dictionary char values",
                    )?,
                    _ => {
                        return Err(XqdbError::DeserializationErr(
                            "dictionary values changed type during decoding".to_string(),
                        ))
                    }
                };
                let mut dict = IndexMap::new();
                dict.try_reserve_exact(value_length).map_err(|error| {
                    XqdbError::DeserializationErr(format!(
                        "unable to allocate q dictionary with {value_length} entries: {error}"
                    ))
                })?;
                for (key, value) in keys.zip(values) {
                    let key = try_own_str(key.unwrap_or(""), "q dictionary key")?;
                    match dict.entry(key) {
                        Entry::Vacant(entry) => {
                            entry.insert(value);
                        }
                        Entry::Occupied(entry) => {
                            return Err(XqdbError::DeserializationErr(format!(
                                "duplicate symbol dictionary key {:?}; use lossless value mode to preserve duplicate entries",
                                entry.key()
                            )))
                        }
                    }
                }
                Ok(K::Dict(dict))
            } else {
                Err(XqdbError::Err(format!(
                    "Only support symbol keys dictionary or keyed table, got k type {key_type:?}"
                )))
            }
        }
        98 => {
            *pos += 3;
            let end_pos = calculate_array_end_index(vec, *pos, 11)?;
            let k = deserialize_series(&vec[*pos..end_pos], 11, false, encoding)?;
            *pos = end_pos;
            let symbols = if let K::Series(series) = k {
                series
            } else {
                return Err(XqdbError::DeserializationErr(format!(
                    "Expecting array, but got {k:?}"
                )));
            };
            let symbols = symbols.str().unwrap();
            *pos += 6;
            let mut k_types = try_vec_with_capacity::<u8>(symbols.len(), "q table column types")?;
            k_types.resize(symbols.len(), 0);
            let mut vectors: Vec<&[u8]> =
                try_vec_with_capacity(symbols.len(), "q table column payloads")?;
            for k_type in k_types.iter_mut().take(symbols.len()) {
                *k_type = vec[*pos];
                *pos += 1;
                let end_pos = calculate_array_end_index(vec, *pos, *k_type)?;
                vectors.push(&vec[*pos..end_pos]);
                *pos = end_pos;
            }

            let mut decoded: Vec<Result<Series, XqdbError>> =
                try_vec_with_capacity(vectors.len(), "q table column results")?;
            vectors
                .par_iter()
                .zip(k_types.par_iter().copied())
                .map(|(values, k_type)| {
                    deserialize_series(values, k_type, true, encoding)?.try_into()
                })
                .collect_into_vec(&mut decoded);
            let mut columns = try_vec_with_capacity::<Series>(decoded.len(), "q table columns")?;
            for column in decoded {
                columns.push(column?);
            }
            columns.iter_mut().zip(symbols.iter()).for_each(|(c, n)| {
                c.rename(n.unwrap_or("").into());
            });
            let mut frame_columns = try_vec_with_capacity(columns.len(), "q table frame columns")?;
            frame_columns.extend(columns.into_iter().map(|column| column.into()));
            DataFrame::new_infer_height(frame_columns)
                .map(K::DataFrame)
                .map_err(|error| XqdbError::DeserializationErr(error.to_string()))
        }
        100 => deserialize_lambda(vec, pos, start_pos, encoding),
        101..=103 => {
            let opcode = *vec.get(start_pos).ok_or_else(|| {
                XqdbError::DeserializationErr(format!(
                    "q primitive type {k_type} omitted its opcode"
                ))
            })?;
            *pos += 1;
            if k_type == 101 && opcode == 0 {
                Ok(K::Null)
            } else {
                QOperator::from_wire(k_type, opcode)
                    .map(K::Operator)
                    .ok_or(XqdbError::NotSupportedKOperatorErr(opcode))
            }
        }
        // q error
        128 => {
            // The message is diagnostic text rather than data, so surfacing the server's error
            // with replacement characters beats hiding it behind a decoding failure.
            let message = take_utf8_until_nul(vec, pos, SymbolEncoding::Lossy, "q error message")?;
            Err(XqdbError::ServerErr(message.into_owned()))
        }
        _ => Err(XqdbError::NotSupportedKTypeErr(k_type)),
    }
}

fn create_field(k_type: u8, name: &str) -> Result<Field, XqdbError> {
    match k_type {
        1 => Ok(Field::new(name.into(), ArrowDataType::Boolean, false)),
        2 => Ok(Field::new(name.into(), ArrowDataType::Binary, false)),
        4 => Ok(Field::new(name.into(), ArrowDataType::UInt8, false)),
        5 => Ok(Field::new(name.into(), ArrowDataType::Int16, true)),
        6 => Ok(Field::new(name.into(), ArrowDataType::Int32, true)),
        7 => Ok(Field::new(name.into(), ArrowDataType::Int64, true)),
        8 => Ok(Field::new(name.into(), ArrowDataType::Float32, false)),
        9 => Ok(Field::new(name.into(), ArrowDataType::Float64, false)),
        10 => Ok(Field::new(name.into(), ArrowDataType::LargeUtf8, false)),
        11 => Ok(Field::new(name.into(), ArrowDataType::LargeUtf8, false)),
        12 => Ok(Field::new(
            name.into(),
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )),
        14 => Ok(Field::new(name.into(), ArrowDataType::Date32, true)),
        15 => Ok(Field::new(
            name.into(),
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )),
        16 => Ok(Field::new(
            name.into(),
            ArrowDataType::Time64(TimeUnit::Nanosecond),
            true,
        )),
        17 => Ok(Field::new(
            name.into(),
            ArrowDataType::Time32(TimeUnit::Millisecond),
            true,
        )),
        18 => Ok(Field::new(
            name.into(),
            ArrowDataType::Time32(TimeUnit::Millisecond),
            true,
        )),
        19 => Ok(Field::new(
            name.into(),
            ArrowDataType::Time32(TimeUnit::Millisecond),
            true,
        )),
        _ => Err(XqdbError::NotSupportedKListErr(k_type)),
    }
}

fn calculate_array_end_index(vec: &[u8], start_pos: usize, k_type: u8) -> Result<usize, XqdbError> {
    match ListScan::new(k_type, start_pos)?.advance(vec, vec.len())? {
        ScanOutcome::Complete(end) => Ok(end),
        ScanOutcome::Incomplete => Err(XqdbError::DeserializationErr(
            "q list ends before its declared length".to_string(),
        )),
    }
}

/// Result of scanning a list for its end within the bytes that have arrived.
pub(crate) enum ScanOutcome {
    Complete(usize),
    Incomplete,
}

/// Resumable scan for the end of one q list payload, starting at its attribute byte.
///
/// The scan only ever looks at `received`, the prefix of the body delivered so far, and reports
/// `Incomplete` instead of an error when the list continues beyond it, so a table can be walked
/// column by column while the rest of the frame is still in flight. `total` is the full body
/// length, which turns a list that overruns it into an error straight away.
pub(crate) struct ListScan {
    k_type: u8,
    pos: usize,
    rows_left: Option<usize>,
    child_type: Option<u8>,
    child_symbols_left: usize,
}

impl ListScan {
    pub(crate) fn new(k_type: u8, start_pos: usize) -> Result<Self, XqdbError> {
        let fixed_width = (1..=19).contains(&k_type) && K_TYPE_SIZE[k_type as usize] > 0;
        if !(k_type == 0 || k_type == 11 || fixed_width) {
            return Err(XqdbError::NotSupportedKListErr(k_type));
        }
        Ok(Self {
            k_type,
            pos: start_pos,
            rows_left: None,
            child_type: None,
            child_symbols_left: 0,
        })
    }

    pub(crate) fn advance(
        &mut self,
        received: &[u8],
        total: usize,
    ) -> Result<ScanOutcome, XqdbError> {
        if self.rows_left.is_none() {
            let header_end = self.pos.saturating_add(5);
            if header_end > received.len() {
                return incomplete_or_overrun(header_end, total);
            }
            let rows = take_list_length(received, &mut self.pos, "q list")?;
            if self.k_type == 0 || self.k_type == 11 {
                self.rows_left = Some(rows);
            } else {
                let payload_length = rows
                    .checked_mul(K_TYPE_SIZE[self.k_type as usize])
                    .ok_or_else(|| {
                        XqdbError::DeserializationErr(
                            "q typed-list payload length overflowed".to_string(),
                        )
                    })?;
                self.pos = self.pos.checked_add(payload_length).ok_or_else(|| {
                    XqdbError::DeserializationErr(
                        "q typed-list payload offset overflowed".to_string(),
                    )
                })?;
                if self.pos > total {
                    return Err(XqdbError::DeserializationErr(format!(
                        "q typed-list payload length {payload_length} exceeds the available bytes"
                    )));
                }
                self.rows_left = Some(0);
            }
        }
        match self.k_type {
            11 => {
                let mut rows_left = self.rows_left.unwrap_or(0);
                let outcome = scan_symbols(
                    received,
                    total,
                    &mut self.pos,
                    &mut rows_left,
                    "q symbol-list value",
                )?;
                self.rows_left = Some(rows_left);
                Ok(outcome)
            }
            0 => self.advance_nested(received, total),
            _ => Ok(if self.pos <= received.len() {
                ScanOutcome::Complete(self.pos)
            } else {
                ScanOutcome::Incomplete
            }),
        }
    }

    fn advance_nested(&mut self, received: &[u8], total: usize) -> Result<ScanOutcome, XqdbError> {
        let mut rows_left = self.rows_left.unwrap_or(0);
        while rows_left > 0 {
            if self.child_symbols_left > 0 {
                let outcome = scan_symbols(
                    received,
                    total,
                    &mut self.pos,
                    &mut self.child_symbols_left,
                    "q nested symbol",
                )?;
                if matches!(outcome, ScanOutcome::Incomplete) {
                    self.rows_left = Some(rows_left);
                    return Ok(ScanOutcome::Incomplete);
                }
                rows_left -= 1;
                continue;
            }
            let header_end = self.pos.saturating_add(6);
            if header_end > received.len() {
                self.rows_left = Some(rows_left);
                return incomplete_or_overrun(header_end, total);
            }
            let current_k_type = received[self.pos];
            let child_type = match self.child_type {
                Some(child_type) => child_type,
                None => {
                    if !matches!(current_k_type, 1 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12) {
                        return Err(XqdbError::NotSupportedKNestedListErr(current_k_type));
                    }
                    self.child_type = Some(current_k_type);
                    current_k_type
                }
            };
            if current_k_type != child_type && current_k_type != 0 {
                return Err(XqdbError::NotSupportedKMixedListErr(
                    child_type,
                    current_k_type,
                ));
            }
            self.pos += 1;
            let sub_length = take_list_length(received, &mut self.pos, "q nested-list element")?;
            if current_k_type == 0 {
                if sub_length != 0 {
                    return Err(XqdbError::NotSupportedKMixedListErr(
                        child_type,
                        current_k_type,
                    ));
                }
                rows_left -= 1;
                continue;
            }
            if current_k_type == 11 {
                if sub_length == 0 {
                    rows_left -= 1;
                } else {
                    self.child_symbols_left = sub_length;
                }
                continue;
            }
            let payload_length = sub_length
                .checked_mul(K_TYPE_SIZE[current_k_type as usize])
                .ok_or_else(|| {
                    XqdbError::DeserializationErr(
                        "q nested-list payload length overflowed".to_string(),
                    )
                })?;
            let end = self.pos.checked_add(payload_length).ok_or_else(|| {
                XqdbError::DeserializationErr("q nested-list payload offset overflowed".to_string())
            })?;
            if end > total {
                return Err(XqdbError::DeserializationErr(format!(
                    "q nested-list payload length {payload_length} exceeds the available bytes"
                )));
            }
            self.pos = end;
            rows_left -= 1;
            if end > received.len() {
                self.rows_left = Some(rows_left);
                return Ok(ScanOutcome::Incomplete);
            }
        }
        self.rows_left = Some(0);
        Ok(if self.pos <= received.len() {
            ScanOutcome::Complete(self.pos)
        } else {
            ScanOutcome::Incomplete
        })
    }
}

/// Advances `pos` past `rows_left` NUL-terminated symbols, counting each one off, and stops with
/// `Incomplete` when the next terminator has not arrived yet.
fn scan_symbols(
    received: &[u8],
    total: usize,
    pos: &mut usize,
    rows_left: &mut usize,
    context: &str,
) -> Result<ScanOutcome, XqdbError> {
    while *rows_left > 0 {
        match memchr::memchr(0, &received[*pos..]) {
            Some(terminator) => {
                *pos += terminator + 1;
                *rows_left -= 1;
            }
            None if received.len() < total => return Ok(ScanOutcome::Incomplete),
            None => {
                return Err(XqdbError::DeserializationErr(format!(
                    "{context} is not NUL-terminated"
                )))
            }
        }
    }
    Ok(ScanOutcome::Complete(*pos))
}

fn incomplete_or_overrun(needed: usize, total: usize) -> Result<ScanOutcome, XqdbError> {
    if needed > total {
        Err(XqdbError::DeserializationErr(
            "q list header extends beyond its frame".to_string(),
        ))
    } else {
        Ok(ScanOutcome::Incomplete)
    }
}

pub(crate) fn deserialize_series(
    vec: &[u8],
    k_type: u8,
    as_column: bool,
    encoding: SymbolEncoding,
) -> Result<K, XqdbError> {
    if k_type as usize >= K_TYPE_SIZE.len() {
        return Err(XqdbError::NotSupportedKListErr(k_type));
    }
    let mut pos = 0;
    let length = take_list_length(vec, &mut pos, "q list")?;
    if length == 0 {
        if pos != vec.len() {
            return Err(XqdbError::DeserializationErr(format!(
                "empty q list has {} trailing byte(s)",
                vec.len() - pos
            )));
        }
        return if k_type == 10 && !as_column {
            Ok(K::CharVector(Vec::new()))
        } else {
            new_empty_series(k_type)
        };
    }

    let array_vec = vec.get(pos..).ok_or_else(|| {
        XqdbError::DeserializationErr("q list payload starts beyond its frame".to_string())
    })?;
    let name = K_TYPE_NAME[k_type as usize];
    if k_type != 0 && k_type != 11 {
        let expected_length = length
            .checked_mul(K_TYPE_SIZE[k_type as usize])
            .ok_or_else(|| {
                XqdbError::DeserializationErr("q list payload length overflowed".to_string())
            })?;
        if array_vec.len() != expected_length {
            return Err(XqdbError::DeserializationErr(format!(
                "q list declares {length} values but has {} payload byte(s), expected {expected_length}",
                array_vec.len()
            )));
        }
    }

    let array_box: Box<dyn Array>;
    let series: Series;
    match k_type {
        0 => deserialize_nested_array(vec, encoding),
        1 => {
            if let Some(value) = array_vec.iter().find(|value| **value > 1) {
                return Err(XqdbError::DeserializationErr(format!(
                    "q boolean list values must be 0 or 1, got {value}"
                )));
            }
            let values = try_bitmap(
                array_vec.iter().map(|value| *value == 1),
                "q boolean list values",
            )?;
            array_box = BooleanArray::new(ArrowDataType::Boolean, values, None).boxed();
            series = Series::from_arrow(name.into(), array_box)
                .map_err(|error| XqdbError::DeserializationErr(error.to_string()))?;
            Ok(K::Series(series))
        }
        2 => {
            array_box = FixedSizeBinaryArray::new(
                ArrowDataType::FixedSizeBinary(16),
                Buffer::from(try_copy_payload(array_vec, "q guid list")?),
                None,
            )
            .boxed();
            series = Series::from_arrow(name.into(), array_box)
                .map_err(|error| XqdbError::DeserializationErr(error.to_string()))?;
            Ok(K::Series(series))
        }
        4 => {
            array_box = UInt8Array::from_vec(try_copy_payload(array_vec, "q byte list")?).boxed();
            series = Series::from_arrow(name.into(), array_box)
                .map_err(|error| XqdbError::DeserializationErr(error.to_string()))?;
            Ok(K::Series(series))
        }
        5 => {
            let values = decode_wire_numbers::<i16>(array_vec, "q short list")?;
            let validity = optional_validity(
                values
                    .iter()
                    .map(|value| *value > i16::MIN + 1 && *value < i16::MAX),
                "q short list validity",
            )?;
            let mut array = Int16Array::from_vec(values);
            array.set_validity(validity);
            series = Series::from_arrow(name.into(), array.boxed())
                .map_err(|error| XqdbError::DeserializationErr(error.to_string()))?;
            Ok(K::Series(series))
        }
        6 => {
            let values = decode_wire_numbers::<i32>(array_vec, "q int list")?;
            let validity = optional_validity(
                values
                    .iter()
                    .map(|value| *value > i32::MIN + 1 && *value < i32::MAX),
                "q int list validity",
            )?;
            let mut array = Int32Array::from_vec(values);
            array.set_validity(validity);
            series = Series::from_arrow(name.into(), array.boxed())
                .map_err(|error| XqdbError::DeserializationErr(error.to_string()))?;
            Ok(K::Series(series))
        }
        7 => {
            let values = decode_wire_numbers::<i64>(array_vec, "q long list")?;
            let validity = optional_validity(
                values
                    .iter()
                    .map(|value| *value > i64::MIN + 1 && *value < i64::MAX),
                "q long list validity",
            )?;
            let mut array = Int64Array::from_vec(values);
            array.set_validity(validity);
            series = Series::from_arrow(name.into(), array.boxed())
                .map_err(|error| XqdbError::DeserializationErr(error.to_string()))?;
            Ok(K::Series(series))
        }
        8 => {
            let values = decode_wire_numbers::<f32>(array_vec, "q real list")?;
            let validity = optional_validity(
                values.iter().map(|value| !value.is_nan()),
                "q real list validity",
            )?;
            let mut array = Float32Array::from_vec(values);
            array.set_validity(validity);
            series = Series::from_arrow(name.into(), array.boxed())
                .map_err(|error| XqdbError::DeserializationErr(error.to_string()))?;
            Ok(K::Series(series))
        }
        9 => {
            let values = decode_wire_numbers::<f64>(array_vec, "q float list")?;
            let validity = optional_validity(
                values.iter().map(|value| !value.is_nan()),
                "q float list validity",
            )?;
            let mut array = Float64Array::from_vec(values);
            array.set_validity(validity);
            series = Series::from_arrow(name.into(), array.boxed())
                .map_err(|error| XqdbError::DeserializationErr(error.to_string()))?;
            Ok(K::Series(series))
        }
        10 => {
            if as_column {
                let mut views: Vec<View> =
                    try_vec_with_capacity(array_vec.len(), "q char column views")?;
                let total_bytes_len = if array_vec.is_ascii() {
                    views.extend(
                        array_vec
                            .iter()
                            .map(|byte| View::new_inline(std::slice::from_ref(byte))),
                    );
                    array_vec.len()
                } else if encoding == SymbolEncoding::Lossy {
                    // A char column holds one byte per row, so a byte outside ASCII is an
                    // invalid sequence on its own and every such row becomes U+FFFD.
                    views.extend(array_vec.iter().map(|byte| {
                        if byte.is_ascii() {
                            View::new_inline(std::slice::from_ref(byte))
                        } else {
                            View::new_inline(REPLACEMENT_CHARACTER.as_bytes())
                        }
                    }));
                    let replaced = array_vec.iter().filter(|byte| !byte.is_ascii()).count();
                    array_vec.len() + replaced * (REPLACEMENT_CHARACTER.len() - 1)
                } else {
                    return Err(XqdbError::DeserializationErr(
                        "q char columns require valid one-byte UTF-8 characters".to_string(),
                    ));
                };
                // SAFETY: every view is inline (a single ASCII byte or the three-byte U+FFFD,
                // well below the 12-byte inline limit) and references no data buffers.
                let array = unsafe {
                    Utf8ViewArray::new_unchecked(
                        ArrowDataType::Utf8View,
                        views.into(),
                        Buffer::default(),
                        None,
                        Some(total_bytes_len),
                        0,
                    )
                };
                series = Series::from_arrow(name.into(), array.boxed())
                    .map_err(|error| XqdbError::DeserializationErr(error.to_string()))?;
                Ok(K::Series(series))
            } else {
                Ok(K::CharVector(try_copy_payload(array_vec, "q char vector")?))
            }
        }
        11 => {
            // NUL is a one-byte UTF-8 code point, so decoding the whole payload once proves every
            // NUL-delimited symbol is valid UTF-8; lossy transcoding replaces only invalid
            // subsequences, so every terminator survives into the transcoded bytes.
            let text = decode_utf8(array_vec, encoding, "q symbol-list value")?;
            let array_vec = text.as_bytes();
            if as_column {
                return decode_symbol_column(array_vec, length, name);
            }
            let mut values = try_vec_with_capacity::<u8>(
                array_vec.len().saturating_sub(length),
                "q symbol-list values",
            )?;
            let mut offsets = try_vec_with_capacity::<i64>(length + 1, "q symbol-list offsets")?;
            offsets.push(0i64);
            let mut cursor = 0usize;
            for _ in 0..length {
                let terminator = memchr::memchr(0, &array_vec[cursor..]).ok_or_else(|| {
                    XqdbError::DeserializationErr(
                        "q symbol-list value is not NUL-terminated".to_string(),
                    )
                })?;
                values.extend_from_slice(&array_vec[cursor..cursor + terminator]);
                offsets.push(i64::try_from(values.len()).map_err(|_| {
                    XqdbError::DeserializationErr("q symbol-list offsets overflowed".to_string())
                })?);
                cursor += terminator + 1;
            }
            check_symbol_list_consumed(array_vec, cursor)?;
            // SAFETY: offsets start at zero and only ever grow by the pushed slice lengths,
            // so they are monotonically non-decreasing and bounded by values.len().
            let offsets = unsafe { OffsetsBuffer::new_unchecked(Buffer::from(offsets)) };
            array_box = Utf8Array::<i64>::new(
                ArrowDataType::LargeUtf8,
                offsets,
                Buffer::from(values),
                None,
            )
            .boxed();
            series = Series::from_arrow(name.into(), array_box)
                .map_err(|error| XqdbError::DeserializationErr(error.to_string()))?;
            Ok(K::Series(series))
        }
        12 => {
            let mut values = decode_wire_numbers::<i64>(array_vec, "q timestamp list")?;
            for value in values.iter_mut() {
                if *value != i64::MIN {
                    *value = unix_timestamp_nanoseconds(*value)?;
                }
            }
            let validity = optional_validity(
                values.iter().map(|value| *value != i64::MIN),
                "q timestamp list validity",
            )?;
            let array = PrimitiveArray::new(
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                values.into(),
                validity,
            );
            series = Series::from_arrow(name.into(), array.boxed())
                .map_err(|error| XqdbError::DeserializationErr(error.to_string()))?;
            Ok(K::Series(series))
        }
        14 => {
            let mut values = decode_wire_numbers::<i32>(array_vec, "q date list")?;
            let validity = optional_validity(
                values.iter().map(|value| *value != i32::MIN),
                "q date list validity",
            )?;
            for value in values.iter_mut() {
                if *value != i32::MIN {
                    *value = value.checked_add(10_957).ok_or_else(|| {
                        XqdbError::DeserializationErr(
                            "q date is outside Polars' representable range".to_string(),
                        )
                    })?;
                }
            }
            let array = PrimitiveArray::new(ArrowDataType::Date32, values.into(), validity);
            series = Series::from_arrow(name.into(), array.boxed())
                .map_err(|error| XqdbError::DeserializationErr(error.to_string()))?;
            Ok(K::Series(series))
        }
        15 => {
            let q_values = decode_wire_numbers::<f64>(array_vec, "q datetime list")?;
            let values = q_values.into_iter().map(|value| {
                if value.is_nan() {
                    Ok(i64::MIN)
                } else if !value.is_finite() {
                    Err(XqdbError::DeserializationErr(
                        "q datetime infinity cannot be represented as a native timestamp"
                            .to_string(),
                    ))
                } else {
                    let q_milliseconds =
                        finite_f64_to_i64((value * MS_PER_DAY).round(), "finite q datetime")?;
                    q_milliseconds
                        .checked_mul(1_000_000)
                        .and_then(|value| value.checked_add(NANOS_DIFF))
                        .ok_or_else(|| {
                            XqdbError::DeserializationErr(
                                "finite q datetime is outside the Unix nanosecond range"
                                    .to_string(),
                            )
                        })
                }
            });
            let values = try_collect(values, "q datetime list")?;
            let validity = optional_validity(
                values.iter().map(|value| *value != i64::MIN),
                "q datetime list validity",
            )?;
            let array = PrimitiveArray::new(
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                values.into(),
                validity,
            );
            series = Series::from_arrow(name.into(), array.boxed())
                .map_err(|error| XqdbError::DeserializationErr(error.to_string()))?;
            Ok(K::Series(series))
        }
        16 => {
            let values = decode_wire_numbers::<i64>(array_vec, "q timespan list")?;
            let validity = optional_validity(
                values.iter().map(|value| *value != i64::MIN),
                "q timespan list validity",
            )?;
            let array = PrimitiveArray::new(
                ArrowDataType::Duration(TimeUnit::Nanosecond),
                values.into(),
                validity,
            );
            series = Series::from_arrow(name.into(), array.boxed())
                .map_err(|error| XqdbError::DeserializationErr(error.to_string()))?;
            Ok(K::Series(series))
        }
        17..=19 => {
            let q_values = decode_wire_numbers::<i32>(array_vec, "q time list")?;
            let validity = optional_validity(
                q_values.iter().map(|value| *value != i32::MIN),
                "q time list validity",
            )?;
            let multiplier = match k_type {
                17 => 60_000_000_000,
                18 => 1_000_000_000,
                19 => 1_000_000,
                _ => unreachable!(),
            };
            let values = q_values.into_iter().map(|value| {
                if value == i32::MIN {
                    Ok(0)
                } else {
                    i64::from(value)
                        .checked_mul(multiplier)
                        .filter(|value| (0..NANOS_PER_DAY).contains(value))
                        .ok_or_else(|| {
                            XqdbError::DeserializationErr(
                                "q time-list value exceeds one day".to_string(),
                            )
                        })
                }
            });
            let values = try_collect(values, "q time list")?;
            let array = PrimitiveArray::new(
                ArrowDataType::Time64(TimeUnit::Nanosecond),
                values.into(),
                validity,
            );
            series = Series::from_arrow(name.into(), array.boxed())
                .map_err(|error| XqdbError::DeserializationErr(error.to_string()))?;
            Ok(K::Series(series))
        }
        _ => Err(XqdbError::NotSupportedKListErr(k_type)),
    }
}

fn new_empty_series(k_type: u8) -> Result<K, XqdbError> {
    let name = K_TYPE_NAME[k_type as usize];
    let series = match k_type {
        0 => Series::new_empty(name.into(), &PolarsDataType::Null),
        1 => Series::new_empty(name.into(), &PolarsDataType::Boolean),
        2 => Series::new_empty(name.into(), &PolarsDataType::Binary),
        4 | 10 => Series::new_empty(name.into(), &PolarsDataType::String),
        5 => Series::new_empty(name.into(), &PolarsDataType::Int16),
        6 => Series::new_empty(name.into(), &PolarsDataType::Int32),
        7 => Series::new_empty(name.into(), &PolarsDataType::Int64),
        8 => Series::new_empty(name.into(), &PolarsDataType::Float32),
        9 => Series::new_empty(name.into(), &PolarsDataType::Float64),
        11 => Series::new_empty(
            name.into(),
            &PolarsDataType::Categorical(Categories::global(), Categories::global().mapping()),
        ),
        12 | 15 => Series::new_empty(
            name.into(),
            &PolarsDataType::Datetime(PolarTimeUnit::Nanoseconds, None),
        ),
        14 => Series::new_empty(name.into(), &PolarsDataType::Date),
        16 => Series::new_empty(
            name.into(),
            &PolarsDataType::Duration(PolarTimeUnit::Nanoseconds),
        ),
        17..=19 => Series::new_empty(name.into(), &PolarsDataType::Time),
        _ => return Err(XqdbError::NotSupportedKListErr(k_type)),
    };
    Ok(K::Series(series))
}

/// Checks that every row of a nested char list is valid UTF-8. A row is valid exactly when the
/// concatenated payload is valid and every row boundary lands on a code-point start, because
/// UTF-8 is self-synchronizing, so one validation covers every row.
fn validate_nested_char(values: &[u8], boundaries: &[i32]) -> Result<(), XqdbError> {
    std::str::from_utf8(values).map_err(|error| {
        XqdbError::DeserializationErr(format!("q nested char value is not valid UTF-8: {error}"))
    })?;
    for window in boundaries.windows(2) {
        let start = window[0] as usize;
        if start < values.len() && values[start] & 0b1100_0000 == 0b1000_0000 {
            return Err(XqdbError::DeserializationErr(
                "q nested char value is not valid UTF-8: row boundary splits a code point"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

/// Transcodes each row of a nested char list on its own, so a code point split across two rows
/// becomes a replacement character in both instead of validating as a whole.
fn transcode_nested_char_lossy(
    values: &[u8],
    boundaries: &[i32],
) -> Result<(Vec<u8>, Vec<i32>), XqdbError> {
    let mut transcoded = try_vec_with_capacity::<u8>(values.len(), "q nested char values")?;
    let mut offsets = try_vec_with_capacity::<i32>(boundaries.len(), "q nested char offsets")?;
    offsets.push(0i32);
    for window in boundaries.windows(2) {
        let row = &values[window[0] as usize..window[1] as usize];
        let row = decode_utf8(row, SymbolEncoding::Lossy, "q nested char value")?;
        try_extend(&mut transcoded, row.as_bytes(), "q nested char values")?;
        offsets.push(i32::try_from(transcoded.len()).map_err(|_| {
            XqdbError::DeserializationErr(
                "q nested char values exceed i32::MAX bytes after transcoding".to_string(),
            )
        })?);
    }
    Ok((transcoded, offsets))
}

/// Decodes a nested char list into a string-view column in one pass over the rows, validating
/// each row's UTF-8 as it is copied so no offsets or second boundary pass are needed. Returns
/// `None` on the first row that is not valid UTF-8, leaving the strict error or lossy transcoding
/// to the general decoder.
fn decode_char_rows(
    vec: &[u8],
    mut pos: usize,
    length: usize,
    payload_capacity: usize,
) -> Result<Option<Utf8ViewArray>, XqdbError> {
    let mut views = try_vec_with_capacity::<View>(length, "q nested char views")?;
    let mut values = try_vec_with_capacity::<u8>(payload_capacity, "q nested-list values")?;
    for _ in 0..length {
        let current_k_type = *vec.get(pos).ok_or_else(|| {
            XqdbError::DeserializationErr("q nested list omitted an element type".to_string())
        })?;
        pos += 1;
        let sub_length = take_list_length(vec, &mut pos, "q nested-list element")?;
        let payload: &[u8] = match current_k_type {
            0 if sub_length == 0 => &[],
            10 => {
                let end = pos.checked_add(sub_length).ok_or_else(|| {
                    XqdbError::DeserializationErr(
                        "q nested-list payload offset overflowed".to_string(),
                    )
                })?;
                let payload = vec.get(pos..end).ok_or_else(|| {
                    XqdbError::DeserializationErr(format!(
                        "q nested-list payload length {sub_length} exceeds the available bytes"
                    ))
                })?;
                pos = end;
                payload
            }
            _ => return Err(XqdbError::NotSupportedKMixedListErr(10, current_k_type)),
        };
        if !payload.is_ascii() && std::str::from_utf8(payload).is_err() {
            return Ok(None);
        }
        let offset = u32::try_from(values.len()).map_err(|_| {
            XqdbError::DeserializationErr(
                "q nested char values exceed the view buffer limit".to_string(),
            )
        })?;
        views.push(View::new_from_bytes(payload, 0, offset));
        try_extend(&mut values, payload, "q nested-list values")?;
    }
    if pos != vec.len() {
        return Err(XqdbError::DeserializationErr(format!(
            "q nested list has {} trailing byte(s)",
            vec.len() - pos
        )));
    }
    let total_bytes_len = values.len();
    // SAFETY: every view points into the single data buffer registered below at the offset the
    // row's bytes were appended, and every row was validated as UTF-8 on its own.
    let array = unsafe {
        Utf8ViewArray::new_unchecked(
            ArrowDataType::Utf8View,
            views.into(),
            Buffer::from(vec![Buffer::from(values)]),
            None,
            Some(total_bytes_len),
            total_bytes_len,
        )
    };
    Ok(Some(array))
}

fn deserialize_nested_array(vec: &[u8], encoding: SymbolEncoding) -> Result<K, XqdbError> {
    let mut pos = 0;
    let length = take_list_length(vec, &mut pos, "q nested list")?;
    if length == 0 {
        if pos != vec.len() {
            return Err(XqdbError::DeserializationErr(format!(
                "empty q nested list has {} trailing byte(s)",
                vec.len() - pos
            )));
        }
        return new_empty_series(0);
    }

    let k_type = *vec.get(pos).ok_or_else(|| {
        XqdbError::DeserializationErr("q nested list omitted its child type".to_string())
    })?;
    if !matches!(k_type, 1 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12) {
        return Err(XqdbError::NotSupportedKNestedListErr(k_type));
    }
    let name = K_TYPE_NAME[k_type as usize];
    // Every child has a six-byte list header, including empty mixed-list children.
    // Fixed-width payloads can therefore reserve exactly, without geometric slack that
    // would remain retained when the allocation becomes the final Arrow buffer.
    let payload_capacity = if k_type == 11 {
        0
    } else {
        length
            .checked_mul(6)
            .and_then(|headers| (vec.len() - pos).checked_sub(headers))
            .ok_or_else(|| {
                XqdbError::DeserializationErr(
                    "q nested list is too short for its child headers".to_string(),
                )
            })?
    };
    if k_type == 10 {
        if let Some(array) = decode_char_rows(vec, pos, length, payload_capacity)? {
            return Series::from_arrow(name.into(), array.boxed())
                .map(K::Series)
                .map_err(|error| XqdbError::DeserializationErr(error.to_string()));
        }
    }
    let mut offsets = try_vec_with_capacity::<i32>(length + 1, "q nested-list offsets")?;
    offsets.push(0i32);
    let mut values = try_vec_with_capacity::<u8>(payload_capacity, "q nested-list values")?;
    let mut symbol_offsets = if k_type == 11 { Some(vec![0i64]) } else { None };

    for _ in 0..length {
        let current_k_type = *vec.get(pos).ok_or_else(|| {
            XqdbError::DeserializationErr("q nested list omitted an element type".to_string())
        })?;
        pos += 1;
        let sub_length = take_list_length(vec, &mut pos, "q nested-list element")?;
        if current_k_type == 0 {
            if sub_length != 0 {
                return Err(XqdbError::NotSupportedKMixedListErr(k_type, current_k_type));
            }
        } else if current_k_type != k_type {
            return Err(XqdbError::NotSupportedKMixedListErr(k_type, current_k_type));
        }

        let current_offset = *offsets.last().expect("offsets starts with zero");
        let next_offset = current_offset
            .checked_add(i32::try_from(sub_length).map_err(|_| {
                XqdbError::DeserializationErr(
                    "q nested-list child count exceeds i32::MAX".to_string(),
                )
            })?)
            .ok_or_else(|| {
                XqdbError::DeserializationErr(
                    "q nested-list cumulative child count overflowed".to_string(),
                )
            })?;
        offsets.push(next_offset);

        if current_k_type == 0 {
            continue;
        }
        if k_type == 11 {
            let child_offsets = symbol_offsets
                .as_mut()
                .expect("symbol offsets exist for symbol children");
            for _ in 0..sub_length {
                let symbol = take_utf8_until_nul(vec, &mut pos, encoding, "q nested symbol")?;
                try_extend(&mut values, symbol.as_bytes(), "q nested-symbol values")?;
                let offset = i64::try_from(values.len()).map_err(|_| {
                    XqdbError::DeserializationErr("q nested-symbol offsets overflowed".to_string())
                })?;
                try_push(child_offsets, offset, "q nested-symbol offsets")?;
            }
        } else {
            let payload_length = sub_length
                .checked_mul(K_TYPE_SIZE[k_type as usize])
                .ok_or_else(|| {
                    XqdbError::DeserializationErr(
                        "q nested-list payload length overflowed".to_string(),
                    )
                })?;
            let end = pos.checked_add(payload_length).ok_or_else(|| {
                XqdbError::DeserializationErr("q nested-list payload offset overflowed".to_string())
            })?;
            let payload = vec.get(pos..end).ok_or_else(|| {
                XqdbError::DeserializationErr(format!(
                    "q nested-list payload length {payload_length} exceeds the available bytes"
                ))
            })?;
            try_extend(&mut values, payload, "q nested-list values")?;
            pos = end;
        }
    }
    if pos != vec.len() {
        return Err(XqdbError::DeserializationErr(format!(
            "q nested list has {} trailing byte(s)",
            vec.len() - pos
        )));
    }

    let field = create_field(k_type, name)?;
    // SAFETY: outer offsets start at zero and grow by checked additions only, so they are
    // monotonically non-decreasing.
    let outer_offsets = unsafe { OffsetsBuffer::<i32>::new_unchecked(Buffer::from(offsets)) };
    let array_box: Box<dyn Array> = match k_type {
        1 => {
            if let Some(value) = values.iter().find(|value| **value > 1) {
                return Err(XqdbError::DeserializationErr(format!(
                    "q nested boolean values must be 0 or 1, got {value}"
                )));
            }
            let bits = try_bitmap(
                values.iter().map(|value| *value == 1),
                "q nested boolean values",
            )?;
            BooleanArray::new(ArrowDataType::Boolean, bits, None).boxed()
        }
        4 => UInt8Array::from_vec(values).boxed(),
        5 => {
            let decoded = decode_owned_wire_numbers::<i16>(values, "q nested short list")?;
            let validity = optional_validity(
                decoded.iter().map(|value| *value != i16::MIN),
                "q nested short list validity",
            )?;
            Int16Array::new(ArrowDataType::Int16, decoded, validity).boxed()
        }
        6 => {
            let decoded = decode_owned_wire_numbers::<i32>(values, "q nested int list")?;
            let validity = optional_validity(
                decoded.iter().map(|value| *value != i32::MIN),
                "q nested int list validity",
            )?;
            Int32Array::new(ArrowDataType::Int32, decoded, validity).boxed()
        }
        7 => {
            let decoded = decode_owned_wire_numbers::<i64>(values, "q nested long list")?;
            let validity = optional_validity(
                decoded.iter().map(|value| *value != i64::MIN),
                "q nested long list validity",
            )?;
            Int64Array::new(ArrowDataType::Int64, decoded, validity).boxed()
        }
        8 => {
            let decoded = decode_owned_wire_numbers::<f32>(values, "q nested real list")?;
            let validity = optional_validity(
                decoded.iter().map(|value| !value.is_nan()),
                "q nested real list validity",
            )?;
            Float32Array::new(ArrowDataType::Float32, decoded, validity).boxed()
        }
        9 => {
            let decoded = decode_owned_wire_numbers::<f64>(values, "q nested float list")?;
            let validity = optional_validity(
                decoded.iter().map(|value| !value.is_nan()),
                "q nested float list validity",
            )?;
            Float64Array::new(ArrowDataType::Float64, decoded, validity).boxed()
        }
        10 => {
            let boundaries = outer_offsets.as_ref();
            let (values, boundaries) = match validate_nested_char(&values, boundaries) {
                Ok(()) => (values, Cow::Borrowed(boundaries)),
                Err(error) if encoding == SymbolEncoding::Strict => return Err(error),
                Err(_) => {
                    let (values, boundaries) = transcode_nested_char_lossy(&values, boundaries)?;
                    (values, Cow::Owned(boundaries))
                }
            };
            let mut views =
                try_vec_with_capacity::<View>(outer_offsets.len_proxy(), "q nested char views")?;
            for window in boundaries.windows(2) {
                let (start, end) = (window[0] as usize, window[1] as usize);
                views.push(View::new_from_bytes(&values[start..end], 0, start as u32));
            }
            let total_bytes_len = values.len();
            // SAFETY: views were derived from monotonic in-range offsets over the single
            // data buffer registered below, and every row was either validated as UTF-8 with
            // its boundary on a code-point start or transcoded to valid UTF-8 on its own.
            unsafe {
                Utf8ViewArray::new_unchecked(
                    ArrowDataType::Utf8View,
                    views.into(),
                    Buffer::from(vec![Buffer::from(values)]),
                    None,
                    Some(total_bytes_len),
                    total_bytes_len,
                )
            }
            .boxed()
        }
        11 => {
            let symbol_offsets = symbol_offsets.expect("symbol offsets exist for symbol children");
            // SAFETY: symbol offsets start at zero and only grow by pushed slice lengths.
            let symbol_offsets =
                unsafe { OffsetsBuffer::<i64>::new_unchecked(Buffer::from(symbol_offsets)) };
            Utf8Array::<i64>::new(
                ArrowDataType::LargeUtf8,
                symbol_offsets,
                Buffer::from(values),
                None,
            )
            .boxed()
        }
        12 => {
            let mut decoded = decode_owned_wire_numbers::<i64>(values, "q nested timestamp list")?;
            for value in decoded
                .get_mut_slice()
                .expect("decoded values exclusively own their storage")
            {
                if *value != i64::MIN {
                    *value = unix_timestamp_nanoseconds(*value)?;
                }
            }
            let validity = optional_validity(
                decoded.iter().map(|value| *value != i64::MIN),
                "q nested timestamp list validity",
            )?;
            PrimitiveArray::new(
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                decoded,
                validity,
            )
            .boxed()
        }
        _ => return Err(XqdbError::NotSupportedKNestedListErr(k_type)),
    };

    if k_type == 10 {
        return Series::from_arrow(name.into(), array_box)
            .map(K::Series)
            .map_err(|error| XqdbError::DeserializationErr(error.to_string()));
    }

    let list_array = ListArray::<i32>::new(
        ArrowDataType::List(Box::new(field)),
        outer_offsets,
        array_box,
        None,
    );
    let series = Series::from_arrow(name.into(), list_array.boxed())
        .map_err(|error| XqdbError::DeserializationErr(error.to_string()))?;
    if k_type == 11 {
        series
            .cast(&PolarsDataType::List(
                PolarsDataType::Categorical(Categories::global(), Categories::global().mapping())
                    .boxed(),
            ))
            .map(K::Series)
            .map_err(|error| XqdbError::DeserializationErr(error.to_string()))
    } else {
        Ok(K::Series(series))
    }
}

pub fn decompress(vec: &[u8], de_vec: &mut [u8], start_pos: usize) -> Result<(), XqdbError> {
    let decompressed_total_length = match start_pos {
        4 => {
            let prefix: [u8; 4] = vec
                .get(..4)
                .ok_or_else(|| {
                    XqdbError::DeserializationErr(
                        "compressed data is missing its 4-byte length prefix".to_string(),
                    )
                })?
                .try_into()
                .expect("four-byte compressed-data length prefix");
            u64::from(u32::from_le_bytes(prefix))
        }
        8 => {
            let prefix: [u8; 8] = vec
                .get(..8)
                .ok_or_else(|| {
                    XqdbError::DeserializationErr(
                        "compressed data is missing its 8-byte length prefix".to_string(),
                    )
                })?
                .try_into()
                .expect("eight-byte compressed-data length prefix");
            u64::from_le_bytes(prefix)
        }
        _ => {
            return Err(XqdbError::DeserializationErr(format!(
                "unsupported compressed-data prefix length {start_pos}"
            )))
        }
    };
    let expected_body_length = decompressed_total_length
        .checked_sub(8)
        .and_then(|length| usize::try_from(length).ok())
        .ok_or_else(|| {
            XqdbError::DeserializationErr(
                "invalid decompressed message length in compressed-data prefix".to_string(),
            )
        })?;
    if expected_body_length != de_vec.len() {
        return Err(XqdbError::DeserializationErr(format!(
            "decompressed body length mismatch: prefix requires {expected_body_length} bytes, destination has {}",
            de_vec.len()
        )));
    }

    const INVALID_OFFSET: usize = usize::MAX;
    let mut dictionary = [INVALID_OFFSET; 256];
    let mut compressed_position = start_pos;
    let mut output_position = 0usize;
    // The reference KX decoder hashes from byte zero of the uncompressed value body. Using an
    // invalid sentinel rather than zero keeps that first offset representable.
    let mut hash_position = 0usize;
    let mut flags = 0u8;
    let mut mask = 0u8;

    while output_position < de_vec.len() {
        if mask == 0 {
            flags = take_compressed_byte(vec, &mut compressed_position, "compression flags")?;
            mask = 1;
        }
        let is_backreference = flags & mask != 0;
        let previous_output_position = output_position;

        if is_backreference {
            let hash = take_compressed_byte(
                vec,
                &mut compressed_position,
                "compression backreference hash",
            )?;
            let extra_length = usize::from(take_compressed_byte(
                vec,
                &mut compressed_position,
                "compression backreference length",
            )?);
            let source = dictionary[usize::from(hash)];
            if source == INVALID_OFFSET {
                return Err(XqdbError::DeserializationErr(format!(
                    "compression backreference hash {hash} has not been initialized"
                )));
            }
            let source_pair_end = source.checked_add(2).ok_or_else(|| {
                XqdbError::DeserializationErr(
                    "compression backreference source span overflowed".to_string(),
                )
            })?;
            if source_pair_end > output_position {
                return Err(XqdbError::DeserializationErr(format!(
                    "compression backreference source {source} is not fully initialized at output offset {output_position}"
                )));
            }
            let run_length = extra_length + 2;
            let output_end = output_position.checked_add(run_length).ok_or_else(|| {
                XqdbError::DeserializationErr(
                    "compression backreference output span overflowed".to_string(),
                )
            })?;
            if output_end > de_vec.len() {
                return Err(XqdbError::DeserializationErr(format!(
                    "compression backreference writes through offset {output_end}, beyond the {}-byte output",
                    de_vec.len()
                )));
            }
            let source_end = source.checked_add(run_length).ok_or_else(|| {
                XqdbError::DeserializationErr(
                    "compression backreference source span overflowed".to_string(),
                )
            })?;
            if source_end > de_vec.len() {
                return Err(XqdbError::DeserializationErr(format!(
                    "compression backreference reads through offset {source_end}, beyond the {}-byte output",
                    de_vec.len()
                )));
            }

            if source_end <= output_position {
                de_vec.copy_within(source..source_end, output_position);
            } else {
                for index in 0..run_length {
                    de_vec[output_position + index] = de_vec[source + index];
                }
            }
            output_position = output_end;
        } else {
            de_vec[output_position] =
                take_compressed_byte(vec, &mut compressed_position, "compressed literal")?;
            output_position += 1;
        }

        // Match the KX reference update order: a backreference contributes its first pair to the
        // dictionary, then skips hashing the rest of its copied run.
        let hash_end = if is_backreference {
            previous_output_position.checked_add(1).ok_or_else(|| {
                XqdbError::DeserializationErr(
                    "compression dictionary position overflowed".to_string(),
                )
            })?
        } else {
            output_position.saturating_sub(1)
        };
        for position in hash_position..hash_end {
            let next = position.checked_add(1).ok_or_else(|| {
                XqdbError::DeserializationErr(
                    "compression dictionary source position overflowed".to_string(),
                )
            })?;
            let pair = de_vec.get(position..=next).ok_or_else(|| {
                XqdbError::DeserializationErr(
                    "compression dictionary source pair exceeds the output".to_string(),
                )
            })?;
            dictionary[usize::from(pair[0] ^ pair[1])] = position;
        }
        hash_position = if is_backreference {
            output_position
        } else {
            output_position.saturating_sub(1)
        };
        mask <<= 1;
    }

    if compressed_position != vec.len() {
        return Err(XqdbError::DeserializationErr(format!(
            "compressed data has {} trailing byte(s)",
            vec.len() - compressed_position
        )));
    }
    Ok(())
}

fn take_compressed_byte(
    bytes: &[u8],
    position: &mut usize,
    context: &str,
) -> Result<u8, XqdbError> {
    let value = *bytes.get(*position).ok_or_else(|| {
        XqdbError::DeserializationErr(format!("compressed data ends before its {context} byte"))
    })?;
    *position = position.checked_add(1).ok_or_else(|| {
        XqdbError::DeserializationErr("compressed-data position overflowed".to_string())
    })?;
    Ok(value)
}

/// Writes the decompressed-length prefix that follows a compressed frame's 8-byte header and
/// returns the offset where compressed bytes begin. q signals a 4-byte prefix with compression
/// mode 1 and an 8-byte prefix with mode 2, which is required once the length exceeds `u32::MAX`.
fn write_decompressed_length_prefix(header: &mut [u8], decompressed_length: usize) -> usize {
    if decompressed_length > u32::MAX as usize {
        header[2] = 2;
        header[8..16].copy_from_slice(&(decompressed_length as u64).to_le_bytes());
        16
    } else {
        header[2] = 1;
        header[8..12].copy_from_slice(&(decompressed_length as u32).to_le_bytes());
        12
    }
}

pub fn compress(vec: Vec<u8>) -> Result<Vec<u8>, XqdbError> {
    if vec.len() < 2000 {
        Ok(vec)
    } else {
        let scratch_length = vec.len() / 2;
        let mut c_vec = Vec::new();
        c_vec.try_reserve_exact(scratch_length).map_err(|error| {
            XqdbError::Err(format!(
                "Unable to allocate {scratch_length}-byte IPC compression buffer: {error}"
            ))
        })?;
        c_vec.resize(scratch_length, 0);
        // compressed bytes start position
        let mut c_pos: usize = write_decompressed_length_prefix(&mut c_vec, vec.len());
        let mut n_pos: usize = c_pos;
        let mut o_pos: usize = 8;
        let mut x = [0usize; 256];

        let mut px: u8 = 0;
        let mut n: u8 = 0;
        let mut p_pos: usize = 0;

        let mut i: u8 = 0;

        while o_pos < vec.len() {
            if i == 0 {
                if c_pos > c_vec.len() - 17 {
                    return Ok(vec);
                }
                i = 1;
                c_vec[n_pos] = n;
                n_pos = c_pos;
                c_pos += 1;
                n = 0;
            }
            let mut skip = vec.len() - o_pos < 3;
            let mut x_pos: usize = 0;
            let mut cx: u8 = 0;
            if !skip {
                cx = vec[o_pos] ^ vec[o_pos + 1];
                x_pos = x[cx as usize];
                skip = x_pos == 0 || vec[o_pos] != vec[x_pos];
            }

            if p_pos > 0 {
                x[px as usize] = p_pos;
                p_pos = 0;
            }

            if skip {
                px = cx;
                p_pos = o_pos;
                c_vec[c_pos] = vec[o_pos];
                c_pos += 1;
                o_pos += 1;
            } else {
                x[cx as usize] = o_pos;
                n |= i;
                x_pos += 2;
                o_pos += 2;
                let s = o_pos;
                let max_index = min(o_pos + 255, vec.len());
                while o_pos < max_index && vec[x_pos] == vec[o_pos] {
                    o_pos += 1;
                    x_pos += 1;
                }
                c_vec[c_pos] = cx;
                c_pos += 1;
                c_vec[c_pos] = (o_pos - s) as u8;
                c_pos += 1;
            }

            i <<= 1;
        }
        c_vec[n_pos] = n;
        c_vec[0] = vec[0];
        c_vec[1] = vec[1];
        let c_len = u32::to_le_bytes(c_pos as u32);
        c_vec[4..(4 + 4)].copy_from_slice(&c_len);
        c_vec[3] = (c_pos >> 32) as u8;
        c_vec.resize(c_pos, 0u8);
        Ok(c_vec)
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn serialize(k: &K) -> Result<Vec<u8>, XqdbError> {
    let k_length = k.j6_len()?;
    let mut vec = Vec::with_capacity(k_length);
    serialize_into(k, &mut vec)?;
    Ok(vec)
}

/// Callers must pre-validate `k` with [`K::j6_len`], which enforces the nesting-depth limit
/// and sizes the output buffer.
pub(crate) fn serialize_into(k: &K, vec: &mut Vec<u8>) -> Result<(), XqdbError> {
    match k {
        K::Boolean(k) => {
            vec.extend_from_slice(&[255, (*k as u8)]);
        }
        K::Guid(k) => {
            vec.push(254u8);
            vec.extend_from_slice(k.as_bytes());
        }
        K::U8(k) => {
            vec.extend_from_slice(&[252, *k]);
        }
        K::I16(k) => {
            vec.push(251);
            vec.extend_from_slice(&k.to_le_bytes());
        }
        K::I32(k) => {
            vec.push(250);
            vec.extend_from_slice(&k.to_le_bytes());
        }
        K::I64(k) => {
            vec.push(249);
            vec.extend_from_slice(&k.to_le_bytes());
        }
        K::F32(k) => {
            vec.push(248);
            vec.extend_from_slice(&k.to_le_bytes());
        }
        K::F64(k) => {
            vec.push(247);
            vec.extend_from_slice(&k.to_le_bytes());
        }
        K::Char(k) => {
            vec.extend_from_slice(&[246, *k]);
        }
        K::Symbol(k) => {
            validate_q_symbol(k)?;
            vec.push(245);
            vec.extend_from_slice(k.as_bytes());
            vec.push(0);
        }
        K::CharVector(k) => {
            let length = q_list_length(k.len())?;
            vec.extend_from_slice(&[10, 0]);
            vec.extend_from_slice(&length);
            vec.extend_from_slice(k);
        }
        K::String(k) => {
            let length = q_list_length(k.len())?;
            vec.extend_from_slice(&[10, 0]);
            vec.extend_from_slice(&length);
            vec.extend_from_slice(k.as_bytes());
        }
        // to timestamp
        K::DateTime(k) => {
            let unix_nanoseconds = k.timestamp_nanos_opt().ok_or_else(|| {
                XqdbError::NotAbleToSerializeErr(
                    "timestamp is outside q's representable nanosecond range".to_string(),
                )
            })?;
            let q_nanoseconds = q_timestamp_nanoseconds(unix_nanoseconds)?;
            vec.push(244);
            vec.extend_from_slice(&q_nanoseconds.to_le_bytes());
        }
        // to date
        K::Date(k) => {
            vec.push(242);
            let days = k.num_days_from_ce().checked_sub(DAY_DIFF).ok_or_else(|| {
                XqdbError::NotAbleToSerializeErr(
                    "date is outside q's representable range".to_string(),
                )
            })?;
            vec.extend_from_slice(&days.to_le_bytes());
        }
        // to time
        K::Time(k) => {
            vec.push(237);
            let milliseconds = k.num_seconds_from_midnight() * 1000 + k.nanosecond() / 1000000;
            vec.extend_from_slice(&(milliseconds as i32).to_le_bytes());
        }
        // to timespan
        K::Duration(k) => {
            vec.push(240);
            let ns = k.num_nanoseconds().ok_or_else(|| {
                XqdbError::NotAbleToSerializeErr(
                    "duration is outside q's representable nanosecond range".to_string(),
                )
            })?;
            vec.extend_from_slice(&ns.to_le_bytes());
        }
        K::MixedList(l) => {
            vec.extend_from_slice(&[0, 0]);
            vec.extend_from_slice(&q_list_length(l.len())?);
            for atom in l.iter() {
                serialize_into(atom, vec)?;
            }
        }
        // to list
        K::Series(k) => {
            serialize_series_into(k, vec)?;
        }
        // to table
        K::DataFrame(k) => {
            let column_names = k.get_column_names();
            let column_count =
                i32::try_from(column_names.len()).map_err(|_| XqdbError::OverLengthErr())?;
            vec.extend_from_slice(&[98, 0, 99, 11, 0]);
            vec.extend_from_slice(&column_count.to_le_bytes());
            for name in column_names {
                validate_q_symbol(name)?;
                vec.extend_from_slice(name.as_bytes());
                vec.push(0);
            }
            vec.extend_from_slice(&[0, 0]);
            vec.extend_from_slice(&column_count.to_le_bytes());
            for column in k.columns() {
                serialize_series_into(column.as_materialized_series(), vec)?;
            }
        }
        K::QValue(value) => {
            vec.extend_from_slice(value.as_bytes());
        }
        K::Operator(operator) => {
            let (k_type, opcode) = operator.wire_value();
            vec.extend_from_slice(&[k_type, opcode]);
        }
        K::Lambda(lambda) => {
            let source_length = q_list_length(lambda.source().len())?;
            vec.push(100);
            vec.extend_from_slice(lambda.context().as_bytes());
            vec.extend_from_slice(&[0, 10, 0]);
            vec.extend_from_slice(&source_length);
            vec.extend_from_slice(lambda.source().as_bytes());
        }
        // to (::)
        K::Null => {
            vec.extend_from_slice(&[101, 0]);
        }
        // An empty map serializes as (`symbol$())!(), the symbol-keyed form q itself
        // produces for emptied dictionaries and the form this decoder reads back as an
        // empty map.
        K::Dict(dict) => {
            let length = i32::try_from(dict.len()).map_err(|_| XqdbError::OverLengthErr())?;
            vec.extend_from_slice(&[99, 11, 0]);
            vec.extend_from_slice(&length.to_le_bytes());
            for key in dict.keys() {
                validate_q_symbol(key)?;
                vec.extend_from_slice(key.as_bytes());
                vec.push(0);
            }
            vec.extend_from_slice(&[0, 0]);
            vec.extend_from_slice(&length.to_le_bytes());
            for value in dict.values() {
                serialize_into(value, vec)?;
            }
        }
    };
    Ok(())
}

fn write_nested_wire_lists<T>(
    output: &mut Vec<u8>,
    values: &PrimitiveArray<T>,
    offsets: &[i64],
    k_type: u8,
    null_value: T,
) -> Result<(), XqdbError>
where
    T: WireNumber + polars_arrow::types::NativeType,
{
    let raw_values: &[T] = values.values().as_ref();
    let has_nulls = values.null_count() > 0;
    for range in offsets.windows(2) {
        let start = usize::try_from(range[0]).map_err(|_| {
            XqdbError::NotAbleToSerializeErr("nested-list offsets cannot be negative".to_string())
        })?;
        let end = usize::try_from(range[1]).map_err(|_| {
            XqdbError::NotAbleToSerializeErr("nested-list offsets cannot be negative".to_string())
        })?;
        let nested_values = raw_values.get(start..end).ok_or_else(|| {
            XqdbError::NotAbleToSerializeErr(
                "nested-list offsets exceed the child values".to_string(),
            )
        })?;
        output.extend_from_slice(&[k_type, 0]);
        output.extend_from_slice(&q_list_length(nested_values.len())?);
        if has_nulls {
            write_wire_numbers_from_iter(
                output,
                nested_values.iter().enumerate().map(|(offset, &value)| {
                    if values.is_null(start + offset) {
                        null_value
                    } else {
                        value
                    }
                }),
            );
        } else {
            write_wire_numbers(output, nested_values);
        }
    }
    Ok(())
}

fn serialize_series_into(series: &Series, vec: &mut Vec<u8>) -> Result<(), XqdbError> {
    let rechunked;
    let series = if series.n_chunks() > 1 {
        rechunked = series.rechunk();
        &rechunked
    } else {
        series
    };
    let k_length = series.len();
    if k_length > i32::MAX as usize {
        return Err(XqdbError::OverLengthErr());
    }
    let list_header = (k_length as i32).to_le_bytes();
    match series.dtype() {
        PolarsDataType::Boolean => {
            vec.extend_from_slice(&[1, 0]);
            vec.extend_from_slice(&list_header);
            let physical = series.to_physical_repr();
            let values = physical
                .bool()
                .map_err(|error| XqdbError::NotAbleToSerializeErr(error.to_string()))?;
            vec.reserve(k_length);
            vec.extend(values.iter().map(|value| value.unwrap_or(false) as u8));
        }
        PolarsDataType::UInt8 => {
            vec.extend_from_slice(&[4, 0]);
            vec.extend_from_slice(&list_header);
            let physical = series.to_physical_repr();
            let values = physical
                .u8()
                .map_err(|error| XqdbError::NotAbleToSerializeErr(error.to_string()))?;
            match values.cont_slice() {
                Ok(values) => vec.extend_from_slice(values),
                Err(_) => {
                    vec.reserve(k_length);
                    vec.extend(values.iter().map(|value| value.unwrap_or(0)));
                }
            }
        }
        PolarsDataType::Int16 => {
            vec.extend_from_slice(&[5, 0]);
            vec.extend_from_slice(&list_header);
            let values = series
                .i16()
                .map_err(|error| XqdbError::NotAbleToSerializeErr(error.to_string()))?;
            match values.cont_slice() {
                Ok(values) => write_wire_numbers(vec, values),
                Err(_) => write_wire_numbers_from_iter(
                    vec,
                    values.iter().map(|value| value.unwrap_or(i16::MIN)),
                ),
            }
        }
        PolarsDataType::Int32 => {
            vec.extend_from_slice(&[6, 0]);
            vec.extend_from_slice(&list_header);
            let values = series
                .i32()
                .map_err(|error| XqdbError::NotAbleToSerializeErr(error.to_string()))?;
            match values.cont_slice() {
                Ok(values) => write_wire_numbers(vec, values),
                Err(_) => write_wire_numbers_from_iter(
                    vec,
                    values.iter().map(|value| value.unwrap_or(i32::MIN)),
                ),
            }
        }
        PolarsDataType::Int64 => {
            vec.extend_from_slice(&[7, 0]);
            vec.extend_from_slice(&list_header);
            let values = series
                .i64()
                .map_err(|error| XqdbError::NotAbleToSerializeErr(error.to_string()))?;
            match values.cont_slice() {
                Ok(values) => write_wire_numbers(vec, values),
                Err(_) => write_wire_numbers_from_iter(
                    vec,
                    values.iter().map(|value| value.unwrap_or(i64::MIN)),
                ),
            }
        }
        PolarsDataType::Float32 => {
            vec.extend_from_slice(&[8, 0]);
            vec.extend_from_slice(&list_header);
            let values = series
                .f32()
                .map_err(|error| XqdbError::NotAbleToSerializeErr(error.to_string()))?;
            match values.cont_slice() {
                Ok(values) => write_wire_numbers(vec, values),
                Err(_) => write_wire_numbers_from_iter(
                    vec,
                    values.iter().map(|value| value.unwrap_or(f32::NAN)),
                ),
            }
        }
        PolarsDataType::Float64 => {
            vec.extend_from_slice(&[9, 0]);
            vec.extend_from_slice(&list_header);
            let values = series
                .f64()
                .map_err(|error| XqdbError::NotAbleToSerializeErr(error.to_string()))?;
            match values.cont_slice() {
                Ok(values) => write_wire_numbers(vec, values),
                Err(_) => write_wire_numbers_from_iter(
                    vec,
                    values.iter().map(|value| value.unwrap_or(f64::NAN)),
                ),
            }
        }
        PolarsDataType::String => {
            vec.extend_from_slice(&[0, 0]);
            vec.extend_from_slice(&list_header);
            let physical = series.to_physical_repr();
            let values = physical
                .str()
                .map_err(|error| XqdbError::NotAbleToSerializeErr(error.to_string()))?;
            for value in values.iter() {
                if let Some(value) = value {
                    let length = q_list_length(value.len())?;
                    vec.extend_from_slice(&[10, 0]);
                    vec.extend_from_slice(&length);
                    vec.extend_from_slice(value.as_bytes());
                } else {
                    vec.extend_from_slice(&[10, 0, 0, 0, 0, 0]);
                }
            }
        }
        PolarsDataType::Date => {
            vec.extend_from_slice(&[14, 0]);
            vec.extend_from_slice(&list_header);
            let physical = series
                .to_physical_repr()
                .i32()
                .map_err(|error| XqdbError::NotAbleToSerializeErr(error.to_string()))?
                .clone();
            try_write_wire_numbers_from_iter(
                vec,
                physical.iter().map(|value| match value {
                    None => Ok(i32::MIN),
                    Some(value) => value.checked_sub(10_957).ok_or_else(|| {
                        XqdbError::NotAbleToSerializeErr(
                            "date is outside q's representable range".to_string(),
                        )
                    }),
                }),
            )?;
        }
        PolarsDataType::Datetime(unit, _) => {
            let physical = series
                .to_physical_repr()
                .i64()
                .map_err(|error| XqdbError::NotAbleToSerializeErr(error.to_string()))?
                .clone();
            match unit {
                PolarTimeUnit::Milliseconds => {
                    vec.extend_from_slice(&[15, 0]);
                    vec.extend_from_slice(&list_header);
                    write_wire_numbers_from_iter(
                        vec,
                        physical.iter().map(|value| {
                            value
                                .map(|value| value as f64 / MS_PER_DAY - 10_957.0)
                                .unwrap_or(f64::NAN)
                        }),
                    );
                }
                PolarTimeUnit::Microseconds | PolarTimeUnit::Nanoseconds => {
                    vec.extend_from_slice(&[12, 0]);
                    vec.extend_from_slice(&list_header);
                    let multiplier = match unit {
                        PolarTimeUnit::Nanoseconds => 1,
                        PolarTimeUnit::Microseconds => 1_000,
                        PolarTimeUnit::Milliseconds => unreachable!(),
                    };
                    try_write_wire_numbers_from_iter(
                        vec,
                        physical.iter().map(|value| match value {
                            None => Ok(i64::MIN),
                            Some(value) => {
                                let unix_nanoseconds =
                                    value.checked_mul(multiplier).ok_or_else(|| {
                                        XqdbError::NotAbleToSerializeErr(
                                            "timestamp is outside q's representable nanosecond range"
                                                .to_string(),
                                        )
                                    })?;
                                q_timestamp_nanoseconds(unix_nanoseconds)
                            }
                        }),
                    )?;
                }
            }
        }
        PolarsDataType::Duration(unit) => {
            vec.extend_from_slice(&[16, 0]);
            vec.extend_from_slice(&list_header);
            let multiplier = match unit {
                PolarTimeUnit::Nanoseconds => 1,
                PolarTimeUnit::Microseconds => 1_000,
                PolarTimeUnit::Milliseconds => 1_000_000,
            };
            let physical = series
                .to_physical_repr()
                .i64()
                .map_err(|error| XqdbError::NotAbleToSerializeErr(error.to_string()))?
                .clone();
            try_write_wire_numbers_from_iter(
                vec,
                physical.iter().map(|value| match value {
                    None => Ok(i64::MIN),
                    Some(value) => value.checked_mul(multiplier).ok_or_else(|| {
                        XqdbError::NotAbleToSerializeErr(
                            "duration is outside q's representable nanosecond range".to_string(),
                        )
                    }),
                }),
            )?;
        }
        PolarsDataType::Time => {
            validate_q_time_series(series)?;
            vec.extend_from_slice(&[19, 0]);
            vec.extend_from_slice(&list_header);
            let physical = series
                .to_physical_repr()
                .i64()
                .map_err(|error| XqdbError::NotAbleToSerializeErr(error.to_string()))?
                .clone();
            try_write_wire_numbers_from_iter(
                vec,
                physical.iter().map(|value| match value {
                    None => Ok(i32::MIN),
                    Some(value) => i32::try_from(value / 1_000_000).map_err(|_| {
                        XqdbError::NotAbleToSerializeErr(
                            "time is outside q's representable range".to_string(),
                        )
                    }),
                }),
            )?;
        }
        PolarsDataType::Array(data_type, size) => {
            vec.extend_from_slice(&[0, 0]);
            vec.extend_from_slice(&list_header);
            let array = downcast_array::<FixedSizeListArray>(
                series.array().unwrap().chunks()[0].as_ref(),
                "FixedSizeList",
            )?;
            if array.null_count() > 0 {
                return Err(XqdbError::NotAbleToSerializeErr(
                    "null values in Array columns".to_string(),
                ));
            }
            match data_type.as_ref() {
                PolarsDataType::Boolean => {
                    let array = downcast_array::<BooleanArray>(array.values().as_ref(), "Boolean")?;
                    let len_vec = q_list_length(*size)?;
                    for row in 0..k_length {
                        vec.extend_from_slice(&[1, 0]);
                        vec.extend_from_slice(&len_vec);
                        let start = row.checked_mul(*size).ok_or(XqdbError::OverLengthErr())?;
                        let end = start.checked_add(*size).ok_or(XqdbError::OverLengthErr())?;
                        vec.reserve(*size);
                        for value in start..end {
                            vec.push(array.get(value).unwrap_or(false) as u8);
                        }
                    }
                }
                _ => {
                    return Err(XqdbError::NotSupportedPolarsNestedListTypeErr(
                        data_type.as_ref().clone(),
                    ))
                }
            }
        }
        PolarsDataType::List(data_type) => {
            vec.extend_from_slice(&[0, 0]);
            vec.extend_from_slice(&list_header);
            let list = downcast_array::<ListArray<i64>>(
                series.list().unwrap().chunks()[0].as_ref(),
                "List",
            )?;
            let offsets = list.offsets().as_ref();
            if offsets.len() != k_length + 1 {
                return Err(XqdbError::NotAbleToSerializeErr(format!(
                    "List offsets contain {} entries for {k_length} rows",
                    offsets.len()
                )));
            }
            if list.null_count() > 0 {
                return Err(XqdbError::NotAbleToSerializeErr(
                    "null values in List columns".to_string(),
                ));
            }
            match data_type.as_ref() {
                PolarsDataType::Boolean => {
                    let values = downcast_array::<BooleanArray>(list.values().as_ref(), "Boolean")?;
                    for range in offsets.windows(2) {
                        let start_offset = usize::try_from(range[0]).map_err(|_| {
                            XqdbError::NotAbleToSerializeErr(
                                "List offsets cannot be negative".to_string(),
                            )
                        })?;
                        let end_offset = usize::try_from(range[1]).map_err(|_| {
                            XqdbError::NotAbleToSerializeErr(
                                "List offsets cannot be negative".to_string(),
                            )
                        })?;
                        vec.extend_from_slice(&[1, 0]);
                        vec.extend_from_slice(&q_list_length(end_offset - start_offset)?);
                        vec.reserve(end_offset - start_offset);
                        for value in start_offset..end_offset {
                            vec.push(values.get(value).unwrap_or(false) as u8);
                        }
                    }
                }
                PolarsDataType::UInt8 => {
                    let list = downcast_array::<UInt8Array>(list.values().as_ref(), "UInt8")?;
                    let values = list.values().as_ref();
                    let has_nulls = list.null_count() > 0;
                    for range in offsets.windows(2) {
                        let start_offset = usize::try_from(range[0]).map_err(|_| {
                            XqdbError::NotAbleToSerializeErr(
                                "List offsets cannot be negative".to_string(),
                            )
                        })?;
                        let end_offset = usize::try_from(range[1]).map_err(|_| {
                            XqdbError::NotAbleToSerializeErr(
                                "List offsets cannot be negative".to_string(),
                            )
                        })?;
                        let row = values.get(start_offset..end_offset).ok_or_else(|| {
                            XqdbError::NotAbleToSerializeErr(
                                "List offsets exceed the child values".to_string(),
                            )
                        })?;
                        vec.extend_from_slice(&[4, 0]);
                        vec.extend_from_slice(&q_list_length(row.len())?);
                        if has_nulls {
                            vec.reserve(row.len());
                            vec.extend(row.iter().enumerate().map(|(offset, &value)| {
                                if list.is_null(start_offset + offset) {
                                    0
                                } else {
                                    value
                                }
                            }));
                        } else {
                            vec.extend_from_slice(row);
                        }
                    }
                }
                PolarsDataType::Int16 => {
                    let array = downcast_array::<Int16Array>(list.values().as_ref(), "Int16")?;
                    write_nested_wire_lists(vec, array, offsets, 5, i16::MIN)?;
                }
                PolarsDataType::Int32 => {
                    let array = downcast_array::<Int32Array>(list.values().as_ref(), "Int32")?;
                    write_nested_wire_lists(vec, array, offsets, 6, i32::MIN)?;
                }
                PolarsDataType::Int64 => {
                    let array = downcast_array::<Int64Array>(list.values().as_ref(), "Int64")?;
                    write_nested_wire_lists(vec, array, offsets, 7, i64::MIN)?;
                }
                PolarsDataType::Float32 => {
                    let array = downcast_array::<Float32Array>(list.values().as_ref(), "Float32")?;
                    write_nested_wire_lists(vec, array, offsets, 8, f32::NAN)?;
                }
                PolarsDataType::Float64 => {
                    let array = downcast_array::<Float64Array>(list.values().as_ref(), "Float64")?;
                    write_nested_wire_lists(vec, array, offsets, 9, f64::NAN)?;
                }
                _ => {
                    return Err(XqdbError::NotSupportedPolarsNestedListTypeErr(
                        data_type.as_ref().clone(),
                    ))
                }
            }
        }
        PolarsDataType::Categorical(_, _) => {
            let categorical = series
                .cat32()
                .map_err(|error| XqdbError::NotAbleToSerializeErr(error.to_string()))?;
            vec.extend_from_slice(&[11, 0]);
            vec.extend_from_slice(&list_header);
            let mut symbols = SymbolLookup::new(categorical.get_mapping());
            for_each_category(categorical, |category| {
                if let Some(category) = category {
                    vec.extend_from_slice(symbols.resolve(category)?.as_bytes());
                }
                vec.push(0);
                Ok(())
            })?;
        }
        PolarsDataType::Binary => {
            validate_guid_series(series)?;
            vec.extend_from_slice(&[2, 0]);
            vec.extend_from_slice(&list_header);
            let array = series
                .binary()
                .map_err(|error| XqdbError::NotAbleToSerializeErr(error.to_string()))?;
            for chunk in array.chunks() {
                let values = downcast_array::<BinaryViewArray>(chunk.as_ref(), "BinaryView")?;
                for value in values.iter() {
                    vec.extend_from_slice(value.unwrap_or(&[0; 16]));
                }
            }
        }
        PolarsDataType::Null if k_length == 0 => {
            vec.extend_from_slice(&[0, 0, 0, 0, 0, 0]);
        }
        _ => return Err(XqdbError::NotSupportedSeriesTypeErr(series.dtype().clone())),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::types::{
        get_series_len, QLambda, QOperator, MIN_Q_TIMESTAMP_UNIX_NANOS, Q_BINARY_PRIMITIVES,
        Q_TERNARY_PRIMITIVES, Q_UNARY_PRIMITIVES,
    };
    use indexmap::IndexMap;
    use polars::prelude::{CompatLevel, NamedFrom};
    use polars_arrow::{
        array::{BooleanArray, UInt8Array},
        offset::OffsetsBuffer,
    };

    use crate::serde6::*;

    #[test]
    fn decompress_msg() {
        let vec: Vec<u8> = [
            222, 7, 0, 0, 0, 1, 0, 208, 7, 0, 0, 1, 1, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255,
            0, 255, 0, 255, 0, 197,
        ]
        .to_vec();
        let length = u32::from_le_bytes(vec[0..4].try_into().unwrap());
        let mut de_vec = vec![0; (length - 8) as usize];
        decompress(&vec, &mut de_vec, 4).expect("valid compressed message");
        let mut expected_vec = [1u8; 2006].to_vec();
        expected_vec[1] = 0;
        expected_vec[2] = 208;
        expected_vec[3] = 7;
        expected_vec[4] = 0;
        expected_vec[5] = 0;
        assert_eq!(de_vec, expected_vec);
    }

    #[test]
    fn malformed_atom_panics_are_contained_as_deserialization_errors() {
        assert!(matches!(
            deserialize(&[254, 0], &mut 0, SymbolEncoding::Strict),
            Err(XqdbError::DeserializationErr(_))
        ));
    }

    #[test]
    fn mixed_list_count_cannot_amplify_a_tiny_frame() {
        assert!(matches!(
            deserialize(
                &[0, 0, 0xff, 0xff, 0xff, 0xff, 101, 0],
                &mut 0,
                SymbolEncoding::Strict
            ),
            Err(XqdbError::DeserializationErr(_))
        ));
    }

    #[test]
    fn decompress_rejects_a_prefix_only_compressed_body() {
        let compressed = 12u32.to_le_bytes();
        let mut destination = [0u8; 4];
        assert!(matches!(
            decompress(&compressed, &mut destination, 4),
            Err(XqdbError::DeserializationErr(_))
        ));
    }

    #[test]
    fn decompress_rejects_uninitialized_first_backreference() {
        // Full malformed frame: [1,2,1,0,15,0,0,0,14,0,0,0,1,0,4].
        let compressed_body = [14, 0, 0, 0, 1, 0, 4];
        let mut destination = [0u8; 6];
        let error = decompress(&compressed_body, &mut destination, 4)
            .expect_err("first operation cannot reference an uninitialized dictionary slot");
        assert!(error.to_string().contains("has not been initialized"));
    }

    #[test]
    fn decompress_accepts_backreferences_to_output_offset_zero() {
        let compressed_body = [14, 0, 0, 0, 4, b'a', b'b', b'a' ^ b'b', 2];
        let mut destination = [0u8; 6];
        decompress(&compressed_body, &mut destination, 4)
            .expect("offset-zero dictionary entry must be representable");
        assert_eq!(destination, *b"ababab");
    }

    #[test]
    fn decompress_requires_complete_compressed_input_consumption() {
        let compressed_body = [9, 0, 0, 0, 0, 42, 99];
        let mut destination = [0u8; 1];
        let error = decompress(&compressed_body, &mut destination, 4)
            .expect_err("trailing compressed input must be rejected");
        assert!(error.to_string().contains("trailing byte"));
    }

    #[test]
    fn decompress_ignores_unused_control_bits_after_output_is_complete() {
        let compressed_body = [9, 0, 0, 0, 0xfe, 42];
        let mut destination = [0u8; 1];
        decompress(&compressed_body, &mut destination, 4)
            .expect("unused control bits do not consume compressed bytes");
        assert_eq!(destination, [42]);
    }

    #[test]
    fn compress_msg() {
        let mut vec = [0u8; 2014].to_vec();
        vec[0] = 1;
        vec[1] = 1;
        vec[4] = 222;
        vec[5] = 7;
        vec[8] = 1;
        vec[10] = 208;
        vec[11] = 7;
        let c_vec = compress(vec).expect("test frame should compress");
        let expected_vec: Vec<u8> = [
            1, 1, 1, 0, 36, 0, 0, 0, 222, 7, 0, 0, 192, 1, 0, 208, 7, 0, 0, 0, 255, 0, 255, 63, 0,
            255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 199,
        ]
        .to_vec();
        assert_eq!(c_vec, expected_vec);
    }

    #[test]
    fn compressed_length_prefix_matches_the_declared_compression_mode() {
        // Mode 2 only triggers above u32::MAX, which no test can reach by calling `compress`, so
        // the prefix writer is exercised directly at both sides of the boundary.
        let mut header = [0u8; 16];
        for (decompressed_length, mode, start) in [
            (2_000usize, 1u8, 12usize),
            (u32::MAX as usize, 1, 12),
            (u32::MAX as usize + 1, 2, 16),
        ] {
            header.fill(0);
            assert_eq!(
                write_decompressed_length_prefix(&mut header, decompressed_length),
                start
            );
            assert_eq!(header[2], mode);
            let decoded = if mode == 1 {
                u64::from(u32::from_le_bytes(
                    header[8..12].try_into().expect("4-byte prefix"),
                ))
            } else {
                u64::from_le_bytes(header[8..16].try_into().expect("8-byte prefix"))
            };
            assert_eq!(decoded, decompressed_length as u64);
        }
    }

    #[test]
    fn symbol_list_count_beyond_its_payload_reports_an_allocation_error() {
        // 11h symbol list declaring 2^31-1 symbols with a two-byte payload: the offsets vector
        // would need 16 GiB, so the reservation must fail as an error rather than abort.
        let mut bytes = vec![11u8, 0];
        bytes.extend_from_slice(&i32::MAX.to_le_bytes());
        bytes.extend_from_slice(&[0, 0]);
        let error = deserialize(&bytes, &mut 0, SymbolEncoding::Strict)
            .expect_err("symbol count beyond the payload must be rejected");
        assert!(
            matches!(error, XqdbError::DeserializationErr(_)),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn deserialize_and_serialize_boolean_list() {
        let vec = [1, 0, 2, 0, 0, 0, 1, 0].to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            BooleanArray::from([Some(true), Some(false)]).boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_guid_list() {
        let vec = [
            2, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 90, 231, 150, 45, 73,
            242, 64, 77, 90, 236, 247, 200, 171, 186, 226, 136,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let binary_array = FixedSizeBinaryArray::new(
            ArrowDataType::FixedSizeBinary(16),
            Buffer::from(
                [
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 90, 231, 150, 45, 73, 242, 64,
                    77, 90, 236, 247, 200, 171, 186, 226, 136,
                ]
                .to_vec(),
            ),
            None,
        );
        let expect = Series::from_arrow(name.into(), binary_array.boxed()).unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_byte_list() {
        let vec = [4, 0, 2, 0, 0, 0, 0, 1].to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect =
            Series::from_arrow(name.into(), UInt8Array::from([Some(0), Some(1)]).boxed()).unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_short_list() {
        let vec = [5, 0, 4, 0, 0, 0, 0, 128, 1, 128, 0, 0, 255, 127].to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            Int16Array::from([None, None, Some(0), None]).boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        let vec = [5, 0, 4, 0, 0, 0, 0, 128, 0, 128, 0, 0, 0, 128].to_vec();
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_int_list() {
        let vec = [
            6, 0, 4, 0, 0, 0, 0, 0, 0, 128, 1, 0, 0, 128, 0, 0, 0, 0, 255, 255, 255, 127,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            Int32Array::from([None, None, Some(0), None]).boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        let vec = [
            6, 0, 4, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0, 128,
        ]
        .to_vec();
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_long_list() {
        let vec = [
            7, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 1, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0,
            0, 0, 255, 255, 255, 255, 255, 255, 255, 127,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            Int64Array::from([None, None, Some(0), None]).boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        let vec = [
            7, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 128,
        ]
        .to_vec();
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_real_list() {
        let vec = [
            8, 0, 4, 0, 0, 0, 0, 0, 192, 127, 0, 0, 128, 255, 0, 0, 0, 0, 0, 0, 128, 127,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            Float32Array::from([
                None,
                Some(f32::NEG_INFINITY),
                Some(0.0),
                Some(f32::INFINITY),
            ])
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_float_list() {
        let vec = [
            9, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 248, 127, 0, 0, 0, 0, 0, 0, 240, 255, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 127,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            Float64Array::from([
                None,
                Some(f64::NEG_INFINITY),
                Some(0.0),
                Some(f64::INFINITY),
            ])
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_char_vector_losslessly() {
        let vec = [10, 0, 4, 0, 0, 0, 0, 127, 128, 255].to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        assert_eq!(k, K::CharVector(vec![0, 127, 128, 255]));
        assert_eq!(serialize(&k).unwrap(), vec);

        let empty = [10, 0, 0, 0, 0, 0].to_vec();
        assert_eq!(
            deserialize(&empty, &mut 0, SymbolEncoding::Strict).unwrap(),
            K::CharVector(Vec::new())
        );
    }

    #[test]
    fn deserialize_and_serialize_symbol_list() {
        let vec = [11, 0, 3, 0, 0, 0, 97, 0, 0, 97, 98, 99, 0].to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            Utf8Array::<i64>::from([Some("a"), Some(""), Some("abc")]).boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        let expect = expect
            .cast(&PolarsDataType::Categorical(
                Categories::global(),
                Categories::global().mapping(),
            ))
            .unwrap();
        assert_eq!(
            series.to_arrow(0, CompatLevel::newest()),
            expect.to_arrow(0, CompatLevel::newest())
        );
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_string_list() {
        let vec = [
            0, 0, 3, 0, 0, 0, 10, 0, 1, 0, 0, 0, 97, 10, 0, 2, 0, 0, 0, 97, 98, 10, 0, 3, 0, 0, 0,
            97, 98, 99,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let name = K_TYPE_NAME[vec[6] as usize];
        let expect = Series::from_arrow(
            name.into(),
            Utf8Array::<i64>::from([Some("a"), Some("ab"), Some("abc")]).boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_timestamp_list() {
        let vec = [
            12, 0, 3, 0, 0, 0, 21, 45, 32, 237, 183, 167, 114, 10, 0, 0, 0, 0, 0, 0, 0, 128, 0, 0,
            199, 153, 133, 126, 114, 10,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            PrimitiveArray::new(
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                vec![1699533296123456789i64, i64::MIN, 1699488000000000000].into(),
                Some(Bitmap::from([true, false, true])),
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_date_list() {
        let vec = [
            14, 0, 3, 0, 0, 0, 9, 34, 0, 0, 0, 0, 0, 128, 220, 210, 169, 5,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            PrimitiveArray::new(
                ArrowDataType::Date32,
                vec![19670, -96465658, 95026601].into(),
                Some(Bitmap::from([true, false, true])),
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_datetime_null_and_finite_values() {
        let vec = [
            15, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 248, 255, 70, 5, 58, 27, 195, 4, 193, 64,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            PrimitiveArray::new(
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                vec![i64::MIN, 1699533296789000000i64].into(),
                Some(Bitmap::from([false, true])),
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect)
    }

    #[test]
    fn native_datetime_null_and_infinities_are_not_instants() {
        let mut atom = vec![241];
        atom.extend_from_slice(&f64::NAN.to_le_bytes());
        assert_eq!(
            deserialize(&atom, &mut 0, SymbolEncoding::Strict).unwrap(),
            K::Null
        );

        for sentinel in [f64::NEG_INFINITY, f64::INFINITY] {
            let mut atom = vec![241];
            atom.extend_from_slice(&sentinel.to_le_bytes());
            assert!(matches!(
                deserialize(&atom, &mut 0, SymbolEncoding::Strict),
                Err(XqdbError::DeserializationErr(_))
            ));

            let mut list = vec![15, 0, 1, 0, 0, 0];
            list.extend_from_slice(&sentinel.to_le_bytes());
            assert!(matches!(
                deserialize(&list, &mut 0, SymbolEncoding::Strict),
                Err(XqdbError::DeserializationErr(_))
            ));
        }
    }

    #[test]
    fn deserialize_and_serialize_timespan_list() {
        let vec = [
            16, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 21, 45, 89, 83, 50, 41, 0, 0, 1, 0, 0, 0,
            0, 0, 0, 128, 255, 255, 255, 255, 255, 255, 255, 127,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            PrimitiveArray::new(
                ArrowDataType::Duration(TimeUnit::Nanosecond),
                vec![i64::MIN, 45296123456789, i64::MIN + 1, i64::MAX].into(),
                Some(Bitmap::from([false, true, true, true])),
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_minute_list() {
        let vec = [
            17, 0, 4, 0, 0, 0, 0, 0, 0, 128, 242, 2, 0, 0, 0, 0, 0, 0, 159, 5, 0, 0,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            PrimitiveArray::new(
                ArrowDataType::Time64(TimeUnit::Nanosecond),
                vec![
                    i64::MIN,
                    45_240_000_000_000,
                    0i64,
                    NANOS_PER_DAY - 60_000_000_000,
                ]
                .into(),
                Some(Bitmap::from([false, true, true, true])),
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect)
    }

    #[test]
    fn deserialize_second_list() {
        let vec = [
            18, 0, 4, 0, 0, 0, 0, 0, 0, 128, 240, 176, 0, 0, 0, 0, 0, 0, 127, 81, 1, 0,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            PrimitiveArray::new(
                ArrowDataType::Time64(TimeUnit::Nanosecond),
                vec![
                    i64::MIN,
                    45_296_000_000_000,
                    0i64,
                    NANOS_PER_DAY - 1_000_000_000,
                ]
                .into(),
                Some(Bitmap::from([false, true, true, true])),
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect)
    }

    #[test]
    fn deserialize_and_serialize_time_list() {
        let vec = [
            19, 0, 4, 0, 0, 0, 0, 0, 0, 128, 149, 44, 179, 2, 0, 0, 0, 0, 255, 91, 38, 5,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            PrimitiveArray::new(
                ArrowDataType::Time64(TimeUnit::Nanosecond),
                vec![i64::MIN, 45_296_789_000_000, 0i64, 86_399_999_000_000].into(),
                Some(Bitmap::from([false, true, true, true])),
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_bool_nested_list() {
        let vec = [
            0, 0, 3, 0, 0, 0, 1, 0, 1, 0, 0, 0, 1, 1, 0, 2, 0, 0, 0, 1, 1, 1, 0, 3, 0, 0, 0, 1, 1,
            1,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let k_type = vec[6];
        let name = K_TYPE_NAME[k_type as usize];
        let offsets = OffsetsBuffer::<i32>::try_from([0, 1, 3, 6].to_vec()).unwrap();
        let array = BooleanArray::from([true; 6].map(Some));
        let field = create_field(k_type, name).unwrap();
        let expect = Series::from_arrow(
            name.into(),
            ListArray::new(
                ArrowDataType::List(Box::new(field)),
                offsets,
                array.boxed(),
                None,
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_bool_nested_array() {
        let vec = [
            0, 0, 3, 0, 0, 0, 1, 0, 2, 0, 0, 0, 1, 0, 1, 0, 2, 0, 0, 0, 1, 0, 1, 0, 2, 0, 0, 0, 1,
            0,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let k_type = vec[6];
        let name = K_TYPE_NAME[k_type as usize];
        let array = BooleanArray::from([true, false, true, false, true, false].map(Some));
        let field = create_field(k_type, name).unwrap();
        let expect = Series::from_arrow(
            name.into(),
            FixedSizeListArray::new(
                ArrowDataType::FixedSizeList(Box::new(field), 2),
                array.len() / 2,
                array.boxed(),
                None,
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_byte_nested_list() {
        let vec = [
            0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 0, 0, 4, 0, 1, 0, 0, 0, 1, 4, 0, 2, 0, 0, 0, 1, 2,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let k_type = vec[6];
        let name = K_TYPE_NAME[k_type as usize];
        let offsets = OffsetsBuffer::<i32>::try_from([0, 0, 1, 3].to_vec()).unwrap();
        let array = UInt8Array::from_slice(vec![1, 1, 2]);
        let field = create_field(k_type, name).unwrap();
        let expect = Series::from_arrow(
            name.into(),
            ListArray::new(
                ArrowDataType::List(Box::new(field)),
                offsets,
                array.boxed(),
                None,
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_short_nested_list() {
        let vec = [
            0, 0, 3, 0, 0, 0, 5, 0, 0, 0, 0, 0, 5, 0, 1, 0, 0, 0, 0, 128, 5, 0, 2, 0, 0, 0, 1, 0,
            2, 0,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let k_type = vec[6];
        let name = K_TYPE_NAME[k_type as usize];
        let offsets = OffsetsBuffer::<i32>::try_from([0, 0, 1, 3].to_vec()).unwrap();
        let array = Int16Array::from([None, Some(1), Some(2)]);
        let field = create_field(k_type, name).unwrap();
        let expect = Series::from_arrow(
            name.into(),
            ListArray::new(
                ArrowDataType::List(Box::new(field)),
                offsets,
                array.boxed(),
                None,
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_int_nested_list() {
        let vec = [
            0, 0, 3, 0, 0, 0, 6, 0, 0, 0, 0, 0, 6, 0, 1, 0, 0, 0, 0, 0, 0, 128, 6, 0, 2, 0, 0, 0,
            1, 0, 0, 0, 2, 0, 0, 0,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let k_type = vec[6];
        let name = K_TYPE_NAME[k_type as usize];
        let offsets = OffsetsBuffer::<i32>::try_from([0, 0, 1, 3].to_vec()).unwrap();
        let array = Int32Array::from([None, Some(1), Some(2)]);
        let field = create_field(k_type, name).unwrap();
        let expect = Series::from_arrow(
            name.into(),
            ListArray::new(
                ArrowDataType::List(Box::new(field)),
                offsets,
                array.boxed(),
                None,
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_long_nested_list() {
        let vec = [
            0, 0, 3, 0, 0, 0, 7, 0, 0, 0, 0, 0, 7, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 7, 0,
            2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let k_type = vec[6];
        let name = K_TYPE_NAME[k_type as usize];
        let offsets = OffsetsBuffer::<i32>::try_from([0, 0, 1, 3].to_vec()).unwrap();
        let array = Int64Array::from([None, Some(1), Some(2)]);
        let field = create_field(k_type, name).unwrap();
        let expect = Series::from_arrow(
            name.into(),
            ListArray::new(
                ArrowDataType::List(Box::new(field)),
                offsets,
                array.boxed(),
                None,
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_real_nested_list() {
        let vec = [
            0, 0, 3, 0, 0, 0, 8, 0, 0, 0, 0, 0, 8, 0, 1, 0, 0, 0, 0, 0, 128, 127, 8, 0, 2, 0, 0, 0,
            0, 0, 128, 63, 0, 0, 128, 255,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let k_type = vec[6];
        let name = K_TYPE_NAME[k_type as usize];
        let offsets = OffsetsBuffer::<i32>::try_from([0, 0, 1, 3].to_vec()).unwrap();
        let array =
            Float32Array::from([Some(f32::INFINITY), Some(1.0f32), Some(f32::NEG_INFINITY)]);
        let field = create_field(k_type, name).unwrap();
        let expect = Series::from_arrow(
            name.into(),
            ListArray::new(
                ArrowDataType::List(Box::new(field)),
                offsets,
                array.boxed(),
                None,
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_float_nested_list() {
        let vec = [
            0, 0, 3, 0, 0, 0, 9, 0, 0, 0, 0, 0, 9, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 127, 9, 0,
            2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 255,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let k_type = vec[6];
        let name = K_TYPE_NAME[k_type as usize];
        let offsets = OffsetsBuffer::<i32>::try_from([0, 0, 1, 3].to_vec()).unwrap();
        let array = Float64Array::from([Some(f64::INFINITY), Some(1.0), Some(f64::NEG_INFINITY)]);
        let field = create_field(k_type, name).unwrap();
        let expect = Series::from_arrow(
            name.into(),
            ListArray::new(
                ArrowDataType::List(Box::new(field)),
                offsets,
                array.boxed(),
                None,
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_timestamp_nested_list() {
        let vec = [
            0, 0, 3, 0, 0, 0, 7, 0, 0, 0, 0, 0, 7, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 7, 0,
            2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let k_type = vec[6];
        let name = K_TYPE_NAME[k_type as usize];
        let offsets = OffsetsBuffer::<i32>::try_from([0, 0, 1, 3].to_vec()).unwrap();
        let array = Int64Array::from([None, Some(1), Some(2)]);
        let field = create_field(k_type, name).unwrap();
        let expect = Series::from_arrow(
            name.into(),
            ListArray::new(
                ArrowDataType::List(Box::new(field)),
                offsets,
                array.boxed(),
                None,
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_mixed_list() {
        let vec = [
            0, 0, 3, 0, 0, 0, 245, 117, 112, 100, 0, 245, 116, 0, 98, 0, 99, 11, 0, 1, 0, 0, 0, 97,
            0, 0, 0, 1, 0, 0, 0, 7, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let expect = K::MixedList(vec![
            K::Symbol("upd".to_owned()),
            K::Symbol("t".to_owned()),
            K::DataFrame(
                DataFrame::new_infer_height(vec![Series::new("a".into(), [1i64].as_ref()).into()])
                    .unwrap(),
            ),
        ]);
        assert_eq!(k, expect);
        assert_eq!(vec, serialize(&expect).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_table() {
        let vec = [
            98, 0, 99, 11, 0, 2, 0, 0, 0, 97, 0, 98, 0, 0, 0, 2, 0, 0, 0, 7, 0, 1, 0, 0, 0, 1, 0,
            0, 0, 0, 0, 0, 0, 9, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let df: DataFrame = k.try_into().unwrap();
        let s0 = Series::new("a".into(), [1i64].as_ref());
        let s1 = Series::new("b".into(), [1.0f64].as_ref());
        let expect = DataFrame::new_infer_height(vec![s0.into(), s1.into()]).unwrap();
        assert_eq!(df, expect);
        assert_eq!(vec, serialize(&K::DataFrame(expect)).unwrap());
    }

    #[test]
    fn deserialize_keyed_table() {
        let vec = [
            99, 98, 0, 99, 11, 0, 1, 0, 0, 0, 97, 0, 0, 0, 1, 0, 0, 0, 9, 0, 1, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 240, 63, 98, 0, 99, 11, 0, 1, 0, 0, 0, 98, 0, 0, 0, 1, 0, 0, 0, 9, 0, 1, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 240, 63,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap();
        let df: DataFrame = k.try_into().unwrap();
        let s0 = Series::new("a".into(), [1i64].as_ref());
        let s1 = Series::new("b".into(), [1.0f64].as_ref());
        let expect = DataFrame::new_infer_height(vec![s0.into(), s1.into()]).unwrap();
        assert_eq!(df, expect);
    }

    #[test]
    fn serialize_bool() {
        let k = K::Boolean(true);
        assert_eq!(serialize(&k).unwrap(), [255, 1]);
    }

    #[test]
    fn serialize_guid() {
        let k = K::Guid(
            Uuid::from_slice(&[
                88, 13, 140, 135, 229, 87, 13, 177, 58, 25, 203, 58, 68, 214, 35, 177,
            ])
            .unwrap(),
        );
        assert_eq!(
            serialize(&k).unwrap(),
            [254, 88, 13, 140, 135, 229, 87, 13, 177, 58, 25, 203, 58, 68, 214, 35, 177,]
        );
    }

    #[test]
    fn serialize_byte() {
        let k = K::U8(99);
        assert_eq!(serialize(&k).unwrap(), [252, 99]);
    }

    #[test]
    fn serialize_short() {
        let k = K::I16(99);
        assert_eq!(serialize(&k).unwrap(), [251, 99, 0]);
    }

    #[test]
    fn serialize_int() {
        let k = K::I32(99999999);
        assert_eq!(serialize(&k).unwrap(), [250, 255, 224, 245, 5]);
    }

    #[test]
    fn serialize_long() {
        let k = K::I64(9999_9999_9999_9999);
        assert_eq!(
            serialize(&k).unwrap(),
            [249, 255, 255, 192, 111, 242, 134, 35, 0]
        );
    }

    #[test]
    fn serialize_real() {
        let k = K::F32(9.9e10);
        assert_eq!(serialize(&k).unwrap(), [248, 225, 102, 184, 81]);
    }

    #[test]
    fn serialize_float() {
        let k = K::F64(9.9e10);
        assert_eq!(serialize(&k).unwrap(), [247, 0, 0, 0, 30, 220, 12, 55, 66]);
    }

    #[test]
    fn serialize_symbol() {
        let k = K::Symbol("abc".to_string());
        assert_eq!(serialize(&k).unwrap(), [245, 97, 98, 99, 0]);
    }

    #[test]
    fn serialize_string() {
        let k = K::String("abc".to_string());
        assert_eq!(serialize(&k).unwrap(), [10, 0, 3, 0, 0, 0, 97, 98, 99]);
    }

    #[test]
    fn char_vector_length_must_fit_q_list_length() {
        assert!(matches!(
            q_list_length(i32::MAX as usize + 1),
            Err(XqdbError::OverLengthErr())
        ));
    }

    #[test]
    fn serialize_timestamp() {
        let k = K::DateTime(DateTime::<chrono::Utc>::from_timestamp(0, 123456789).unwrap());
        assert_eq!(
            serialize(&k).unwrap(),
            [244, 21, 205, 24, 181, 48, 179, 220, 242]
        );
    }

    #[test]
    fn serialize_date() {
        let k = K::Date(NaiveDate::from_ymd_opt(2023, 11, 15).unwrap());
        assert_eq!(serialize(&k).unwrap(), [242, 15, 34, 0, 0]);
    }

    #[test]
    fn serialize_time() {
        let k = K::Time(NaiveTime::from_hms_milli_opt(0, 17, 24, 70).unwrap());
        assert_eq!(serialize(&k).unwrap(), [237, 102, 238, 15, 0]);
    }
    #[test]
    fn primitive_operators_have_exact_wire_values_and_round_trip() {
        let plus = K::Operator(QOperator::PLUS);
        assert_eq!(plus.j6_len().unwrap(), 2);
        assert_eq!(plus.try_get_j_type_code().unwrap(), 102);
        assert_eq!(serialize(&plus).unwrap(), [102, 1]);
        assert_eq!(
            deserialize(&[102, 1], &mut 0, SymbolEncoding::Strict).unwrap(),
            K::Operator(QOperator::PLUS)
        );

        for (k_type, primitives) in [
            (101, Q_UNARY_PRIMITIVES.as_slice()),
            (102, Q_BINARY_PRIMITIVES.as_slice()),
            (103, Q_TERNARY_PRIMITIVES.as_slice()),
        ] {
            for (opcode, name) in primitives.iter().enumerate() {
                if name.is_empty() {
                    continue;
                }
                let operator = QOperator::new(name).expect("documented primitive name");
                let bytes = [k_type, opcode as u8];
                assert_eq!(
                    K::Operator(operator).try_get_j_type_code().unwrap(),
                    i16::from(k_type)
                );
                assert_eq!(serialize(&K::Operator(operator)).unwrap(), bytes);
                assert_eq!(
                    deserialize(&bytes, &mut 0, SymbolEncoding::Strict).unwrap(),
                    K::Operator(operator)
                );
            }
        }
    }

    #[test]
    fn null_and_unsupported_operator_values_remain_distinct() {
        assert!(QOperator::new("::").is_err());
        assert!(QOperator::new("plus").is_err());
        assert!(QOperator::new("+\0").is_err());
        assert_eq!(
            deserialize(&[101, 0], &mut 0, SymbolEncoding::Strict).unwrap(),
            K::Null
        );

        for bytes in [[101, 42], [102, 34], [103, 3]] {
            assert!(matches!(
                deserialize(&bytes, &mut 0, SymbolEncoding::Strict),
                Err(XqdbError::NotSupportedKOperatorErr(_))
            ));
        }
        for k_type in 104..=112 {
            assert!(matches!(
                deserialize(&[k_type, 0], &mut 0, SymbolEncoding::Strict),
                Err(XqdbError::NotSupportedKTypeErr(value)) if value == k_type
            ));
        }
    }

    #[test]
    fn lambdas_have_exact_root_and_context_wire_values_and_round_trip() {
        let root = QLambda::new("{x+y}").unwrap();
        let root_bytes = [100, 0, 10, 0, 5, 0, 0, 0, b'{', b'x', b'+', b'y', b'}'];
        assert_eq!(K::Lambda(root.clone()).j6_len().unwrap(), root_bytes.len());
        assert_eq!(serialize(&K::Lambda(root.clone())).unwrap(), root_bytes);
        assert_eq!(K::Lambda(root.clone()).try_get_j_type_code().unwrap(), 100);
        let mut root_pos = 0;
        assert_eq!(
            deserialize(&root_bytes, &mut root_pos, SymbolEncoding::Strict).unwrap(),
            K::Lambda(root)
        );
        assert_eq!(root_pos, root_bytes.len());

        let contextual = QLambda::with_context(" {x*2} ", "ctx").unwrap();
        let contextual_bytes = [
            100, b'c', b't', b'x', 0, 10, 0, 7, 0, 0, 0, b' ', b'{', b'x', b'*', b'2', b'}', b' ',
        ];
        assert_eq!(
            K::Lambda(contextual.clone()).j6_len().unwrap(),
            contextual_bytes.len()
        );
        assert_eq!(
            serialize(&K::Lambda(contextual.clone())).unwrap(),
            contextual_bytes
        );
        assert_eq!(
            deserialize(&contextual_bytes, &mut 0, SymbolEncoding::Strict).unwrap(),
            K::Lambda(contextual.clone())
        );
        assert_eq!(
            contextual.clone().into_parts(),
            (" {x*2} ".to_string(), "ctx".to_string())
        );

        let k_dialect = QLambda::new(" k){x+y} ").unwrap();
        let k_dialect_bytes = serialize(&K::Lambda(k_dialect.clone())).unwrap();
        assert_eq!(
            deserialize(&k_dialect_bytes, &mut 0, SymbolEncoding::Strict).unwrap(),
            K::Lambda(k_dialect)
        );
    }

    #[test]
    fn lambda_validation_and_malformed_frames_fail_explicitly() {
        assert!(QLambda::new("x+y").is_err());
        assert!(QLambda::new("k)x+y").is_err());
        assert!(QLambda::new("{x\0+y}").is_err());
        assert!(QLambda::with_context("{x+y}", "bad\0context").is_err());
        assert!(QLambda::with_context("{x+y}", ".ctx")
            .unwrap_err()
            .to_string()
            .contains("q lambda context omits the leading dot"));

        let malformed = [
            vec![100],
            vec![100, 0, 11, 0, 0, 0, 0, 0],
            vec![100, 0, 10, 1, 2, 0, 0, 0, b'{', b'}'],
            vec![100, 0, 10, 0, 255, 255, 255, 255],
            vec![100, 0, 10, 0, 2, 0, 0, 0, b'{'],
            vec![100, 0, 10, 0, 1, 0, 0, 0, 0xff],
            vec![100, 0, 10, 0, 1, 0, 0, 0, b'x'],
            vec![100, 0, 10, 0, 3, 0, 0, 0, b'{', 0, b'}'],
        ];
        for bytes in malformed {
            assert!(
                matches!(
                    deserialize(&bytes, &mut 0, SymbolEncoding::Strict),
                    Err(XqdbError::DeserializationErr(_))
                ),
                "malformed lambda unexpectedly decoded: {bytes:?}"
            );
        }
    }

    #[test]
    fn operator_and_lambda_ipc_frames_have_exact_declared_lengths() {
        let plus = crate::io::generate_j6_ipc_msg(
            crate::types::MsgType::Sync,
            false,
            K::Operator(QOperator::PLUS),
        )
        .unwrap();
        assert_eq!(plus, [1, 1, 0, 0, 10, 0, 0, 0, 102, 1]);

        let root = crate::io::generate_j6_ipc_msg(
            crate::types::MsgType::Sync,
            false,
            K::Lambda(QLambda::new("{x+y}").unwrap()),
        )
        .unwrap();
        assert_eq!(
            root,
            [1, 1, 0, 0, 21, 0, 0, 0, 100, 0, 10, 0, 5, 0, 0, 0, b'{', b'x', b'+', b'y', b'}',]
        );

        let contextual = crate::io::generate_j6_ipc_msg(
            crate::types::MsgType::Sync,
            false,
            K::Lambda(QLambda::with_context("{x+y}", "ctx").unwrap()),
        )
        .unwrap();
        assert_eq!(
            contextual,
            [
                1, 1, 0, 0, 24, 0, 0, 0, 100, b'c', b't', b'x', 0, 10, 0, 5, 0, 0, 0, b'{', b'x',
                b'+', b'y', b'}',
            ]
        );
    }

    #[test]
    fn serialize_duration() {
        let k = K::Duration(Duration::nanoseconds(822896123456789));
        assert_eq!(
            serialize(&k).unwrap(),
            [240, 21, 45, 32, 111, 107, 236, 2, 0]
        );
    }

    #[test]
    fn serialize_none() {
        let k = K::Null;
        assert_eq!(serialize(&k).unwrap(), [101, 0]);
    }

    #[test]
    fn deserialize_and_serialize_dict() {
        let vec = [
            99, 11, 0, 2, 0, 0, 0, 97, 0, 98, 0, 0, 0, 2, 0, 0, 0, 249, 1, 0, 0, 0, 0, 0, 0, 0,
            247, 0, 0, 0, 0, 0, 0, 240, 63,
        ]
        .to_vec();
        let mut dict = IndexMap::with_capacity(2);
        dict.insert("a".to_string(), K::I64(1));
        dict.insert("b".to_string(), K::F64(1.0));
        let k = K::Dict(dict);
        assert_eq!(
            deserialize(&vec, &mut 0, SymbolEncoding::Strict).unwrap(),
            k
        );
        assert_eq!(vec, serialize(&k).unwrap());
    }

    #[test]
    fn native_dictionary_rejects_duplicate_symbol_keys() {
        let duplicate = [
            99, 11, 0, 2, 0, 0, 0, b'a', 0, b'a', 0, 7, 0, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2,
            0, 0, 0, 0, 0, 0, 0,
        ];
        let error = deserialize(&duplicate, &mut 0, SymbolEncoding::Strict)
            .expect_err("native dictionary conversion must not overwrite duplicate keys");
        assert!(error
            .to_string()
            .contains("duplicate symbol dictionary key"));
    }

    #[test]
    fn qvalue_variant_serializes_its_validated_body_unchanged() {
        let body = [
            12, 4, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 1, 0, 0, 0, 0, 0, 0, 128,
        ];
        let value = crate::QValue::from_bytes(&body).expect("valid attributed timestamp vector");
        let k = K::QValue(value);
        assert_eq!(k.j6_len().unwrap(), body.len());
        assert_eq!(k.try_get_j_type_code().unwrap(), 12);
        assert_eq!(serialize(&k).unwrap(), body);
    }

    #[test]
    fn empty_dictionary_round_trips_as_symbol_keyed_dictionary() {
        let empty = || K::Dict(IndexMap::new());
        // (`symbol$())!()
        let symbol_keyed = [99, 11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(serialize(&empty()).unwrap(), symbol_keyed);
        assert_eq!(empty().j6_len().unwrap(), symbol_keyed.len());
        assert_eq!(
            deserialize(&symbol_keyed, &mut 0, SymbolEncoding::Strict).unwrap(),
            empty()
        );
        assert_ipc_header_matches_body(empty());

        // ()!() keys with an empty general list; 0#`a`b!1 2 values with an empty long list.
        for wire in [
            [99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [99, 11, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0],
        ] {
            assert_eq!(
                deserialize(&wire, &mut 0, SymbolEncoding::Strict).unwrap(),
                empty()
            );
        }

        let nested = K::Dict(IndexMap::from([("nested".to_string(), empty())]));
        let wire = serialize(&nested).unwrap();
        assert_eq!(
            deserialize(&wire, &mut 0, SymbolEncoding::Strict).unwrap(),
            nested
        );

        // (1;`a)!2 3: general-list keys stay unsupported once they hold anything.
        let mixed_keys = [
            99, 0, 0, 2, 0, 0, 0, 249, 1, 0, 0, 0, 0, 0, 0, 0, 245, 97, 0, 7, 0, 2, 0, 0, 0, 2, 0,
            0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
        ];
        assert!(matches!(
            deserialize(&mixed_keys, &mut 0, SymbolEncoding::Strict),
            Err(XqdbError::Err(message)) if message.ends_with("got k type 0")
        ));
        assert!(matches!(
            deserialize(&[99], &mut 0, SymbolEncoding::Strict),
            Err(XqdbError::DeserializationErr(_))
        ));
        assert!(matches!(
            deserialize(&[99, 11, 0, 0, 0, 0, 0], &mut 0, SymbolEncoding::Strict),
            Err(XqdbError::DeserializationErr(_))
        ));
    }

    fn assert_ipc_header_matches_body(value: K) {
        let body_length = value.j6_len().expect("calculate body length");
        let message = crate::io::generate_j6_ipc_msg(crate::types::MsgType::Sync, false, value)
            .expect("generate IPC message");
        let header_length =
            u32::from_le_bytes(message[4..8].try_into().expect("IPC length header")) as usize;
        assert_eq!(header_length, message.len());
        assert_eq!(body_length + 8, message.len());
    }

    fn time_series(values: Vec<i64>, validity: Option<Bitmap>) -> Series {
        Series::from_arrow(
            "time".into(),
            PrimitiveArray::new(
                ArrowDataType::Time64(TimeUnit::Nanosecond),
                values.into(),
                validity,
            )
            .boxed(),
        )
        .expect("time series")
    }

    #[test]
    fn series_and_table_lengths_match_generated_ipc_headers() {
        let byte = Series::new("byte".into(), &[Some(7u8), None, Some(9)]);
        let date = Series::new("date".into(), &[Some(0i32), None, Some(1)])
            .cast(&PolarsDataType::Date)
            .expect("date series");
        let time = time_series(
            vec![0, i64::MIN, 86_399_999_000_000],
            Some(Bitmap::from([true, false, true])),
        );
        let long = "long string payload".repeat(8);
        let text = Series::new("text".into(), &[Some(long.as_str()), Some(""), None]);

        for series in [byte.clone(), date.clone(), time.clone(), text.clone()] {
            assert_ipc_header_matches_body(K::Series(series));
        }

        let table = DataFrame::new_infer_height(
            [byte, date, time, text]
                .into_iter()
                .map(Into::into)
                .collect(),
        )
        .expect("table");
        assert_ipc_header_matches_body(K::DataFrame(table));
    }

    #[test]
    fn nullable_boolean_and_byte_series_use_deterministic_q_values() {
        let boolean = Series::from_arrow(
            "boolean".into(),
            BooleanArray::from([Some(true), None, Some(false)]).boxed(),
        )
        .expect("boolean series");
        let byte = Series::from_arrow(
            "byte".into(),
            UInt8Array::from([Some(7), None, Some(9)]).boxed(),
        )
        .expect("byte series");

        let boolean_bytes = serialize(&K::Series(boolean.clone())).expect("serialize boolean");
        let byte_bytes = serialize(&K::Series(byte.clone())).expect("serialize byte");
        assert_eq!(boolean_bytes, [1, 0, 3, 0, 0, 0, 1, 0, 0]);
        assert_eq!(byte_bytes, [4, 0, 3, 0, 0, 0, 7, 0, 9]);
        assert_eq!(
            serialize(
                &deserialize(&boolean_bytes, &mut 0, SymbolEncoding::Strict)
                    .expect("deserialize boolean")
            )
            .expect("reserialize boolean"),
            boolean_bytes
        );
        assert_eq!(
            serialize(
                &deserialize(&byte_bytes, &mut 0, SymbolEncoding::Strict)
                    .expect("deserialize byte")
            )
            .expect("reserialize byte"),
            byte_bytes
        );

        let table = DataFrame::new_infer_height(vec![boolean.clone().into(), byte.clone().into()])
            .expect("nullable table");
        let table_bytes = serialize(&K::DataFrame(table.clone())).expect("serialize table");
        assert_eq!(
            serialize(
                &deserialize(&table_bytes, &mut 0, SymbolEncoding::Strict)
                    .expect("deserialize table")
            )
            .expect("reserialize table"),
            table_bytes
        );
        assert_ipc_header_matches_body(K::Series(boolean));
        assert_ipc_header_matches_body(K::Series(byte));
        assert_ipc_header_matches_body(K::DataFrame(table));
    }

    #[test]
    fn rejects_sub_millisecond_polars_and_scalar_times() {
        let time = time_series(vec![1], None);
        assert!(matches!(
            get_series_len(&time),
            Err(XqdbError::NotAbleToSerializeErr(_))
        ));
        assert!(matches!(
            serialize_series_into(&time, &mut Vec::new()),
            Err(XqdbError::NotAbleToSerializeErr(_))
        ));

        let scalar = K::Time(NaiveTime::from_hms_nano_opt(0, 0, 0, 1).expect("time"));
        assert!(matches!(
            serialize(&scalar),
            Err(XqdbError::NotAbleToSerializeErr(_))
        ));
    }

    #[test]
    fn rejects_mixed_width_guid_series_and_zero_fills_nulls() {
        let exact = [7u8; 16];
        let valid = Series::new("guid".into(), &[Some(exact.as_slice()), None]);
        let serialized = serialize(&K::Series(valid)).expect("serialize GUIDs");
        assert_eq!(&serialized[6..22], exact.as_slice());
        assert_eq!(&serialized[22..38], &[0u8; 16]);

        let short = [1u8; 15];
        let malformed = Series::new(
            "guid".into(),
            &[Some(exact.as_slice()), Some(short.as_slice())],
        );
        assert!(matches!(
            get_series_len(&malformed),
            Err(XqdbError::NotAbleToSerializeErr(_))
        ));
        assert!(matches!(
            serialize_series_into(&malformed, &mut Vec::new()),
            Err(XqdbError::NotAbleToSerializeErr(_))
        ));
    }

    #[test]
    fn rejects_non_utf8_q_char_columns_without_losing_top_level_bytes() {
        let top_level = [10, 0, 1, 0, 0, 0, 0xff];
        assert_eq!(
            deserialize(&top_level, &mut 0, SymbolEncoding::Strict).expect("top-level bytes"),
            K::CharVector(vec![0xff])
        );

        let nested = [0, 0, 1, 0, 0, 0, 10, 0, 1, 0, 0, 0, 0xff];
        assert!(matches!(
            deserialize(&nested, &mut 0, SymbolEncoding::Strict),
            Err(XqdbError::DeserializationErr(_))
        ));

        let table = [
            98, 0, 99, 11, 0, 1, 0, 0, 0, b'c', 0, 0, 0, 1, 0, 0, 0, 10, 0, 1, 0, 0, 0, 0xff,
        ];
        assert!(matches!(
            deserialize(&table, &mut 0, SymbolEncoding::Strict),
            Err(XqdbError::DeserializationErr(_))
        ));
    }

    fn string_values(series: &Series) -> Vec<String> {
        series
            .cast(&PolarsDataType::String)
            .expect("series should cast to strings")
            .str()
            .expect("string series")
            .iter()
            .map(|value| value.expect("non-null value").to_owned())
            .collect()
    }

    #[test]
    fn symbol_encoding_names_round_trip_and_default_to_strict() {
        assert_eq!(SymbolEncoding::default(), SymbolEncoding::Strict);
        for encoding in [SymbolEncoding::Strict, SymbolEncoding::Lossy] {
            assert_eq!(SymbolEncoding::from_name(encoding.name()), Some(encoding));
        }
        assert_eq!(SymbolEncoding::from_name("latin1"), None);
        assert_eq!(SymbolEncoding::from_name("Lossy"), None);
    }

    #[test]
    fn lossy_symbol_lists_transcode_invalid_bytes_and_keep_every_terminator() {
        // Symbols: "a", a lone Latin-1 byte, an invalid byte inside a symbol, and a truncated
        // three-byte sequence right before its NUL terminator.
        let vec = [
            11, 0, 4, 0, 0, 0, b'a', 0, 0xe9, 0, b'c', 0xff, b'd', 0, 0xe2, 0x82, 0,
        ];
        assert!(matches!(
            deserialize(&vec, &mut 0, SymbolEncoding::Strict),
            Err(XqdbError::DeserializationErr(_))
        ));

        let k = deserialize(&vec, &mut 0, SymbolEncoding::Lossy).expect("lossy symbol list");
        let mut expected = vec![11, 0, 4, 0, 0, 0, b'a', 0];
        expected.extend_from_slice("\u{FFFD}\0c\u{FFFD}d\0\u{FFFD}\0".as_bytes());
        assert_eq!(serialize(&k).unwrap(), expected);
        let series: Series = k.try_into().unwrap();
        assert_eq!(
            string_values(&series),
            ["a", "\u{FFFD}", "c\u{FFFD}d", "\u{FFFD}"]
        );
    }

    #[test]
    fn lossy_symbol_atoms_column_names_and_dictionary_keys_transcode() {
        let atom = [245, 0xe9, b'x', 0];
        assert!(matches!(
            deserialize(&atom, &mut 0, SymbolEncoding::Strict),
            Err(XqdbError::DeserializationErr(_))
        ));
        assert_eq!(
            deserialize(&atom, &mut 0, SymbolEncoding::Lossy).expect("lossy symbol atom"),
            K::Symbol("\u{FFFD}x".to_string())
        );

        let table = [
            98, 0, 99, 11, 0, 1, 0, 0, 0, 0xe9, 0, 0, 0, 1, 0, 0, 0, 7, 0, 1, 0, 0, 0, 1, 0, 0, 0,
            0, 0, 0, 0,
        ];
        assert!(matches!(
            deserialize(&table, &mut 0, SymbolEncoding::Strict),
            Err(XqdbError::DeserializationErr(_))
        ));
        let frame: DataFrame = deserialize(&table, &mut 0, SymbolEncoding::Lossy)
            .expect("lossy table")
            .try_into()
            .unwrap();
        assert_eq!(frame.get_column_names(), ["\u{FFFD}"]);

        let dictionary = [
            99, 11, 0, 1, 0, 0, 0, 0xe9, 0, 7, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
        ];
        assert!(matches!(
            deserialize(&dictionary, &mut 0, SymbolEncoding::Strict),
            Err(XqdbError::DeserializationErr(_))
        ));
        assert_eq!(
            deserialize(&dictionary, &mut 0, SymbolEncoding::Lossy).expect("lossy dictionary"),
            K::Dict(IndexMap::from([("\u{FFFD}".to_string(), K::I64(1))]))
        );
    }

    #[test]
    fn lossy_char_columns_replace_each_non_ascii_row() {
        let table = [
            98, 0, 99, 11, 0, 1, 0, 0, 0, b'c', 0, 0, 0, 1, 0, 0, 0, 10, 0, 3, 0, 0, 0, b'a', 0xe9,
            b'z',
        ];
        assert!(matches!(
            deserialize(&table, &mut 0, SymbolEncoding::Strict),
            Err(XqdbError::DeserializationErr(_))
        ));
        let frame: DataFrame = deserialize(&table, &mut 0, SymbolEncoding::Lossy)
            .expect("lossy char column")
            .try_into()
            .unwrap();
        let column = frame.column("c").unwrap().as_materialized_series();
        assert_eq!(column.dtype(), &PolarsDataType::String);
        assert_eq!(string_values(column), ["a", "\u{FFFD}", "z"]);
    }

    #[test]
    fn lossy_string_columns_transcode_each_row_on_its_own() {
        // Concatenated, the rows read "a" C3 A9 "b" C3 A9, which is valid UTF-8 as a whole; the
        // boundary between the first two rows splits the code point, so both rows are invalid
        // and only the third decodes as "é".
        let vec = [
            0, 0, 3, 0, 0, 0, 10, 0, 2, 0, 0, 0, b'a', 0xc3, 10, 0, 2, 0, 0, 0, 0xa9, b'b', 10, 0,
            2, 0, 0, 0, 0xc3, 0xa9,
        ];
        assert!(matches!(
            deserialize(&vec, &mut 0, SymbolEncoding::Strict),
            Err(XqdbError::DeserializationErr(_))
        ));
        let k = deserialize(&vec, &mut 0, SymbolEncoding::Lossy).expect("lossy string column");
        let mut expected = vec![0, 0, 3, 0, 0, 0, 10, 0, 4, 0, 0, 0];
        expected.extend_from_slice("a\u{FFFD}".as_bytes());
        expected.extend_from_slice(&[10, 0, 4, 0, 0, 0]);
        expected.extend_from_slice("\u{FFFD}b".as_bytes());
        expected.extend_from_slice(&[10, 0, 2, 0, 0, 0, 0xc3, 0xa9]);
        assert_eq!(serialize(&k).unwrap(), expected);
        let series: Series = k.try_into().unwrap();
        assert_eq!(string_values(&series), ["a\u{FFFD}", "\u{FFFD}b", "é"]);
    }

    #[test]
    fn lossy_nested_symbol_lists_transcode_invalid_symbols() {
        let vec = [
            0, 0, 2, 0, 0, 0, 11, 0, 1, 0, 0, 0, 0xe9, 0, 11, 0, 2, 0, 0, 0, b'a', 0, b'b', 0,
        ];
        assert!(matches!(
            deserialize(&vec, &mut 0, SymbolEncoding::Strict),
            Err(XqdbError::DeserializationErr(_))
        ));
        let series: Series = deserialize(&vec, &mut 0, SymbolEncoding::Lossy)
            .expect("lossy nested symbols")
            .try_into()
            .unwrap();
        let strings = series
            .cast(&PolarsDataType::List(Box::new(PolarsDataType::String)))
            .expect("nested symbols cast to nested strings");
        let rows: Vec<Vec<String>> = (0..strings.len())
            .map(|row| string_values(&strings.list().unwrap().get_as_series(row).unwrap()))
            .collect();
        assert_eq!(
            rows,
            [vec!["\u{FFFD}".to_string()], vec!["a".into(), "b".into()]]
        );
    }

    #[test]
    fn lossy_lambdas_transcode_source_and_context() {
        let mut vec = vec![100, 0xe9, 0, 10, 0, 5, 0, 0, 0];
        vec.extend_from_slice(&[b'{', b'x', b'+', 0xe9, b'}']);
        assert!(matches!(
            deserialize(&vec, &mut 0, SymbolEncoding::Strict),
            Err(XqdbError::DeserializationErr(_))
        ));
        assert_eq!(
            deserialize(&vec, &mut 0, SymbolEncoding::Lossy).expect("lossy lambda"),
            K::Lambda(QLambda::with_context("{x+\u{FFFD}}", "\u{FFFD}").unwrap())
        );
    }

    #[test]
    fn q_error_messages_surface_under_every_encoding() {
        let error = [128, b'c', b'a', b'f', 0xe9, 0];
        for encoding in [SymbolEncoding::Strict, SymbolEncoding::Lossy] {
            assert!(matches!(
                deserialize(&error, &mut 0, encoding),
                Err(XqdbError::ServerErr(message)) if message == "caf\u{FFFD}"
            ));
        }
    }

    #[test]
    fn valid_text_decodes_identically_under_both_encodings() {
        let table = [
            98, 0, 99, 11, 0, 2, 0, 0, 0, b's', 0, b't', 0, 0, 0, 2, 0, 0, 0, 11, 0, 2, 0, 0, 0,
            0xc3, 0xa9, 0, b'x', 0, 0, 0, 2, 0, 0, 0, 10, 0, 2, 0, 0, 0, 0xc3, 0xa9, 10, 0, 1, 0,
            0, 0, b'y',
        ];
        let strict = deserialize(&table, &mut 0, SymbolEncoding::Strict).expect("strict table");
        let lossy = deserialize(&table, &mut 0, SymbolEncoding::Lossy).expect("lossy table");
        assert_eq!(strict, lossy);
        let frame: DataFrame = lossy.try_into().unwrap();
        assert_eq!(
            string_values(frame.column("s").unwrap().as_materialized_series()),
            ["é", "x"]
        );
        assert_eq!(
            string_values(frame.column("t").unwrap().as_materialized_series()),
            ["é", "y"]
        );
    }

    #[test]
    fn table_column_failures_are_propagated() {
        let table = [
            98, 0, 99, 11, 0, 1, 0, 0, 0, b'x', 0, 0, 0, 1, 0, 0, 0, 13, 0, 1, 0, 0, 0, 0, 0, 0, 0,
        ];
        assert!(matches!(
            deserialize(&table, &mut 0, SymbolEncoding::Strict),
            Err(XqdbError::NotSupportedKListErr(13))
        ));
    }

    #[test]
    fn bounds_mixed_list_and_dictionary_deserialization_depth() {
        let mut mixed = vec![101, 0];
        for _ in 0..=MAX_VALUE_DEPTH {
            let mut outer = vec![0, 0, 1, 0, 0, 0];
            outer.extend(mixed);
            mixed = outer;
        }
        assert!(matches!(
            deserialize(&mixed, &mut 0, SymbolEncoding::Strict),
            Err(XqdbError::DeserializationErr(_))
        ));

        let mut dictionary = vec![101, 0];
        for _ in 0..=MAX_VALUE_DEPTH / 2 {
            let mut outer = vec![99, 11, 0, 1, 0, 0, 0, b'k', 0, 0, 0, 1, 0, 0, 0];
            outer.extend(dictionary);
            dictionary = outer;
        }
        assert!(matches!(
            deserialize(&dictionary, &mut 0, SymbolEncoding::Strict),
            Err(XqdbError::DeserializationErr(_))
        ));
    }

    #[test]
    fn sizing_and_serialization_reject_sixty_five_nested_values() {
        fn nested_mixed(depth: usize) -> K {
            (0..depth).fold(K::Null, |value, _| K::MixedList(vec![value]))
        }

        fn nested_dict(depth: usize) -> K {
            (0..depth).fold(K::Null, |value, _| {
                K::Dict(IndexMap::from([("k".to_string(), value)]))
            })
        }

        assert!(serialize(&nested_mixed(MAX_VALUE_DEPTH)).is_ok());
        assert!(serialize(&nested_dict(MAX_VALUE_DEPTH)).is_ok());

        for value in [
            nested_mixed(MAX_VALUE_DEPTH + 1),
            nested_dict(MAX_VALUE_DEPTH + 1),
        ] {
            assert!(matches!(
                value.j6_len(),
                Err(XqdbError::NotAbleToSerializeErr(_))
            ));
            assert!(matches!(
                serialize(&value),
                Err(XqdbError::NotAbleToSerializeErr(_))
            ));
        }
    }

    #[test]
    fn decodes_char_and_guid_dictionary_values_without_truncation() {
        let chars = [
            99, 11, 0, 2, 0, 0, 0, b'a', 0, b'b', 0, 10, 0, 2, 0, 0, 0, b'x', b'y',
        ];
        let mut expected_chars = IndexMap::new();
        expected_chars.insert("a".to_string(), K::Char(b'x'));
        expected_chars.insert("b".to_string(), K::Char(b'y'));
        assert_eq!(
            deserialize(&chars, &mut 0, SymbolEncoding::Strict).expect("char dictionary"),
            K::Dict(expected_chars)
        );

        let first = [1u8; 16];
        let second = [2u8; 16];
        let mut guids = vec![99, 11, 0, 2, 0, 0, 0, b'a', 0, b'b', 0, 2, 0, 2, 0, 0, 0];
        guids.extend(first);
        guids.extend(second);
        let mut expected_guids = IndexMap::new();
        expected_guids.insert("a".to_string(), K::Guid(Uuid::from_bytes(first)));
        expected_guids.insert("b".to_string(), K::Guid(Uuid::from_bytes(second)));
        assert_eq!(
            deserialize(&guids, &mut 0, SymbolEncoding::Strict).expect("GUID dictionary"),
            K::Dict(expected_guids)
        );

        let mismatched = [
            99, 11, 0, 2, 0, 0, 0, b'a', 0, b'b', 0, 10, 0, 1, 0, 0, 0, b'x',
        ];
        assert!(matches!(
            deserialize(&mismatched, &mut 0, SymbolEncoding::Strict),
            Err(XqdbError::DeserializationErr(_))
        ));
    }

    #[test]
    fn rejects_nul_symbols_and_dictionary_keys_during_serialization() {
        assert!(matches!(
            serialize(&K::Symbol("bad\0symbol".to_string())),
            Err(XqdbError::NotAbleToSerializeErr(_))
        ));
        let dictionary = K::Dict(IndexMap::from([("bad\0key".to_string(), K::I64(1))]));
        assert!(matches!(
            serialize(&dictionary),
            Err(XqdbError::NotAbleToSerializeErr(_))
        ));

        let categorical = Series::new("symbol".into(), &["ok", "bad\0symbol"])
            .cast(&PolarsDataType::Categorical(
                Categories::global(),
                Categories::global().mapping(),
            ))
            .expect("categorical series");
        assert!(matches!(
            serialize(&K::Series(categorical)),
            Err(XqdbError::NotAbleToSerializeErr(_))
        ));
    }

    #[test]
    fn native_timestamp_null_and_infinities_are_not_instants() {
        let mut atom = vec![244];
        atom.extend_from_slice(&i64::MIN.to_le_bytes());
        assert_eq!(
            deserialize(&atom, &mut 0, SymbolEncoding::Strict).unwrap(),
            K::Null
        );

        for sentinel in [i64::MIN + 1, i64::MAX] {
            let mut atom = vec![244];
            atom.extend_from_slice(&sentinel.to_le_bytes());
            assert!(matches!(
                deserialize(&atom, &mut 0, SymbolEncoding::Strict),
                Err(XqdbError::DeserializationErr(_))
            ));

            let mut list = vec![12, 0, 1, 0, 0, 0];
            list.extend_from_slice(&sentinel.to_le_bytes());
            assert!(matches!(
                deserialize(&list, &mut 0, SymbolEncoding::Strict),
                Err(XqdbError::DeserializationErr(_))
            ));
        }
    }

    #[test]
    fn timestamp_encoding_is_exact_at_q_boundaries() {
        let values = [MIN_Q_TIMESTAMP_UNIX_NANOS, -1, i64::MAX];
        for unix_nanoseconds in values {
            let timestamp = DateTime::from_timestamp(
                unix_nanoseconds.div_euclid(1_000_000_000),
                unix_nanoseconds.rem_euclid(1_000_000_000) as u32,
            )
            .expect("timestamp");
            let serialized = serialize(&K::DateTime(timestamp)).expect("serialize timestamp");
            let raw = i64::from_le_bytes(serialized[1..9].try_into().expect("q timestamp payload"));
            assert_eq!(raw, unix_nanoseconds - NANOS_DIFF);
            assert_eq!(
                deserialize(&serialized, &mut 0, SymbolEncoding::Strict)
                    .expect("deserialize timestamp"),
                K::DateTime(timestamp)
            );
        }

        let below = MIN_Q_TIMESTAMP_UNIX_NANOS - 1;
        let below = DateTime::from_timestamp(
            below.div_euclid(1_000_000_000),
            below.rem_euclid(1_000_000_000) as u32,
        )
        .expect("timestamp below q range");
        assert!(matches!(
            serialize(&K::DateTime(below)),
            Err(XqdbError::NotAbleToSerializeErr(_))
        ));

        let below_series = Series::from_arrow(
            "timestamp".into(),
            PrimitiveArray::new(
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                vec![MIN_Q_TIMESTAMP_UNIX_NANOS - 1].into(),
                None,
            )
            .boxed(),
        )
        .expect("timestamp series");
        assert!(matches!(
            serialize(&K::Series(below_series)),
            Err(XqdbError::NotAbleToSerializeErr(_))
        ));

        let above = DateTime::from_timestamp(i64::MAX.div_euclid(1_000_000_000) + 1, 0)
            .expect("timestamp above q range");
        assert!(matches!(
            serialize(&K::DateTime(above)),
            Err(XqdbError::NotAbleToSerializeErr(_))
        ));

        let first_unrepresentable_q_timestamp = i64::MAX - NANOS_DIFF + 1;
        let mut atom = vec![244];
        atom.extend_from_slice(&first_unrepresentable_q_timestamp.to_le_bytes());
        assert!(matches!(
            deserialize(&atom, &mut 0, SymbolEncoding::Strict),
            Err(XqdbError::DeserializationErr(_))
        ));

        let mut list = vec![12, 0, 1, 0, 0, 0];
        list.extend_from_slice(&first_unrepresentable_q_timestamp.to_le_bytes());
        assert!(matches!(
            deserialize(&list, &mut 0, SymbolEncoding::Strict),
            Err(XqdbError::DeserializationErr(_))
        ));
    }
}
