use std::fs::File;
use std::io::Cursor;
use std::io::{self, BufReader, Read};
use std::str;

use lz4_flex::frame::FrameDecoder;
use polars::frame::DataFrame;
use xxhash_rust::xxh32;

use crate::connector::{deserialize_ipc_frame, ipc_length_header};
use crate::errors::XqdbError;
use crate::qvalue::ValueMode;
use crate::serde6;
use crate::types::{MsgType, SymbolEncoding, K};

const KXZIP_MAGIC: &[u8; 8] = b"kxzipped";
const KXZIP_HEADER_LENGTH: usize = 8;
const KXZIP_FOOTER_WORD_LENGTH: usize = 8;
const KXZIP_FIXED_FOOTER_WORDS: usize = 5;
/// Upper bound on how far an LZ4 block can expand its input: a match-length continuation byte
/// costs one input byte and adds 255 output bytes, and the exact supremum `255 * L / (L + 746)`
/// stays strictly below it. Real kdb+ LZ4 ratios on tabular data are under 30:1, and even an
/// all-zero column reaches only ~254.96:1, so nothing legitimate is refused.
const MAX_LZ4_EXPANSION_RATIO: usize = 255;
/// Bytes reserved before the first read of a file whose reported length is zero, and the floor for
/// each fallible growth step.
const INITIAL_FILE_RESERVATION: usize = 64 * 1024;

fn checked_j6_ipc_total_length(body_length: usize) -> Result<(usize, u8, u32), XqdbError> {
    let total_length = body_length
        .checked_add(8)
        .ok_or_else(|| XqdbError::Err("J6 IPC message length overflowed".to_string()))?;
    let (high_byte, low_length) = ipc_length_header(total_length)?;
    Ok((total_length, high_byte, low_length))
}

/// Reads a file whose reported length is only a hint. The reservation starts from that length and
/// grows fallibly, so a file that gained bytes mid-read, or a regular file that reports zero length
/// while holding content, still reads completely without an infallible allocation or a size cap.
fn read_file_fallibly(file: File, hint: usize, description: &str) -> Result<Vec<u8>, XqdbError> {
    let mut reader = BufReader::new(file);
    let mut buffer = Vec::new();
    let grow = |buffer: &mut Vec<u8>| -> Result<(), XqdbError> {
        let target = buffer
            .capacity()
            .saturating_mul(2)
            .max(INITIAL_FILE_RESERVATION);
        buffer
            .try_reserve_exact(target - buffer.len())
            .map_err(|error| {
                XqdbError::Err(format!(
                    "Unable to allocate {target}-byte {description} buffer: {error}"
                ))
            })
    };
    if hint > 0 {
        buffer.try_reserve_exact(hint).map_err(|error| {
            XqdbError::Err(format!(
                "Unable to allocate {hint}-byte {description} buffer: {error}"
            ))
        })?;
    } else {
        grow(&mut buffer)?;
    }
    loop {
        let spare = buffer.capacity() - buffer.len();
        if spare > 0 {
            let read = (&mut reader)
                .take(spare as u64)
                .read_to_end(&mut buffer)
                .map_err(XqdbError::IOError)?;
            if read < spare {
                return Ok(buffer);
            }
        }
        // Capacity is full. Probe a single byte to tell EOF from a longer file, which keeps
        // `read_to_end` off the infallible growth path it takes when the buffer is full.
        let mut probe = [0u8; 1];
        let probed = loop {
            match reader.read(&mut probe) {
                Ok(read) => break read,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(XqdbError::IOError(error)),
            }
        };
        if probed == 0 {
            return Ok(buffer);
        }
        grow(&mut buffer)?;
        buffer.push(probe[0]);
    }
}

fn kxzip_usize(bytes: &[u8], description: &str) -> Result<usize, XqdbError> {
    let bytes: [u8; 8] = bytes.try_into().map_err(|_| {
        XqdbError::Err(format!(
            "Kxzip {description} is shorter than the required 8 bytes"
        ))
    })?;
    usize::try_from(u64::from_le_bytes(bytes)).map_err(|_| {
        XqdbError::Err(format!(
            "Kxzip {description} cannot be represented on this platform"
        ))
    })
}

fn kxzip_footer_length(block_num: usize) -> Result<usize, XqdbError> {
    block_num
        .checked_add(KXZIP_FIXED_FOOTER_WORDS)
        .and_then(|words| words.checked_mul(KXZIP_FOOTER_WORD_LENGTH))
        .ok_or_else(|| XqdbError::Err("Kxzip footer length overflowed".to_owned()))
}

fn lz4_block_size(footer: &[u8], block_index: usize) -> Result<u32, XqdbError> {
    let offset = block_index
        .checked_mul(KXZIP_FOOTER_WORD_LENGTH)
        .and_then(|offset| 32usize.checked_add(offset))
        .ok_or_else(|| XqdbError::Err("Kxzip LZ4 block metadata offset overflowed".to_owned()))?;
    let end = offset
        .checked_add(4)
        .ok_or_else(|| XqdbError::Err("Kxzip LZ4 block metadata range overflowed".to_owned()))?;
    let bytes: [u8; 4] = footer
        .get(offset..end)
        .ok_or_else(|| {
            XqdbError::Err(format!(
                "Kxzip footer is truncated at LZ4 block {block_index}"
            ))
        })?
        .try_into()
        .map_err(|_| XqdbError::Err("Invalid Kxzip LZ4 block size".to_owned()))?;
    Ok(u32::from_le_bytes(bytes))
}

fn decompression_error(error: io::Error) -> XqdbError {
    XqdbError::IOError(io::Error::new(
        error.kind(),
        format!("Failed to decompress Kxzip LZ4 data: {error}"),
    ))
}

pub fn read_j6_binary_table(path: &str, encoding: SymbolEncoding) -> Result<DataFrame, XqdbError> {
    let metadata = std::fs::metadata(path).map_err(XqdbError::IOError)?;
    if !metadata.is_file() {
        return Err(XqdbError::Err(format!(
            "Q binary table path is not a regular file: {path}"
        )));
    }
    let f = File::open(path).map_err(XqdbError::IOError)?;

    let file_length = usize::try_from(metadata.len()).map_err(|_| {
        XqdbError::Err("Q binary table file size cannot be represented on this platform".to_owned())
    })?;
    let mut buffer = read_file_fallibly(f, file_length, "Q binary table")?;
    if buffer.starts_with(KXZIP_MAGIC) {
        buffer = unzip(&buffer)?;
    }

    let k_type = buffer.get(2).copied().ok_or_else(|| {
        XqdbError::DeserializationErr(
            "Q binary table file is shorter than its 2-byte header and value type".to_owned(),
        )
    })?;
    let minimum_length = match k_type {
        98 => 17,
        99 => 33,
        _ => return Err(XqdbError::Err("Not a table".to_owned())),
    };
    if buffer.len() < minimum_length {
        return Err(XqdbError::DeserializationErr(format!(
            "Q binary table file is structurally too short: expected at least {minimum_length} bytes, got {}",
            buffer.len()
        )));
    }

    match serde6::deserialize(&buffer, &mut 2, encoding)? {
        K::DataFrame(k) => Ok(k),
        _ => Err(XqdbError::Err("Not a table".to_owned())),
    }
}

pub fn unzip(buf: &[u8]) -> Result<Vec<u8>, XqdbError> {
    if !buf.starts_with(KXZIP_MAGIC) {
        return Err(XqdbError::Err(
            "Invalid or truncated Kxzip header".to_owned(),
        ));
    }
    let block_count_start = buf
        .len()
        .checked_sub(KXZIP_FOOTER_WORD_LENGTH)
        .ok_or_else(|| XqdbError::Err("Kxzip file is missing its block count".to_owned()))?;
    let block_num = kxzip_usize(&buf[block_count_start..], "block count")?;
    let footer_length = kxzip_footer_length(block_num)?;
    let footer_index = buf.len().checked_sub(footer_length).ok_or_else(|| {
        XqdbError::Err(format!(
            "Kxzip footer length {footer_length} exceeds file length {}",
            buf.len()
        ))
    })?;
    if footer_index < KXZIP_HEADER_LENGTH {
        return Err(XqdbError::Err(
            "Kxzip file is shorter than its 8-byte header and footer".to_owned(),
        ));
    }
    let algorithm_index = footer_index
        .checked_add(4)
        .ok_or_else(|| XqdbError::Err("Kxzip compression metadata offset overflowed".to_owned()))?;
    let algorithm = buf.get(algorithm_index).copied().ok_or_else(|| {
        XqdbError::Err("Kxzip footer is missing its compression algorithm".to_owned())
    })?;
    match algorithm {
        4 => unzip_lz4(buf, footer_index, block_num),
        _ => Err(XqdbError::Err(format!(
            "Not supported compression algo - {algorithm}"
        ))),
    }
}

pub fn unzip_lz4(buf: &[u8], footer_index: usize, block_num: usize) -> Result<Vec<u8>, XqdbError> {
    if !buf.starts_with(KXZIP_MAGIC) {
        return Err(XqdbError::Err(
            "Invalid or truncated Kxzip header".to_owned(),
        ));
    }
    if footer_index < KXZIP_HEADER_LENGTH {
        return Err(XqdbError::Err(
            "Kxzip compressed data starts inside its 8-byte header".to_owned(),
        ));
    }
    let expected_footer_length = kxzip_footer_length(block_num)?;
    let footer = buf
        .get(footer_index..)
        .ok_or_else(|| XqdbError::Err("Kxzip footer offset exceeds file length".to_owned()))?;
    if footer.len() != expected_footer_length {
        return Err(XqdbError::Err(format!(
            "Kxzip footer length mismatch: expected {expected_footer_length} bytes, got {}",
            footer.len()
        )));
    }
    if footer.get(4).copied() != Some(4) {
        return Err(XqdbError::Err(
            "Kxzip footer does not describe LZ4 compression".to_owned(),
        ));
    }

    let block_size = kxzip_usize(
        footer
            .get(24..32)
            .ok_or_else(|| XqdbError::Err("Kxzip footer is missing LZ4 block size".to_owned()))?,
        "LZ4 block size",
    )?;
    if !((1usize << 12)..=(1usize << 22)).contains(&block_size) || !block_size.is_power_of_two() {
        return Err(XqdbError::Err(format!(
            "Unsupported Kxzip LZ4 logical block size {block_size}; expected a power of two from 4096 through 4194304"
        )));
    }
    // The q footer records the logical power-of-two block size, while an LZ4 frame can only
    // advertise 64 KiB, 256 KiB, 1 MiB, or 4 MiB. Use the smallest enclosing descriptor.
    let block_descriptor = match block_size {
        ..=65_536 => 0x40,
        65_537..=262_144 => 0x50,
        262_145..=1_048_576 => 0x60,
        _ => 0x70,
    };
    let unzipped_size = kxzip_usize(
        footer
            .get(8..16)
            .ok_or_else(|| XqdbError::Err("Kxzip footer is missing output size".to_owned()))?,
        "decompressed size",
    )?;

    let mut frame_capacity = 19usize;
    let mut reachable_unzipped_size = 0usize;
    let mut has_empty_block = false;
    let mut block_start = KXZIP_HEADER_LENGTH;
    for block_index in 0..block_num {
        let size = usize::try_from(lz4_block_size(footer, block_index)?).map_err(|_| {
            XqdbError::Err(format!(
                "Kxzip LZ4 block {block_index} size cannot be represented on this platform"
            ))
        })?;
        has_empty_block |= size == 0;
        let block_end = block_start.checked_add(size).ok_or_else(|| {
            XqdbError::Err(format!("Kxzip LZ4 block {block_index} range overflowed"))
        })?;
        if block_end > footer_index {
            return Err(XqdbError::Err(format!(
                "Kxzip LZ4 block {block_index} ends at {block_end}, beyond compressed data ending at {footer_index}"
            )));
        }
        frame_capacity = frame_capacity
            .checked_add(4)
            .and_then(|capacity| capacity.checked_add(size))
            .ok_or_else(|| XqdbError::Err("Kxzip LZ4 frame size overflowed".to_owned()))?;
        // A block decodes to at most `block_size` bytes and to at most 255x its own compressed
        // bytes, so the tighter of the two, summed over the blocks actually present, bounds the
        // output. Unlike a block-count product this cannot be inflated by footer metadata alone.
        reachable_unzipped_size = size
            .checked_mul(MAX_LZ4_EXPANSION_RATIO)
            .map(|expanded| expanded.min(block_size))
            .and_then(|expanded| reachable_unzipped_size.checked_add(expanded))
            .ok_or_else(|| XqdbError::Err("Kxzip output size bound overflowed".to_owned()))?;
        block_start = block_end;
    }
    if block_start != footer_index {
        return Err(XqdbError::Err(format!(
            "Kxzip data contains {} trailing compressed bytes",
            footer_index - block_start
        )));
    }
    if unzipped_size > reachable_unzipped_size {
        return Err(XqdbError::Err(format!(
            "Kxzip declares {unzipped_size} decompressed bytes, unreachable from its {} compressed bytes, which expand to at most {reachable_unzipped_size}",
            footer_index - KXZIP_HEADER_LENGTH
        )));
    }
    if has_empty_block {
        return Err(XqdbError::Err(
            "Kxzip LZ4 metadata contains an empty block".to_owned(),
        ));
    }

    let mut zipped_bytes = Vec::new();
    zipped_bytes
        .try_reserve_exact(frame_capacity)
        .map_err(|error| {
            XqdbError::Err(format!(
                "Unable to allocate {frame_capacity}-byte Kxzip LZ4 frame: {error}"
            ))
        })?;
    zipped_bytes.extend_from_slice(&[4, 34, 77, 24, 104, block_descriptor]);
    zipped_bytes.extend_from_slice(&footer[8..16]);
    let header_checksum = xxh32::xxh32(&zipped_bytes[4..], 0).to_le_bytes();
    zipped_bytes.push(header_checksum[1]);

    block_start = KXZIP_HEADER_LENGTH;
    for block_index in 0..block_num {
        let encoded_size = lz4_block_size(footer, block_index)?;
        let size = usize::try_from(encoded_size).map_err(|_| {
            XqdbError::Err(format!(
                "Kxzip LZ4 block {block_index} size cannot be represented on this platform"
            ))
        })?;
        let block_end = block_start.checked_add(size).ok_or_else(|| {
            XqdbError::Err(format!("Kxzip LZ4 block {block_index} range overflowed"))
        })?;
        let block = buf
            .get(block_start..block_end)
            .ok_or_else(|| XqdbError::Err(format!("Kxzip LZ4 block {block_index} is truncated")))?;
        zipped_bytes.extend_from_slice(&encoded_size.to_le_bytes());
        zipped_bytes.extend_from_slice(block);
        block_start = block_end;
    }
    zipped_bytes.extend_from_slice(&[0, 0, 0, 0]);

    let reader = Cursor::new(zipped_bytes);
    let mut decoder = FrameDecoder::new(reader);
    let mut unzipped_bytes = Vec::new();
    unzipped_bytes
        .try_reserve_exact(unzipped_size)
        .map_err(|error| {
            XqdbError::Err(format!(
                "Unable to allocate {unzipped_size}-byte Kxzip output: {error}"
            ))
        })?;
    unzipped_bytes.resize(unzipped_size, 0);
    decoder
        .read_exact(&mut unzipped_bytes)
        .map_err(decompression_error)?;
    let mut trailing = [0u8; 1];
    match decoder.read(&mut trailing).map_err(decompression_error)? {
        0 => {
            let reader = decoder.into_inner();
            if reader.position() != reader.get_ref().len() as u64 {
                return Err(XqdbError::Err(
                    "Kxzip LZ4 stream contains trailing encoded bytes".to_owned(),
                ));
            }
            Ok(unzipped_bytes)
        }
        _ => Err(XqdbError::Err(format!(
            "Kxzip LZ4 output exceeds declared size {unzipped_size}"
        ))),
    }
}

pub fn generate_j6_ipc_msg(
    msg_type: MsgType,
    enable_compression: bool,
    k: K,
) -> Result<Vec<u8>, XqdbError> {
    let body_length = k.j6_len()?;
    let (total_length, high_byte, low_length) = checked_j6_ipc_total_length(body_length)?;
    let mut vec = Vec::new();
    vec.try_reserve_exact(total_length).map_err(|error| {
        XqdbError::Err(format!(
            "Unable to allocate {total_length}-byte J6 IPC message: {error}"
        ))
    })?;
    vec.extend_from_slice(&[1, msg_type as u8, 0, high_byte]);
    vec.extend_from_slice(&low_length.to_le_bytes());
    serde6::serialize_into(&k, &mut vec)?;
    if vec.len() != total_length {
        return Err(XqdbError::Err(
            "Serialized q value length differs from its declared J6 length".to_string(),
        ));
    }
    if enable_compression {
        serde6::compress(vec)
    } else {
        Ok(vec)
    }
}

pub fn deserialize_j6(
    buf: &[u8],
    encoding: SymbolEncoding,
    mode: ValueMode,
) -> Result<K, XqdbError> {
    match mode {
        ValueMode::Native => serde6::deserialize(buf, &mut 0, encoding),
        ValueMode::Lossless => crate::qvalue::QValue::from_bytes(buf).map(K::QValue),
    }
}

pub fn deserialize_j6_ipc_msg(
    buf: &[u8],
    encoding: SymbolEncoding,
    mode: ValueMode,
) -> Result<(MsgType, K), XqdbError> {
    deserialize_ipc_frame(buf, encoding, mode)
}

#[cfg(test)]
mod tests {
    use polars::{
        datatypes::DataType,
        frame::DataFrame,
        prelude::{Categories, NamedFrom},
        series::Series,
    };
    use polars_arrow::array::Utf8Array;

    use super::{checked_j6_ipc_total_length, MAX_LZ4_EXPANSION_RATIO};
    use crate::{
        io,
        qvalue::ValueMode,
        serde6::{deserialize, serialize},
        types::{MsgType, SymbolEncoding, K},
    };

    fn one_block_kxzip_with_logical_size(
        block: &[u8],
        encoded_size: u32,
        unzipped_size: u64,
        logical_block_size: u64,
    ) -> Vec<u8> {
        let footer_index = 8 + block.len();
        let mut bytes = b"kxzipped".to_vec();
        bytes.extend_from_slice(block);
        let mut footer = vec![0u8; 48];
        footer[4] = 4;
        footer[8..16].copy_from_slice(&unzipped_size.to_le_bytes());
        footer[16..24].copy_from_slice(&(footer_index as u64).to_le_bytes());
        footer[24..32].copy_from_slice(&logical_block_size.to_le_bytes());
        footer[32..36].copy_from_slice(&encoded_size.to_le_bytes());
        footer[40..48].copy_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&footer);
        bytes
    }

    fn one_block_kxzip(block: &[u8], encoded_size: u32, unzipped_size: u64) -> Vec<u8> {
        one_block_kxzip_with_logical_size(block, encoded_size, unzipped_size, 65_536)
    }

    #[test]
    fn unzip_rejects_an_early_end_marker_hiding_later_blocks() {
        let hidden_block = lz4_flex::block::compress(b"hidden");
        let mut bytes = b"kxzipped".to_vec();
        bytes.extend_from_slice(&hidden_block);
        let mut footer = vec![0u8; 56];
        footer[4] = 4;
        footer[16..24].copy_from_slice(&(bytes.len() as u64).to_le_bytes());
        footer[24..32].copy_from_slice(&65_536u64.to_le_bytes());
        footer[40..44].copy_from_slice(&(hidden_block.len() as u32).to_le_bytes());
        footer[48..56].copy_from_slice(&2u64.to_le_bytes());
        bytes.extend_from_slice(&footer);

        assert!(io::unzip(&bytes).is_err());
    }

    #[test]
    fn read_binary_table_rejects_short_files() {
        let path = std::env::temp_dir().join(format!("xqdb-short-j6-{}.bin", std::process::id()));
        let cases: &[&[u8]] = &[&[], &[255], &[255, 1], &[255, 1, 98], &[255, 1, 99]];
        for bytes in cases {
            std::fs::write(&path, bytes).expect("failed to write short q binary fixture");
            let error = io::read_j6_binary_table(
                path.to_str()
                    .expect("temporary q binary fixture path is not UTF-8"),
                SymbolEncoding::Strict,
            )
            .expect_err("short q binary file should fail");
            assert!(matches!(
                error,
                crate::errors::XqdbError::DeserializationErr(_)
            ));
        }
        std::fs::remove_file(path).expect("failed to remove short q binary fixture");
    }

    #[test]
    fn read_binary_table_rejects_missing_symbol_terminator_without_panicking() {
        let path =
            std::env::temp_dir().join(format!("xqdb-missing-nul-j6-{}.bin", std::process::id()));
        let bytes = [255, 1, 98, 0, 99, 11, 0, 1, 0, 0, 0, b'x', 1, 1, 1, 1, 1];
        std::fs::write(&path, bytes).expect("failed to write missing-NUL q binary fixture");
        let error = io::read_j6_binary_table(
            path.to_str()
                .expect("temporary q binary fixture path is not UTF-8"),
            SymbolEncoding::Strict,
        )
        .expect_err("missing symbol terminator must fail");
        assert!(matches!(
            error,
            crate::errors::XqdbError::DeserializationErr(_)
        ));
        std::fs::remove_file(path).expect("failed to remove missing-NUL q binary fixture");
    }

    #[test]
    fn generate_ipc_helper_is_bounded_only_by_the_q_length_field() {
        let maximum_body = ((1usize << 40) - 1) - 8;
        let (total_length, high_byte, low_length) =
            checked_j6_ipc_total_length(maximum_body).expect("limit-sized helper frame");
        assert_eq!(total_length, (1usize << 40) - 1);
        assert_eq!(high_byte, 0xff);
        assert_eq!(low_length, u32::MAX);
        // A body above 4 GiB is framed through the 40-bit form rather than refused.
        let (_, high_byte, low_length) =
            checked_j6_ipc_total_length(4 * 1024 * 1024 * 1024).expect("body above u32::MAX");
        assert_eq!(high_byte, 1);
        assert_eq!(low_length, 8);
        assert!(checked_j6_ipc_total_length(maximum_body + 1).is_err());
        assert!(checked_j6_ipc_total_length(usize::MAX).is_err());
    }

    #[test]
    fn read_binary_table_rejects_non_file_path() {
        let error = io::read_j6_binary_table(
            std::env::temp_dir()
                .to_str()
                .expect("temporary directory path is not UTF-8"),
            SymbolEncoding::Strict,
        )
        .expect_err("directory path should fail");
        assert!(error.to_string().contains("not a regular file"));
    }

    #[test]
    fn unzip_rejects_short_header_and_footer() {
        assert!(io::unzip(&[]).is_err());
        assert!(io::unzip(b"kxzipped").is_err());
    }

    #[test]
    fn unzip_rejects_footer_length_overflow() {
        let mut bytes = b"kxzipped".to_vec();
        bytes.extend_from_slice(&u64::MAX.to_le_bytes());
        assert!(io::unzip(&bytes).is_err());
    }

    #[test]
    fn unzip_rejects_truncated_block_range() {
        let bytes = one_block_kxzip(&[], 1, 1);
        let error = io::unzip(&bytes).expect_err("truncated LZ4 block should fail");
        assert!(error.to_string().contains("beyond compressed data"));
    }

    #[test]
    fn unzip_propagates_decompressor_error() {
        let bytes = one_block_kxzip(&[0], 1, 1);
        assert!(io::unzip(&bytes).is_err());
    }

    #[test]
    fn unzip_rejects_output_unreachable_from_its_compressed_bytes() {
        // One compressed byte can expand to at most 255, so both an absurd claim and a merely
        // over-optimistic one are rejected before the output buffer is allocated.
        for declared in [u64::MAX, 65_537, MAX_LZ4_EXPANSION_RATIO as u64 + 1] {
            let bytes = one_block_kxzip(&[0], 1, declared);
            let error = io::unzip(&bytes).expect_err("over-claimed Kxzip output should fail");
            let message = error.to_string();
            assert!(
                message.contains("unreachable from its 1 compressed bytes")
                    && message.contains("at most 255"),
                "unexpected error for {declared}: {message}"
            );
        }
    }

    #[test]
    fn unzip_rejects_a_footer_inflated_output_claim() {
        // A crafted footer buys 4 MiB of permitted claim for every 8 bytes of block metadata, so a
        // block-count product is no bound at all: 1024 zero-length blocks in an 8,240-byte file
        // would otherwise admit a 4 GiB zero-filled allocation.
        let block_num = 1024usize;
        let mut bytes = b"kxzipped".to_vec();
        let mut footer = vec![0u8; (block_num + 5) * 8];
        footer[4] = 4;
        footer[8..16].copy_from_slice(&u64::from(u32::MAX).to_le_bytes());
        footer[16..24].copy_from_slice(&8u64.to_le_bytes());
        footer[24..32].copy_from_slice(&4_194_304u64.to_le_bytes());
        let block_count_start = footer.len() - 8;
        footer[block_count_start..].copy_from_slice(&(block_num as u64).to_le_bytes());
        bytes.extend_from_slice(&footer);
        assert_eq!(bytes.len(), 8240);

        let error = io::unzip(&bytes).expect_err("footer-inflated claim should fail");
        let message = error.to_string();
        assert!(
            message.contains("unreachable from its 0 compressed bytes")
                && message.contains("at most 0"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn unzip_rejects_trailing_compressed_bytes() {
        let bytes = one_block_kxzip(&[0, 0], 1, 1);
        let error = io::unzip(&bytes).expect_err("trailing compressed bytes should fail");
        assert!(error.to_string().contains("trailing compressed bytes"));
    }

    #[test]
    fn deserialize_j6_rejects_trailing_bytes() {
        let mut bytes = serialize(&K::I32(42)).expect("test value should serialize");
        bytes.push(0);
        let error = io::deserialize_j6(&bytes, SymbolEncoding::Strict, ValueMode::Native)
            .expect_err("trailing J6 bytes should fail");
        assert!(error.to_string().contains("trailing byte"));
    }

    #[test]
    fn read_binary_table_rejects_trailing_bytes() {
        let table =
            DataFrame::new_infer_height(vec![Series::new("value".into(), [1i32].as_ref()).into()])
                .expect("test table should build");
        let mut bytes = vec![255, 1];
        bytes.extend_from_slice(
            &serialize(&K::DataFrame(table)).expect("test table should serialize"),
        );
        bytes.push(0);
        let path =
            std::env::temp_dir().join(format!("xqdb-trailing-j6-{}.bin", std::process::id()));
        std::fs::write(&path, bytes).expect("failed to write trailing-byte q binary fixture");
        let error = io::read_j6_binary_table(
            path.to_str()
                .expect("temporary q binary fixture path is not UTF-8"),
            SymbolEncoding::Strict,
        )
        .expect_err("trailing q binary file bytes should fail");
        assert!(error.to_string().contains("trailing byte"));
        std::fs::remove_file(path).expect("failed to remove q binary fixture");
    }

    #[test]
    fn unzip_lz4() {
        let zipped: Vec<u8> = [
            107, 120, 122, 105, 112, 112, 101, 100, 241, 11, 255, 1, 98, 0, 99, 11, 0, 3, 0, 0, 0,
            115, 121, 109, 0, 113, 116, 121, 0, 112, 114, 105, 99, 101, 0, 0, 20, 0, 177, 11, 0, 2,
            0, 0, 0, 97, 0, 98, 0, 7, 10, 0, 34, 1, 0, 1, 0, 4, 8, 0, 17, 9, 22, 0, 2, 1, 0, 160,
            240, 63, 0, 0, 0, 0, 0, 0, 240, 63, 3, 0, 0, 0, 4, 6, 0, 0, 85, 0, 0, 0, 0, 0, 0, 0,
            78, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 70, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0,
            0, 0, 0, 0,
        ]
        .to_vec();
        let unzipped: Vec<u8> = [
            255, 1, 98, 0, 99, 11, 0, 3, 0, 0, 0, 115, 121, 109, 0, 113, 116, 121, 0, 112, 114,
            105, 99, 101, 0, 0, 0, 3, 0, 0, 0, 11, 0, 2, 0, 0, 0, 97, 0, 98, 0, 7, 0, 2, 0, 0, 0,
            1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 9, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            240, 63, 0, 0, 0, 0, 0, 0, 240, 63,
        ]
        .to_vec();
        assert_eq!(io::unzip(&zipped).unwrap(), unzipped);
        let k = deserialize(&unzipped, &mut 2, SymbolEncoding::Strict).unwrap();
        let df: DataFrame = k.try_into().unwrap();
        let sym = Series::from_arrow(
            "sym".into(),
            Utf8Array::<i64>::from([Some("a"), Some("b")]).boxed(),
        )
        .unwrap()
        .cast(&DataType::Categorical(
            Categories::global(),
            Categories::global().mapping(),
        ))
        .unwrap();
        let qty = Series::new("qty".into(), [1i64, 1].as_ref());
        let price = Series::new("price".into(), [1.0f64, 1.0].as_ref());
        let expect =
            DataFrame::new_infer_height(vec![sym.into(), qty.into(), price.into()]).unwrap();
        assert_eq!(df, expect);
    }
    #[test]
    fn deserialize_ipc_message_retains_kind_and_validates_exact_framing() {
        for (kind, value) in [
            (MsgType::Async, K::I32(1)),
            (MsgType::Sync, K::I32(2)),
            (MsgType::Response, K::I32(3)),
        ] {
            let frame =
                io::generate_j6_ipc_msg(kind, false, value).expect("valid in-memory IPC frame");
            let (decoded_kind, decoded) =
                io::deserialize_j6_ipc_msg(&frame, SymbolEncoding::Strict, ValueMode::Native)
                    .expect("valid frame should decode");
            assert!(matches!(
                (decoded_kind, decoded),
                (MsgType::Async, K::I32(1))
                    | (MsgType::Sync, K::I32(2))
                    | (MsgType::Response, K::I32(3))
            ));
        }

        let valid =
            io::generate_j6_ipc_msg(MsgType::Response, false, K::I32(42)).expect("valid frame");
        for (index, invalid) in [(0usize, 0u8), (0, 2), (1, 3), (2, 3)] {
            let mut frame = valid.clone();
            frame[index] = invalid;
            assert!(
                io::deserialize_j6_ipc_msg(&frame, SymbolEncoding::Strict, ValueMode::Native,)
                    .is_err(),
                "invalid header byte {index} must fail"
            );
        }
        assert!(io::deserialize_j6_ipc_msg(
            &valid[..valid.len() - 1],
            SymbolEncoding::Strict,
            ValueMode::Native,
        )
        .is_err());
        let mut trailing = valid;
        trailing.push(0);
        assert!(
            io::deserialize_j6_ipc_msg(&trailing, SymbolEncoding::Strict, ValueMode::Native,)
                .is_err()
        );
    }

    #[test]
    fn deserialize_ipc_message_supports_compressed_and_lossless_values() {
        let body = serialize(&K::CharVector(vec![b'x'; 4096])).expect("value body");
        let frame =
            io::generate_j6_ipc_msg(MsgType::Response, true, K::CharVector(vec![b'x'; 4096]))
                .expect("compressed frame");
        assert_ne!(frame[2], 0);

        let (kind, native) =
            io::deserialize_j6_ipc_msg(&frame, SymbolEncoding::Strict, ValueMode::Native)
                .expect("compressed native value");
        assert!(matches!(kind, MsgType::Response));
        assert_eq!(native, K::CharVector(vec![b'x'; 4096]));

        let (kind, lossless) =
            io::deserialize_j6_ipc_msg(&frame, SymbolEncoding::Strict, ValueMode::Lossless)
                .expect("compressed lossless value");
        assert!(matches!(kind, MsgType::Response));
        match lossless {
            K::QValue(value) => assert_eq!(value.as_bytes(), body),
            value => panic!("expected lossless q value, got {value:?}"),
        }
    }

    #[test]
    fn lz4_accepts_q_logical_block_exponents_17_and_19() {
        let raw = vec![b'q'; 4096];
        let block = lz4_flex::block::compress(&raw);
        for exponent in [12u32, 17, 19, 22] {
            let file = one_block_kxzip_with_logical_size(
                &block,
                u32::try_from(block.len()).unwrap(),
                raw.len() as u64,
                1u64 << exponent,
            );
            assert_eq!(
                io::unzip(&file).expect("legal q LZ4 logical block size"),
                raw
            );
        }
    }

    #[test]
    fn lz4_rejects_non_power_of_two_and_out_of_range_logical_sizes() {
        let raw = vec![b'q'; 4096];
        let block = lz4_flex::block::compress(&raw);
        for logical_size in [(1u64 << 12) - 1, (1u64 << 17) + 1, 1u64 << 23] {
            let file = one_block_kxzip_with_logical_size(
                &block,
                u32::try_from(block.len()).unwrap(),
                raw.len() as u64,
                logical_size,
            );
            assert!(
                io::unzip(&file).is_err(),
                "invalid logical size {logical_size} must fail"
            );
        }
    }
}
