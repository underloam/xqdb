use std::{fmt, sync::Arc};

use crate::{
    errors::XqdbError,
    types::{QOperator, K_TYPE_SIZE, MAX_VALUE_DEPTH},
};

/// Controls whether q values are converted to the convenient native model or retained byte-for-byte.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ValueMode {
    #[default]
    Native,
    Lossless,
}

/// An immutable, structurally validated q IPC value body.
///
/// The bytes do not include the eight-byte IPC message header. Clones share the backing allocation,
/// while [`QValue::from_owned_bytes`] adopts its input allocation without copying it.
#[derive(Clone, Eq, PartialEq)]
pub struct QValue {
    bytes: Arc<Vec<u8>>,
    type_code: i16,
    len: usize,
    max_depth: usize,
    is_table: bool,
}

impl fmt::Debug for QValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QValue")
            .field("type_code", &self.type_code)
            .field("len", &self.len)
            .field("is_table", &self.is_table)
            .field("body_bytes", &self.bytes.len())
            .finish()
    }
}

impl QValue {
    /// Copies and validates one complete q IPC value body.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, XqdbError> {
        let metadata = validate(bytes)?;
        let mut owned = Vec::new();
        owned.try_reserve_exact(bytes.len()).map_err(|error| {
            XqdbError::DeserializationErr(format!(
                "unable to allocate {}-byte lossless q value: {error}",
                bytes.len()
            ))
        })?;
        owned.extend_from_slice(bytes);
        Ok(Self::from_validated(owned, metadata))
    }

    /// Adopts and validates one complete q IPC value body without copying its payload.
    pub fn from_owned_bytes(bytes: Vec<u8>) -> Result<Self, XqdbError> {
        let metadata = validate(&bytes)?;
        Ok(Self::from_validated(bytes, metadata))
    }

    fn from_validated(bytes: Vec<u8>, metadata: Metadata) -> Self {
        Self {
            bytes: Arc::new(bytes),
            type_code: metadata.type_code,
            len: metadata.len,
            is_table: metadata.is_table,
            max_depth: metadata.max_depth,
        }
    }

    /// Returns the complete q IPC value body, without a frame header.
    pub fn as_bytes(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    /// Returns an owned value body, moving the original allocation when this is the only owner.
    pub fn into_bytes(self) -> Result<Vec<u8>, XqdbError> {
        match Arc::try_unwrap(self.bytes) {
            Ok(bytes) => Ok(bytes),
            Err(shared) => {
                let mut bytes = Vec::new();
                bytes.try_reserve_exact(shared.len()).map_err(|error| {
                    XqdbError::Err(format!(
                        "unable to allocate {}-byte lossless q value copy: {error}",
                        shared.len()
                    ))
                })?;
                bytes.extend_from_slice(shared.as_slice());
                Ok(bytes)
            }
        }
    }

    /// Returns q's signed atom type code or positive container/function type code.
    pub const fn type_code(&self) -> i16 {
        self.type_code
    }

    /// Returns q's logical count: one for a scalar, the element count for a list, the key count for
    /// a dictionary, or the row count for a table/keyed table.
    pub const fn len(&self) -> usize {
        self.len
    }

    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns whether this value has q table shape, including keyed tables represented by a
    /// type-99 dictionary whose key and value are both tables.
    pub const fn is_table(&self) -> bool {
        self.is_table
    }

    /// Constructs a q atom from a positive atom kind and its raw little-endian payload.
    /// Symbol payloads include their terminating NUL byte.
    pub fn atom(kind: u8, payload: &[u8]) -> Result<Self, XqdbError> {
        if !(1..=19).contains(&kind) || kind == 3 {
            return Err(XqdbError::NotAbleToSerializeErr(format!(
                "unsupported q atom kind {kind}; expected 1..=19 excluding reserved kind 3"
            )));
        }
        if kind == 11 {
            if payload.last() != Some(&0) || payload[..payload.len().saturating_sub(1)].contains(&0)
            {
                return Err(XqdbError::NotAbleToSerializeErr(
                    "q symbol atom payload must contain exactly one terminating NUL".to_string(),
                ));
            }
        } else {
            let expected = K_TYPE_SIZE[kind as usize];
            if payload.len() != expected {
                return Err(XqdbError::NotAbleToSerializeErr(format!(
                    "q atom kind {kind} requires {expected} payload byte(s), got {}",
                    payload.len()
                )));
            }
            if kind == 1 && payload[0] > 1 {
                return Err(XqdbError::NotAbleToSerializeErr(format!(
                    "q boolean atom must be 0 or 1, got {}",
                    payload[0]
                )));
            }
        }
        let body_length = payload
            .len()
            .checked_add(1)
            .ok_or(XqdbError::OverLengthErr())?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(body_length).map_err(|error| {
            XqdbError::NotAbleToSerializeErr(format!(
                "unable to allocate {body_length}-byte q atom: {error}"
            ))
        })?;
        bytes.push(0u8.wrapping_sub(kind));
        bytes.extend_from_slice(payload);
        Self::from_owned_bytes(bytes)
    }

    /// Constructs a general q list without changing any child value representation.
    pub fn list(values: &[QValue]) -> Result<Self, XqdbError> {
        let count = i32::try_from(values.len()).map_err(|_| XqdbError::OverLengthErr())?;
        if values
            .iter()
            .any(|value| value.max_depth() >= MAX_VALUE_DEPTH)
        {
            return Err(XqdbError::NotAbleToSerializeErr(format!(
                "q value nesting exceeds {MAX_VALUE_DEPTH} levels"
            )));
        }
        let body_length = values.iter().try_fold(6usize, |length, value| {
            length
                .checked_add(value.as_bytes().len())
                .ok_or(XqdbError::OverLengthErr())
        })?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(body_length).map_err(|error| {
            XqdbError::NotAbleToSerializeErr(format!(
                "unable to allocate {body_length}-byte q general list: {error}"
            ))
        })?;
        bytes.extend_from_slice(&[0, 0]);
        bytes.extend_from_slice(&count.to_le_bytes());
        for value in values {
            bytes.extend_from_slice(value.as_bytes());
        }
        Self::from_owned_bytes(bytes)
    }

    /// Constructs a q dictionary, preserving arbitrary key values and duplicate entries.
    /// A keyed table is formed when both inputs are tables.
    pub fn dictionary(keys: &QValue, values: &QValue) -> Result<Self, XqdbError> {
        let keyed_table = keys.type_code() == 98 && values.type_code() == 98;
        if (keys.type_code() == 98 || values.type_code() == 98) && !keyed_table {
            return Err(XqdbError::NotAbleToSerializeErr(
                "q keyed-table dictionary requires a table on both sides".to_string(),
            ));
        }
        if !keyed_table
            && (!is_sequence_type(keys.type_code()) || !is_sequence_type(values.type_code()))
        {
            return Err(XqdbError::NotAbleToSerializeErr(
                "q dictionary keys and values must be lists, or both must be tables".to_string(),
            ));
        }
        if keys.len() != values.len() {
            return Err(XqdbError::NotAbleToSerializeErr(format!(
                "q dictionary key/value count mismatch: {} and {}",
                keys.len(),
                values.len()
            )));
        }
        if keys.max_depth() >= MAX_VALUE_DEPTH || values.max_depth() >= MAX_VALUE_DEPTH {
            return Err(XqdbError::NotAbleToSerializeErr(format!(
                "q value nesting exceeds {MAX_VALUE_DEPTH} levels"
            )));
        }
        let body_length = 1usize
            .checked_add(keys.as_bytes().len())
            .and_then(|length| length.checked_add(values.as_bytes().len()))
            .ok_or(XqdbError::OverLengthErr())?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(body_length).map_err(|error| {
            XqdbError::NotAbleToSerializeErr(format!(
                "unable to allocate {body_length}-byte q dictionary: {error}"
            ))
        })?;
        bytes.push(99);
        bytes.extend_from_slice(keys.as_bytes());
        bytes.extend_from_slice(values.as_bytes());
        Self::from_owned_bytes(bytes)
    }

    pub(crate) const fn max_depth(&self) -> usize {
        self.max_depth
    }
}

#[derive(Clone, Copy)]
struct Metadata {
    type_code: i16,
    len: usize,
    is_table: bool,
    max_depth: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Shape {
    Atom,
    Sequence { element_type: Option<u8> },
    Dictionary { keyed_table: bool },
    Table,
    Function,
}

#[derive(Clone, Copy)]
struct Parsed {
    type_code: i16,
    len: usize,
    shape: Shape,
}

impl Parsed {
    fn sequence_len(self) -> Option<usize> {
        match self.shape {
            Shape::Sequence { .. } => Some(self.len),
            _ => None,
        }
    }
}

fn validate(bytes: &[u8]) -> Result<Metadata, XqdbError> {
    let mut parser = Parser {
        bytes,
        position: 0,
        max_depth: 0,
    };
    let value = parser.parse_value(0)?;
    if parser.position != bytes.len() {
        return Err(XqdbError::DeserializationErr(format!(
            "q value has {} trailing byte(s)",
            bytes.len() - parser.position
        )));
    }
    Ok(Metadata {
        type_code: value.type_code,
        len: value.len,
        max_depth: parser.max_depth,
        is_table: matches!(
            value.shape,
            Shape::Table | Shape::Dictionary { keyed_table: true }
        ),
    })
}

struct Parser<'a> {
    bytes: &'a [u8],
    position: usize,
    max_depth: usize,
}

impl Parser<'_> {
    fn parse_value(&mut self, depth: usize) -> Result<Parsed, XqdbError> {
        self.note_depth(depth)?;
        let wire_type = self.take_byte("q value type")?;
        match wire_type {
            237..=255 => self.parse_atom(wire_type),
            0 => self.parse_general_list(depth),
            1..=19 => self.parse_typed_list(wire_type),
            98 => self.parse_table(depth),
            99 | 127 => self.parse_dictionary(wire_type, depth),
            100 => self.parse_lambda(depth),
            101..=103 => self.parse_primitive(wire_type),
            // KX's reference Java IPC reader encodes projection and composition as a direct i32
            // count followed by that many complete values, and derived functions as one value.
            104..=105 => self.parse_counted_function(wire_type, depth),
            106..=111 => self.parse_derived_function(wire_type, depth),
            112..=126 => Err(XqdbError::DeserializationErr(format!(
                "unsupported q function type {wire_type}: its layout is not accepted without a source-verified definition"
            ))),
            128 => {
                let message = self.take_until_nul("q error message")?;
                Err(XqdbError::ServerErr(try_lossy_text(
                    message,
                    "q error message",
                )?))
            }
            unsupported => Err(XqdbError::DeserializationErr(format!(
                "unsupported q value type {unsupported}"
            ))),
        }
    }

    fn parse_atom(&mut self, wire_type: u8) -> Result<Parsed, XqdbError> {
        let kind = 0u8.wrapping_sub(wire_type);
        if kind == 3 || !(1..=19).contains(&kind) {
            return Err(XqdbError::DeserializationErr(format!(
                "unsupported q atom kind {kind}"
            )));
        }
        if kind == 11 {
            self.take_until_nul("q symbol atom")?;
        } else {
            let width = K_TYPE_SIZE[kind as usize];
            let payload = self.take(width, "q atom payload")?;
            if kind == 1 {
                validate_boolean_bytes(payload, "q boolean atom")?;
            }
        }
        Ok(Parsed {
            type_code: -(i16::from(kind)),
            len: 1,
            shape: Shape::Atom,
        })
    }

    fn parse_typed_list(&mut self, element_type: u8) -> Result<Parsed, XqdbError> {
        if element_type == 3 {
            return Err(XqdbError::DeserializationErr(
                "q list element type 3 is reserved".to_string(),
            ));
        }
        let count = self.take_typed_list_count("q typed list")?;
        if element_type == 11 {
            for _ in 0..count {
                self.take_until_nul("q symbol-list element")?;
            }
        } else {
            let width = K_TYPE_SIZE[element_type as usize];
            let payload_length = count.checked_mul(width).ok_or_else(|| {
                XqdbError::DeserializationErr("q typed-list payload length overflowed".to_string())
            })?;
            let payload = self.take(payload_length, "q typed-list payload")?;
            if element_type == 1 {
                validate_boolean_bytes(payload, "q boolean list")?;
            }
        }
        Ok(Parsed {
            type_code: i16::from(element_type),
            len: count,
            shape: Shape::Sequence {
                element_type: Some(element_type),
            },
        })
    }

    fn parse_general_list(&mut self, depth: usize) -> Result<Parsed, XqdbError> {
        self.take_attribute("q general list")?;
        let count = self.take_count("q general list")?;
        if count > self.bytes.len().saturating_sub(self.position) / 2 {
            return Err(XqdbError::DeserializationErr(format!(
                "q general-list count {count} exceeds the remaining payload"
            )));
        }
        for _ in 0..count {
            self.parse_value(depth + 1)?;
        }
        Ok(Parsed {
            type_code: 0,
            len: count,
            shape: Shape::Sequence { element_type: None },
        })
    }

    fn parse_dictionary(&mut self, wire_type: u8, depth: usize) -> Result<Parsed, XqdbError> {
        let keys = self.parse_value(depth + 1)?;
        let values = self.parse_value(depth + 1)?;
        let (len, keyed_table) = match (keys.shape, values.shape) {
            (Shape::Table, Shape::Table) => {
                ensure_matching_counts(keys.len, values.len, "keyed-table row")?;
                (keys.len, true)
            }
            (Shape::Table, _) | (_, Shape::Table) => {
                return Err(XqdbError::DeserializationErr(
                    "q keyed-table dictionary must contain a table on both sides".to_string(),
                ))
            }
            _ => {
                let key_count = keys.sequence_len().ok_or_else(|| {
                    XqdbError::DeserializationErr(
                        "q dictionary keys must be a list or table".to_string(),
                    )
                })?;
                let value_count = values.sequence_len().ok_or_else(|| {
                    XqdbError::DeserializationErr(
                        "q dictionary values must be a list or table".to_string(),
                    )
                })?;
                ensure_matching_counts(key_count, value_count, "dictionary key/value")?;
                (key_count, false)
            }
        };
        Ok(Parsed {
            type_code: i16::from(wire_type),
            len,
            shape: Shape::Dictionary { keyed_table },
        })
    }

    fn parse_table(&mut self, depth: usize) -> Result<Parsed, XqdbError> {
        self.take_attribute("q table")?;
        self.note_depth(depth + 1)?;
        let dictionary_type = self.take_byte("q table dictionary type")?;
        if dictionary_type != 99 {
            return Err(XqdbError::DeserializationErr(format!(
                "q table must contain a type-99 column dictionary, got type {dictionary_type}"
            )));
        }

        let names = self.parse_value(depth + 2)?;
        if names.shape
            != (Shape::Sequence {
                element_type: Some(11),
            })
        {
            return Err(XqdbError::DeserializationErr(
                "q table column names must be a symbol list".to_string(),
            ));
        }

        self.note_depth(depth + 2)?;
        let columns_type = self.take_byte("q table columns type")?;
        if columns_type != 0 {
            return Err(XqdbError::DeserializationErr(format!(
                "q table columns must be a general list, got type {columns_type}"
            )));
        }
        self.take_attribute("q table column list")?;
        let column_count = self.take_count("q table column list")?;
        ensure_matching_counts(names.len, column_count, "table name/column")?;
        if column_count > self.bytes.len().saturating_sub(self.position) / 6 {
            return Err(XqdbError::DeserializationErr(format!(
                "q table column count {column_count} exceeds the remaining payload"
            )));
        }

        let mut row_count = None;
        for _ in 0..column_count {
            let column = self.parse_value(depth + 3)?;
            let column_len = column.sequence_len().ok_or_else(|| {
                XqdbError::DeserializationErr(
                    "q table columns must each be list values".to_string(),
                )
            })?;
            match row_count {
                Some(expected) => ensure_matching_counts(expected, column_len, "table column row")?,
                None => row_count = Some(column_len),
            }
        }

        Ok(Parsed {
            type_code: 98,
            len: row_count.unwrap_or(0),
            shape: Shape::Table,
        })
    }

    fn parse_lambda(&mut self, depth: usize) -> Result<Parsed, XqdbError> {
        self.take_until_nul("q lambda context")?;
        self.note_depth(depth + 1)?;
        let source_type = self.take_byte("q lambda source type")?;
        if source_type != 10 {
            return Err(XqdbError::DeserializationErr(format!(
                "q lambda source must be a type-10 character vector, got type {source_type}"
            )));
        }
        let attribute = self.take_byte("q lambda source attribute")?;
        if attribute != 0 {
            return Err(XqdbError::DeserializationErr(format!(
                "q lambda source has unsupported attribute {attribute}"
            )));
        }
        let source_length = self.take_count("q lambda source")?;
        self.take(source_length, "q lambda source")?;
        Ok(Parsed {
            type_code: 100,
            len: 1,
            shape: Shape::Function,
        })
    }

    fn parse_primitive(&mut self, wire_type: u8) -> Result<Parsed, XqdbError> {
        let opcode = self.take_byte("q primitive opcode")?;
        if !(wire_type == 101 && matches!(opcode, 0 | 255))
            && QOperator::from_wire(wire_type, opcode).is_none()
        {
            return Err(XqdbError::DeserializationErr(format!(
                "unsupported q primitive type {wire_type} opcode {opcode}"
            )));
        }
        Ok(Parsed {
            type_code: i16::from(wire_type),
            len: 1,
            shape: Shape::Function,
        })
    }

    fn parse_counted_function(&mut self, wire_type: u8, depth: usize) -> Result<Parsed, XqdbError> {
        let name = if wire_type == 104 {
            "q projection"
        } else {
            "q composition"
        };
        let count = self.take_count(name)?;
        if count > self.bytes.len().saturating_sub(self.position) / 2 {
            return Err(XqdbError::DeserializationErr(format!(
                "{name} value count {count} exceeds the remaining payload"
            )));
        }
        for _ in 0..count {
            self.parse_value(depth + 1)?;
        }
        Ok(Parsed {
            type_code: i16::from(wire_type),
            len: 1,
            shape: Shape::Function,
        })
    }

    fn parse_derived_function(&mut self, wire_type: u8, depth: usize) -> Result<Parsed, XqdbError> {
        self.parse_value(depth + 1)?;
        Ok(Parsed {
            type_code: i16::from(wire_type),
            len: 1,
            shape: Shape::Function,
        })
    }

    fn note_depth(&mut self, depth: usize) -> Result<(), XqdbError> {
        if depth > MAX_VALUE_DEPTH {
            return Err(XqdbError::DeserializationErr(format!(
                "q value nesting exceeds {MAX_VALUE_DEPTH} levels"
            )));
        }
        self.max_depth = self.max_depth.max(depth);
        Ok(())
    }

    fn take_attribute(&mut self, context: &str) -> Result<u8, XqdbError> {
        let attribute = self.take_byte(context)?;
        if attribute > 4 {
            return Err(XqdbError::DeserializationErr(format!(
                "{context} has invalid attribute {attribute}"
            )));
        }
        Ok(attribute)
    }

    fn take_typed_list_count(&mut self, context: &str) -> Result<usize, XqdbError> {
        let raw_attribute = self.take_byte(context)?;
        let attribute = raw_attribute & 0x7f;
        if attribute > 4 {
            return Err(XqdbError::DeserializationErr(format!(
                "{context} has invalid attribute {attribute}"
            )));
        }
        if raw_attribute & 0x80 == 0 {
            return self.take_count(context);
        }
        let raw = i64::from_le_bytes(
            self.take(8, context)?
                .try_into()
                .expect("eight-byte extended count slice"),
        );
        usize::try_from(raw).map_err(|_| {
            XqdbError::DeserializationErr(format!(
                "{context} extended count cannot be negative or exceed this platform's limits"
            ))
        })
    }

    fn take_count(&mut self, context: &str) -> Result<usize, XqdbError> {
        let raw = i32::from_le_bytes(
            self.take(4, context)?
                .try_into()
                .expect("four-byte count slice"),
        );
        usize::try_from(raw).map_err(|_| {
            XqdbError::DeserializationErr(format!("{context} count cannot be negative"))
        })
    }

    fn take_until_nul(&mut self, context: &str) -> Result<&[u8], XqdbError> {
        let tail = self.bytes.get(self.position..).ok_or_else(|| {
            XqdbError::DeserializationErr(format!("{context} starts beyond the available payload"))
        })?;
        let length = memchr::memchr(0, tail).ok_or_else(|| {
            XqdbError::DeserializationErr(format!("{context} is not NUL-terminated"))
        })?;
        let start = self.position;
        let end = start
            .checked_add(length)
            .ok_or_else(|| XqdbError::DeserializationErr(format!("{context} length overflowed")))?;
        self.position = end.checked_add(1).ok_or_else(|| {
            XqdbError::DeserializationErr(format!("{context} terminator offset overflowed"))
        })?;
        Ok(&self.bytes[start..end])
    }

    fn take_byte(&mut self, context: &str) -> Result<u8, XqdbError> {
        Ok(self.take(1, context)?[0])
    }

    fn take(&mut self, length: usize, context: &str) -> Result<&[u8], XqdbError> {
        let end = self.position.checked_add(length).ok_or_else(|| {
            XqdbError::DeserializationErr(format!("{context} byte range overflowed"))
        })?;
        let value = self.bytes.get(self.position..end).ok_or_else(|| {
            XqdbError::DeserializationErr(format!(
                "{context} needs {length} byte(s), but the value body ends early"
            ))
        })?;
        self.position = end;
        Ok(value)
    }
}

fn is_sequence_type(type_code: i16) -> bool {
    (0..=19).contains(&type_code) && type_code != 3
}

fn try_lossy_text(bytes: &[u8], context: &str) -> Result<String, XqdbError> {
    let length = bytes.utf8_chunks().fold(0usize, |length, chunk| {
        let replacement_length = if chunk.invalid().is_empty() {
            0
        } else {
            '\u{FFFD}'.len_utf8()
        };
        length
            .saturating_add(chunk.valid().len())
            .saturating_add(replacement_length)
    });
    let mut text = String::new();
    text.try_reserve_exact(length).map_err(|error| {
        XqdbError::DeserializationErr(format!(
            "unable to allocate {length}-byte {context}: {error}"
        ))
    })?;
    for chunk in bytes.utf8_chunks() {
        text.push_str(chunk.valid());
        if !chunk.invalid().is_empty() {
            text.push('\u{FFFD}');
        }
    }
    Ok(text)
}

fn ensure_matching_counts(left: usize, right: usize, context: &str) -> Result<(), XqdbError> {
    if left == right {
        Ok(())
    } else {
        Err(XqdbError::DeserializationErr(format!(
            "q {context} count mismatch: {left} and {right}"
        )))
    }
}

fn validate_boolean_bytes(bytes: &[u8], context: &str) -> Result<(), XqdbError> {
    if let Some(value) = bytes.iter().find(|value| **value > 1) {
        return Err(XqdbError::DeserializationErr(format!(
            "{context} must contain only 0 or 1, got {value}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn typed_list(element_type: u8, attribute: u8, count: i32, payload: &[u8]) -> QValue {
        let mut bytes = vec![element_type, attribute];
        bytes.extend_from_slice(&count.to_le_bytes());
        bytes.extend_from_slice(payload);
        QValue::from_owned_bytes(bytes).expect("valid typed list")
    }

    #[test]
    fn raw_sentinel_atoms_round_trip_exactly() {
        for (kind, payload) in [
            (12, i64::MIN.to_le_bytes().to_vec()),
            (12, (i64::MIN + 1).to_le_bytes().to_vec()),
            (12, i64::MAX.to_le_bytes().to_vec()),
            (13, i32::MIN.to_le_bytes().to_vec()),
            (13, (i32::MIN + 1).to_le_bytes().to_vec()),
            (13, i32::MAX.to_le_bytes().to_vec()),
            (15, f64::NAN.to_le_bytes().to_vec()),
            (15, f64::NEG_INFINITY.to_le_bytes().to_vec()),
            (15, f64::INFINITY.to_le_bytes().to_vec()),
            (16, i64::MIN.to_le_bytes().to_vec()),
            (16, (i64::MIN + 1).to_le_bytes().to_vec()),
            (16, i64::MAX.to_le_bytes().to_vec()),
        ] {
            let value = QValue::atom(kind, &payload).expect("valid sentinel atom");
            let mut expected = vec![0u8.wrapping_sub(kind)];
            expected.extend_from_slice(&payload);
            assert_eq!(value.as_bytes(), expected);
            assert_eq!(QValue::from_bytes(&expected).unwrap(), value);
            assert_eq!(value.type_code(), -(i16::from(kind)));
            assert_eq!(value.len(), 1);
        }
    }

    #[test]
    fn raw_vectors_keep_attributes_and_month_payloads() {
        let months = [i32::MIN, -1, i32::MAX]
            .into_iter()
            .flat_map(i32::to_le_bytes)
            .collect::<Vec<_>>();
        let value = typed_list(13, 3, 3, &months);
        assert_eq!(value.as_bytes()[..6], [13, 3, 3, 0, 0, 0]);
        assert_eq!(value.type_code(), 13);
        assert_eq!(value.len(), 3);
        assert_eq!(QValue::from_bytes(value.as_bytes()).unwrap(), value);
    }

    #[test]
    fn raw_dictionary_preserves_duplicate_arbitrary_keys() {
        let keys = typed_list(6, 0, 2, &[7i32.to_le_bytes(), 7i32.to_le_bytes()].concat());
        let values = typed_list(11, 0, 2, b"first\0second\0");
        let dictionary = QValue::dictionary(&keys, &values).expect("valid dictionary");
        let mut expected = vec![99];
        expected.extend_from_slice(keys.as_bytes());
        expected.extend_from_slice(values.as_bytes());
        assert_eq!(dictionary.as_bytes(), expected);
        assert_eq!(dictionary.type_code(), 99);
        assert_eq!(dictionary.len(), 2);
        assert!(!dictionary.is_table());
        assert_eq!(QValue::from_bytes(&expected).unwrap(), dictionary);
    }

    #[test]
    fn raw_sorted_dictionary_preserves_type_and_table_shape() {
        let ordinary = [
            127, 11, 1, 2, 0, 0, 0, b'a', 0, b'b', 0, 6, 0, 2, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0,
        ];
        let value = QValue::from_bytes(&ordinary).expect("valid sorted dictionary");
        assert_eq!(value.as_bytes(), ordinary);
        assert_eq!(value.type_code(), 127);
        assert_eq!(value.len(), 2);
        assert!(!value.is_table());
    }

    #[test]
    fn raw_keyed_table_round_trips_without_flattening() {
        let body = [
            99, 98, 0, 99, 11, 0, 1, 0, 0, 0, b'a', 0, 0, 0, 1, 0, 0, 0, 9, 0, 1, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 240, 63, 98, 0, 99, 11, 0, 1, 0, 0, 0, b'b', 0, 0, 0, 1, 0, 0, 0, 9, 0, 1, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 240, 63,
        ];
        let value = QValue::from_bytes(&body).expect("valid keyed table");
        assert_eq!(value.type_code(), 99);
        assert_eq!(value.len(), 1);
        assert!(value.is_table());
        let mut sorted = body;
        sorted[0] = 127;
        let sorted = QValue::from_bytes(&sorted).expect("valid sorted keyed table");
        assert_eq!(sorted.type_code(), 127);
        assert_eq!(sorted.len(), 1);
        assert!(sorted.is_table());
        let table = QValue::from_bytes(&body[1..32]).expect("valid ordinary table");
        assert!(table.is_table());
        assert_eq!(value.as_bytes(), body);
        assert_eq!(value.into_bytes().unwrap(), body);
    }

    #[test]
    fn raw_typed_vectors_preserve_extended_count_headers() {
        let mut body = vec![6, 0x83];
        body.extend_from_slice(&2i64.to_le_bytes());
        body.extend_from_slice(&42i32.to_le_bytes());
        body.extend_from_slice(&43i32.to_le_bytes());
        let value = QValue::from_bytes(&body).expect("valid extended-count int vector");
        assert_eq!(value.as_bytes(), body);
        assert_eq!(value.type_code(), 6);
        assert_eq!(value.len(), 2);
    }

    #[test]
    fn raw_general_list_constructor_keeps_child_bodies_exact() {
        let null = QValue::from_bytes(&[101, 0]).unwrap();
        let timestamp = QValue::atom(12, &i64::MIN.to_le_bytes()).unwrap();
        let list = QValue::list(&[null, timestamp.clone()]).unwrap();
        let mut expected = vec![0, 0, 2, 0, 0, 0, 101, 0];
        expected.extend_from_slice(timestamp.as_bytes());
        assert_eq!(list.as_bytes(), expected);
        assert_eq!(list.type_code(), 0);
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn source_verified_function_forms_round_trip_exactly() {
        let projection = [104, 2, 0, 0, 0, 102, 1, 249, 42, 0, 0, 0, 0, 0, 0, 0];
        // Captured from q's `-8!+[;1]`: 101/255 is the unfilled argument.
        let projection_with_hole = [
            104, 3, 0, 0, 0, 102, 1, 101, 255, 249, 1, 0, 0, 0, 0, 0, 0, 0,
        ];
        let composition = [105, 2, 0, 0, 0, 102, 1, 102, 2];
        let derived = [106, 102, 1];
        let derived_list = [106, 7, 0, 1, 0, 0, 0, 42, 0, 0, 0, 0, 0, 0, 0];
        for body in [
            projection.as_slice(),
            projection_with_hole.as_slice(),
            composition.as_slice(),
            derived.as_slice(),
            derived_list.as_slice(),
        ] {
            let value = QValue::from_bytes(body).expect("source-verified function layout");
            assert_eq!(value.as_bytes(), body);
            assert_eq!(value.len(), 1);
        }
    }

    #[test]
    fn q_error_bodies_keep_server_error_semantics_in_lossless_mode() {
        let error = QValue::from_bytes(&[128, 0xff, 0])
            .expect_err("q error bodies are not ordinary lossless values");
        assert!(matches!(error, XqdbError::ServerErr(message) if message == "\u{FFFD}"));
    }

    #[test]
    fn malformed_raw_wire_bodies_are_rejected() {
        let malformed = [
            vec![],
            vec![249],
            vec![7, 0, 0xff, 0xff, 0xff, 0xff],
            vec![13, 0, 1, 0, 0, 0, 1, 2, 3],
            vec![245, b'x'],
            vec![101, 0, 0],
            vec![
                99, 6, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 7, 0, 1, 0, 0, 0, 3, 0, 0, 0, 0, 0,
                0, 0,
            ],
            vec![98, 0, 99, 11, 0, 1, 0, 0, 0, b'a', 0, 0, 0, 0, 0, 0, 0],
            vec![104, 0],
            vec![105, 0],
            vec![6, 0x80, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
        ];
        for body in malformed {
            assert!(QValue::from_owned_bytes(body).is_err());
        }
    }

    #[test]
    fn dictionary_constructor_rejects_mismatched_counts() {
        let keys = typed_list(6, 0, 2, &[0; 8]);
        let values = typed_list(7, 0, 1, &[0; 8]);
        assert!(QValue::dictionary(&keys, &values).is_err());
    }
}
