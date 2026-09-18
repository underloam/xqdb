use chrono::{DateTime, Duration, NaiveDate, NaiveTime, Timelike, Utc};
use indexmap::IndexMap;
use napi::bindgen_prelude::{Array, BigInt, Buffer, BufferSlice, FromNapiValue, Object, Unknown};
use napi::{JsString, JsValue, ValueType};
use napi_derive::napi;
use std::mem::size_of;
use uuid::Uuid;
use xqdb::qvalue::QValue;
use xqdb::types::{QLambda, QOperator, SymbolEncoding, K, MIN_Q_TIMESTAMP_UNIX_NANOS};

use crate::admission::QueueReservation;
use crate::arrow::{dataframe_from_ipc, dataframe_to_ipc, series_from_ipc, series_to_ipc};
use crate::error::BindingError;

const MAX_VALUE_DEPTH: usize = 64;
const MAX_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024;
const MAX_FIELD_NAME_BYTES: usize = 32;
const MAX_TAG_BYTES: usize = 32;
const FIELD_BOOL: u16 = 1 << 0;
const FIELD_NUMBER: u16 = 1 << 1;
const FIELD_BIGINT: u16 = 1 << 2;
const FIELD_STRING: u16 = 1 << 3;
const FIELD_CONTEXT: u16 = 1 << 4;
const FIELD_BYTES: u16 = 1 << 5;
const FIELD_ITEMS: u16 = 1 << 6;
const FIELD_ENTRIES: u16 = 1 << 7;
const FIELD_TYPE_CODE: u16 = 1 << 8;
const FIELD_LENGTH: u16 = 1 << 9;
const FIELD_IS_TABLE: u16 = 1 << 10;
const PAYLOAD_FIELDS: [(&str, u16); 11] = [
    ("boolValue", FIELD_BOOL),
    ("numberValue", FIELD_NUMBER),
    ("bigintValue", FIELD_BIGINT),
    ("stringValue", FIELD_STRING),
    ("context", FIELD_CONTEXT),
    ("bytesValue", FIELD_BYTES),
    ("items", FIELD_ITEMS),
    ("entries", FIELD_ENTRIES),
    ("typeCode", FIELD_TYPE_CODE),
    ("length", FIELD_LENGTH),
    ("isTable", FIELD_IS_TABLE),
];
const NANOS_PER_MILLISECOND: i64 = 1_000_000;
const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_DAY: i64 = 86_400 * NANOS_PER_SECOND;

#[napi(object)]
pub struct NativeOptions {
    pub host: String,
    pub port: f64,
    pub user: Option<String>,
    pub password: Option<String>,
    pub tls: Option<bool>,
    pub timeout_milliseconds: Option<f64>,
    pub symbol_encoding: Option<String>,
    pub lossless: Option<bool>,
    pub compression: Option<String>,
    pub compression_threshold: Option<f64>,
    pub connect_timeout_milliseconds: Option<f64>,
    pub read_timeout_milliseconds: Option<f64>,
    pub write_timeout_milliseconds: Option<f64>,
    pub max_message_bytes: Option<f64>,
    pub max_pending_notifications: Option<f64>,
    pub tls_ca: Option<String>,
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
    pub tls_server_name: Option<String>,
    pub queue_capacity: Option<f64>,
    pub max_argument_bytes: Option<f64>,
    pub max_queued_bytes: Option<f64>,
}

/// Parses the `symbolEncoding` option shared by `NativeConnector` and `readBinary6`; absent
/// means strict, so invalid UTF-8 is only ever transcoded on request.
pub(crate) fn parse_symbol_encoding(value: Option<&str>) -> Result<SymbolEncoding, String> {
    match value {
        None => Ok(SymbolEncoding::Strict),
        Some(name) => SymbolEncoding::from_name(name)
            .ok_or_else(|| format!("symbolEncoding must be \"strict\" or \"lossy\", got {name:?}")),
    }
}

#[napi(object)]
pub struct NativeEntry {
    pub key: String,
    pub value: NativeValue,
}

#[napi(object)]
pub struct NativeValue {
    pub tag: String,
    pub bool_value: Option<bool>,
    pub number_value: Option<f64>,
    pub bigint_value: Option<BigInt>,
    pub string_value: Option<String>,
    pub context: Option<String>,
    pub bytes_value: Option<Buffer>,
    pub items: Option<Vec<NativeValue>>,
    pub entries: Option<Vec<NativeEntry>>,
    pub type_code: Option<i32>,
    pub length: Option<f64>,
    pub is_table: Option<bool>,
}

#[napi(object)]
#[derive(Clone)]
pub struct NativeError {
    pub code: String,
    pub message: String,
}

#[napi(object)]
pub struct NativeResult {
    pub ok: bool,
    pub value: Option<NativeValue>,
    pub error: Option<NativeError>,
    pub message_type: Option<String>,
}

impl NativeResult {
    pub(crate) fn success(value: Option<NativeValue>) -> Self {
        Self {
            ok: true,
            value,
            error: None,
            message_type: None,
        }
    }

    pub(crate) fn success_with_message(message_type: String, value: NativeValue) -> Self {
        Self {
            ok: true,
            value: Some(value),
            error: None,
            message_type: Some(message_type),
        }
    }

    pub(crate) fn failure(error: BindingError) -> Self {
        Self {
            ok: false,
            value: None,
            error: Some(NativeError {
                code: error.code.to_owned(),
                message: error.message,
            }),
            message_type: None,
        }
    }

    pub(crate) fn from_result(result: Result<Option<NativeValue>, BindingError>) -> Self {
        match result {
            Ok(value) => Self::success(value),
            Err(error) => Self::failure(error),
        }
    }
}

#[derive(Debug)]
pub(crate) struct OwnedNativeEntry {
    key: String,
    value: OwnedNativeValue,
}

#[derive(Debug)]
pub(crate) struct OwnedNativeValue {
    tag: String,
    bool_value: Option<bool>,
    number_value: Option<f64>,
    bigint_value: Option<i64>,
    string_value: Option<String>,
    context: Option<String>,
    bytes_value: Option<Vec<u8>>,
    items: Option<Vec<OwnedNativeValue>>,
    entries: Option<Vec<OwnedNativeEntry>>,
    type_code: Option<i16>,
    length: Option<usize>,
    is_table: Option<bool>,
}
struct SnapshotBudget<'a> {
    limit: usize,
    remaining: usize,
    reservation: Option<&'a mut QueueReservation>,
}

impl<'a> SnapshotBudget<'a> {
    fn new(limit: usize) -> Self {
        Self::with_reservation(limit, None)
    }

    fn with_reservation(limit: usize, reservation: Option<&'a mut QueueReservation>) -> Self {
        Self {
            limit,
            remaining: limit,
            reservation,
        }
    }

    fn charge(&mut self, bytes: usize) -> Result<(), BindingError> {
        self.ensure(bytes)?;
        if let Some(reservation) = self.reservation.as_mut() {
            reservation.charge_bytes(bytes)?;
        }
        self.remaining -= bytes;
        Ok(())
    }

    fn ensure(&self, bytes: usize) -> Result<(), BindingError> {
        if bytes > self.remaining {
            return Err(BindingError::conversion(format!(
                "native value snapshot exceeds its {} byte limit",
                self.limit
            )));
        }
        Ok(())
    }

    fn used(&self) -> usize {
        self.limit - self.remaining
    }
}

impl TryFrom<NativeValue> for OwnedNativeValue {
    type Error = BindingError;

    fn try_from(value: NativeValue) -> Result<Self, Self::Error> {
        Self::snapshot_with_limit(value, MAX_SNAPSHOT_BYTES)
    }
}

impl OwnedNativeValue {
    fn snapshot_with_limit(value: NativeValue, limit: usize) -> Result<Self, BindingError> {
        let mut budget = SnapshotBudget::new(limit);
        budget.charge(size_of::<OwnedNativeValue>())?;
        Self::snapshot(value, 0, &mut budget)
    }

    fn snapshot(
        value: NativeValue,
        depth: usize,
        budget: &mut SnapshotBudget<'_>,
    ) -> Result<Self, BindingError> {
        if depth > MAX_VALUE_DEPTH {
            return Err(BindingError::conversion(format!(
                "native value nesting exceeds {MAX_VALUE_DEPTH} levels"
            )));
        }
        validate_native_value_shape(&value)?;
        budget.charge(value.tag.len())?;
        if let Some(string) = &value.string_value {
            budget.charge(string.len())?;
        }
        if let Some(context) = &value.context {
            budget.charge(context.len())?;
        }
        if let Some(bytes) = &value.bytes_value {
            budget.charge(bytes.len())?;
        }

        let tag = value.tag;
        let bigint_value = value.bigint_value.map(bigint_to_i64).transpose()?;
        let type_code = value
            .type_code
            .map(|code| {
                i16::try_from(code).map_err(|_| {
                    BindingError::conversion("typeCode is outside the signed 16-bit range")
                })
            })
            .transpose()?;
        let length = value
            .length
            .map(|length| number_to_usize(length, "length"))
            .transpose()?;
        let is_table = value.is_table;
        let items = value
            .items
            .map(|items| {
                budget.charge(allocation_size::<OwnedNativeValue>(items.len())?)?;
                let mut snapshot = try_vec_with_capacity(items.len(), "native list snapshot")?;
                for item in items {
                    snapshot.push(Self::snapshot(item, depth + 1, budget)?);
                }
                Ok::<_, BindingError>(snapshot)
            })
            .transpose()?;
        let entries = value
            .entries
            .map(|entries| {
                budget.charge(allocation_size::<OwnedNativeEntry>(entries.len())?)?;
                let mut snapshot =
                    try_vec_with_capacity(entries.len(), "native dictionary snapshot")?;
                for entry in entries {
                    budget.charge(entry.key.len())?;
                    snapshot.push(OwnedNativeEntry {
                        key: entry.key,
                        value: Self::snapshot(entry.value, depth + 1, budget)?,
                    });
                }
                Ok::<_, BindingError>(snapshot)
            })
            .transpose()?;
        let bytes_value = value
            .bytes_value
            .map(|bytes| copy_bytes(&bytes, "native bytes snapshot"))
            .transpose()?;

        Ok(Self {
            tag,
            bool_value: value.bool_value,
            number_value: value.number_value,
            bigint_value,
            string_value: value.string_value,
            context: value.context,
            bytes_value,
            items,
            entries,
            type_code,
            length,
            is_table,
        })
    }

    pub(crate) fn into_k(mut self) -> Result<K, BindingError> {
        let tag = self.tag.clone();
        match tag.as_str() {
            "null" => Ok(K::Null),
            "boolean" => Ok(K::Boolean(self.required_bool()?)),
            "guid" => Ok(K::Guid(Uuid::parse_str(&self.required_string()?).map_err(
                |error| BindingError::conversion(format!("invalid guid: {error}")),
            )?)),
            "u8" => Ok(K::U8(self.required_integer(0, u8::MAX as i64)? as u8)),
            "i16" => Ok(K::I16(
                self.required_integer(i16::MIN as i64, i16::MAX as i64)? as i16,
            )),
            "i32" => Ok(K::I32(
                self.required_integer(i32::MIN as i64, i32::MAX as i64)? as i32,
            )),
            "i64" => Ok(K::I64(self.required_bigint()?)),
            "f32" => {
                let value = self.required_number()?;
                if value.is_finite() && (value < f32::MIN as f64 || value > f32::MAX as f64) {
                    return Err(BindingError::conversion(
                        "numberValue is outside the f32 range",
                    ));
                }
                Ok(K::F32(value as f32))
            }
            "f64" => Ok(K::F64(self.required_number()?)),
            "char" => Ok(K::Char(self.required_integer(0, u8::MAX as i64)? as u8)),
            "symbol" => {
                let value = self.required_string()?;
                validate_q_symbol(&value, "symbol stringValue")?;
                Ok(K::Symbol(value))
            }
            "string" => Ok(K::String(self.required_string()?)),
            "operator" => QOperator::new(&self.required_string()?)
                .map(K::Operator)
                .map_err(BindingError::from),
            "lambda" => {
                let source = self.required_string()?;
                let context = self.required_context()?;
                QLambda::with_context(source, context)
                    .map(K::Lambda)
                    .map_err(BindingError::from)
            }
            "bytes" => Ok(K::CharVector(self.required_bytes()?)),
            "timestamp" => {
                let nanos = self.required_bigint()?;
                if nanos < MIN_Q_TIMESTAMP_UNIX_NANOS {
                    return Err(BindingError::conversion(
                        "bigintValue is outside q's representable timestamp range",
                    ));
                }
                let seconds = nanos.div_euclid(NANOS_PER_SECOND);
                let subsecond_nanos = nanos.rem_euclid(NANOS_PER_SECOND) as u32;
                let value =
                    DateTime::<Utc>::from_timestamp(seconds, subsecond_nanos).ok_or_else(|| {
                        BindingError::conversion("bigintValue is outside the timestamp range")
                    })?;
                Ok(K::DateTime(value))
            }
            "date" => Ok(K::Date(
                NaiveDate::parse_from_str(&self.required_string()?, "%Y-%m-%d")
                    .map_err(|error| BindingError::conversion(format!("invalid date: {error}")))?,
            )),
            "time" => {
                let nanos = self.required_bigint()?;
                if !(0..NANOS_PER_DAY).contains(&nanos) {
                    return Err(BindingError::conversion(
                        "bigintValue for time must be within one day",
                    ));
                }
                if nanos % NANOS_PER_MILLISECOND != 0 {
                    return Err(BindingError::conversion(
                        "bigintValue for time must use millisecond precision",
                    ));
                }
                let seconds = (nanos / NANOS_PER_SECOND) as u32;
                let subsecond_nanos = (nanos % NANOS_PER_SECOND) as u32;
                Ok(K::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(seconds, subsecond_nanos)
                        .ok_or_else(|| BindingError::conversion("invalid time"))?,
                ))
            }
            "timespan" => Ok(K::Duration(Duration::nanoseconds(self.required_bigint()?))),
            "list" => {
                let items = self.required_items()?;
                let mut values = try_vec_with_capacity(items.len(), "native input list")?;
                for item in items {
                    values.push(item.into_k()?);
                }
                Ok(K::MixedList(values))
            }
            "dictionary" => {
                let entries = self.required_entries()?;
                let mut dictionary = IndexMap::new();
                dictionary.try_reserve(entries.len()).map_err(|error| {
                    BindingError::conversion(format!(
                        "unable to allocate native input dictionary: {error}"
                    ))
                })?;
                for entry in entries {
                    validate_q_symbol(&entry.key, "dictionary key")?;
                    if dictionary.contains_key(&entry.key) {
                        return Err(BindingError::conversion(format!(
                            "dictionary contains duplicate key {:?}",
                            entry.key
                        )));
                    }
                    dictionary.insert(entry.key, entry.value.into_k()?);
                }
                Ok(K::Dict(dictionary))
            }
            "series" => Ok(K::Series(series_from_ipc(self.required_bytes()?)?)),
            "qvalue" => {
                let value =
                    QValue::from_owned_bytes(self.required_bytes()?).map_err(BindingError::from)?;
                if self.required_type_code()? != value.type_code() {
                    return Err(BindingError::conversion(
                        "qvalue typeCode does not match its validated bytes",
                    ));
                }
                if self.required_length()? != value.len() {
                    return Err(BindingError::conversion(
                        "qvalue length does not match its validated bytes",
                    ));
                }
                if self.required_is_table()? != value.is_table() {
                    return Err(BindingError::conversion(
                        "qvalue isTable does not match its validated bytes",
                    ));
                }
                Ok(K::QValue(value))
            }
            "table" => Ok(K::DataFrame(dataframe_from_ipc(self.required_bytes()?)?)),
            tag => Err(BindingError::conversion(format!(
                "unsupported native value tag {tag:?}"
            ))),
        }
    }

    fn required_bool(&self) -> Result<bool, BindingError> {
        self.bool_value
            .ok_or_else(|| BindingError::conversion(format!("{} requires boolValue", self.tag)))
    }

    fn required_number(&self) -> Result<f64, BindingError> {
        self.number_value
            .ok_or_else(|| BindingError::conversion(format!("{} requires numberValue", self.tag)))
    }

    fn required_integer(&self, minimum: i64, maximum: i64) -> Result<i64, BindingError> {
        let value = self.required_number()?;
        if !value.is_finite()
            || value.fract() != 0.0
            || value < minimum as f64
            || value > maximum as f64
        {
            return Err(BindingError::conversion(format!(
                "numberValue for {} must be an integer in {minimum}..={maximum}",
                self.tag
            )));
        }
        Ok(value as i64)
    }

    fn required_bigint(&self) -> Result<i64, BindingError> {
        self.bigint_value
            .ok_or_else(|| BindingError::conversion(format!("{} requires bigintValue", self.tag)))
    }

    fn required_string(&mut self) -> Result<String, BindingError> {
        self.string_value
            .take()
            .ok_or_else(|| BindingError::conversion(format!("{} requires stringValue", self.tag)))
    }

    fn required_context(&mut self) -> Result<String, BindingError> {
        self.context
            .take()
            .ok_or_else(|| BindingError::conversion(format!("{} requires context", self.tag)))
    }

    fn required_bytes(&mut self) -> Result<Vec<u8>, BindingError> {
        self.bytes_value
            .take()
            .ok_or_else(|| BindingError::conversion(format!("{} requires bytesValue", self.tag)))
    }

    fn required_items(&mut self) -> Result<Vec<OwnedNativeValue>, BindingError> {
        self.items
            .take()
            .ok_or_else(|| BindingError::conversion(format!("{} requires items", self.tag)))
    }

    fn required_entries(&mut self) -> Result<Vec<OwnedNativeEntry>, BindingError> {
        self.entries
            .take()
            .ok_or_else(|| BindingError::conversion(format!("{} requires entries", self.tag)))
    }

    fn required_type_code(&self) -> Result<i16, BindingError> {
        self.type_code
            .ok_or_else(|| BindingError::conversion(format!("{} requires typeCode", self.tag)))
    }

    fn required_length(&self) -> Result<usize, BindingError> {
        self.length
            .ok_or_else(|| BindingError::conversion(format!("{} requires length", self.tag)))
    }

    fn required_is_table(&self) -> Result<bool, BindingError> {
        self.is_table
            .ok_or_else(|| BindingError::conversion(format!("{} requires isTable", self.tag)))
    }
}

fn expected_payload_fields(tag: &str) -> Result<u16, BindingError> {
    match tag {
        "null" => Ok(0),
        "boolean" => Ok(FIELD_BOOL),
        "guid" | "symbol" | "string" | "operator" | "date" => Ok(FIELD_STRING),
        "u8" | "i16" | "i32" | "f32" | "f64" | "char" => Ok(FIELD_NUMBER),
        "i64" | "timestamp" | "time" | "timespan" => Ok(FIELD_BIGINT),
        "lambda" => Ok(FIELD_STRING | FIELD_CONTEXT),
        "bytes" | "series" | "table" => Ok(FIELD_BYTES),
        "qvalue" => Ok(FIELD_BYTES | FIELD_TYPE_CODE | FIELD_LENGTH | FIELD_IS_TABLE),
        "list" => Ok(FIELD_ITEMS),
        "dictionary" => Ok(FIELD_ENTRIES),
        tag => Err(BindingError::conversion(format!(
            "unsupported native value tag {tag:?}"
        ))),
    }
}

fn validate_payload_fields(tag: &str, actual: u16) -> Result<u16, BindingError> {
    let expected = expected_payload_fields(tag)?;
    for (field, bit) in PAYLOAD_FIELDS {
        if expected & bit != 0 && actual & bit == 0 {
            return Err(BindingError::conversion(format!("{tag} requires {field}")));
        }
    }
    for (field, bit) in PAYLOAD_FIELDS {
        if expected & bit == 0 && actual & bit != 0 {
            return Err(BindingError::conversion(format!(
                "{tag} does not accept {field}"
            )));
        }
    }
    Ok(expected)
}

fn native_payload_fields(value: &NativeValue) -> u16 {
    (u16::from(value.bool_value.is_some()) * FIELD_BOOL)
        | (u16::from(value.number_value.is_some()) * FIELD_NUMBER)
        | (u16::from(value.bigint_value.is_some()) * FIELD_BIGINT)
        | (u16::from(value.string_value.is_some()) * FIELD_STRING)
        | (u16::from(value.context.is_some()) * FIELD_CONTEXT)
        | (u16::from(value.bytes_value.is_some()) * FIELD_BYTES)
        | (u16::from(value.items.is_some()) * FIELD_ITEMS)
        | (u16::from(value.entries.is_some()) * FIELD_ENTRIES)
        | (u16::from(value.type_code.is_some()) * FIELD_TYPE_CODE)
        | (u16::from(value.length.is_some()) * FIELD_LENGTH)
        | (u16::from(value.is_table.is_some()) * FIELD_IS_TABLE)
}

fn validate_native_value_shape(value: &NativeValue) -> Result<(), BindingError> {
    validate_payload_fields(&value.tag, native_payload_fields(value)).map(|_| ())
}

fn bigint_to_i64(value: BigInt) -> Result<i64, BindingError> {
    let (value, lossless) = value.get_i64();
    if lossless {
        Ok(value)
    } else {
        Err(BindingError::conversion(
            "bigintValue is outside the signed 64-bit range",
        ))
    }
}

fn number_to_usize(value: f64, field: &str) -> Result<usize, BindingError> {
    if !value.is_finite()
        || value.fract() != 0.0
        || value < 0.0
        || value > usize::MAX as f64
        || value > 9_007_199_254_740_991.0
    {
        return Err(BindingError::conversion(format!(
            "{field} must be a non-negative safe integer representable on this platform"
        )));
    }
    Ok(value as usize)
}

fn allocation_size<T>(length: usize) -> Result<usize, BindingError> {
    length
        .checked_mul(size_of::<T>())
        .ok_or_else(|| BindingError::conversion("native value snapshot allocation size overflowed"))
}

fn try_vec_with_capacity<T>(length: usize, description: &str) -> Result<Vec<T>, BindingError> {
    let mut values = Vec::new();
    values.try_reserve_exact(length).map_err(|error| {
        BindingError::conversion(format!(
            "unable to allocate {description} with {length} elements: {error}"
        ))
    })?;
    Ok(values)
}

fn copy_bytes(bytes: &[u8], description: &str) -> Result<Vec<u8>, BindingError> {
    let mut snapshot = try_vec_with_capacity(bytes.len(), description)?;
    snapshot.extend_from_slice(bytes);
    Ok(snapshot)
}

fn napi_conversion(field: &str, error: napi::Error) -> BindingError {
    BindingError::conversion(format!("invalid {field}: {}", error.reason))
}

fn check_napi_status(status: napi::sys::napi_status, operation: &str) -> Result<(), BindingError> {
    if status == napi::sys::Status::napi_ok {
        Ok(())
    } else {
        Err(BindingError::conversion(format!(
            "{operation} failed with Node-API status {status:?}"
        )))
    }
}

fn object_keys_limited(
    object: &Object<'_>,
    maximum: usize,
    description: &str,
) -> Result<Vec<String>, BindingError> {
    let value = object.value();
    let mut raw_names = std::ptr::null_mut();
    // SAFETY: `value` is owned by this callback environment and `raw_names` is a valid
    // out-pointer. The status is checked before the result is interpreted.
    check_napi_status(
        unsafe { napi::sys::napi_get_property_names(value.env, value.value, &mut raw_names) },
        "reading native object property names",
    )?;
    // SAFETY: a successful napi_get_property_names call returns an Array in the same
    // callback environment, and the resulting wrapper does not escape the callback.
    let names = unsafe { Array::from_napi_value(value.env, raw_names) }
        .map_err(|error| napi_conversion(description, error))?;
    let length = names.len() as usize;
    if length > maximum {
        return Err(BindingError::conversion(format!(
            "{description} has more than {maximum} fields"
        )));
    }

    let mut keys = try_vec_with_capacity(length, "native object field list")?;
    for index in 0..names.len() {
        let key = names
            .get::<JsString>(index)
            .map_err(|error| napi_conversion(description, error))?
            .ok_or_else(|| BindingError::conversion(format!("{description} field is missing")))?;
        let length = key
            .utf8_len()
            .map_err(|error| napi_conversion(description, error))?;
        if length > MAX_FIELD_NAME_BYTES {
            return Err(BindingError::conversion(format!(
                "{description} contains an unsupported field name"
            )));
        }
        keys.push(
            key.into_utf8()
                .and_then(|value| value.into_owned())
                .map_err(|error| napi_conversion(description, error))?,
        );
    }
    Ok(keys)
}

fn payload_fields_from_keys(keys: &[String], description: &str) -> Result<u16, BindingError> {
    if !keys.iter().any(|key| key == "tag") {
        return Err(BindingError::conversion(format!(
            "{description} requires tag"
        )));
    }
    let mut fields = 0;
    for key in keys {
        if key == "tag" {
            continue;
        }
        let Some((_, bit)) = PAYLOAD_FIELDS.iter().find(|(field, _)| key == field) else {
            return Err(BindingError::conversion(format!(
                "{description} contains unsupported field {key:?}"
            )));
        };
        fields |= *bit;
    }
    Ok(fields)
}

fn required_property<'env, T: FromNapiValue>(
    object: &Object<'env>,
    field: &str,
) -> Result<T, BindingError> {
    object
        .get(field)
        .map_err(|error| napi_conversion(field, error))?
        .ok_or_else(|| BindingError::conversion(format!("native value requires {field}")))
}
fn required_bigint_i64_property(object: &Object<'_>, field: &str) -> Result<i64, BindingError> {
    let value = required_property::<Unknown>(object, field)?;
    if value
        .get_type()
        .map_err(|error| napi_conversion(field, error))?
        != ValueType::BigInt
    {
        return Err(BindingError::conversion(format!(
            "{field} must be a BigInt"
        )));
    }
    let raw = value.value();
    let mut result = 0i64;
    let mut lossless = false;
    // SAFETY: `raw` is a callback-scoped value from this environment and its type was
    // checked as BigInt immediately above; both output pointers are valid locals.
    check_napi_status(
        unsafe {
            napi::sys::napi_get_value_bigint_int64(raw.env, raw.value, &mut result, &mut lossless)
        },
        "reading native bigintValue",
    )?;
    if !lossless {
        return Err(BindingError::conversion(
            "bigintValue is outside the signed 64-bit range",
        ));
    }
    Ok(result)
}

fn required_exact_integer_property(
    object: &Object<'_>,
    field: &str,
    minimum: f64,
    maximum: f64,
) -> Result<i64, BindingError> {
    let value = required_property::<f64>(object, field)?;
    if !value.is_finite() || value.fract() != 0.0 || value < minimum || value > maximum {
        return Err(BindingError::conversion(format!(
            "{field} must be an integer from {minimum} through {maximum}"
        )));
    }
    Ok(value as i64)
}

fn required_string_property(
    object: &Object<'_>,
    field: &str,
    maximum: usize,
    budget: &mut SnapshotBudget<'_>,
) -> Result<String, BindingError> {
    let value = required_property::<JsString>(object, field)?;
    let length = value
        .utf8_len()
        .map_err(|error| napi_conversion(field, error))?;
    if length > maximum {
        return Err(BindingError::conversion(format!(
            "{field} exceeds its {maximum} byte limit"
        )));
    }
    budget.charge(length)?;
    value
        .into_utf8()
        .and_then(|value| value.into_owned())
        .map_err(|error| napi_conversion(field, error))
}

fn ensure_not_ancestor(value: Unknown<'_>, ancestors: &[Unknown<'_>]) -> Result<(), BindingError> {
    let raw = value.value();
    for ancestor in ancestors {
        let mut equal = false;
        // SAFETY: both values are callback-scoped handles from the same environment and
        // `equal` is a valid out-pointer whose status is checked before use.
        check_napi_status(
            unsafe {
                napi::sys::napi_strict_equals(raw.env, value.raw(), ancestor.raw(), &mut equal)
            },
            "checking native value object identity",
        )?;
        if equal {
            return Err(BindingError::conversion(
                "native value contains a reference cycle",
            ));
        }
    }
    Ok(())
}

fn snapshot_unknown_value<'env>(
    value: Unknown<'env>,
    depth: usize,
    budget: &mut SnapshotBudget<'_>,
    ancestors: &mut Vec<Unknown<'env>>,
) -> Result<OwnedNativeValue, BindingError> {
    if depth > MAX_VALUE_DEPTH {
        return Err(BindingError::conversion(format!(
            "native value nesting exceeds {MAX_VALUE_DEPTH} levels"
        )));
    }
    if value
        .get_type()
        .map_err(|error| napi_conversion("native value", error))?
        != ValueType::Object
    {
        return Err(BindingError::conversion("native value must be an object"));
    }
    ensure_not_ancestor(value, ancestors)?;
    ancestors.push(value);
    let result = snapshot_unknown_object(value, depth, budget, ancestors);
    ancestors.pop();
    result
}

fn snapshot_unknown_object<'env>(
    value: Unknown<'env>,
    depth: usize,
    budget: &mut SnapshotBudget<'_>,
    ancestors: &mut Vec<Unknown<'env>>,
) -> Result<OwnedNativeValue, BindingError> {
    // SAFETY: the caller checked ValueType::Object immediately before dispatching here;
    // the wrapper remains callback-scoped and does not outlive the N-API handle.
    let object = unsafe { value.cast::<Object>() }
        .map_err(|error| napi_conversion("native value", error))?;
    let keys = object_keys_limited(&object, PAYLOAD_FIELDS.len() + 1, "native value")?;
    let actual_fields = payload_fields_from_keys(&keys, "native value")?;

    let tag = required_string_property(&object, "tag", MAX_TAG_BYTES, budget)?;
    let expected_fields = validate_payload_fields(&tag, actual_fields)?;

    let bool_value = (expected_fields == FIELD_BOOL)
        .then(|| required_property(&object, "boolValue"))
        .transpose()?;
    let number_value = (expected_fields == FIELD_NUMBER)
        .then(|| required_property(&object, "numberValue"))
        .transpose()?;
    let bigint_value = (expected_fields == FIELD_BIGINT)
        .then(|| required_bigint_i64_property(&object, "bigintValue"))
        .transpose()?;
    let string_value = (expected_fields & FIELD_STRING != 0)
        .then(|| required_string_property(&object, "stringValue", MAX_SNAPSHOT_BYTES, budget))
        .transpose()?;
    let context = (expected_fields & FIELD_CONTEXT != 0)
        .then(|| required_string_property(&object, "context", MAX_SNAPSHOT_BYTES, budget))
        .transpose()?;
    let bytes_value = (expected_fields & FIELD_BYTES != 0)
        .then(|| {
            let bytes = required_property::<BufferSlice>(&object, "bytesValue")?;
            budget.charge(bytes.len())?;
            copy_bytes(&bytes, "native bytes snapshot")
        })
        .transpose()?;
    let type_code = (expected_fields & FIELD_TYPE_CODE != 0)
        .then(|| {
            required_exact_integer_property(&object, "typeCode", i16::MIN as f64, i16::MAX as f64)
                .map(|code| code as i16)
        })
        .transpose()?;
    let length = (expected_fields & FIELD_LENGTH != 0)
        .then(|| {
            required_property::<f64>(&object, "length")
                .and_then(|length| number_to_usize(length, "length"))
        })
        .transpose()?;
    let is_table = (expected_fields & FIELD_IS_TABLE != 0)
        .then(|| required_property::<bool>(&object, "isTable"))
        .transpose()?;
    let items = (expected_fields == FIELD_ITEMS)
        .then(|| {
            let items = required_property::<Array>(&object, "items")?;
            let length = items.len() as usize;
            budget.charge(allocation_size::<OwnedNativeValue>(length)?)?;
            let mut snapshot = try_vec_with_capacity(length, "native list snapshot")?;
            for index in 0..items.len() {
                let item = items
                    .get::<Unknown>(index)
                    .map_err(|error| napi_conversion("list item", error))?
                    .ok_or_else(|| BindingError::conversion("native list item is missing"))?;
                snapshot.push(snapshot_unknown_value(item, depth + 1, budget, ancestors)?);
            }
            Ok::<_, BindingError>(snapshot)
        })
        .transpose()?;
    let entries = (expected_fields == FIELD_ENTRIES)
        .then(|| {
            let entries = required_property::<Array>(&object, "entries")?;
            let length = entries.len() as usize;
            budget.charge(allocation_size::<OwnedNativeEntry>(length)?)?;
            let mut snapshot = try_vec_with_capacity(length, "native dictionary snapshot")?;
            for index in 0..entries.len() {
                let entry = entries
                    .get::<Unknown>(index)
                    .map_err(|error| napi_conversion("dictionary entry", error))?
                    .ok_or_else(|| {
                        BindingError::conversion("native dictionary entry is missing")
                    })?;
                snapshot.push(snapshot_unknown_entry(entry, depth, budget, ancestors)?);
            }
            Ok::<_, BindingError>(snapshot)
        })
        .transpose()?;

    Ok(OwnedNativeValue {
        tag,
        bool_value,
        number_value,
        bigint_value,
        string_value,
        context,
        bytes_value,
        items,
        entries,
        type_code,
        length,
        is_table,
    })
}

fn snapshot_unknown_entry<'env>(
    value: Unknown<'env>,
    depth: usize,
    budget: &mut SnapshotBudget<'_>,
    ancestors: &mut Vec<Unknown<'env>>,
) -> Result<OwnedNativeEntry, BindingError> {
    if value
        .get_type()
        .map_err(|error| napi_conversion("dictionary entry", error))?
        != ValueType::Object
    {
        return Err(BindingError::conversion(
            "dictionary entry must be an object",
        ));
    }
    // SAFETY: ValueType::Object was checked immediately above and this callback-scoped
    // wrapper does not escape the N-API call.
    let object = unsafe { value.cast::<Object>() }
        .map_err(|error| napi_conversion("dictionary entry", error))?;
    let mut keys = object_keys_limited(&object, 2, "dictionary entry")?;
    keys.sort_unstable();
    if keys.len() != 2 || keys[0] != "key" || keys[1] != "value" {
        return Err(BindingError::conversion(
            "dictionary entry must contain exactly key and value",
        ));
    }

    let key = required_string_property(&object, "key", MAX_SNAPSHOT_BYTES, budget)?;
    let child = required_property::<Unknown>(&object, "value")?;
    Ok(OwnedNativeEntry {
        key,
        value: snapshot_unknown_value(child, depth + 1, budget, ancestors)?,
    })
}

pub(crate) fn snapshot_native_value(value: Unknown<'_>) -> Result<OwnedNativeValue, BindingError> {
    let mut budget = SnapshotBudget::new(MAX_SNAPSHOT_BYTES);
    budget.charge(size_of::<OwnedNativeValue>())?;
    snapshot_unknown_value(value, 0, &mut budget, &mut Vec::new())
}

pub(crate) fn snapshot_native_value_list(
    values: Array<'_>,
    limit: usize,
) -> Result<(Vec<OwnedNativeValue>, usize), BindingError> {
    snapshot_native_value_array(values, limit, None, "qvalue list item", None)
}

pub(crate) fn snapshot_native_values_admitted(
    values: Array<'_>,
    limit: usize,
    reservation: &mut QueueReservation,
) -> Result<(Vec<OwnedNativeValue>, usize), BindingError> {
    snapshot_native_value_array(values, limit, Some(8), "native argument", Some(reservation))
}

fn snapshot_native_value_array(
    values: Array<'_>,
    limit: usize,
    maximum: Option<usize>,
    description: &str,
    reservation: Option<&mut QueueReservation>,
) -> Result<(Vec<OwnedNativeValue>, usize), BindingError> {
    let length = values.len() as usize;
    if maximum.is_some_and(|maximum| length > maximum) {
        return Err(BindingError::conversion("Too many arguments (8 max)"));
    }
    let mut budget = SnapshotBudget::with_reservation(limit, reservation);
    budget.charge(allocation_size::<OwnedNativeValue>(length)?)?;
    let mut snapshot = try_vec_with_capacity(length, "native value array snapshot")?;
    let mut ancestors = Vec::new();
    for index in 0..values.len() {
        let value = values
            .get::<Unknown>(index)
            .map_err(|error| napi_conversion(description, error))?
            .ok_or_else(|| BindingError::conversion(format!("{description} is missing")))?;
        snapshot.push(snapshot_unknown_value(
            value,
            0,
            &mut budget,
            &mut ancestors,
        )?);
    }
    Ok((snapshot, budget.used()))
}

fn validate_q_symbol(value: &str, field: &str) -> Result<(), BindingError> {
    if value.as_bytes().contains(&0) {
        return Err(BindingError::conversion(format!(
            "{field} cannot contain NUL bytes"
        )));
    }
    Ok(())
}

pub(crate) fn native_values_into_k(values: Vec<OwnedNativeValue>) -> Result<Vec<K>, BindingError> {
    let mut converted = try_vec_with_capacity(values.len(), "native q argument list")?;
    for value in values {
        converted.push(value.into_k()?);
    }
    Ok(converted)
}

pub(crate) fn k_into_native(value: K) -> Result<NativeValue, BindingError> {
    k_into_native_with_depth(value, 0)
}

fn k_into_native_with_depth(value: K, depth: usize) -> Result<NativeValue, BindingError> {
    if depth > MAX_VALUE_DEPTH {
        return Err(BindingError::conversion(format!(
            "native value nesting exceeds {MAX_VALUE_DEPTH} levels"
        )));
    }
    let mut native = empty_native_value();
    match value {
        K::Operator(value) => set_string(&mut native, "operator", value.name().to_owned()),
        K::Lambda(value) => {
            let (source, context) = value.into_parts();
            native.tag = "lambda".into();
            native.string_value = Some(source);
            native.context = Some(context);
        }
        K::Null => native.tag = "null".into(),
        K::Boolean(value) => {
            native.tag = "boolean".into();
            native.bool_value = Some(value);
        }
        K::Guid(value) => {
            native.tag = "guid".into();
            native.string_value = Some(value.to_string());
        }
        K::U8(value) => set_number(&mut native, "u8", value as f64),
        K::I16(value) => set_number(&mut native, "i16", value as f64),
        K::I32(value) => set_number(&mut native, "i32", value as f64),
        K::I64(value) => set_bigint(&mut native, "i64", value),
        K::F32(value) => set_number(&mut native, "f32", value as f64),
        K::F64(value) => set_number(&mut native, "f64", value),
        K::Char(value) => set_number(&mut native, "char", value as f64),
        K::Symbol(value) => {
            validate_q_symbol(&value, "symbol")?;
            set_string(&mut native, "symbol", value);
        }
        K::String(value) => set_string(&mut native, "string", value),
        K::CharVector(value) => {
            native.tag = "bytes".into();
            native.bytes_value = Some(value.into());
        }
        K::QValue(value) => {
            native.tag = "qvalue".into();
            native.type_code = Some(i32::from(value.type_code()));
            native.length = Some(value.len() as f64);
            native.is_table = Some(value.is_table());
            native.bytes_value = Some(value.into_bytes().map_err(BindingError::from)?.into());
        }
        K::DateTime(value) => {
            let nanos = value.timestamp_nanos_opt().ok_or_else(|| {
                BindingError::conversion("timestamp cannot be represented as signed nanoseconds")
            })?;
            set_bigint(&mut native, "timestamp", nanos);
        }
        K::Date(value) => set_string(&mut native, "date", value.format("%Y-%m-%d").to_string()),
        K::Time(value) => {
            let nanos = value.num_seconds_from_midnight() as i64 * NANOS_PER_SECOND
                + value.nanosecond() as i64;
            if nanos % NANOS_PER_MILLISECOND != 0 {
                return Err(BindingError::conversion(
                    "q time only supports millisecond precision",
                ));
            }
            set_bigint(&mut native, "time", nanos);
        }
        K::Duration(value) => {
            let nanos = value.num_nanoseconds().ok_or_else(|| {
                BindingError::conversion("timespan cannot be represented as signed nanoseconds")
            })?;
            set_bigint(&mut native, "timespan", nanos);
        }
        K::MixedList(values) => {
            native.tag = "list".into();
            let mut items = try_vec_with_capacity(values.len(), "native output list")?;
            for value in values {
                items.push(k_into_native_with_depth(value, depth + 1)?);
            }
            native.items = Some(items);
        }
        K::Series(series) => {
            native.tag = "series".into();
            native.bytes_value = Some(series_to_ipc(series)?.into());
        }
        K::DataFrame(dataframe) => {
            native.tag = "table".into();
            native.bytes_value = Some(dataframe_to_ipc(dataframe)?.into());
        }
        K::Dict(dictionary) => {
            native.tag = "dictionary".into();
            let mut entries = try_vec_with_capacity(dictionary.len(), "native output dictionary")?;
            for (key, value) in dictionary {
                validate_q_symbol(&key, "dictionary key")?;
                entries.push(NativeEntry {
                    key,
                    value: k_into_native_with_depth(value, depth + 1)?,
                });
            }
            native.entries = Some(entries);
        }
    }
    Ok(native)
}

fn empty_native_value() -> NativeValue {
    NativeValue {
        tag: String::new(),
        bool_value: None,
        number_value: None,
        bigint_value: None,
        string_value: None,
        bytes_value: None,
        context: None,
        items: None,
        entries: None,
        type_code: None,
        length: None,
        is_table: None,
    }
}

fn set_number(native: &mut NativeValue, tag: &str, value: f64) {
    native.tag = tag.to_owned();
    native.number_value = Some(value);
}

fn set_bigint(native: &mut NativeValue, tag: &str, value: i64) {
    native.tag = tag.to_owned();
    native.bigint_value = Some(BigInt {
        sign_bit: value.is_negative(),
        words: vec![value.unsigned_abs()],
    });
}

fn set_string(native: &mut NativeValue, tag: &str, value: String) {
    native.tag = tag.to_owned();
    native.string_value = Some(value);
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Duration, NaiveDate, NaiveTime};
    use indexmap::IndexMap;
    use napi::bindgen_prelude::{BigInt, Buffer};
    use xqdb::types::{QLambda, QOperator, K, MIN_Q_TIMESTAMP_UNIX_NANOS};

    use super::{
        empty_native_value, k_into_native, NativeEntry, OwnedNativeValue, MAX_VALUE_DEPTH,
    };

    fn bigint_value(tag: &str, sign_bit: bool, word: u64) -> super::NativeValue {
        let mut value = empty_native_value();
        value.tag = tag.into();
        value.bigint_value = Some(BigInt {
            sign_bit,
            words: vec![word],
        });
        value
    }

    #[test]
    fn accepts_exact_i64_bigint_boundaries() {
        let minimum = OwnedNativeValue::try_from(bigint_value("i64", true, 1u64 << 63))
            .expect("snapshot i64::MIN")
            .into_k()
            .expect("convert i64::MIN");
        let maximum = OwnedNativeValue::try_from(bigint_value("i64", false, i64::MAX as u64))
            .expect("snapshot i64::MAX")
            .into_k()
            .expect("convert i64::MAX");
        assert_eq!(minimum, K::I64(i64::MIN));
        assert_eq!(maximum, K::I64(i64::MAX));

        for expected in [i64::MIN, i64::MAX] {
            let actual = OwnedNativeValue::try_from(
                k_into_native(K::I64(expected)).expect("encode i64 boundary"),
            )
            .expect("snapshot output")
            .into_k()
            .expect("decode output");
            assert_eq!(actual, K::I64(expected));
        }
    }

    #[test]
    fn rejects_bigints_outside_i64() {
        let positive = OwnedNativeValue::try_from(bigint_value("i64", false, 1u64 << 63))
            .expect_err("positive overflow must fail");
        let negative = OwnedNativeValue::try_from(bigint_value("i64", true, (1u64 << 63) + 1))
            .expect_err("negative overflow must fail");
        assert_eq!(positive.code, "XQDB_CONVERSION");
        assert_eq!(negative.code, "XQDB_CONVERSION");
    }

    #[test]
    fn rejects_unknown_tags_and_inconsistent_payloads_before_conversion() {
        let mut unknown = empty_native_value();
        unknown.tag = "mystery".into();
        let unknown_error = OwnedNativeValue::try_from(unknown).expect_err("unknown tag must fail");

        let mut missing = empty_native_value();
        missing.tag = "i64".into();
        let missing_error =
            OwnedNativeValue::try_from(missing).expect_err("missing payload must fail");

        let mut extraneous = empty_native_value();
        extraneous.tag = "null".into();
        extraneous.bool_value = Some(false);
        let extraneous_error =
            OwnedNativeValue::try_from(extraneous).expect_err("extraneous payload must fail");

        let mut inconsistent = empty_native_value();
        inconsistent.tag = "list".into();
        inconsistent.items = Some(Vec::new());
        inconsistent.entries = Some(Vec::new());
        let inconsistent_error =
            OwnedNativeValue::try_from(inconsistent).expect_err("inconsistent payload must fail");

        for error in [
            unknown_error,
            missing_error,
            extraneous_error,
            inconsistent_error,
        ] {
            assert_eq!(error.code, "XQDB_CONVERSION");
        }
    }

    #[test]
    fn preserves_arbitrary_character_vector_bytes() {
        let bytes = vec![0, 0x80, 0xff, b'q'];
        let native = k_into_native(K::CharVector(bytes.clone())).expect("encode char vector");
        assert_eq!(native.tag, "bytes");
        assert_eq!(native.bytes_value.as_deref(), Some(bytes.as_slice()));

        let mut input = empty_native_value();
        input.tag = "bytes".into();
        input.bytes_value = Some(Buffer::from(bytes.clone()));
        let decoded = OwnedNativeValue::try_from(input)
            .expect("snapshot bytes")
            .into_k()
            .expect("decode bytes");
        assert_eq!(decoded, K::CharVector(bytes));
    }

    #[test]
    fn round_trips_operator_and_lambda_contracts() {
        let operator = k_into_native(K::Operator(QOperator::PLUS)).expect("encode operator");
        assert_eq!(operator.tag, "operator");
        assert_eq!(operator.string_value.as_deref(), Some("+"));
        assert_eq!(
            OwnedNativeValue::try_from(operator)
                .expect("snapshot operator")
                .into_k()
                .expect("decode operator"),
            K::Operator(QOperator::PLUS)
        );

        let lambda = QLambda::with_context(" {x+y} ", "ctx").expect("lambda");
        let native = k_into_native(K::Lambda(lambda.clone())).expect("encode lambda");
        assert_eq!(native.tag, "lambda");
        assert_eq!(native.string_value.as_deref(), Some(" {x+y} "));
        assert_eq!(native.context.as_deref(), Some("ctx"));
        assert_eq!(
            OwnedNativeValue::try_from(native)
                .expect("snapshot lambda")
                .into_k()
                .expect("decode lambda"),
            K::Lambda(lambda)
        );
    }

    #[test]
    fn rejects_invalid_operator_and_lambda_contracts() {
        let mut operator = empty_native_value();
        operator.tag = "operator".into();
        operator.string_value = Some("plus".into());
        let operator_error = OwnedNativeValue::try_from(operator)
            .expect("snapshot operator")
            .into_k()
            .expect_err("unknown operator must fail");
        assert_eq!(operator_error.code, "XQDB_CONVERSION");

        let mut missing_context = empty_native_value();
        missing_context.tag = "lambda".into();
        missing_context.string_value = Some("{x+y}".into());
        let context_error =
            OwnedNativeValue::try_from(missing_context).expect_err("missing context must fail");
        assert_eq!(context_error.code, "XQDB_CONVERSION");

        let mut nul_source = empty_native_value();
        nul_source.tag = "lambda".into();
        nul_source.string_value = Some("{x\0+y}".into());
        nul_source.context = Some(String::new());
        let source_error = OwnedNativeValue::try_from(nul_source)
            .expect("snapshot lambda")
            .into_k()
            .expect_err("NUL lambda source must fail");
        assert_eq!(source_error.code, "XQDB_CONVERSION");
    }

    #[test]
    fn round_trips_lossless_temporal_values() {
        let timestamp = DateTime::from_timestamp(1_700_000_000, 123_456_789).expect("timestamp");
        let date = NaiveDate::from_ymd_opt(2026, 8, 22).expect("date");
        let time = NaiveTime::from_hms_nano_opt(23, 59, 58, 987_000_000).expect("time");
        let values = [
            K::DateTime(timestamp.to_owned()),
            K::Date(date.to_owned()),
            K::Time(time.to_owned()),
            K::Duration(Duration::nanoseconds(-123_456_789)),
        ];

        for expected in values {
            let actual = OwnedNativeValue::try_from(k_into_native(expected).expect("encode"))
                .expect("snapshot")
                .into_k()
                .expect("decode");
            match actual {
                K::DateTime(value) => assert_eq!(value, timestamp),
                K::Date(value) => assert_eq!(value, date),
                K::Time(value) => assert_eq!(value, time),
                K::Duration(value) => assert_eq!(value, Duration::nanoseconds(-123_456_789)),
                value => panic!("unexpected temporal value: {value:?}"),
            }
        }
    }

    #[test]
    fn round_trips_pre_epoch_and_q_timestamp_boundaries() {
        let pre_epoch = DateTime::from_timestamp(-1, 999_999_999).expect("pre-epoch timestamp");
        let minimum = DateTime::from_timestamp(
            MIN_Q_TIMESTAMP_UNIX_NANOS.div_euclid(1_000_000_000),
            MIN_Q_TIMESTAMP_UNIX_NANOS.rem_euclid(1_000_000_000) as u32,
        )
        .expect("minimum q timestamp");
        let maximum = DateTime::from_timestamp(
            i64::MAX.div_euclid(1_000_000_000),
            i64::MAX.rem_euclid(1_000_000_000) as u32,
        )
        .expect("maximum q timestamp");

        for expected in [pre_epoch, minimum, maximum] {
            let actual = OwnedNativeValue::try_from(
                k_into_native(K::DateTime(expected)).expect("encode timestamp"),
            )
            .expect("snapshot timestamp")
            .into_k()
            .expect("decode timestamp");
            assert_eq!(actual, K::DateTime(expected));
        }

        let error = OwnedNativeValue::try_from(bigint_value(
            "timestamp",
            true,
            (MIN_Q_TIMESTAMP_UNIX_NANOS - 1).unsigned_abs(),
        ))
        .expect("snapshot lower timestamp")
        .into_k()
        .expect_err("timestamp below q range must fail");
        assert_eq!(error.code, "XQDB_CONVERSION");

        let error = OwnedNativeValue::try_from(bigint_value("timestamp", false, 1u64 << 63))
            .expect_err("timestamp above signed nanosecond range must fail");
        assert_eq!(error.code, "XQDB_CONVERSION");
    }

    #[test]
    fn rejects_sub_millisecond_time_in_both_directions() {
        let input = OwnedNativeValue::try_from(bigint_value("time", false, 1))
            .expect("snapshot time")
            .into_k()
            .expect_err("sub-millisecond input must fail");
        let output = match k_into_native(K::Time(
            NaiveTime::from_hms_nano_opt(0, 0, 0, 1).expect("time"),
        )) {
            Err(error) => error,
            Ok(_) => panic!("sub-millisecond output must fail"),
        };
        assert_eq!(input.code, "XQDB_CONVERSION");
        assert_eq!(output.code, "XQDB_CONVERSION");
    }

    #[test]
    fn rejects_nul_symbols_and_dictionary_keys() {
        let mut symbol = empty_native_value();
        symbol.tag = "symbol".into();
        symbol.string_value = Some("bad\0symbol".into());
        let symbol_error = OwnedNativeValue::try_from(symbol)
            .expect("snapshot symbol")
            .into_k()
            .expect_err("NUL symbol must fail");
        let mut null = empty_native_value();
        null.tag = "null".into();
        let mut dictionary = empty_native_value();
        dictionary.tag = "dictionary".into();
        dictionary.entries = Some(vec![NativeEntry {
            key: "bad\0key".into(),
            value: null,
        }]);
        let key_error = OwnedNativeValue::try_from(dictionary)
            .expect("snapshot dictionary")
            .into_k()
            .expect_err("NUL dictionary key must fail");
        assert_eq!(symbol_error.code, "XQDB_CONVERSION");
        assert_eq!(key_error.code, "XQDB_CONVERSION");
    }

    #[test]
    fn bounds_native_output_recursion() {
        let mut value = K::Null;
        for _ in 0..=MAX_VALUE_DEPTH {
            value = K::MixedList(vec![value]);
        }
        let error = match k_into_native(value) {
            Err(error) => error,
            Ok(_) => panic!("deep output must fail"),
        };
        assert_eq!(error.code, "XQDB_CONVERSION");

        let mut dictionary = K::Null;
        for depth in 0..=MAX_VALUE_DEPTH {
            dictionary = K::Dict(IndexMap::from([(format!("level{depth}"), dictionary)]));
        }
        let error = match k_into_native(dictionary) {
            Err(error) => error,
            Ok(_) => panic!("deep dictionary output must fail"),
        };
        assert_eq!(error.code, "XQDB_CONVERSION");
    }

    #[test]
    fn bounds_native_input_recursion_before_conversion() {
        let mut value = empty_native_value();
        value.tag = "null".into();
        for _ in 0..=MAX_VALUE_DEPTH {
            let mut parent = empty_native_value();
            parent.tag = "list".into();
            parent.items = Some(vec![value]);
            value = parent;
        }

        let error = OwnedNativeValue::try_from(value).expect_err("deep input must fail");
        assert_eq!(error.code, "XQDB_CONVERSION");
    }

    #[test]
    fn validates_qvalue_bytes_and_rejects_spoofed_metadata() {
        let mut exact = empty_native_value();
        exact.tag = "qvalue".into();
        exact.bytes_value = Some(Buffer::from(vec![0xf9, 42, 0, 0, 0, 0, 0, 0, 0]));
        exact.type_code = Some(-7);
        exact.length = Some(1.0);
        exact.is_table = Some(false);
        let value = OwnedNativeValue::try_from(exact)
            .expect("snapshot qvalue")
            .into_k()
            .expect("validate qvalue");
        let native = k_into_native(value).expect("convert qvalue output");
        assert_eq!(native.tag, "qvalue");
        assert_eq!(native.type_code, Some(-7));
        assert_eq!(native.length, Some(1.0));
        assert_eq!(native.is_table, Some(false));
        assert_eq!(
            native.bytes_value.expect("qvalue bytes").as_ref(),
            &[0xf9, 42, 0, 0, 0, 0, 0, 0, 0]
        );

        let mut spoofed = empty_native_value();
        spoofed.tag = "qvalue".into();
        spoofed.bytes_value = Some(Buffer::from(vec![0xf9, 42, 0, 0, 0, 0, 0, 0, 0]));
        spoofed.type_code = Some(-6);
        spoofed.length = Some(1.0);
        spoofed.is_table = Some(false);
        let error = OwnedNativeValue::try_from(spoofed)
            .expect("snapshot spoofed qvalue")
            .into_k()
            .expect_err("metadata mismatch must fail");
        assert_eq!(error.code, "XQDB_CONVERSION");
    }
}
