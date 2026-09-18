use crate::arrow::{import_value, ArrowSeries, ArrowTable};
use crate::error::PyXqdbError;
use chrono::{Datelike, Timelike};
use indexmap::IndexMap;
use polars::prelude::{DataFrame, Series};
use pyo3::exceptions::{PyMemoryError, PyOverflowError, PyTypeError, PyValueError};
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::{
    PyBool, PyBytes, PyDate, PyDateTime, PyDelta, PyDict, PyFloat, PyInt, PyList, PyModule,
    PyString, PyTime, PyTuple, PyTzInfo, PyTzInfoAccess,
};
use pyo3::{prelude::*, IntoPyObjectExt};
use std::collections::HashSet;
use std::sync::Mutex;
use std::time::Duration;
use xqdb::connector::{CompressionMode, Connector, ConnectorAbortHandle, ConnectorSettings};
use xqdb::qvalue::{QValue, ValueMode};
use xqdb::types::{MsgType, QLambda, QOperator, SymbolEncoding, K, MIN_Q_TIMESTAMP_UNIX_NANOS};

pub(crate) enum ArrowValue {
    DataFrame(DataFrame),
    Series(Series),
}

#[pyclass(frozen, eq, module = "xqdb", skip_from_py_object)]
#[derive(Clone, Eq, PartialEq)]
pub struct XqdbQOperator {
    operator: QOperator,
}

#[pymethods]
impl XqdbQOperator {
    #[new]
    fn new(name: &str) -> Result<Self, PyXqdbError> {
        Ok(Self {
            operator: QOperator::new(name)?,
        })
    }

    #[classattr]
    #[pyo3(name = "PLUS")]
    fn plus() -> Self {
        Self {
            operator: QOperator::PLUS,
        }
    }

    #[getter]
    fn name(&self) -> &str {
        self.operator.name()
    }

    fn __repr__(&self) -> String {
        format!("XqdbQOperator({:?})", self.operator.name())
    }
}

#[pyclass(frozen, eq, module = "xqdb", skip_from_py_object)]
#[derive(Clone, Eq, PartialEq)]
pub struct XqdbQLambda {
    lambda: QLambda,
}

#[pymethods]
impl XqdbQLambda {
    #[new]
    #[pyo3(signature = (source, context = ""))]
    fn new(source: &str, context: &str) -> Result<Self, PyXqdbError> {
        Ok(Self {
            lambda: QLambda::with_context(source, context)?,
        })
    }

    #[getter]
    fn source(&self) -> &str {
        self.lambda.source()
    }

    #[getter]
    fn context(&self) -> &str {
        self.lambda.context()
    }

    fn __repr__(&self) -> String {
        if self.lambda.context().is_empty() {
            format!("XqdbQLambda({:?})", self.lambda.source())
        } else {
            format!(
                "XqdbQLambda({:?}, {:?})",
                self.lambda.source(),
                self.lambda.context()
            )
        }
    }
}

#[pyclass(frozen, eq, module = "xqdb", skip_from_py_object)]
#[derive(Clone, Eq, PartialEq)]
pub struct XqdbQValue {
    value: QValue,
}

impl XqdbQValue {
    fn from_qvalue(value: QValue) -> Self {
        Self { value }
    }
}

fn parse_atom_kind(kind: &Bound<'_, PyAny>) -> PyResult<u8> {
    let parsed = if kind.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err(
            "q atom kind must be a type name or integer code",
        ));
    } else if let Ok(name) = kind.extract::<&str>() {
        match name {
            "boolean" | "bool" => 1,
            "guid" => 2,
            "byte" => 4,
            "short" => 5,
            "int" => 6,
            "long" => 7,
            "real" => 8,
            "float" => 9,
            "char" => 10,
            "symbol" => 11,
            "timestamp" => 12,
            "month" => 13,
            "date" => 14,
            "datetime" => 15,
            "timespan" => 16,
            "minute" => 17,
            "second" => 18,
            "time" => 19,
            _ => {
                return Err(PyValueError::new_err(format!(
                    "unsupported q atom kind {name:?}"
                )))
            }
        }
    } else {
        let numeric = kind
            .extract::<i64>()
            .map_err(|_| PyTypeError::new_err("q atom kind must be a type name or integer code"))?;
        u8::try_from(numeric)
            .map_err(|_| PyValueError::new_err(format!("unsupported q atom kind {numeric}")))?
    };
    if !(1..=19).contains(&parsed) || parsed == 3 {
        return Err(PyValueError::new_err(format!(
            "unsupported q atom kind {parsed}"
        )));
    }
    Ok(parsed)
}

fn parse_guid_bytes(value: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(bytes) = value.cast::<PyBytes>() {
        if bytes.as_bytes().len() != 16 {
            return Err(PyValueError::new_err(
                "q GUID atom value must contain exactly 16 bytes",
            ));
        }
        return Ok(bytes.as_bytes().to_vec());
    }

    if let Ok(text) = value.extract::<&str>() {
        if !matches!(text.len(), 32 | 36) {
            return Err(PyValueError::new_err(
                "q GUID atom value must be 16 bytes or a UUID string",
            ));
        }
        let compact: String = text.chars().filter(|character| *character != '-').collect();
        if compact.len() != 32 || !compact.is_ascii() {
            return Err(PyValueError::new_err(
                "q GUID atom value must be 16 bytes or a UUID string",
            ));
        }
        let mut bytes = Vec::with_capacity(16);
        for offset in (0..32).step_by(2) {
            bytes.push(
                u8::from_str_radix(&compact[offset..offset + 2], 16).map_err(|_| {
                    PyValueError::new_err("q GUID atom value must be 16 bytes or a UUID string")
                })?,
            );
        }
        return Ok(bytes);
    }

    if let Some(attribute) = value.getattr_opt("bytes")? {
        if let Ok(bytes) = attribute.cast::<PyBytes>() {
            if bytes.as_bytes().len() == 16 {
                return Ok(bytes.as_bytes().to_vec());
            }
        }
    }
    Err(PyTypeError::new_err(
        "q GUID atom value must be 16 bytes, a UUID string, or uuid.UUID",
    ))
}

fn null_atom_payload(kind: u8) -> Vec<u8> {
    match kind {
        1 | 4 => vec![0],
        2 => vec![0; 16],
        5 => i16::MIN.to_le_bytes().to_vec(),
        6 | 13 | 14 | 17 | 18 | 19 => i32::MIN.to_le_bytes().to_vec(),
        7 | 12 | 16 => i64::MIN.to_le_bytes().to_vec(),
        8 => f32::NAN.to_le_bytes().to_vec(),
        9 | 15 => f64::NAN.to_le_bytes().to_vec(),
        10 => vec![b' '],
        11 => vec![0],
        _ => unreachable!("atom kind is validated before building a null payload"),
    }
}

fn atom_payload(kind: u8, value: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if value.is_none() {
        return Ok(null_atom_payload(kind));
    }
    if value.is_instance_of::<PyBool>() && kind != 1 {
        return Err(PyTypeError::new_err(
            "bool is only valid for a q boolean atom",
        ));
    }
    match kind {
        1 => {
            if !value.is_instance_of::<PyBool>() {
                return Err(PyTypeError::new_err(
                    "q boolean atom value must be bool or None",
                ));
            }
            Ok(vec![u8::from(value.extract::<bool>()?)])
        }
        2 => parse_guid_bytes(value),
        4 => Ok(vec![value.extract::<u8>()?]),
        5 => Ok(value.extract::<i16>()?.to_le_bytes().to_vec()),
        6 => Ok(value.extract::<i32>()?.to_le_bytes().to_vec()),
        7 => Ok(value.extract::<i64>()?.to_le_bytes().to_vec()),
        8 => {
            let number = value.extract::<f64>()?;
            let real = number as f32;
            if number.is_finite() && !real.is_finite() {
                return Err(PyValueError::new_err(
                    "q real atom value exceeds the finite 32-bit float range",
                ));
            }
            Ok(real.to_le_bytes().to_vec())
        }
        9 => Ok(value.extract::<f64>()?.to_le_bytes().to_vec()),
        10 => {
            let bytes = if let Ok(bytes) = value.cast::<PyBytes>() {
                bytes.as_bytes()
            } else if let Ok(text) = value.extract::<&str>() {
                text.as_bytes()
            } else {
                return Err(PyTypeError::new_err(
                    "q char atom value must be one byte or a one-byte UTF-8 string",
                ));
            };
            if bytes.len() != 1 {
                return Err(PyValueError::new_err(
                    "q char atom value must contain exactly one byte",
                ));
            }
            Ok(bytes.to_vec())
        }
        11 => {
            let bytes = if let Ok(bytes) = value.cast::<PyBytes>() {
                bytes.as_bytes()
            } else if let Ok(text) = value.extract::<&str>() {
                text.as_bytes()
            } else {
                return Err(PyTypeError::new_err(
                    "q symbol atom value must be str, bytes, or None",
                ));
            };
            if bytes.contains(&0) {
                return Err(PyValueError::new_err(
                    "q symbol atom value cannot contain NUL bytes",
                ));
            }
            let mut payload = Vec::with_capacity(bytes.len() + 1);
            payload.extend_from_slice(bytes);
            payload.push(0);
            Ok(payload)
        }
        12 | 16 => Ok(value.extract::<i64>()?.to_le_bytes().to_vec()),
        13 | 14 | 17 | 18 | 19 => Ok(value.extract::<i32>()?.to_le_bytes().to_vec()),
        15 => Ok(value.extract::<f64>()?.to_le_bytes().to_vec()),
        _ => unreachable!("atom kind is validated before extracting its payload"),
    }
}

fn native_qvalue(value: Bound<'_, PyAny>) -> Result<QValue, PyXqdbError> {
    let converted = PyModule::import(value.py(), "xqdb._conversion")?
        .getattr("to_arrow_inputs")?
        .call1((value,))?;
    let k = cast_to_k(converted)?;
    let mut frame = xqdb::io::generate_j6_ipc_msg(MsgType::Sync, false, k)?;
    drop(frame.drain(..8));
    Ok(QValue::from_owned_bytes(frame)?)
}

fn extract_qvalue(value: Bound<'_, PyAny>) -> Result<QValue, PyXqdbError> {
    if value.is_instance_of::<XqdbQValue>() {
        Ok(value
            .extract::<PyRef<XqdbQValue>>()
            .map_err(PyErr::from)?
            .value
            .clone())
    } else {
        native_qvalue(value)
    }
}

#[pymethods]
impl XqdbQValue {
    #[new]
    fn new(body: &[u8]) -> Result<Self, PyXqdbError> {
        Ok(Self::from_qvalue(QValue::from_bytes(body)?))
    }

    #[staticmethod]
    fn atom(kind: Bound<'_, PyAny>, value: Bound<'_, PyAny>) -> Result<Self, PyXqdbError> {
        let kind = parse_atom_kind(&kind)?;
        let payload = atom_payload(kind, &value)?;
        Ok(Self::from_qvalue(QValue::atom(kind, &payload)?))
    }

    #[staticmethod]
    fn list(values: Bound<'_, PyAny>) -> Result<Self, PyXqdbError> {
        if values.is_instance_of::<PyBytes>() || values.is_instance_of::<PyString>() {
            return Err(
                PyTypeError::new_err("XqdbQValue.list expects an iterable of values").into(),
            );
        }
        let mut qvalues = Vec::new();
        for item in values.try_iter()? {
            let item = item?;
            if qvalues.len() == i32::MAX as usize {
                return Err(PyOverflowError::new_err(format!(
                    "q general lists support at most {} values",
                    i32::MAX
                ))
                .into());
            }
            qvalues.try_reserve(1).map_err(|error| {
                PyMemoryError::new_err(format!(
                    "unable to grow q general-list input to {} values: {error}",
                    qvalues.len() + 1
                ))
            })?;
            qvalues.push(extract_qvalue(item)?);
        }
        Ok(Self::from_qvalue(QValue::list(&qvalues)?))
    }

    #[staticmethod]
    fn dictionary(keys: Bound<'_, PyAny>, values: Bound<'_, PyAny>) -> Result<Self, PyXqdbError> {
        let keys = extract_qvalue(keys)?;
        let values = extract_qvalue(values)?;
        Ok(Self::from_qvalue(QValue::dictionary(&keys, &values)?))
    }

    #[staticmethod]
    fn native(value: Bound<'_, PyAny>) -> Result<Self, PyXqdbError> {
        Ok(Self::from_qvalue(native_qvalue(value)?))
    }

    #[getter]
    fn body<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.value.as_bytes())
    }

    #[getter]
    fn type_code(&self) -> i16 {
        self.value.type_code()
    }

    #[getter]
    fn len(&self) -> usize {
        self.value.len()
    }

    #[getter]
    fn is_table(&self) -> bool {
        self.value.is_table()
    }

    fn __bytes__<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.value.as_bytes())
    }

    fn __len__(&self) -> usize {
        self.value.len()
    }

    fn __repr__(&self) -> String {
        format!(
            "XqdbQValue(type_code={}, len={}, body=<{} bytes>)",
            self.value.type_code(),
            self.value.len(),
            self.value.as_bytes().len()
        )
    }
}

#[pyclass(frozen, module = "xqdb", skip_from_py_object)]
pub struct XqdbAbortHandle {
    handle: ConnectorAbortHandle,
}

#[pymethods]
impl XqdbAbortHandle {
    fn cancel(&self, py: Python) -> Result<(), PyXqdbError> {
        let handle = self.handle.clone();
        py.detach(move || handle.abort().map_err(PyXqdbError::from))
    }
}

#[pyclass]
pub struct XqdbConnector {
    q: Mutex<Connector>,
    abort_handle: ConnectorAbortHandle,
}

fn parse_symbol_encoding(value: &str) -> PyResult<SymbolEncoding> {
    SymbolEncoding::from_name(value).ok_or_else(|| {
        PyValueError::new_err(format!(
            "symbol_encoding must be 'strict' or 'lossy', got {value:?}"
        ))
    })
}

fn parse_compression(value: &str) -> PyResult<CompressionMode> {
    match value {
        "auto" => Ok(CompressionMode::Auto),
        "on" => Ok(CompressionMode::On),
        "off" => Ok(CompressionMode::Off),
        _ => Err(PyValueError::new_err(format!(
            "compression must be 'auto', 'on', or 'off', got {value:?}"
        ))),
    }
}

const MAX_TIMEOUT_SECONDS: f64 = 86_400.0;

fn parse_timeout(name: &str, seconds: f64) -> PyResult<Duration> {
    if !seconds.is_finite() || seconds < 0.0 || seconds > MAX_TIMEOUT_SECONDS {
        return Err(PyValueError::new_err(format!(
            "{name} must be a finite, non-negative number of seconds no greater than 86400"
        )));
    }
    let duration = Duration::from_secs_f64(seconds);
    Ok(if seconds > 0.0 && duration.is_zero() {
        Duration::from_nanos(1)
    } else {
        duration
    })
}

fn parse_optional_timeout(name: &str, value: Option<f64>) -> PyResult<Option<Duration>> {
    value
        .map(|seconds| parse_timeout(name, seconds))
        .transpose()
}

fn parse_pem(name: &str, value: Option<Bound<'_, PyAny>>) -> PyResult<Option<Vec<u8>>> {
    value
        .map(|value| {
            if let Ok(bytes) = value.cast::<PyBytes>() {
                Ok(bytes.as_bytes().to_vec())
            } else if let Ok(text) = value.extract::<&str>() {
                Ok(text.as_bytes().to_vec())
            } else {
                Err(PyTypeError::new_err(format!(
                    "{name} must be PEM text as str or bytes"
                )))
            }
        })
        .transpose()
}

const MAX_CONVERSION_DEPTH: usize = 64;
const MAX_CALL_ARGUMENTS: usize = 8;
const MICROSECONDS_PER_SECOND: i64 = 1_000_000;
const MICROSECONDS_PER_DAY: i64 = 86_400 * MICROSECONDS_PER_SECOND;

impl XqdbConnector {
    fn execute(&self, py: Python, expr: &str, args: Bound<PyTuple>) -> PyResult<Py<PyAny>> {
        let args = cast_to_k_vec(args)?;
        let k = py
            .detach(move || {
                self.q
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .execute(expr, &args)
            })
            .map_err(PyXqdbError::from)?;
        cast_k_to_py(py, k)
    }

    fn execute_async(
        &self,
        py: Python,
        expr: &str,
        args: Bound<PyTuple>,
    ) -> Result<(), PyXqdbError> {
        let args = cast_to_k_vec(args)?;
        py.detach(move || {
            self.q
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .execute_async(expr, &args)
        })
        .map_err(PyXqdbError::from)
    }
}

fn python_date_parts(value: &impl Datelike) -> PyResult<(i32, u8, u8)> {
    let year = value.year();
    if !(1..=9999).contains(&year) {
        return Err(PyOverflowError::new_err(format!(
            "year {year} is outside Python's supported range"
        )));
    }
    Ok((year, value.month() as u8, value.day() as u8))
}

fn python_microseconds(nanoseconds: u32, type_name: &str) -> PyResult<u32> {
    if !nanoseconds.is_multiple_of(1_000) {
        return Err(PyValueError::new_err(format!(
            "{type_name} has sub-microsecond precision that Python cannot represent"
        )));
    }
    Ok(nanoseconds / 1_000)
}

fn cast_k_to_py(py: Python, k: K) -> PyResult<Py<PyAny>> {
    cast_k_to_py_inner(py, k, 0)
}

fn cast_k_to_py_inner(py: Python, k: K, depth: usize) -> PyResult<Py<PyAny>> {
    if depth > MAX_CONVERSION_DEPTH {
        return Err(PyValueError::new_err(format!(
            "q value nesting exceeds {MAX_CONVERSION_DEPTH} levels"
        )));
    }

    match k {
        K::Boolean(k) => k.into_py_any(py),
        K::Guid(k) => k.to_string().into_py_any(py),
        K::U8(k) => k.into_py_any(py),
        K::I16(k) => k.into_py_any(py),
        K::I32(k) => k.into_py_any(py),
        K::I64(k) => k.into_py_any(py),
        K::F32(k) => k.into_py_any(py),
        K::F64(k) => k.into_py_any(py),
        K::Char(k) => (k as char).into_py_any(py),
        K::CharVector(k) => match std::str::from_utf8(&k) {
            Ok(text) => text.into_py_any(py),
            Err(_) => PyBytes::new(py, &k).into_py_any(py),
        },
        K::Symbol(k) => k.into_py_any(py),
        K::String(k) => k.into_py_any(py),
        K::DateTime(k) => {
            let (year, month, day) = python_date_parts(&k)?;
            let microsecond = python_microseconds(k.nanosecond(), "q timestamp")?;
            // A q timestamp carries no timezone, so it maps to a naive datetime. This
            // matches the naive `timestamp[ns]` Arrow columns and avoids asserting UTC
            // over q processes that store local wall-clock times.
            PyDateTime::new(
                py,
                year,
                month,
                day,
                k.hour() as u8,
                k.minute() as u8,
                k.second() as u8,
                microsecond,
                None,
            )?
            .into_py_any(py)
        }
        K::Date(k) => {
            let (year, month, day) = python_date_parts(&k)?;
            PyDate::new(py, year, month, day)?.into_py_any(py)
        }
        K::Time(k) => {
            let microsecond = python_microseconds(k.nanosecond(), "q time")?;
            PyTime::new(
                py,
                k.hour() as u8,
                k.minute() as u8,
                k.second() as u8,
                microsecond,
                None,
            )?
            .into_py_any(py)
        }
        K::Duration(k) => {
            let nanoseconds = k.num_nanoseconds().ok_or_else(|| {
                PyOverflowError::new_err("q timespan is outside Python's supported range")
            })?;
            if nanoseconds % 1_000 != 0 {
                return Err(PyValueError::new_err(
                    "q timespan has sub-microsecond precision that Python cannot represent",
                ));
            }
            let microseconds = nanoseconds / 1_000;
            let days = microseconds.div_euclid(MICROSECONDS_PER_DAY);
            let day_microseconds = microseconds.rem_euclid(MICROSECONDS_PER_DAY);
            let seconds = day_microseconds / MICROSECONDS_PER_SECOND;
            let remaining_microseconds = day_microseconds % MICROSECONDS_PER_SECOND;
            let days = i32::try_from(days).map_err(|_| {
                PyOverflowError::new_err("q timespan is outside Python's supported range")
            })?;
            PyDelta::new(
                py,
                days,
                seconds as i32,
                remaining_microseconds as i32,
                false,
            )?
            .into_py_any(py)
        }
        K::MixedList(values) => {
            let mut py_objects = Vec::new();
            py_objects.try_reserve_exact(values.len()).map_err(|_| {
                pyo3::exceptions::PyMemoryError::new_err("cannot allocate q mixed-list result")
            })?;
            for value in values {
                py_objects.push(cast_k_to_py_inner(py, value, depth + 1)?);
            }
            PyTuple::new(py, py_objects)?.into_py_any(py)
        }
        K::Series(k) => Ok(Py::new(py, ArrowSeries::new(k))?.into_any()),
        K::DataFrame(k) => Ok(Py::new(py, ArrowTable::new(k))?.into_any()),
        K::Operator(operator) => Ok(Py::new(py, XqdbQOperator { operator })?.into_any()),
        K::Lambda(lambda) => Ok(Py::new(py, XqdbQLambda { lambda })?.into_any()),
        K::QValue(value) => Ok(Py::new(py, XqdbQValue::from_qvalue(value))?.into_any()),
        K::Null => Ok(py.None()),
        K::Dict(dict) => {
            let py_dict = PyDict::new(py);
            for (key, value) in dict {
                py_dict.set_item(key, cast_k_to_py_inner(py, value, depth + 1)?)?;
            }
            Ok(py_dict.into())
        }
    }
}

#[pymethods]
impl XqdbConnector {
    #[new]
    #[pyo3(signature = (
        host,
        port,
        user,
        password,
        enable_tls,
        timeout,
        version,
        *,
        lossless = false,
        compression = "auto",
        compression_threshold = 10_000_000,
        connect_timeout = None,
        read_timeout = None,
        write_timeout = None,
        max_message_bytes = None,
        max_pending_notifications = 1024,
        tls_ca = None,
        tls_cert = None,
        tls_key = None,
        tls_server_name = None
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn __init__(
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        enable_tls: bool,
        timeout: f64,
        version: u8,
        lossless: bool,
        compression: &str,
        compression_threshold: usize,
        connect_timeout: Option<f64>,
        read_timeout: Option<f64>,
        write_timeout: Option<f64>,
        max_message_bytes: Option<usize>,
        max_pending_notifications: usize,
        tls_ca: Option<Bound<'_, PyAny>>,
        tls_cert: Option<Bound<'_, PyAny>>,
        tls_key: Option<Bound<'_, PyAny>>,
        tls_server_name: Option<String>,
    ) -> Result<Self, PyXqdbError> {
        if compression_threshold == 0 {
            return Err(
                PyValueError::new_err("compression_threshold must be a positive integer").into(),
            );
        }
        if max_message_bytes == Some(0) {
            return Err(
                PyValueError::new_err("max_message_bytes must be a positive integer").into(),
            );
        }
        if max_pending_notifications == 0 {
            return Err(PyValueError::new_err(
                "max_pending_notifications must be a positive integer",
            )
            .into());
        }

        let timeout = parse_timeout("timeout", timeout)?;
        let mut q = Connector::new(host, port, user, password, enable_tls, 0, version);
        q.timeout = timeout;
        q.configure(ConnectorSettings {
            value_mode: if lossless {
                ValueMode::Lossless
            } else {
                ValueMode::Native
            },
            compression: parse_compression(compression)?,
            compression_threshold,
            connect_timeout: parse_optional_timeout("connect_timeout", connect_timeout)?,
            read_timeout: parse_optional_timeout("read_timeout", read_timeout)?,
            write_timeout: parse_optional_timeout("write_timeout", write_timeout)?,
            max_message_bytes,
            max_pending_notifications,
            tls_ca: parse_pem("tls_ca", tls_ca)?,
            tls_cert: parse_pem("tls_cert", tls_cert)?,
            tls_key: parse_pem("tls_key", tls_key)?,
            tls_server_name,
        })?;
        let abort_handle = q.abort_handle();
        Ok(Self {
            q: Mutex::new(q),
            abort_handle,
        })
    }

    #[getter]
    fn symbol_encoding(&self, py: Python<'_>) -> &'static str {
        py.detach(|| {
            self.q
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .symbol_encoding
                .name()
        })
    }

    #[setter]
    fn set_symbol_encoding(&self, py: Python<'_>, value: &str) -> PyResult<()> {
        let encoding = parse_symbol_encoding(value)?;
        py.detach(|| {
            self.q
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .symbol_encoding = encoding;
        });
        Ok(())
    }

    fn abort_handle(&self) -> XqdbAbortHandle {
        XqdbAbortHandle {
            handle: self.abort_handle.clone(),
        }
    }

    pub fn connect(&self, py: Python) -> Result<(), PyXqdbError> {
        py.detach(|| {
            self.q
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .connect()
                .map_err(PyXqdbError::from)
        })
    }

    pub fn shutdown(&self, py: Python) -> Result<(), PyXqdbError> {
        self.abort_handle.abort()?;
        py.detach(|| {
            match self
                .q
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .shutdown()
            {
                Ok(()) | Err(xqdb::errors::XqdbError::NotConnectedErr()) => Ok(()),
                Err(error) => Err(PyXqdbError::from(error)),
            }
        })
    }

    #[pyo3(signature = (expr, *args))]
    pub fn sync(&self, py: Python, expr: &str, args: Bound<PyTuple>) -> PyResult<Py<PyAny>> {
        self.execute(py, expr, args)
    }

    #[pyo3(signature = (expr, *args))]
    pub fn asyn(&self, py: Python, expr: &str, args: Bound<PyTuple>) -> Result<(), PyXqdbError> {
        self.execute_async(py, expr, args)
    }

    pub fn receive(&self, py: Python) -> PyResult<Py<PyAny>> {
        let k = py
            .detach(move || {
                self.q
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .receive()
            })
            .map_err(PyXqdbError::from)?;
        cast_k_to_py(py, k)
    }
}

fn cast_to_k_vec(tuple: Bound<PyTuple>) -> Result<Vec<K>, PyXqdbError> {
    if tuple.len() > MAX_CALL_ARGUMENTS {
        return Err(PyTypeError::new_err(format!(
            "q functions accept at most {MAX_CALL_ARGUMENTS} arguments"
        ))
        .into());
    }

    let mut active_containers = HashSet::new();
    tuple
        .into_iter()
        .map(|value| cast_to_k_inner(value, 0, &mut active_containers))
        .collect::<PyResult<Vec<_>>>()
        .map_err(PyXqdbError::from)
}

fn cast_to_k(any: Bound<PyAny>) -> PyResult<K> {
    cast_to_k_inner(any, 0, &mut HashSet::new())
}

fn cast_to_k_inner(
    any: Bound<PyAny>,
    depth: usize,
    active_containers: &mut HashSet<usize>,
) -> PyResult<K> {
    if depth > MAX_CONVERSION_DEPTH {
        return Err(PyValueError::new_err(format!(
            "Python value nesting exceeds {MAX_CONVERSION_DEPTH} levels"
        )));
    }

    if any.is_instance_of::<XqdbQValue>() {
        let value = any.extract::<PyRef<XqdbQValue>>()?;
        Ok(K::QValue(value.value.clone()))
    } else if any.is_instance_of::<XqdbQOperator>() {
        let value = any.extract::<PyRef<XqdbQOperator>>()?;
        Ok(K::Operator(value.operator))
    } else if any.is_instance_of::<XqdbQLambda>() {
        let value = any.extract::<PyRef<XqdbQLambda>>()?;
        Ok(K::Lambda(value.lambda.clone()))
    } else if any.is_instance_of::<PyBool>() {
        Ok(K::Boolean(any.extract()?))
    } else if any.is_instance_of::<PyInt>() {
        Ok(K::I64(any.extract()?))
    } else if any.is_instance_of::<PyFloat>() {
        Ok(K::F64(any.extract()?))
    } else if any.is_instance_of::<PyString>() {
        Ok(K::Symbol(any.extract::<&str>()?.to_owned()))
    } else if any.is_instance_of::<PyBytes>() {
        let value = any.cast::<PyBytes>()?;
        Ok(K::CharVector(value.as_bytes().to_vec()))
    } else if any.hasattr("__arrow_c_stream__")? {
        match import_value(&any)? {
            ArrowValue::Series(series) => Ok(K::Series(series)),
            ArrowValue::DataFrame(frame) => Ok(K::DataFrame(frame)),
        }
    } else if any.is_none() {
        Ok(K::Null)
    } else if any.is_instance_of::<PyDateTime>() {
        let datetime = any.cast::<PyDateTime>()?;
        let py = any.py();
        // A q timestamp carries no timezone, so a naive datetime is taken as the q wall
        // clock as-is and values read out of naive `timestamp[ns]` columns and atoms
        // round-trip unchanged. Awareness uses Python's documented test,
        // `d.tzinfo is not None and d.tzinfo.utcoffset(d) is not None`, asking the tzinfo
        // directly rather than the datetime's own overrideable `utcoffset()`. Misjudging
        // this would hand the value to `astimezone()`, which presumes the host's local
        // zone for a naive datetime and would silently shift the wall clock.
        let value: chrono::DateTime<chrono::Utc> = match datetime.get_tzinfo() {
            None => datetime.extract::<chrono::NaiveDateTime>()?.and_utc(),
            Some(tz) => {
                let offset = tz.call_method1(pyo3::intern!(py, "utcoffset"), (&datetime,))?;
                if offset.is_none() {
                    // Python considers this naive. Drop the offsetless tzinfo without
                    // touching the wall-clock fields.
                    let kwargs = PyDict::new(py);
                    kwargs.set_item(pyo3::intern!(py, "tzinfo"), py.None())?;
                    datetime
                        .call_method(pyo3::intern!(py, "replace"), (), Some(&kwargs))?
                        .extract::<chrono::NaiveDateTime>()?
                        .and_utc()
                } else {
                    // Let Python resolve the instant so fixed offsets, `zoneinfo` zones,
                    // and any other tzinfo normalize identically, including across DST.
                    let utc = PyTzInfo::utc(py)?;
                    datetime
                        .call_method1(pyo3::intern!(py, "astimezone"), (utc,))?
                        .extract::<chrono::DateTime<chrono::Utc>>()?
                }
            }
        };
        // `pandas.Timestamp` subclasses `datetime` and keeps its sub-microsecond digits in
        // `.nanosecond`, which the datetime accessors never expose. PyArrow hands back that
        // type for `timestamp[ns]` scalars, so fold the remainder in to keep a nanosecond
        // value read out of an Arrow column exact. Python offsets have at most microsecond
        // resolution, so the remainder is invariant under the conversion above and the
        // original object is the right source.
        let value = match datetime.getattr_opt(pyo3::intern!(py, "nanosecond"))? {
            Some(attr) => {
                let remainder = attr.extract::<i64>()?;
                if !(0..1_000).contains(&remainder) {
                    return Err(PyValueError::new_err(
                        "datetime.nanosecond must be between 0 and 999",
                    ));
                }
                value
                    .checked_add_signed(chrono::TimeDelta::nanoseconds(remainder))
                    .ok_or_else(|| {
                        PyOverflowError::new_err(
                            "datetime is outside q's representable timestamp range",
                        )
                    })?
            }
            None => value,
        };
        let nanoseconds = value.timestamp_nanos_opt().ok_or_else(|| {
            PyOverflowError::new_err("datetime is outside q's representable timestamp range")
        })?;
        if nanoseconds < MIN_Q_TIMESTAMP_UNIX_NANOS {
            return Err(PyOverflowError::new_err(
                "datetime is outside q's representable timestamp range",
            ));
        }
        Ok(K::DateTime(value))
    } else if any.is_instance_of::<PyDate>() {
        let value: chrono::NaiveDate = any.cast::<PyDate>()?.extract()?;
        Ok(K::Date(value))
    } else if any.is_instance_of::<PyTime>() {
        let value: chrono::NaiveTime = any.cast::<PyTime>()?.extract()?;
        if !value.nanosecond().is_multiple_of(1_000_000) {
            return Err(PyValueError::new_err(
                "q time only supports millisecond precision",
            ));
        }
        Ok(K::Time(value))
    } else if any.is_instance_of::<PyDelta>() {
        let value: chrono::Duration = any.cast::<PyDelta>()?.extract()?;
        if value.num_nanoseconds().is_none() {
            return Err(PyOverflowError::new_err(
                "timedelta is outside q's representable timespan range",
            ));
        }
        Ok(K::Duration(value))
    } else if any.is_instance_of::<PyDict>() {
        let identity = any.as_ptr() as usize;
        if !active_containers.insert(identity) {
            return Err(PyValueError::new_err(
                "cyclic Python containers cannot be converted to q",
            ));
        }
        let result = (|| {
            let py_dict = any.cast::<PyDict>()?;
            let mut dict = IndexMap::with_capacity(py_dict.len());
            for (key, value) in py_dict {
                let key = key.extract::<&str>()?.to_owned();
                dict.insert(key, cast_to_k_inner(value, depth + 1, active_containers)?);
            }
            Ok(K::Dict(dict))
        })();
        active_containers.remove(&identity);
        result
    } else if any.is_instance_of::<PyList>() {
        let identity = any.as_ptr() as usize;
        if !active_containers.insert(identity) {
            return Err(PyValueError::new_err(
                "cyclic Python containers cannot be converted to q",
            ));
        }
        let result = (|| {
            let py_list = any.cast::<PyList>()?;
            let mut values = Vec::with_capacity(py_list.len());
            for value in py_list {
                values.push(cast_to_k_inner(value, depth + 1, active_containers)?);
            }
            Ok(K::MixedList(values))
        })();
        active_containers.remove(&identity);
        result
    } else if any.is_instance_of::<PyTuple>() {
        let identity = any.as_ptr() as usize;
        if !active_containers.insert(identity) {
            return Err(PyValueError::new_err(
                "cyclic Python containers cannot be converted to q",
            ));
        }
        let result = (|| {
            let py_tuple = any.cast::<PyTuple>()?;
            let mut values = Vec::with_capacity(py_tuple.len());
            for value in py_tuple {
                values.push(cast_to_k_inner(value, depth + 1, active_containers)?);
            }
            Ok(K::MixedList(values))
        })();
        active_containers.remove(&identity);
        result
    } else {
        Err(PyTypeError::new_err(format!(
            "unsupported Python type {:?}",
            any.get_type()
        )))
    }
}

#[pyfunction]
#[pyo3(signature = (filepath, symbol_encoding = "strict"))]
pub fn read_j6_binary_table(
    py: Python,
    filepath: &str,
    symbol_encoding: &str,
) -> PyResult<Py<ArrowTable>> {
    let encoding = parse_symbol_encoding(symbol_encoding)?;
    let filepath = filepath.to_owned();
    let frame = py
        .detach(move || xqdb::io::read_j6_binary_table(&filepath, encoding))
        .map_err(PyXqdbError::from)?;
    Py::new(py, ArrowTable::new(frame))
}

fn value_mode(lossless: bool) -> ValueMode {
    if lossless {
        ValueMode::Lossless
    } else {
        ValueMode::Native
    }
}

#[pyfunction]
#[pyo3(signature = (body, symbol_encoding = "strict", lossless = false))]
pub fn deserialize_value6(
    py: Python,
    body: Bound<'_, PyBytes>,
    symbol_encoding: &str,
    lossless: bool,
) -> PyResult<Py<PyAny>> {
    let encoding = parse_symbol_encoding(symbol_encoding)?;
    let body = PyBackedBytes::from(body);
    let value = py
        .detach(move || xqdb::io::deserialize_j6(body.as_ref(), encoding, value_mode(lossless)))
        .map_err(PyXqdbError::from)?;
    cast_k_to_py(py, value)
}

#[pyfunction]
#[pyo3(signature = (frame, symbol_encoding = "strict", lossless = false))]
pub fn deserialize_ipc_bytes6(
    py: Python,
    frame: Bound<'_, PyBytes>,
    symbol_encoding: &str,
    lossless: bool,
) -> PyResult<Py<PyTuple>> {
    let encoding = parse_symbol_encoding(symbol_encoding)?;
    let frame = PyBackedBytes::from(frame);
    let (message_type, value) = py
        .detach(move || {
            xqdb::io::deserialize_j6_ipc_msg(frame.as_ref(), encoding, value_mode(lossless))
        })
        .map_err(PyXqdbError::from)?;
    let message_type = match message_type {
        MsgType::Async => "async",
        MsgType::Sync => "sync",
        MsgType::Response => "response",
    };
    let message_type = message_type.into_py_any(py)?;
    let value = cast_k_to_py(py, value)?;
    Ok(PyTuple::new(py, [message_type, value])?.unbind())
}

#[pyfunction]
pub fn generate_j6_ipc_msg<'a>(
    py: Python<'a>,
    msg_type: u8,
    enable_compression: bool,
    any: Bound<PyAny>,
) -> PyResult<Bound<'a, PyBytes>> {
    let msg_type = match msg_type {
        0 => MsgType::Async,
        1 => MsgType::Sync,
        2 => MsgType::Response,
        value => {
            return Err(PyValueError::new_err(format!(
                "msg_type must be 0, 1, or 2; got {value}"
            )))
        }
    };
    let value = cast_to_k(any)?;
    let bytes = py
        .detach(move || xqdb::io::generate_j6_ipc_msg(msg_type, enable_compression, value))
        .map_err(PyXqdbError::from)?;
    Ok(PyBytes::new(py, &bytes))
}
