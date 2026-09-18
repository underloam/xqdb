use std::panic::{catch_unwind, AssertUnwindSafe};

use napi::bindgen_prelude::{Array, AsyncTask, Buffer, Env, Task, Unknown};
use napi_derive::napi;
use xqdb::io::{deserialize_j6, deserialize_j6_ipc_msg, generate_j6_ipc_msg, read_j6_binary_table};
use xqdb::qvalue::{QValue, ValueMode};
use xqdb::types::{MsgType, SymbolEncoding, K};

use crate::dto::{
    k_into_native, native_values_into_k, parse_symbol_encoding, snapshot_native_value,
    snapshot_native_value_list, NativeResult, OwnedNativeValue,
};
use crate::error::BindingError;

const UTILITY_SNAPSHOT_LIMIT: usize = 64 * 1024 * 1024;

enum UtilityOperation {
    Ready(Option<NativeResult>),
    ReadBinary {
        path: String,
        encoding: SymbolEncoding,
    },
    Serialize {
        message_type: String,
        compress: bool,
        value: OwnedNativeValue,
    },
    DeserializeValue {
        bytes: Buffer,
        encoding: SymbolEncoding,
        mode: ValueMode,
    },
    DeserializeIpc {
        bytes: Buffer,
        encoding: SymbolEncoding,
        mode: ValueMode,
    },
    QValueBytes(Buffer),
    QValueAtom {
        kind: u8,
        payload: Buffer,
    },
    QValueList(Vec<OwnedNativeValue>),
    QValueDictionary {
        keys: OwnedNativeValue,
        values: OwnedNativeValue,
    },
    QValueNative(OwnedNativeValue),
}

fn is_windows_remote_or_device_path(path: &str) -> bool {
    let bytes = path.as_bytes();
    (bytes.len() >= 2 && matches!(bytes[0], b'\\' | b'/') && matches!(bytes[1], b'\\' | b'/'))
        || path.starts_with(r"\??\")
        || path.starts_with("/??/")
}

fn validate_read_binary_path(path: &str) -> Result<(), BindingError> {
    if cfg!(windows) && is_windows_remote_or_device_path(path) {
        return Err(BindingError::conversion(
            "readBinary6 does not accept Windows UNC or device paths",
        ));
    }
    Ok(())
}

pub struct UtilityTask {
    operation: UtilityOperation,
}

#[napi]
impl Task for UtilityTask {
    type Output = NativeResult;
    type JsValue = NativeResult;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        let operation = std::mem::replace(&mut self.operation, UtilityOperation::Ready(None));
        Ok(catch_unwind(AssertUnwindSafe(|| match operation {
            UtilityOperation::Ready(result) => result.unwrap_or_else(|| {
                NativeResult::failure(BindingError::internal(
                    "native utility task was computed more than once",
                ))
            }),
            UtilityOperation::ReadBinary { path, encoding } => {
                let result = validate_read_binary_path(&path)
                    .and_then(|()| {
                        read_j6_binary_table(&path, encoding)
                            .map(K::DataFrame)
                            .map_err(BindingError::from)
                    })
                    .and_then(k_into_native)
                    .map(Some);
                NativeResult::from_result(result)
            }
            UtilityOperation::Serialize {
                message_type,
                compress,
                value,
            } => {
                let result = parse_message_type(&message_type)
                    .and_then(|message_type| {
                        value.into_k().and_then(|value| {
                            generate_j6_ipc_msg(message_type, compress, value)
                                .map_err(BindingError::from)
                        })
                    })
                    .and_then(|bytes| k_into_native(K::CharVector(bytes)))
                    .map(Some);
                NativeResult::from_result(result)
            }
            UtilityOperation::DeserializeValue {
                bytes,
                encoding,
                mode,
            } => {
                let result = deserialize_j6(&bytes, encoding, mode)
                    .map_err(BindingError::from)
                    .and_then(k_into_native)
                    .map(Some);
                NativeResult::from_result(result)
            }
            UtilityOperation::DeserializeIpc {
                bytes,
                encoding,
                mode,
            } => match deserialize_j6_ipc_msg(&bytes, encoding, mode)
                .map_err(BindingError::from)
                .and_then(|(message_type, value)| {
                    Ok((
                        message_type_name(message_type).to_owned(),
                        k_into_native(value)?,
                    ))
                }) {
                Ok((message_type, value)) => {
                    NativeResult::success_with_message(message_type, value)
                }
                Err(error) => NativeResult::failure(error),
            },
            UtilityOperation::QValueBytes(bytes) => {
                qvalue_result(QValue::from_bytes(&bytes).map_err(BindingError::from))
            }
            UtilityOperation::QValueAtom { kind, payload } => {
                qvalue_result(QValue::atom(kind, &payload).map_err(BindingError::from))
            }
            UtilityOperation::QValueList(values) => {
                let result = native_values_into_k(values)
                    .and_then(require_qvalues)
                    .and_then(|values| QValue::list(&values).map_err(BindingError::from));
                qvalue_result(result)
            }
            UtilityOperation::QValueDictionary { keys, values } => {
                let result = keys.into_k().and_then(require_qvalue).and_then(|keys| {
                    values.into_k().and_then(require_qvalue).and_then(|values| {
                        QValue::dictionary(&keys, &values).map_err(BindingError::from)
                    })
                });
                qvalue_result(result)
            }
            UtilityOperation::QValueNative(value) => {
                let result = value.into_k().and_then(qvalue_from_native);
                qvalue_result(result)
            }
        }))
        .unwrap_or_else(|_| {
            NativeResult::failure(BindingError::internal("native utility operation panicked"))
        }))
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        Ok(output)
    }
}

#[napi(js_name = "readBinary6")]
pub fn read_binary6(path: String, symbol_encoding: Option<String>) -> AsyncTask<UtilityTask> {
    let operation = match parse_symbol_encoding(symbol_encoding.as_deref()) {
        Ok(encoding) => UtilityOperation::ReadBinary { path, encoding },
        Err(message) => ready_operation(BindingError::conversion(message)),
    };
    AsyncTask::new(UtilityTask { operation })
}

#[napi(
    js_name = "serializeAsIpcBytes6",
    ts_args_type = "messageType: string, compress: boolean, value: NativeValue"
)]
pub fn serialize_as_ipc_bytes6(
    message_type: String,
    compress: bool,
    value: Unknown<'_>,
) -> AsyncTask<UtilityTask> {
    let operation = match snapshot_native_value(value) {
        Ok(value) => UtilityOperation::Serialize {
            message_type,
            compress,
            value,
        },
        Err(error) => ready_operation(error),
    };
    AsyncTask::new(UtilityTask { operation })
}

#[napi(js_name = "deserializeValue6")]
pub fn deserialize_value6(
    body: Buffer,
    symbol_encoding: Option<String>,
    lossless: Option<bool>,
) -> AsyncTask<UtilityTask> {
    AsyncTask::new(UtilityTask {
        operation: decoder_operation(symbol_encoding, lossless, |encoding, mode| {
            UtilityOperation::DeserializeValue {
                bytes: body,
                encoding,
                mode,
            }
        }),
    })
}

#[napi(js_name = "deserializeIpcBytes6")]
pub fn deserialize_ipc_bytes6(
    frame: Buffer,
    symbol_encoding: Option<String>,
    lossless: Option<bool>,
) -> AsyncTask<UtilityTask> {
    AsyncTask::new(UtilityTask {
        operation: decoder_operation(symbol_encoding, lossless, |encoding, mode| {
            UtilityOperation::DeserializeIpc {
                bytes: frame,
                encoding,
                mode,
            }
        }),
    })
}

#[napi(js_name = "qValueFromBytes")]
pub fn qvalue_from_bytes(bytes: Buffer) -> AsyncTask<UtilityTask> {
    AsyncTask::new(UtilityTask {
        operation: UtilityOperation::QValueBytes(bytes),
    })
}

#[napi(js_name = "qValueAtom")]
pub fn qvalue_atom(kind: f64, payload: Buffer) -> AsyncTask<UtilityTask> {
    let operation = if !kind.is_finite()
        || kind.fract() != 0.0
        || kind < u8::MIN as f64
        || kind > u8::MAX as f64
    {
        ready_operation(BindingError::conversion(
            "qValueAtom kind must be an integer from 0 through 255",
        ))
    } else {
        UtilityOperation::QValueAtom {
            kind: kind as u8,
            payload,
        }
    };
    AsyncTask::new(UtilityTask { operation })
}

#[napi(js_name = "qValueList", ts_args_type = "values: NativeValue[]")]
pub fn qvalue_list(values: Array<'_>) -> AsyncTask<UtilityTask> {
    let operation = match snapshot_native_value_list(values, UTILITY_SNAPSHOT_LIMIT) {
        Ok((values, _)) => UtilityOperation::QValueList(values),
        Err(error) => ready_operation(error),
    };
    AsyncTask::new(UtilityTask { operation })
}

#[napi(
    js_name = "qValueDictionary",
    ts_args_type = "keys: NativeValue, values: NativeValue"
)]
pub fn qvalue_dictionary(keys: Unknown<'_>, values: Unknown<'_>) -> AsyncTask<UtilityTask> {
    let operation = snapshot_native_value(keys).and_then(|keys| {
        snapshot_native_value(values)
            .map(|values| UtilityOperation::QValueDictionary { keys, values })
    });
    AsyncTask::new(UtilityTask {
        operation: operation.unwrap_or_else(ready_operation),
    })
}

#[napi(js_name = "qValueFromNative", ts_args_type = "value: NativeValue")]
pub fn qvalue_from_native_value(value: Unknown<'_>) -> AsyncTask<UtilityTask> {
    let operation = snapshot_native_value(value)
        .map(UtilityOperation::QValueNative)
        .unwrap_or_else(ready_operation);
    AsyncTask::new(UtilityTask { operation })
}

fn decoder_operation(
    symbol_encoding: Option<String>,
    lossless: Option<bool>,
    operation: impl FnOnce(SymbolEncoding, ValueMode) -> UtilityOperation,
) -> UtilityOperation {
    match parse_symbol_encoding(symbol_encoding.as_deref()) {
        Ok(encoding) => operation(
            encoding,
            if lossless.unwrap_or(false) {
                ValueMode::Lossless
            } else {
                ValueMode::Native
            },
        ),
        Err(message) => ready_operation(BindingError::conversion(message)),
    }
}

fn ready_operation(error: BindingError) -> UtilityOperation {
    UtilityOperation::Ready(Some(NativeResult::failure(error)))
}

fn qvalue_result(result: Result<QValue, BindingError>) -> NativeResult {
    NativeResult::from_result(result.map(K::QValue).and_then(k_into_native).map(Some))
}

fn require_qvalue(value: K) -> Result<QValue, BindingError> {
    match value {
        K::QValue(value) => Ok(value),
        _ => Err(BindingError::conversion(
            "qvalue list/dictionary constructors require XqdbQValue inputs",
        )),
    }
}

fn require_qvalues(values: Vec<K>) -> Result<Vec<QValue>, BindingError> {
    let mut output = Vec::new();
    output.try_reserve_exact(values.len()).map_err(|error| {
        BindingError::conversion(format!(
            "unable to allocate qvalue constructor input list: {error}"
        ))
    })?;
    for value in values {
        output.push(require_qvalue(value)?);
    }
    Ok(output)
}

fn qvalue_from_native(value: K) -> Result<QValue, BindingError> {
    match value {
        K::QValue(value) => Ok(value),
        value => {
            let mut frame =
                generate_j6_ipc_msg(MsgType::Response, false, value).map_err(BindingError::from)?;
            if frame.len() < 8 {
                return Err(BindingError::internal(
                    "serialized q value omitted its IPC header",
                ));
            }
            frame.drain(..8);
            QValue::from_owned_bytes(frame).map_err(BindingError::from)
        }
    }
}

fn parse_message_type(value: &str) -> Result<MsgType, BindingError> {
    match value {
        "async" => Ok(MsgType::Async),
        "sync" => Ok(MsgType::Sync),
        "response" => Ok(MsgType::Response),
        value => Err(BindingError::conversion(format!(
            "unsupported messageType {value:?}; expected async, sync, or response"
        ))),
    }
}

fn message_type_name(value: MsgType) -> &'static str {
    match value {
        MsgType::Async => "async",
        MsgType::Sync => "sync",
        MsgType::Response => "response",
    }
}
#[cfg(test)]
mod tests {
    #[cfg(windows)]
    use super::validate_read_binary_path;
    use super::{is_windows_remote_or_device_path, parse_message_type};

    #[test]
    fn detects_windows_unc_and_device_paths() {
        for path in [
            r"\\server\share\table.bin",
            r"\\?\C:\data\table.bin",
            r"\\.\PhysicalDrive0",
            "//server/share/table.bin",
            "//?/C:/data/table.bin",
            r"\/server\share\table.bin",
            r"/\server\share\table.bin",
            r"\??\C:\data\table.bin",
            "/??/C:/data/table.bin",
        ] {
            assert!(
                is_windows_remote_or_device_path(path),
                "expected unsafe Windows path: {path}"
            );
        }
    }

    #[test]
    fn accepts_local_path_syntax() {
        for path in [
            "table.bin",
            "data/table.bin",
            r"C:\data\table.bin",
            "/var/lib/xqdb/table.bin",
        ] {
            assert!(
                !is_windows_remote_or_device_path(path),
                "expected local path: {path}"
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn rejects_windows_remote_paths_before_reading() {
        let error = validate_read_binary_path(r"\\server\share\table.bin")
            .expect_err("UNC path should fail");
        assert_eq!(error.code, "XQDB_CONVERSION");
    }

    #[test]
    fn rejects_unknown_message_type_as_conversion_error() {
        let error = match parse_message_type("query") {
            Err(error) => error,
            Ok(_) => panic!("unknown message type"),
        };
        assert_eq!(error.code, "XQDB_CONVERSION");
    }
}
