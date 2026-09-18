use std::io::{self, Cursor, Write};

use polars::prelude::{DataFrame, IpcStreamReader, IpcStreamWriter, SerReader, SerWriter, Series};

use crate::error::BindingError;

#[derive(Default)]
struct FallibleVecWriter {
    bytes: Vec<u8>,
}

impl FallibleVecWriter {
    fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for FallibleVecWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.bytes.try_reserve(buffer.len()).map_err(|error| {
            io::Error::other(format!(
                "unable to grow Arrow IPC output by {} bytes: {error}",
                buffer.len()
            ))
        })?;
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) fn dataframe_to_ipc(mut dataframe: DataFrame) -> Result<Vec<u8>, BindingError> {
    let mut bytes = FallibleVecWriter::default();
    IpcStreamWriter::new(&mut bytes)
        .finish(&mut dataframe)
        .map_err(|error| {
            BindingError::conversion(format!("failed to encode Arrow IPC stream: {error}"))
        })?;
    Ok(bytes.into_inner())
}

pub(crate) fn series_to_ipc(series: Series) -> Result<Vec<u8>, BindingError> {
    let dataframe = DataFrame::new_infer_height(vec![series.into()]).map_err(|error| {
        BindingError::conversion(format!("failed to materialize series: {error}"))
    })?;
    dataframe_to_ipc(dataframe)
}

pub(crate) fn dataframe_from_ipc(bytes: Vec<u8>) -> Result<DataFrame, BindingError> {
    IpcStreamReader::new(Cursor::new(bytes))
        .finish()
        .map_err(|error| {
            BindingError::conversion(format!("invalid Arrow IPC table stream: {error}"))
        })
}

pub(crate) fn series_from_ipc(bytes: Vec<u8>) -> Result<Series, BindingError> {
    let dataframe = dataframe_from_ipc(bytes)?;
    let mut columns = dataframe.into_columns();
    if columns.len() != 1 {
        return Err(BindingError::conversion(format!(
            "Arrow IPC series stream must contain exactly one column, found {}",
            columns.len()
        )));
    }
    Ok(columns.remove(0).take_materialized_series())
}

#[cfg(test)]
mod tests {
    use polars::prelude::{Categories, DataFrame, DataType, NamedFrom, Series};
    use xqdb::{
        io::generate_j6_ipc_msg,
        types::{MsgType, K},
    };

    use super::{dataframe_from_ipc, dataframe_to_ipc, series_from_ipc, series_to_ipc};

    #[test]
    fn round_trips_series_as_one_column_stream() {
        let series = Series::new("values".into(), [1i64, 2, 3]);
        let decoded = series_from_ipc(series_to_ipc(series.clone()).expect("encode series"))
            .expect("decode series");
        assert_eq!(decoded, series);
    }

    #[test]
    fn round_trips_empty_categorical_series_as_q_symbol_vector() {
        let series = Series::new_empty(
            "symbols".into(),
            &DataType::Categorical(Categories::global(), Categories::global().mapping()),
        );
        let decoded =
            series_from_ipc(series_to_ipc(series).expect("encode empty categorical series"))
                .expect("decode empty categorical series");

        assert!(matches!(decoded.dtype(), DataType::Categorical(_, _)));
        assert_eq!(decoded.len(), 0);
        let frame = generate_j6_ipc_msg(MsgType::Sync, false, K::Series(decoded))
            .expect("serialize empty q symbol vector");
        assert_eq!(&frame[8..], &[11, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn round_trips_populated_nullable_categorical_series_as_q_symbols() {
        let series = Series::new("symbols".into(), [Some("AAPL"), None, Some("MSFT")])
            .cast(&DataType::Categorical(
                Categories::global(),
                Categories::global().mapping(),
            ))
            .expect("categorical symbols");
        let decoded = series_from_ipc(series_to_ipc(series).expect("encode categorical series"))
            .expect("decode categorical series");

        assert!(matches!(decoded.dtype(), DataType::Categorical(_, _)));
        assert_eq!(decoded.len(), 3);
        let frame = generate_j6_ipc_msg(MsgType::Sync, false, K::Series(decoded))
            .expect("serialize q symbol vector");
        assert_eq!(
            &frame[8..],
            &[11, 0, 3, 0, 0, 0, b'A', b'A', b'P', b'L', 0, 0, b'M', b'S', b'F', b'T', 0,]
        );
    }

    #[test]
    fn keeps_populated_utf8_series_as_q_string_list() {
        let series = Series::new("strings".into(), ["AAPL", "MSFT"]);
        let decoded = series_from_ipc(series_to_ipc(series).expect("encode UTF-8 series"))
            .expect("decode UTF-8 series");

        assert_eq!(decoded.dtype(), &DataType::String);
        let frame = generate_j6_ipc_msg(MsgType::Sync, false, K::Series(decoded))
            .expect("serialize q string list");
        assert_eq!(
            &frame[8..],
            &[
                0, 0, 2, 0, 0, 0, 10, 0, 4, 0, 0, 0, b'A', b'A', b'P', b'L', 10, 0, 4, 0, 0, 0,
                b'M', b'S', b'F', b'T',
            ]
        );
    }

    #[test]
    fn round_trips_dataframe_as_stream() {
        let dataframe = DataFrame::new_infer_height(vec![
            Series::new("id".into(), [1i64, 2]).into(),
            Series::new("price".into(), [10.5f64, 20.25]).into(),
        ])
        .expect("dataframe");
        let decoded =
            dataframe_from_ipc(dataframe_to_ipc(dataframe.clone()).expect("encode dataframe"))
                .expect("decode dataframe");
        assert_eq!(decoded, dataframe);
    }

    #[test]
    fn rejects_multi_column_series_stream() {
        let dataframe = DataFrame::new_infer_height(vec![
            Series::new("left".into(), [1i32]).into(),
            Series::new("right".into(), [2i32]).into(),
        ])
        .expect("dataframe");
        let error = series_from_ipc(dataframe_to_ipc(dataframe).expect("encode dataframe"))
            .expect_err("multiple columns must be rejected");
        assert_eq!(error.code, "XQDB_CONVERSION");
    }
}
