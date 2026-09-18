import gc
import struct

import narwhals as nw
import pyarrow as pa
import pytest

from xqdb import deserialize_value6, serialize_as_ipc_bytes6


@pytest.mark.parametrize(
    ("dtype", "values"),
    [
        (pa.int16(), [-32767, None, 32767]),
        (pa.int32(), [-2147483647, None, 2147483647]),
        (pa.int64(), [-(1 << 63) + 1, None, (1 << 63) - 1]),
        (pa.float32(), [float("-inf"), None, float("inf")]),
        (pa.float64(), [float("-inf"), None, float("inf")]),
    ],
)
def test_nested_numeric_results_own_buffers_after_source_release(
    dtype: pa.DataType, values: list[object]
) -> None:
    expected = pa.table({"depth": pa.array([[], values, []], type=pa.large_list(dtype))})
    body = serialize_as_ipc_bytes6("response", enable_compression=False, any=expected)[8:]
    result = deserialize_value6(body)
    del body
    gc.collect()

    actual = nw.to_native(result)
    assert actual.equals(expected)
    # The Arrow result must remain usable as an input after the source frame is gone.
    round_trip = deserialize_value6(
        serialize_as_ipc_bytes6("response", enable_compression=False, any=result)[8:]
    )
    assert nw.to_native(round_trip).equals(expected)


def test_nested_numeric_lists_with_no_values_keep_typed_empty_rows() -> None:
    expected = pa.table({"depth": pa.array([[], []], type=pa.large_list(pa.int64()))})
    body = serialize_as_ipc_bytes6("response", enable_compression=False, any=expected)[8:]
    assert nw.to_native(deserialize_value6(body)).equals(expected)


def test_nested_timestamps_preserve_nanoseconds_after_source_release() -> None:
    empty_row = bytes([12, 0, 0, 0, 0, 0])
    body = (
        bytes([0, 0, 3, 0, 0, 0])
        + empty_row
        + bytes([12, 0, 3, 0, 0, 0])
        + struct.pack("<3q", 1, -(1 << 63), 3)
        + empty_row
    )
    result = deserialize_value6(body)
    del body
    gc.collect()

    expected = pa.chunked_array(
        [[[], [946684800000000001, None, 946684800000000003], []]],
        type=pa.large_list(pa.timestamp("ns")),
    )
    assert nw.to_native(result).equals(expected)
