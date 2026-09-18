import contextlib
import importlib
import math
import multiprocessing
import os
import select
import socket
import struct
import threading
import time as clock
from collections.abc import Callable, Iterable, Iterator
from datetime import date, datetime, time, timedelta, timezone, tzinfo
from multiprocessing.connection import Connection
from pathlib import Path
from types import TracebackType
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import narwhals as nw
import pyarrow as pa
import pytest

import xqdb
from xqdb import (
    Q,
    XqdbError,
    XqdbIOError,
    XqdbQLambda,
    XqdbQOperator,
    XqdbQValue,
    deserialize_ipc_bytes6,
    deserialize_value6,
    read_binary6,
    serialize_as_ipc_bytes6,
)
from xqdb.xqdb import XqdbConnector, generate_j6_ipc_msg


@contextlib.contextmanager
def _capture_thread_failures() -> Iterator[list[BaseException]]:
    failures: list[BaseException] = []
    previous_hook = threading.excepthook

    def capture_failure(args: threading.ExceptHookArgs) -> None:
        failures.append(args.exc_value)

    threading.excepthook = capture_failure
    try:
        yield failures
    finally:
        threading.excepthook = previous_hook


def _raise_thread_failure(failures: list[BaseException]) -> None:
    if failures:
        raise failures[0]


def _naive_q_datetime(*parts: int) -> datetime:
    aware = datetime(*parts, tzinfo=timezone.utc)
    return aware.replace(tzinfo=None)


def _exercise_property_access_during_receive(
    host: str,
    port: int,
    operation: Literal["get", "set"],
    connection: Connection,
) -> None:
    client: Q | None = None
    try:
        client = Q(host, port, read_timeout=0)
        client.connect()
        receive_started = threading.Event()
        receive_errors: list[str] = []

        def receive() -> None:
            receive_started.set()
            try:
                client.receive()
            except XqdbIOError as error:
                receive_errors.append(type(error).__name__)

        receiver = threading.Thread(target=receive, daemon=True)
        access_started = threading.Event()

        def access_property() -> None:
            access_started.set()
            if operation == "get":
                _ = client.symbol_encoding
            else:
                client.symbol_encoding = "lossy"

        accessor = threading.Thread(target=access_property, daemon=True)
        with _capture_thread_failures() as failures:
            receiver.start()
            assert receive_started.wait(timeout=1), "receive thread did not start"
            clock.sleep(0.05)
            accessor.start()
            assert access_started.wait(timeout=1), "property-access thread did not start"
            clock.sleep(0.05)

            client.cancel()
            receiver.join(timeout=2)
            accessor.join(timeout=2)
        _raise_thread_failure(failures)
        client.disconnect()
        connection.send(
            (
                "ok",
                receiver.is_alive(),
                accessor.is_alive(),
                receive_errors,
                [],
            )
        )
    finally:
        if client is not None:
            with contextlib.suppress(XqdbIOError):
                client.disconnect()
        connection.close()


def _exercise_cancel_and_disconnect_during_receive(
    host: str,
    port: int,
    connection: Connection,
) -> None:
    client: Q | None = None
    try:
        client = Q(host, port, read_timeout=0)
        client.connect()

        def interrupt_receive(
            operation: Literal["cancel", "disconnect"],
        ) -> tuple[bool, list[str]]:
            started = threading.Event()
            errors: list[str] = []

            def receive() -> None:
                started.set()
                try:
                    client.receive()
                except XqdbIOError as error:
                    errors.append(type(error).__name__)

            receiver = threading.Thread(target=receive, daemon=True)
            with _capture_thread_failures() as failures:
                receiver.start()
                assert started.wait(timeout=1), "receive thread did not start"
                clock.sleep(0.05)
                getattr(client, operation)()
                receiver.join(timeout=2)
            _raise_thread_failure(failures)
            return receiver.is_alive(), errors

        cancel_result = interrupt_receive("cancel")
        client.connect()
        disconnect_result = interrupt_receive("disconnect")
        connection.send(("ok", cancel_result, disconnect_result))
    finally:
        if client is not None:
            with contextlib.suppress(XqdbIOError):
                client.disconnect()
        connection.close()


def _exercise_positive_subnanosecond_timeout(
    host: str,
    port: int,
    connection: Connection,
) -> None:
    client = Q(host, port, read_timeout=float.fromhex("0x0.0000000000001p-1022"))
    started = clock.monotonic()
    try:
        client.connect()
        client.receive()
    except XqdbIOError as error:
        connection.send(("ok", type(error).__name__, clock.monotonic() - started))
    else:
        connection.send(("error", "receive unexpectedly returned"))
    finally:
        with contextlib.suppress(XqdbIOError):
            client.disconnect()
        connection.close()


@pytest.fixture
def silent_q_server() -> Iterator[int]:
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(3)
    port = listener.getsockname()[1]
    release = threading.Event()
    errors: list[OSError] = []

    def serve() -> None:
        try:
            with listener:
                peer, _ = listener.accept()
            with peer:
                peer.settimeout(3)
                credentials = bytearray()
                while 0 not in credentials:
                    data = peer.recv(1024)
                    if not data:
                        message = "client disconnected before q IPC authentication"
                        raise ConnectionError(message)
                    credentials.extend(data)
                try:
                    peer.sendall(b"\x06")
                except OSError:
                    # A sub-nanosecond read timeout may close the client during authentication.
                    return
                release.wait(timeout=5)
        except OSError as error:
            if not release.is_set():
                errors.append(error)

    thread = threading.Thread(target=serve, daemon=True)
    with _capture_thread_failures() as failures:
        thread.start()
        try:
            yield port
        finally:
            release.set()
            listener.close()
            thread.join(timeout=5)
    _raise_thread_failure(failures)
    assert not thread.is_alive()
    assert not errors


def _chunked(values: Iterable[object], dtype: pa.DataType) -> pa.ChunkedArray:
    return pa.chunked_array([pa.array(values, type=dtype)])


def _native_series(value: object) -> pa.ChunkedArray:
    assert isinstance(value, nw.Series)
    assert value.implementation is nw.Implementation.PYARROW
    native = nw.to_native(value)
    if isinstance(native, pa.Array):
        native = pa.chunked_array([native])
    assert isinstance(native, pa.ChunkedArray)
    return native


def _assert_series(value: object, expected: pa.ChunkedArray) -> None:
    assert _native_series(value).equals(expected)


def _native_table(value: object) -> pa.Table:
    assert isinstance(value, nw.DataFrame)
    assert value.implementation is nw.Implementation.PYARROW
    native = nw.to_native(value)
    assert isinstance(native, pa.Table)
    return native


def _assert_table(value: object, expected: pa.Table) -> None:
    assert _native_table(value).equals(expected)


def test_q_function_value_exports_validation_and_exact_frames() -> None:
    plus = XqdbQOperator.PLUS
    assert plus.name == "+"
    assert XqdbQOperator("+").name == "+"
    assert XqdbQOperator("+") == plus
    assert XqdbQOperator.__module__ == "xqdb"
    assert repr(plus) == 'XqdbQOperator("+")'
    with pytest.raises(AttributeError):
        plus.name = "-"

    root = XqdbQLambda("{x+y}")
    contextual = XqdbQLambda(" {x+y} ", "ctx")
    assert (root.source, root.context) == ("{x+y}", "")
    assert (contextual.source, contextual.context) == (" {x+y} ", "ctx")
    assert repr(root) == 'XqdbQLambda("{x+y}")'
    assert repr(contextual) == 'XqdbQLambda(" {x+y} ", "ctx")'
    with pytest.raises(AttributeError):
        root.source = "{x-y}"

    with pytest.raises(XqdbError, match="unsupported q primitive"):
        XqdbQOperator("plus")
    with pytest.raises(XqdbError, match="NUL"):
        XqdbQOperator("+\0")
    with pytest.raises(XqdbError, match="brace-delimited"):
        XqdbQLambda("x+y")
    with pytest.raises(XqdbError, match="NUL"):
        XqdbQLambda("{x\0+y}")

    assert serialize_as_ipc_bytes6("sync", enable_compression=False, any=plus) == bytes(
        [1, 1, 0, 0, 10, 0, 0, 0, 102, 1]
    )
    assert (
        serialize_as_ipc_bytes6("sync", enable_compression=False, any=root)
        == bytes([1, 1, 0, 0, 21, 0, 0, 0, 100, 0, 10, 0, 5, 0, 0, 0]) + b"{x+y}"
    )


def test_qtype_facade_is_removed() -> None:
    assert not hasattr(xqdb, "QType")
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("xqdb.type")


def test_python_container_conversion_rejects_cycles_but_allows_reuse() -> None:
    cyclic_list = []
    cyclic_list.append(cyclic_list)
    with pytest.raises(ValueError, match="cyclic Python containers"):
        serialize_as_ipc_bytes6("sync", enable_compression=False, any=cyclic_list)

    cyclic_dict = {}
    cyclic_dict["self"] = cyclic_dict
    with pytest.raises(ValueError, match="cyclic Python containers"):
        serialize_as_ipc_bytes6("sync", enable_compression=False, any=cyclic_dict)

    shared = [1]
    assert serialize_as_ipc_bytes6("sync", enable_compression=False, any=[shared, shared])
    assert serialize_as_ipc_bytes6("sync", enable_compression=False, any=(shared, shared))


def test_python_container_conversion_enforces_maximum_depth() -> None:
    value = 0
    for _ in range(64):
        value = [value]
    assert serialize_as_ipc_bytes6("sync", enable_compression=False, any=value)
    with pytest.raises(ValueError, match="nesting exceeds 64 levels"):
        serialize_as_ipc_bytes6("sync", enable_compression=False, any=[value])


def test_serialization_preserves_python_error_types_and_message_validation() -> None:
    with pytest.raises(OverflowError):
        serialize_as_ipc_bytes6("sync", enable_compression=False, any=1 << 100)
    with pytest.raises(ValueError, match="msg_type must be 0, 1, or 2"):
        generate_j6_ipc_msg(3, enable_compression=False, any=1 << 100)
    with pytest.raises(TypeError):
        serialize_as_ipc_bytes6("sync", enable_compression=False, any={1: "not a symbol key"})
    with pytest.raises(ValueError, match=r"expected 'async'.*got 'invalid'"):
        serialize_as_ipc_bytes6("invalid", enable_compression=False, any=1)


def test_empty_dictionary_serializes_as_symbol_keyed_dictionary() -> None:
    # (`symbol$())!(): the form q sends for emptied symbol dictionaries.
    compression_enabled = False
    assert serialize_as_ipc_bytes6("sync", compression_enabled, {}) == bytes(
        [1, 1, 0, 0, 21, 0, 0, 0, 99, 11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    )


def test_exception_module_exports_names() -> None:
    exceptions = importlib.import_module("xqdb.exceptions")

    assert exceptions.__all__ == ["XqdbAuthError", "XqdbError", "XqdbIOError"]


@pytest.mark.parametrize(
    ("value", "error"),
    [
        (datetime(1700, 1, 1, tzinfo=timezone.utc), OverflowError),
        (timedelta(days=106_752), OverflowError),
        (time(0, 0, 0, 1), ValueError),
    ],
)
def test_serialization_rejects_unrepresentable_temporal_values(
    value: object, error: type[Exception]
) -> None:
    with pytest.raises(error):
        serialize_as_ipc_bytes6("sync", enable_compression=False, any=value)


def test_call_argument_limit_is_checked_before_conversion() -> None:
    client = Q("does-not-exist.invalid", 1800, user="test")
    with pytest.raises(TypeError, match="at most 8 arguments"):
        client.sync("", 1 << 100, *([0] * 8))


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("0b", False),
        ("1b", True),
        ("0Ng", "00000000-0000-0000-0000-000000000000"),
        ("0xFF", 255),
        ("0Nh", -32768),
        ("0Ni", -2147483648),
        ("0N", -9223372036854775808),
        ("9", 9),
        ('"J"', "J"),
        ('"JS"', "JS"),
        ("`", ""),
        ("`q", "q"),
        (
            "1969.12.31D12:00:00.123456",
            _naive_q_datetime(1969, 12, 31, 12, 0, 0, 123456),
        ),
        ("0001.01.01", date(1, 1, 1)),
        ("9999.12.31", date(9999, 12, 31)),
        ("0D12:34:56.123456", timedelta(seconds=45296, microseconds=123456)),
        (
            "-0D00:00:00.000001",
            timedelta(days=-1, seconds=86399, microseconds=999999),
        ),
        ("12:34:56.789", time(12, 34, 56, 789000)),
        (
            "2023.11.11T12:34:56.789",
            _naive_q_datetime(2023, 11, 11, 12, 34, 56, 789000),
        ),
    ],
)
def test_read_scalar_invariance(q: Q, query: str, expected: object) -> None:
    assert q.sync(query) == expected


def test_native_timestamp_null_and_infinities_are_not_epoch(q: Q) -> None:
    assert q.sync("0Np") is None
    with pytest.raises(XqdbError, match="infinity"):
        q.sync("0Wp")
    with pytest.raises(XqdbError, match="infinity"):
        q.sync("-0Wp")


def test_timestamp_atoms_are_naive_and_agree_with_columns(q: Q) -> None:
    """A q timestamp has no timezone, so atoms and Arrow columns must match exactly."""
    atom = q.sync("first exec time from ([] time:enlist 2024.01.02D03:04:05.000006)")
    assert atom.tzinfo is None

    frame = q.sync("([] time:enlist 2024.01.02D03:04:05.000006)")
    column_value = nw.to_native(frame).column(0)[0].as_py()
    assert column_value.tzinfo is None
    assert atom == column_value


@pytest.mark.parametrize(
    "value",
    [
        _naive_q_datetime(2024, 1, 2, 3, 4, 5, 6),
        datetime(2024, 1, 2, 3, 4, 5, 6, tzinfo=timezone.utc),
    ],
)
def test_datetime_arguments_accept_naive_and_aware(q: Q, value: datetime) -> None:
    """Values read back from naive columns must be usable as query arguments."""
    assert q.sync("{x}", value) == value.replace(tzinfo=None)


def test_nanosecond_column_scalars_round_trip_without_loss(q: Q) -> None:
    """A sub-microsecond column scalar must reach q intact.

    PyArrow only yields a nanosecond-capable scalar when pandas is installed; it keeps
    the sub-microsecond digits in `.nanosecond` rather than `.microsecond`, so those must
    not be dropped. Without pandas, PyArrow refuses the conversion instead of truncating,
    which is why this test requires pandas rather than asserting a fallback.
    """
    pytest.importorskip("pandas")
    expr = "([] t:enlist 2024.01.02D03:04:05.000000001)"
    scalar = nw.to_native(q.sync(expr)).column(0)[0].as_py()
    assert scalar.nanosecond == 1
    assert scalar.microsecond == 0

    assert q.sync("{`long$x}", scalar) == q.sync(f"`long$first {expr}`t")


@pytest.mark.parametrize("backend", ["pyarrow", "pandas", "polars"])
def test_whole_frame_round_trips_are_nanosecond_exact(q: Q, backend: str) -> None:
    """Series and DataFrames cross as Arrow, so nanoseconds survive on every backend.

    Parametrized per backend so a missing optional package skips only its own case
    instead of hiding the backends that are installed.

    Single scalars are deliberately not covered: their precision is decided by the
    backend before XQDB sees the value, and the Polars backend materializes a plain
    microsecond `datetime`. The Series and DataFrame paths are the guarantee XQDB makes.
    """
    pytest.importorskip(backend)
    expr = "([] t:enlist 2024.01.02D03:04:05.000000001)"
    original_backend = q.backend
    try:
        q.backend = backend
        expected = q.sync(f"`long$first {expr}`t")

        frame = q.sync(expr)
        assert q.sync("{([] t:`long$x`t)}", frame)["t"].to_list()[0] == expected

        series = q.sync(f"exec t from {expr}")
        assert q.sync("{`long$x}", series).to_list()[0] == expected
    finally:
        q.backend = original_backend


@pytest.mark.parametrize(
    ("zone", "value", "expected"),
    [
        # Fixed offset.
        (
            timezone(timedelta(hours=-5)),
            _naive_q_datetime(2024, 1, 2, 3, 4, 5),
            _naive_q_datetime(2024, 1, 2, 8, 4, 5),
        ),
        # IANA zone on standard time (EST, UTC-5).
        (
            "America/New_York",
            _naive_q_datetime(2024, 1, 2, 3, 4, 5),
            _naive_q_datetime(2024, 1, 2, 8, 4, 5),
        ),
        # Same IANA zone on daylight time (EDT, UTC-4) must pick the other offset.
        (
            "America/New_York",
            _naive_q_datetime(2024, 7, 2, 3, 4, 5),
            _naive_q_datetime(2024, 7, 2, 7, 4, 5),
        ),
    ],
)
def test_aware_datetime_arguments_are_normalized_to_utc(
    q: Q, zone: tzinfo | str, value: datetime, expected: datetime
) -> None:
    """Any tzinfo must resolve to the correct UTC instant rather than being rejected.

    Zone keys are resolved inside the test body: Windows has no system tz database, so
    building a `ZoneInfo` at collection time would fail the whole module instead of
    skipping the zone-dependent cases.
    """
    if isinstance(zone, str):
        try:
            zone = ZoneInfo(zone)
        except ZoneInfoNotFoundError:
            pytest.skip(f"no system tz database for {zone!r}; install tzdata")
    assert q.sync("{x}", value.replace(tzinfo=zone)) == expected


def test_tzinfo_without_utcoffset_is_treated_as_naive(q: Q) -> None:
    """Python defines aware as tzinfo present AND `utcoffset()` not None.

    A tzinfo whose `utcoffset()` returns None leaves the datetime naive, so its wall
    clock must reach q unchanged. Routing it through `astimezone()` instead would make
    the result depend on the host's local timezone.
    """

    class NoOffset(tzinfo):
        def utcoffset(self, _dt: datetime | None) -> timedelta | None:
            return None

        def dst(self, _dt: datetime | None) -> timedelta | None:
            return None

        def tzname(self, _dt: datetime | None) -> str | None:
            return None

    value = datetime(2024, 1, 2, 3, 4, 5, tzinfo=NoOffset())
    assert value.utcoffset() is None
    assert q.sync("{x}", value) == _naive_q_datetime(2024, 1, 2, 3, 4, 5)


@pytest.mark.parametrize(
    "query",
    [
        "0Wp",
        "0Nd",
        "-0Wd",
        "0Wd",
        "1969.12.31D12:00:00.123456789",
        "0D12:34:56.123456789",
    ],
)
def test_read_scalar_rejects_unrepresentable_temporal_values(q: Q, query: str) -> None:
    with pytest.raises((XqdbError, OverflowError, ValueError)):
        q.sync(query)


def test_read_char_vector_string_and_arbitrary_bytes(q: Q) -> None:
    assert q.sync('"xqdb"') == "xqdb"
    value = b"\x00\x7f\x80\xff"
    assert q.sync("{x}", value) == value


def test_round_trip_q_operator_and_lambda_values(q: Q) -> None:
    expression = "{[op;a;b] .[op;(a;b)]}"
    assert q.sync(expression, XqdbQOperator.PLUS, 1, 2) == 3
    assert q.sync(expression, XqdbQLambda("{x+y}"), 1, 2) == 3
    operator = q.sync("+")
    q_lambda = q.sync("{x+y}")
    assert isinstance(operator, XqdbQOperator)
    assert isinstance(q_lambda, XqdbQLambda)
    assert q.sync(expression, operator, 1, 2) == 3
    assert q.sync(expression, q_lambda, 1, 2) == 3


@pytest.mark.parametrize(
    ("query", "dtype", "values"),
    [
        ("10b", pa.bool_(), [True, False]),
        ("(,)0b", pa.bool_(), [False]),
        ("(,)0Ng", pa.binary_view(), [bytes(16)]),
        ("0x00FF", pa.uint8(), [0, 255]),
        ("0N -0W 9 0Wh", pa.int16(), [None, None, 9, None]),
        ("0N -0W 9 0Wi", pa.int32(), [None, None, 9, None]),
        ("0N -0W 9 0W", pa.int64(), [None, None, 9, None]),
        ("0n -0w 9 0we", pa.float32(), [None, -math.inf, 9.0, math.inf]),
        ("0n -0w 9 0w", pa.float64(), [None, -math.inf, 9.0, math.inf]),
        ('("";"string")', pa.string_view(), ["", "string"]),
        (
            "0N 2021.06.03D0 2021.06.03D12:34:56.123456789p",
            pa.timestamp("ns"),
            [None, 1622678400000000000, 1622723696123456789],
        ),
        ("0N 2022.05.30d", pa.date32(), [None, date(2022, 5, 30)]),
        (
            "0N 0D00 0D12:34:56.123456789n",
            pa.duration("ns"),
            [None, 0, 45296123456789],
        ),
        ("0N 00:00 12:34u", pa.time64("ns"), [None, 0, 45240000000000]),
        ("0N 00:00:00 12:34:56v", pa.time64("ns"), [None, 0, 45296000000000]),
        ("0n 00:00:00.000 12:34:56.789t", pa.time64("ns"), [None, 0, 45296789000000]),
        (
            "0n 2022.06.03T00:00:00.000 2022.06.03T12:34:56.789z",
            pa.timestamp("ns"),
            [None, 1654214400000000000, 1654259696789000000],
        ),
        ("(1 2;();3 4)", pa.large_list(pa.int64()), [[1, 2], [], [3, 4]]),
        ("()", pa.null(), []),
    ],
)
def test_read_vector_types_and_null_semantics(
    q: Q, query: str, dtype: pa.DataType, values: Iterable[object]
) -> None:
    _assert_series(q.sync(query), _chunked(values, dtype))


def test_read_symbol_vector_is_dictionary_encoded(q: Q) -> None:
    actual = _native_series(q.sync("``q`kdb"))
    assert pa.types.is_dictionary(actual.type)
    assert actual.to_pylist() == ["", "q", "kdb"]


def test_recursive_dataframe_results_and_series_table_distinction(q: Q) -> None:
    table = pa.table({"value": pa.array([1, 2], type=pa.int64())})
    series = pa.chunked_array([[3, 4]], type=pa.int64())
    result = q.sync("{x}", {"table": table, "nested": [series, table]})
    _assert_table(result["table"], table)
    _assert_series(result["nested"][0], series)
    _assert_table(result["nested"][1], table)

    assert q.sync("{type x}", table) == 98
    assert q.sync("{type x}", series) == 7


def test_arrow_stream_capsule_is_owned_exactly_once(q: Q) -> None:
    class ReusedStream:
        def __init__(self, capsule: object) -> None:
            self.capsule = capsule

        def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
            return self.capsule

    table = pa.table({"value": [1, 2]})
    stream = ReusedStream(table.__arrow_c_stream__())
    assert serialize_as_ipc_bytes6("sync", enable_compression=False, any=stream)
    with pytest.raises((TypeError, ValueError), match=r"unused|released"):
        serialize_as_ipc_bytes6("sync", enable_compression=False, any=stream)

    bridge = q.q.sync("([]v:1 2)")
    assert bridge.shape == (2, 1)
    assert bridge.columns == ["v"]
    assert "ArrowTable" in repr(bridge)
    with pytest.raises(ValueError, match="schema negotiation"):
        bridge.__arrow_c_stream__(object())
    exported = ReusedStream(bridge.__arrow_c_stream__())
    converted = nw.from_arrow(exported, backend="pyarrow")
    assert isinstance(nw.to_native(converted), pa.Table)
    with pytest.raises((TypeError, ValueError)):
        nw.from_arrow(exported, backend="pyarrow")

    series_bridge = q.q.sync("1 2")
    assert series_bridge.shape == (2,)
    assert isinstance(series_bridge.name, str)
    assert "ArrowSeries" in repr(series_bridge)
    with pytest.raises(ValueError, match="schema negotiation"):
        series_bridge.__arrow_c_stream__(object())


def test_arrow_stream_requires_capsule_and_struct_schema() -> None:
    class BadCapsule:
        def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
            return object()

    class ArrayStream:
        def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
            return pa.chunked_array([[1, 2]]).__arrow_c_stream__()

    with pytest.raises(TypeError, match="must return a PyCapsule"):
        serialize_as_ipc_bytes6("sync", enable_compression=False, any=BadCapsule())
    with pytest.raises(TypeError, match="schema must be a struct"):
        serialize_as_ipc_bytes6("sync", enable_compression=False, any=ArrayStream())


class RawArrowStream:
    def __init__(self, table: pa.Table) -> None:
        self.table = table

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        return self.table.__arrow_c_stream__(requested_schema=requested_schema)


@pytest.mark.parametrize(
    ("dtype", "values"),
    [
        pytest.param(pa.float16(), [None], id="float16"),
        pytest.param(pa.decimal128(10, 2), [None], id="decimal128"),
        pytest.param(pa.list_(pa.float16()), [None], id="nested-float16"),
    ],
)
def test_unsupported_arrow_dtype_returns_conversion_error(
    dtype: pa.DataType, values: Iterable[object]
) -> None:
    frame = pa.table({"value": pa.array(values, type=dtype)})
    with pytest.raises(ValueError, match="unsupported Arrow datatype"):
        serialize_as_ipc_bytes6("sync", enable_compression=False, any=RawArrowStream(frame))


def test_arrow_import_strips_untrusted_top_level_polars_metadata() -> None:
    dtype = pa.dictionary(pa.int32(), pa.string())
    field = pa.field(
        "value",
        dtype,
        metadata={b"_PL_ENUM_VALUES2": b"malformed"},
    )
    frame = pa.Table.from_arrays(
        [pa.array(["a", "b"], type=dtype)],
        schema=pa.schema([field]),
    )

    assert serialize_as_ipc_bytes6("sync", enable_compression=False, any=RawArrowStream(frame))


def test_arrow_import_preserves_benign_nested_metadata() -> None:
    dtype = pa.list_(pa.field("item", pa.int64(), metadata={b"semantic": b"identifier"}))
    frame = pa.table({"value": pa.array([[1, 2]], type=dtype)})

    assert serialize_as_ipc_bytes6("sync", enable_compression=False, any=RawArrowStream(frame))


def test_write_multichunk_and_sliced_nested_tables(q: Q) -> None:
    frame = pa.table(
        {
            "value": pa.chunked_array([[1, 2], [3, 4]], type=pa.int64()),
            "depth": pa.chunked_array(
                [[[1.0, 2.0], []], [[3.0], [4.0, 5.0]]],
                type=pa.list_(pa.float64()),
            ),
        }
    )
    expected = pa.table(
        {
            "value": pa.array([1, 2, 3, 4], type=pa.int64()),
            "depth": pa.array(
                [[1.0, 2.0], [], [3.0], [4.0, 5.0]],
                type=pa.large_list(pa.float64()),
            ),
        }
    )
    _assert_table(q.sync("{x}", frame), expected)

    sliced = pa.table({"depth": pa.array([[0.0], [1.0, None], [2.0, 3.0], [4.0]])}).slice(1, 2)
    expected_slice = pa.table(
        {
            "depth": pa.array(
                [[1.0, None], [2.0, 3.0]],
                type=pa.large_list(pa.float64()),
            )
        }
    )
    _assert_table(q.sync("{x}", sliced), expected_slice)


@pytest.mark.parametrize("dtype", [pa.int16(), pa.int32(), pa.int64(), pa.float32(), pa.float64()])
def test_write_nested_numeric_lists(q: Q, dtype: pa.DataType) -> None:
    frame = pa.table({"depth": pa.array([[1, None, 2], [], [3, 4, 5, 6]], type=pa.list_(dtype))})
    expected = pa.table(
        {
            "depth": pa.array(
                [[1, None, 2], [], [3, 4, 5, 6]],
                type=pa.large_list(dtype),
            )
        }
    )
    _assert_table(q.sync("{x}", frame), expected)


@pytest.mark.parametrize(
    ("dtype", "values", "expected"),
    [
        (pa.bool_(), [[True, None, False], []], [[True, False, False], []]),
        (pa.uint8(), [[1, None, 2], []], [[1, 0, 2], []]),
    ],
)
def test_write_nested_bool_and_byte_nulls(
    q: Q, dtype: pa.DataType, values: Iterable[object], expected: object
) -> None:
    frame = pa.table({"depth": pa.array(values, type=pa.list_(dtype))})
    expected_frame = pa.table({"depth": pa.array(expected, type=pa.large_list(dtype))})
    _assert_table(q.sync("{x}", frame), expected_frame)


def test_write_null_nested_containers_are_rejected(q: Q) -> None:
    list_frame = pa.table({"depth": pa.array([[1.0], None], type=pa.list_(pa.float64()))})
    with pytest.raises(XqdbError, match="null values in List columns"):
        q.sync("{x}", list_frame)

    fixed_frame = pa.table({"flags": pa.array([[True, False], None], type=pa.list_(pa.bool_(), 2))})
    with pytest.raises(XqdbError, match="null values in Array columns"):
        q.sync("{x}", fixed_frame)


@pytest.mark.parametrize(
    ("k_list", "series"),
    [
        ("10b", pa.chunked_array([[True, False]], type=pa.bool_())),
        ("0x00FF", pa.chunked_array([[0, 255]], type=pa.uint8())),
        ("0N -0W 9 0Wh", _chunked([None, -32767, 9, 32767], pa.int16())),
        ("0N -0W 9 0Wi", _chunked([None, -2147483647, 9, 2147483647], pa.int32())),
        (
            "0N -0W 9 0W",
            _chunked([None, -9223372036854775807, 9, 9223372036854775807], pa.int64()),
        ),
        ("0n -0w 9 0We", _chunked([math.nan, -math.inf, 9.0, math.inf], pa.float32())),
        ("0n -0w 9 0W", _chunked([math.nan, -math.inf, 9.0, math.inf], pa.float64())),
        ('("";"string")', _chunked(["", "string"], pa.string())),
        (
            "0N 2021.06.03D0 2021.06.03D12:34:56.123456789p",
            _chunked([None, 1622678400000000000, 1622723696123456789], pa.timestamp("ns")),
        ),
        ("0N 2022.05.30d", _chunked([None, date(2022, 5, 30)], pa.date32())),
        (
            "0N 0D00 0D12:34:56.123456789n",
            _chunked([None, 0, 45296123456789], pa.duration("ns")),
        ),
        (
            "0n 00:00:00.000 12:34:56.789t",
            _chunked([None, 0, 45296789000000], pa.time64("ns")),
        ),
        (
            "0n 2022.06.03T00:00:00.000 2022.06.03T12:34:56.789z",
            _chunked(
                [
                    None,
                    _naive_q_datetime(2022, 6, 3),
                    _naive_q_datetime(2022, 6, 3, 12, 34, 56, 789000),
                ],
                pa.timestamp("ms"),
            ),
        ),
    ],
)
def test_write_vector_types(q: Q, k_list: str, series: pa.ChunkedArray) -> None:
    assert q.sync("{x~" + k_list + "}", series)


def test_write_symbol_vector_from_dictionary_array(q: Q) -> None:
    symbols = pa.chunked_array([pa.array(["", "q", "kdb"]).dictionary_encode()])
    assert q.sync("{x~``q`kdb}", symbols)


@pytest.mark.parametrize(
    ("q_table", "table"),
    [
        (
            'enlist `float`long`char`string!(9.0;9;(,)"c";"string")',
            pa.table(
                {
                    "float": [9.0],
                    "long": pa.array([9], type=pa.int64()),
                    "char": ["c"],
                    "string": ["string"],
                }
            ),
        ),
        (
            'enlist `float`long`char`string!(0n;0N;(,)" ";"")',
            pa.table(
                {
                    "float": [math.nan],
                    "long": pa.array([None], type=pa.int64()),
                    "char": [" "],
                    "string": [""],
                }
            ),
        ),
        (
            "enlist `sym`timestamp`bool!(`sym;2021.06.03D;1b)",
            pa.table(
                {
                    "sym": pa.array(["sym"]).dictionary_encode(),
                    "timestamp": pa.array([1622678400000000000], type=pa.timestamp("ns")),
                    "bool": [True],
                }
            ),
        ),
    ],
)
def test_write_table_types(q: Q, q_table: str, table: pa.Table) -> None:
    assert q.sync("{x~" + q_table + "}", table)


def test_read_table_returns_narwhals_with_arrow_schema(q: Q) -> None:
    result = q.sync("([]sym:`a`b`c;prices:3 3#til 9)")
    native = _native_table(result)
    assert native.column_names == ["sym", "prices"]
    assert pa.types.is_dictionary(native.schema.field("sym").type)
    assert native["sym"].to_pylist() == ["a", "b", "c"]
    assert native["prices"].to_pylist() == [[0, 1, 2], [3, 4, 5], [6, 7, 8]]


def test_read_empty_table_preserves_schema(q: Q) -> None:
    result = _native_table(q.sync("0#enlist `sym`timestamp`bool!(`sym;2022.06.05D;1b)"))
    assert result.num_rows == 0
    assert pa.types.is_dictionary(result.schema.field("sym").type)
    assert result.schema.field("timestamp").type == pa.timestamp("ns")
    assert result.schema.field("bool").type == pa.bool_()


def test_empty_dictionary_round_trips(q: Q) -> None:
    assert q.sync("()!()") == {}
    assert q.sync("(`symbol$())!()") == {}
    assert q.sync("0#`a`b!1 2") == {}
    assert q.sync("{x}", {}) == {}
    assert q.sync("{x~(`symbol$())!()}", {})
    assert q.sync("{x}", {"nested": {}}) == {"nested": {}}


def test_output_backend_is_exact_and_input_backend_is_independent(q: Q) -> None:
    pd = importlib.import_module("pandas")
    pl = importlib.import_module("polars")

    inputs = {
        "pyarrow": pa.table({"value": [1, 2]}),
        "pandas": pd.DataFrame({"value": [1, 2]}),
        "polars": pl.DataFrame({"value": [1, 2]}),
    }
    native_types = {
        "pyarrow": pa.Table,
        "pandas": pd.DataFrame,
        "polars": pl.DataFrame,
    }
    original_backend = q.backend
    try:
        for output_backend, native_type in native_types.items():
            q.backend = output_backend
            for input_value in inputs.values():
                result = q.sync("{x}", input_value)
                assert isinstance(result, nw.DataFrame)
                assert result.implementation.value == output_backend
                assert isinstance(nw.to_native(result), native_type)
    finally:
        q.backend = original_backend


def test_series_backends_remain_series(q: Q) -> None:
    pd = importlib.import_module("pandas")
    pl = importlib.import_module("polars")

    series_inputs = [
        pa.chunked_array([[1, 2]], type=pa.int64()),
        pd.Series([1, 2], name="value", dtype="int64"),
        pl.Series("value", [1, 2], dtype=pl.Int64),
    ]
    original_backend = q.backend
    try:
        for backend in ("pyarrow", "pandas", "polars"):
            q.backend = backend
            for series in series_inputs:
                result = q.sync("{x}", series)
                assert isinstance(result, nw.Series)
                assert result.implementation.value == backend
    finally:
        q.backend = original_backend


@pytest.mark.parametrize("backend", ["not-a-dataframe-backend", "duckdb", "modin"])
def test_unsupported_backend_raises_at_construction(backend: str) -> None:
    with pytest.raises(ValueError, match="unsupported dataframe backend"):
        Q("does-not-exist.invalid", 1800, backend=backend)


def test_unsupported_backend_assignment_does_not_fallback(q: Q) -> None:
    original_backend = q.backend

    for backend in ("not-a-dataframe-backend", "duckdb", "modin"):
        with pytest.raises(ValueError, match="unsupported dataframe backend"):
            q.backend = backend
        assert q.backend == original_backend

    assert q.sync("1+1") == 2


def test_unsupported_symbol_encoding_raises_at_construction_and_assignment() -> None:
    with pytest.raises(ValueError, match="symbol_encoding must be 'strict' or 'lossy'"):
        Q("does-not-exist.invalid", 1800, symbol_encoding="latin1")

    q = Q("does-not-exist.invalid", 1800)
    assert q.symbol_encoding == "strict"
    q.symbol_encoding = "lossy"
    assert q.symbol_encoding == "lossy"
    with pytest.raises(ValueError, match="symbol_encoding must be 'strict' or 'lossy'"):
        q.symbol_encoding = "Lossy"
    assert q.symbol_encoding == "lossy"


# A q table `([] s:enlist `$"caf\351"; t:enlist "caf\351")` as J6 bytes: the symbol and the
# string both carry the Latin-1 byte 0xE9, which is not valid UTF-8.
_NON_UTF8_TABLE = bytes(
    [
        98,
        0,
        99,
        11,
        0,
        2,
        0,
        0,
        0,
        *b"s\0t\0",
        0,
        0,
        2,
        0,
        0,
        0,
        11,
        0,
        1,
        0,
        0,
        0,
        *b"caf\xe9\0",
        0,
        0,
        1,
        0,
        0,
        0,
        10,
        0,
        4,
        0,
        0,
        0,
        *b"caf\xe9",
    ]
)


def test_read_binary6_symbol_encoding_controls_invalid_utf8(tmp_path: Path) -> None:
    path = tmp_path / "latin1.bin"
    path.write_bytes(b"\xff\x01" + _NON_UTF8_TABLE)

    with pytest.raises(XqdbError, match="not valid UTF-8"):
        read_binary6(str(path))
    with pytest.raises(XqdbError, match="not valid UTF-8"):
        read_binary6(str(path), symbol_encoding="strict")
    with pytest.raises(ValueError, match="symbol_encoding must be 'strict' or 'lossy'"):
        read_binary6(str(path), symbol_encoding="latin1")

    table = _native_table(read_binary6(str(path), symbol_encoding="lossy"))
    assert table.column("s").to_pylist() == ["caf\ufffd"]
    assert table.column("t").to_pylist() == ["caf\ufffd"]


def test_lazy_inputs_are_rejected_before_ipc(q: Q) -> None:
    pl = importlib.import_module("polars")

    with pytest.raises(TypeError, match="lazy dataframe inputs"):
        q.sync("{x}", pl.LazyFrame({"value": [1, 2]}))


def test_logically_identical_backend_inputs_have_identical_ipc_bytes() -> None:
    pd = importlib.import_module("pandas")
    pl = importlib.import_module("polars")

    values = {
        "long": [1, 2, 3],
        "float": [1.5, None, 3.5],
        "string": ["a", "", "c"],
    }
    inputs = [pa.table(values), pd.DataFrame(values), pl.DataFrame(values)]
    encoded = [
        serialize_as_ipc_bytes6("sync", enable_compression=False, any=value) for value in inputs
    ]
    assert encoded[0] == encoded[1] == encoded[2]


def test_read_binary6_honors_selected_backend(tmp_path: Path) -> None:
    pd = importlib.import_module("pandas")
    pl = importlib.import_module("polars")

    path = tmp_path / "table.bin"
    ipc = serialize_as_ipc_bytes6("sync", enable_compression=False, any=pa.table({"v": [1, 2]}))
    path.write_bytes(b"\xff\x01" + ipc[8:])
    expected_types = {
        "pyarrow": pa.Table,
        "pandas": pd.DataFrame,
        "polars": pl.DataFrame,
    }
    for backend, expected_type in expected_types.items():
        result = read_binary6(str(path), backend=backend)
        assert isinstance(result, nw.DataFrame)
        assert result.implementation.value == backend
        assert isinstance(nw.to_native(result), expected_type)


def test_receive_uses_q_backend(q: Q) -> None:
    pd = importlib.import_module("pandas")

    class Receiver:
        def __init__(self, value: object) -> None:
            self.value = value

        def receive(self) -> object:
            return self.value

    client = object.__new__(Q)
    client.retries = 0
    client.backend = "pandas"
    client.q = Receiver(q.q.sync("([]v:1 2)"))
    result = client.receive()
    assert isinstance(result, nw.DataFrame)
    assert result.implementation is nw.Implementation.PANDAS
    assert isinstance(nw.to_native(result), pd.DataFrame)


def test_asyn_accepts_arrow_backed_inputs(q: Q) -> None:
    frame = pa.table({"value": [1, 2, 3]})
    assert q.asyn("{`xqdbTestX set count x}", frame) is None
    assert q.sync("xqdbTestX") == 3


def test_error_auto_connect_and_fixture(q: Q) -> None:
    with pytest.raises(XqdbError, match="type"):
        q.sync("1+`a")

    q.disconnect()
    assert q.sync("1+1") == 2
    q.connect()

    rows = int(os.environ.get("XQDB_Q_ROWS", "10000"))
    assert q.sync(".xqdb.ready")
    assert rows == q.sync("count trade")
    assert rows == q.sync("count wide")
    assert rows == q.sync("count depth")
    assert q.sync("count cols trade") == 14
    assert q.sync("count cols wide") == 64
    assert q.sync("count cols depth") == 5


def test_io_error() -> None:
    q = Q("does-not-exist.invalid", 1800)
    with pytest.raises(XqdbIOError):
        q.sync("1+`a")
    with pytest.raises(XqdbIOError):
        q.asyn("1+`a")


@pytest.mark.parametrize(
    ("kind", "value", "type_code", "payload"),
    [
        ("boolean", True, -1, b"\x01"),
        ("guid", bytes(range(16)), -2, bytes(range(16))),
        ("byte", 255, -4, b"\xff"),
        ("short", -2, -5, struct.pack("<h", -2)),
        ("int", -3, -6, struct.pack("<i", -3)),
        ("long", -4, -7, struct.pack("<q", -4)),
        ("real", 1.25, -8, struct.pack("<f", 1.25)),
        ("float", 2.5, -9, struct.pack("<d", 2.5)),
        ("char", b"x", -10, b"x"),
        ("symbol", b"sym", -11, b"sym\0"),
        ("timestamp", 123, -12, struct.pack("<q", 123)),
        ("month", 24, -13, struct.pack("<i", 24)),
        ("date", 365, -14, struct.pack("<i", 365)),
        ("datetime", 1.5, -15, struct.pack("<d", 1.5)),
        ("timespan", -123, -16, struct.pack("<q", -123)),
        ("minute", 60, -17, struct.pack("<i", 60)),
        ("second", 3600, -18, struct.pack("<i", 3600)),
        ("time", 1000, -19, struct.pack("<i", 1000)),
    ],
)
def test_exact_q_atom_constructors(
    kind: str, value: object, type_code: int, payload: bytes
) -> None:
    atom = XqdbQValue.atom(kind, value)
    assert atom.type_code == type_code
    assert atom.body == bytes([type_code % 256]) + payload
    assert atom.len == 1


@pytest.mark.parametrize(
    ("kind", "expression"),
    [
        ("boolean", "0b"),
        ("guid", "0Ng"),
        ("byte", "0x00"),
        ("short", "0Nh"),
        ("int", "0Ni"),
        ("long", "0Nj"),
        ("real", "0Ne"),
        ("float", "0n"),
        ("char", '" "'),
        ("symbol", "`"),
        ("timestamp", "0Np"),
        ("month", "0Nm"),
        ("date", "0Nd"),
        ("datetime", "0Nz"),
        ("timespan", "0Nn"),
        ("minute", "0Nu"),
        ("second", "0Nv"),
        ("time", "0Nt"),
    ],
)
def test_missing_atom_values_preserve_q_typed_identity(q: Q, kind: str, expression: str) -> None:
    atom = XqdbQValue.atom(kind, None)
    assert q.sync(f"{{x~{expression}}}", atom)


def test_real_constructor_rejects_finite_overflow_but_allows_explicit_infinity() -> None:
    with pytest.raises(ValueError, match="finite 32-bit float range"):
        XqdbQValue.atom("real", 1e100)
    infinity = XqdbQValue.atom("real", float("inf"))
    assert deserialize_value6(infinity.body) == float("inf")


def test_lossless_value_constructors_and_in_memory_deserializers() -> None:
    raw_long = XqdbQValue.atom("long", 42)
    assert raw_long.body == bytes([249]) + (42).to_bytes(8, "little", signed=True)
    assert XqdbQValue.atom(7, 42) == raw_long
    assert bytes(raw_long) == raw_long.body
    assert raw_long.type_code == -7
    assert raw_long.len == len(raw_long) == 1
    with pytest.raises(AttributeError):
        raw_long.body = b""

    timestamp_null = XqdbQValue.atom("timestamp", None)
    assert timestamp_null.body == bytes([244]) + (-(1 << 63)).to_bytes(8, "little", signed=True)

    keys = XqdbQValue.list([XqdbQValue.atom("symbol", "a"), XqdbQValue.atom("symbol", "a")])
    values = XqdbQValue.list([XqdbQValue.atom("long", 1), XqdbQValue.atom("long", 2)])
    streamed_values = XqdbQValue.list(
        value for value in (XqdbQValue.atom("long", 1), XqdbQValue.atom("long", 2))
    )
    assert streamed_values == values
    dictionary = XqdbQValue.dictionary(keys, values)
    assert dictionary.type_code == 99
    assert dictionary.len == 2
    assert not dictionary.is_table

    table = XqdbQValue.native(pa.table({"value": [1, 2]}))
    assert table.type_code == 98
    assert table.len == 2
    assert table.is_table

    native = XqdbQValue.native((1, "a"))
    assert native.type_code == 0
    assert native.len == 2

    frame = serialize_as_ipc_bytes6("sync", enable_compression=False, any=raw_long)
    assert frame[8:] == raw_long.body
    assert deserialize_value6(raw_long.body) == 42
    assert deserialize_value6(raw_long.body, lossless=True) == raw_long
    message_type, decoded = deserialize_ipc_bytes6(frame, lossless=True)
    assert message_type == "sync"
    assert decoded == raw_long
    assert deserialize_ipc_bytes6(frame) == ("sync", 42)
    with pytest.raises(TypeError):
        deserialize_value6(bytearray(raw_long.body))
    with pytest.raises(TypeError):
        deserialize_ipc_bytes6(bytearray(frame))
    with pytest.raises(XqdbError):
        XqdbQValue(b"")
    with pytest.raises(XqdbError):
        deserialize_ipc_bytes6(b"")


@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        ({"retries": -1}, ValueError),
        ({"retries": 32}, ValueError),
        ({"lossless": 1}, TypeError),
        ({"compression_threshold": True}, TypeError),
        ({"connect_timeout": False}, TypeError),
        ({"timeout": False}, TypeError),
        ({"timeout": -float.fromhex("0x0.0000000000001p-1022")}, ValueError),
        ({"timeout": 86_400.000_001}, ValueError),
        ({"timeout": float("nan")}, ValueError),
        ({"timeout": float("inf")}, ValueError),
        ({"compression": "sometimes"}, ValueError),
        ({"compression_threshold": 0}, ValueError),
        ({"connect_timeout": -0.01}, ValueError),
        ({"read_timeout": float("nan")}, ValueError),
        ({"write_timeout": 86_400.000_001}, ValueError),
        ({"max_message_bytes": 0}, ValueError),
        ({"max_pending_notifications": 0}, ValueError),
        ({"tls_ca": "not enabled"}, XqdbError),
        ({"enable_tls": True, "tls_cert": "cert only"}, XqdbError),
    ],
)
def test_connection_option_validation(kwargs: dict[str, object], error: type[Exception]) -> None:
    with pytest.raises(error):
        Q("does-not-exist.invalid", 1800, **kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [
        pytest.param({"timeout": 0.1}, id="inherited-default"),
        pytest.param({"timeout": 2.0, "read_timeout": 0.1}, id="granular-override"),
    ],
)
def test_fractional_default_and_granular_read_timeouts_reach_the_socket(
    silent_q_server: int, kwargs: dict[str, object]
) -> None:
    client = Q("127.0.0.1", silent_q_server, **kwargs)
    try:
        client.connect()
        started = clock.monotonic()
        with pytest.raises(XqdbIOError):
            client.receive()
        elapsed = clock.monotonic() - started
        assert 0.025 < elapsed < 0.8
    finally:
        client.disconnect()


@pytest.mark.parametrize(
    "kwargs",
    [
        pytest.param({"timeout": 0}, id="default"),
        pytest.param({"timeout": 0.1, "read_timeout": 0}, id="granular"),
    ],
)
def test_zero_timeout_disables_the_default_or_granular_socket_limit(
    silent_q_server: int,
    kwargs: dict[str, object],
) -> None:
    client = Q("127.0.0.1", silent_q_server, **kwargs)
    started = threading.Event()
    errors: list[XqdbIOError] = []

    def receive() -> None:
        started.set()
        try:
            client.receive()
        except XqdbIOError as error:
            errors.append(error)

    client.connect()
    receiver = threading.Thread(target=receive, daemon=True)
    with _capture_thread_failures() as failures:
        receiver.start()
        try:
            assert started.wait(timeout=1)
            clock.sleep(0.35)
            assert receiver.is_alive()
            client.cancel()
            receiver.join(timeout=2)
            assert not receiver.is_alive()
        finally:
            if receiver.is_alive():
                client.cancel()
                receiver.join(timeout=2)
            client.disconnect()
    _raise_thread_failure(failures)
    assert len(errors) == 1
    assert isinstance(errors[0], XqdbIOError)


@pytest.mark.parametrize(
    ("timeout", "granular"),
    [
        (-float.fromhex("0x0.0000000000001p-1022"), {}),
        (86_400.000_001, {}),
        (float("nan"), {}),
        (float("inf"), {}),
        (30, {"connect_timeout": -0.001}),
        (30, {"read_timeout": 86_400.000_001}),
        (30, {"write_timeout": float("nan")}),
    ],
)
def test_native_connector_rejects_timeout_outside_boundaries(
    timeout: float,
    granular: dict[str, float],
) -> None:
    with pytest.raises(ValueError, match="finite, non-negative"):
        XqdbConnector(
            "127.0.0.1",
            1800,
            "",
            "",
            enable_tls=False,
            timeout=timeout,
            version=6,
            **granular,
        )


def test_positive_subnanosecond_timeout_is_not_treated_as_disabled(
    silent_q_server: int,
) -> None:
    context = multiprocessing.get_context("spawn")
    receive_connection, send_connection = context.Pipe(duplex=False)
    process = context.Process(
        target=_exercise_positive_subnanosecond_timeout,
        args=("127.0.0.1", silent_q_server, send_connection),
    )
    process.start()
    send_connection.close()
    process.join(timeout=5)
    if process.is_alive():
        process.terminate()
        process.join(timeout=2)
        if process.is_alive():
            process.kill()
            process.join(timeout=2)
        pytest.fail("positive sub-nanosecond read timeout behaved like disabled timeout")

    try:
        assert process.exitcode == 0
        assert receive_connection.poll(timeout=1)
        result = receive_connection.recv()
    finally:
        receive_connection.close()

    assert result[0:2] == ("ok", "XqdbIOError")
    assert result[2] < 5


@pytest.mark.parametrize(
    ("failed_attempts", "succeeds"),
    [
        pytest.param(7, True, id="success-on-final-attempt"),
        pytest.param(8, False, id="complete-exhaustion"),
    ],
)
def test_retries_are_additional_connect_attempts_with_pre_retry_backoff(
    *,
    failed_attempts: int,
    succeeds: bool,
) -> None:
    class Connector:
        def __init__(self) -> None:
            self.attempts = 0

        def connect(self) -> None:
            self.attempts += 1
            if self.attempts <= failed_attempts:
                message = "connect failed"
                raise XqdbIOError(message)

    class RecordingCondition:
        def __init__(self) -> None:
            self.waits: list[float] = []

        def __enter__(self) -> "RecordingCondition":
            return self

        def __exit__(
            self,
            _exception_type: type[BaseException] | None,
            _exception: BaseException | None,
            _traceback: TracebackType | None,
        ) -> bool:
            return False

        def wait_for(self, predicate: Callable[[], bool], timeout: float) -> bool:
            self.waits.append(timeout)
            return predicate()

    class TestableQ(Q):
        def use_cancel_condition(self, condition: RecordingCondition) -> None:
            self._cancel_condition = condition

    client = TestableQ("does-not-exist.invalid", 1800, retries=7)
    connector = Connector()
    condition = RecordingCondition()
    client.q = connector
    client.use_cancel_condition(condition)

    if succeeds:
        client.connect()
    else:
        with pytest.raises(XqdbIOError):
            client.connect()

    assert connector.attempts == 8
    assert condition.waits == [1, 2, 4, 8, 16, 32, 32]


def test_cancel_interrupts_connect_backoff_without_consuming_a_retry() -> None:
    first_attempt = threading.Event()

    class Connector:
        def __init__(self) -> None:
            self.attempts = 0
            self.fail = True

        def connect(self) -> None:
            self.attempts += 1
            first_attempt.set()
            if self.fail:
                message = "connect failed"
                raise XqdbIOError(message)

    client = Q("does-not-exist.invalid", 1800, retries=3)
    client.q = Connector()
    errors: list[XqdbIOError] = []

    def connect() -> None:
        try:
            client.connect()
        except XqdbIOError as error:
            errors.append(error)

    thread = threading.Thread(target=connect, daemon=True)
    with _capture_thread_failures() as failures:
        thread.start()
        assert first_attempt.wait(timeout=1)
        clock.sleep(0.05)
        cancelled = clock.monotonic()
        client.cancel()
        thread.join(timeout=1)
    _raise_thread_failure(failures)

    assert not thread.is_alive()
    assert clock.monotonic() - cancelled < 1
    assert client.q.attempts == 1
    assert len(errors) == 1
    assert isinstance(errors[0], XqdbIOError)
    assert "cancelled" in str(errors[0])

    client.q.fail = False
    client.connect()
    assert client.q.attempts == 2


def test_query_io_failure_is_never_replayed() -> None:
    class Connector:
        connect_calls = 0
        sync_calls = 0

        def connect(self) -> None:
            self.connect_calls += 1

        def sync(self, _expression: str, *_args: object) -> None:
            self.sync_calls += 1
            message = "response was ambiguous"
            raise XqdbIOError(message)

    client = Q("does-not-exist.invalid", 1800, retries=1)
    client.q = Connector()

    with pytest.raises(XqdbIOError, match="ambiguous"):
        client.sync("{x}", 1)

    assert client.q.connect_calls == 1
    assert client.q.sync_calls == 1


def test_connection_retry_policy_never_replays_applied_mutation(q: Q) -> None:
    q.sync("xqdbRetryCounter:0j")
    client = Q(q.host, q.port, retries=1)
    with pytest.raises(XqdbIOError):
        client.sync(
            "{`xqdbRetryCounter set 1+xqdbRetryCounter;"
            "if[1=xqdbRetryCounter;hclose .z.w];"
            "xqdbRetryCounter}[]"
        )
    assert q.sync("xqdbRetryCounter") == 1
    client.disconnect()


def test_sync_preserves_interleaved_notification_for_receive(q: Q) -> None:
    assert q.sync("{neg[.z.w] 99j;42j}[]") == 42
    assert q.sync("1+1") == 2
    assert q.receive() == 99


def test_lossless_queries_preserve_nulls_duplicate_keys_and_resend(q: Q) -> None:
    with Q(q.host, q.port, lossless=True) as raw:
        timestamp_null = raw.sync("0Np")
        assert timestamp_null.type_code == -12
        assert raw.sync("{x}", timestamp_null) == timestamp_null
        assert deserialize_value6(raw.sync("{null x}", timestamp_null).body)

        timestamp_infinity = raw.sync("-0Wp")
        assert timestamp_infinity.type_code == -12
        assert raw.sync("{x}", timestamp_infinity) == timestamp_infinity

        dictionary = raw.sync("`a`a!1 2")
        assert dictionary.type_code == 99
        assert dictionary.len == 2
        assert deserialize_value6(raw.sync("{count key x}", dictionary).body) == 2

        keyed_table = raw.sync("([k:1 2]v:3 4)")
        assert keyed_table.type_code == 99
        assert keyed_table.len == 2
        assert keyed_table.is_table

    with pytest.raises(XqdbError, match="duplicate"):
        q.sync("`a`a!1 2")


@pytest.mark.parametrize("operation", ["get", "set"])
def test_cancel_remains_callable_while_property_access_waits_on_receive(
    q: Q, operation: Literal["get", "set"]
) -> None:
    context = multiprocessing.get_context("spawn")
    receive_connection, send_connection = context.Pipe(duplex=False)
    process = context.Process(
        target=_exercise_property_access_during_receive,
        args=(q.host, q.port, operation, send_connection),
    )
    process.start()
    send_connection.close()
    process.join(timeout=5)
    if process.is_alive():
        process.terminate()
        process.join(timeout=2)
        if process.is_alive():
            process.kill()
            process.join(timeout=2)
        pytest.fail(f"symbol_encoding {operation} deadlocked cancellation while receive was active")

    try:
        assert process.exitcode == 0
        assert receive_connection.poll(timeout=1)
        result = receive_connection.recv()
    finally:
        receive_connection.close()

    assert result == ("ok", False, False, ["XqdbIOError"], [])


def test_cancel_and_disconnect_interrupt_receive_without_mutable_borrow(q: Q) -> None:
    context = multiprocessing.get_context("spawn")
    receive_connection, send_connection = context.Pipe(duplex=False)
    process = context.Process(
        target=_exercise_cancel_and_disconnect_during_receive,
        args=(q.host, q.port, send_connection),
    )
    process.start()
    send_connection.close()
    process.join(timeout=8)
    if process.is_alive():
        process.terminate()
        process.join(timeout=2)
        if process.is_alive():
            process.kill()
            process.join(timeout=2)
        pytest.fail("cancel or disconnect deadlocked while receive was active")

    try:
        assert process.exitcode == 0
        assert receive_connection.poll(timeout=1)
        result = receive_connection.recv()
    finally:
        receive_connection.close()

    assert result == (
        "ok",
        (False, ["XqdbIOError"]),
        (False, ["XqdbIOError"]),
    )


def test_idle_cancel_preserves_the_existing_connection(q: Q) -> None:
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(5)
    port = listener.getsockname()[1]
    errors: list[OSError] = []

    def proxy() -> None:
        try:
            with listener:
                peer, _ = listener.accept()
            with (
                peer,
                socket.create_connection((q.host, q.port), timeout=5) as upstream,
            ):
                peer.settimeout(5)
                while True:
                    readable, _, _ = select.select([peer, upstream], [], [], 5)
                    for source in readable:
                        data = source.recv(65536)
                        if not data:
                            return
                        target = upstream if source is peer else peer
                        target.sendall(data)
        except OSError as error:
            errors.append(error)

    thread = threading.Thread(target=proxy, daemon=True)
    client = Q("127.0.0.1", port, timeout=5)
    with _capture_thread_failures() as failures:
        thread.start()
        try:
            assert client.sync("1+1") == 2
            client.cancel()
            # The proxy has closed its listener: only the existing session can succeed.
            assert client.sync("1+2") == 3
        finally:
            client.disconnect()
            thread.join(timeout=5)
    _raise_thread_failure(failures)
    assert not thread.is_alive()
    assert not errors


def test_iter_batches_uses_bounded_offset_count_contract() -> None:
    batches = [
        nw.from_native(pa.table({"value": [1, 2]})),
        nw.from_native(pa.table({"value": [3]})),
    ]
    requests: list[tuple[str, int, int, tuple[object, ...]]] = []

    class RecordingQ(Q):
        def _sync_converted(
            self,
            expression: str,
            args: tuple[object, ...],
        ) -> object:
            offset, requested_rows, *extra = args
            requests.append((expression, offset, requested_rows, tuple(extra)))
            return batches.pop(0)

    client = object.__new__(RecordingQ)
    result = list(client.iter_batches("page", 2, "snapshot"))

    assert [batch.shape[0] for batch in result] == [2, 1]
    assert requests == [
        ("page", 0, 2, ("snapshot",)),
        ("page", 2, 2, ("snapshot",)),
    ]


def test_iter_batches_q_sublist_pages_terminate_on_short_batch(q: Q) -> None:
    batches = list(
        q.iter_batches(
            "{[offset;n](offset;n) sublist ([]x:til 7)}",
            3,
        )
    )

    assert [batch.shape[0] for batch in batches] == [3, 3, 1]
    assert pa.concat_tables([_native_table(batch) for batch in batches])["x"].to_pylist() == list(
        range(7)
    )


def test_iter_batches_uses_lossless_table_metadata() -> None:
    responses = [
        XqdbQValue.native(pa.table({"value": [1, 2]})),
        XqdbQValue.native(pa.table({"value": []})),
    ]

    class StubQ(Q):
        def _sync_converted(
            self,
            _expression: str,
            _args: tuple[object, ...],
        ) -> object:
            return responses.pop(0)

    client = object.__new__(StubQ)
    result = list(client.iter_batches("page", 2))

    assert len(result) == 1
    assert result[0].is_table

    keys = XqdbQValue.list([XqdbQValue.atom("symbol", "a")])
    values = XqdbQValue.list([XqdbQValue.atom("long", 1)])
    responses.append(XqdbQValue.dictionary(keys, values))
    with pytest.raises(TypeError, match="must return a q table"):
        list(client.iter_batches("page", 2))


def test_iter_batches_rejects_invalid_page_results() -> None:
    responses: list[object] = [
        nw.from_native(pa.table({"value": [1, 2, 3]})),
    ]

    class StubQ(Q):
        def _sync_converted(
            self,
            _expression: str,
            _args: tuple[object, ...],
        ) -> object:
            return responses.pop(0)

    client = object.__new__(StubQ)
    with pytest.raises(ValueError, match="more rows than requested"):
        list(client.iter_batches("page", 2))

    responses.append(42)
    with pytest.raises(TypeError, match="must return a q table"):
        list(client.iter_batches("page", 2))

    with pytest.raises(ValueError, match="batch_size must be a positive integer"):
        list(client.iter_batches("page", 0))
    with pytest.raises(TypeError, match="at most 6 additional"):
        list(client.iter_batches("page", 2, *range(7)))
