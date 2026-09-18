"""Synchronous q IPC client."""

import logging
import math
import socket
import threading
from collections.abc import Iterator
from contextlib import suppress
from types import TracebackType
from typing import Any, Literal, TypeVar

import narwhals as nw

from xqdb._conversion import from_arrow_results, to_arrow_inputs, validate_backend
from xqdb.xqdb import XqdbConnector, XqdbIOError, XqdbQValue

logger = logging.getLogger("xqdb")

_MAX_RETRIES = 31
_MAX_Q_LONG = (1 << 63) - 1
_MAX_TIMEOUT_SECONDS = 86_400.0
_CONNECTION_CANCELLED_MESSAGE = "connection attempt cancelled"
_MAX_ITER_BATCH_ARGS = 6
_PAGING_TABLE_MESSAGE = "paging function must return a q table"

_Q = TypeVar("_Q", bound="Q")


def _validate_count(name: str, value: object, *, minimum: int, maximum: int = _MAX_Q_LONG) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        message = f"{name} must be an integer"
        raise TypeError(message)
    if not minimum <= value <= maximum:
        qualifier = "positive" if minimum == 1 else "non-negative"
        message = f"{name} must be a {qualifier} integer no greater than {maximum}"
        raise ValueError(message)
    return value


def _validate_timeout(name: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        message = f"{name} must be a number of seconds"
        raise TypeError(message)
    seconds = float(value)
    if not math.isfinite(seconds) or seconds < 0 or seconds > _MAX_TIMEOUT_SECONDS:
        message = (
            f"{name} must be a finite, non-negative number of seconds "
            f"no greater than {_MAX_TIMEOUT_SECONDS:g}"
        )
        raise ValueError(message)
    return seconds


def _validate_optional_timeout(name: str, value: object) -> float | None:
    if value is None:
        return None
    return _validate_timeout(name, value)


def _convert_args(args: tuple[object, ...], *, maximum: int = 8) -> tuple[object, ...]:
    if len(args) > maximum:
        message = f"q functions accept at most {maximum} arguments"
        raise TypeError(message)
    return tuple(to_arrow_inputs(arg) for arg in args)


# Q uses IPC protocol version 6, which is compatible with kdb+.
class Q:
    """Manage a synchronous q IPC v6 connection."""

    def __init__(
        self,
        host: str,
        port: int,
        user: str = "",
        passwd: str = "",
        enable_tls: bool = False,
        retries: int = 0,
        timeout: float = 30.0,
        backend: str = "pyarrow",
        symbol_encoding: str = "strict",
        *,
        lossless: bool = False,
        compression: str = "auto",
        compression_threshold: int = 10_000_000,
        connect_timeout: float | None = None,
        read_timeout: float | None = None,
        write_timeout: float | None = None,
        max_message_bytes: int | None = None,
        max_pending_notifications: int = 1024,
        tls_ca: str | bytes | None = None,
        tls_cert: str | bytes | None = None,
        tls_key: str | bytes | None = None,
        tls_server_name: str | None = None,
    ) -> None:
        """Configure a q IPC connection without opening it."""
        if not isinstance(lossless, bool):
            message = "lossless must be bool"
            raise TypeError(message)
        compression_threshold = _validate_count(
            "compression_threshold", compression_threshold, minimum=1
        )
        timeout = _validate_timeout("timeout", timeout)
        connect_timeout = _validate_optional_timeout("connect_timeout", connect_timeout)
        read_timeout = _validate_optional_timeout("read_timeout", read_timeout)
        write_timeout = _validate_optional_timeout("write_timeout", write_timeout)
        if max_message_bytes is not None:
            max_message_bytes = _validate_count("max_message_bytes", max_message_bytes, minimum=1)
        max_pending_notifications = _validate_count(
            "max_pending_notifications", max_pending_notifications, minimum=1
        )
        if (not host) or host == socket.gethostname():
            host = "127.0.0.1"
        self.host = host
        self.port = port
        self.user = user
        self.retries = retries
        self.backend = backend
        self._lossless = lossless
        self.q = XqdbConnector(
            host,
            port,
            user,
            passwd,
            enable_tls,
            timeout,
            6,
            lossless=lossless,
            compression=compression,
            compression_threshold=compression_threshold,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            write_timeout=write_timeout,
            max_message_bytes=max_message_bytes,
            max_pending_notifications=max_pending_notifications,
            tls_ca=tls_ca,
            tls_cert=tls_cert,
            tls_key=tls_key,
            tls_server_name=tls_server_name,
        )
        self._abort = self.q.abort_handle()
        self._cancel_condition = threading.Condition()
        self._cancel_generation = 0
        self.symbol_encoding = symbol_encoding

    @property
    def retries(self) -> int:
        """Return the maximum number of connection retries."""
        return self._retries

    @retries.setter
    def retries(self, retries: int) -> None:
        self._retries = _validate_count("retries", retries, minimum=0, maximum=_MAX_RETRIES)

    @property
    def backend(self) -> str:
        """Return the dataframe backend used for decoded tabular results."""
        return self._backend

    @backend.setter
    def backend(self, backend: str) -> None:
        self._backend = validate_backend(backend)

    @property
    def lossless(self) -> bool:
        """Return whether lossless q values are enabled."""
        return self._lossless

    @property
    def symbol_encoding(self) -> str:
        """How invalid UTF-8 q text is decoded in native mode."""
        return self.q.symbol_encoding

    @symbol_encoding.setter
    def symbol_encoding(self, symbol_encoding: str) -> None:
        self.q.symbol_encoding = symbol_encoding

    def _raise_if_cancelled(self, generation: int, cause: XqdbIOError | None) -> None:
        with self._cancel_condition:
            cancelled = self._cancel_generation != generation
        if cancelled:
            raise XqdbIOError(_CONNECTION_CANCELLED_MESSAGE) from cause

    def _connect_with_retry(self) -> None:
        with self._cancel_condition:
            generation = self._cancel_generation

        last_error: XqdbIOError | None = None
        for attempt in range(self.retries + 1):
            if attempt:
                delay = 2 ** min(attempt - 1, 5)
                logger.info("Connection failed; retrying in %s seconds", delay)
                with self._cancel_condition:
                    self._cancel_condition.wait_for(
                        lambda: self._cancel_generation != generation,
                        timeout=delay,
                    )
            self._raise_if_cancelled(generation, last_error)

            try:
                self.q.connect()
            except XqdbIOError as error:
                last_error = error
                self._raise_if_cancelled(generation, error)
                if attempt == self.retries:
                    raise
            else:
                with self._cancel_condition:
                    cancelled = self._cancel_generation != generation
                if cancelled:
                    with suppress(XqdbIOError):
                        self.q.shutdown()
                    raise XqdbIOError(_CONNECTION_CANCELLED_MESSAGE)
                return

    def connect(self) -> None:
        """Open the q IPC connection, retrying configured failures."""
        self._connect_with_retry()

    def _signal_cancel(self) -> None:
        with self._cancel_condition:
            self._cancel_generation += 1
            self._cancel_condition.notify_all()

    def cancel(self) -> None:
        """Cancel active socket I/O or connection retry backoff."""
        self._signal_cancel()
        self._abort.cancel()

    def disconnect(self) -> None:
        """Close the connection, safely and idempotently."""
        self._signal_cancel()
        self.q.shutdown()

    def __enter__(self: _Q) -> _Q:
        """Connect and return this context-managed client."""
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[False]:
        """Disconnect when leaving a context."""
        self.disconnect()
        return False

    def _sync_converted(self, expr: str, args: tuple[object, ...]) -> object:
        self._connect_with_retry()
        return from_arrow_results(self.q.sync(expr, *args), self.backend)

    def sync(self, expr: str, *args: object) -> Any:
        """Run a synchronous q expression and return its decoded result."""
        return self._sync_converted(expr, _convert_args(args))

    def asyn(self, expr: str, *args: object) -> None:
        """Send an asynchronous q expression."""
        converted = _convert_args(args)
        self._connect_with_retry()
        self.q.asyn(expr, *converted)

    def receive(self) -> Any:
        """Receive and decode the next q message."""
        return from_arrow_results(self.q.receive(), self.backend)

    def iter_batches(
        self, expression: str, batch_size: int = 65_536, *args: object
    ) -> Iterator[Any]:
        """Page a q function called as ``(offset; requested_rows; *args)``."""
        requested_rows = _validate_count("batch_size", batch_size, minimum=1)
        if len(args) > _MAX_ITER_BATCH_ARGS:
            message = f"iter_batches accepts at most {_MAX_ITER_BATCH_ARGS} additional arguments"
            raise TypeError(message)
        converted_args = tuple(to_arrow_inputs(arg) for arg in args)

        offset = 0
        while True:
            batch = self._sync_converted(expression, (offset, requested_rows, *converted_args))
            if isinstance(batch, XqdbQValue):
                if not batch.is_table:
                    raise TypeError(_PAGING_TABLE_MESSAGE)
                row_count = batch.len
            elif isinstance(batch, nw.DataFrame):
                row_count = batch.shape[0]
            else:
                raise TypeError(_PAGING_TABLE_MESSAGE)

            if row_count > requested_rows:
                message = (
                    "paging function returned more rows than requested "
                    f"({row_count} > {requested_rows})"
                )
                raise ValueError(message)
            if row_count == 0:
                return
            yield batch
            if row_count < requested_rows:
                return
            offset += row_count
            if offset > _MAX_Q_LONG:
                message = "iter_batches offset exceeds q long range"
                raise OverflowError(message)
