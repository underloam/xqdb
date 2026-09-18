"""Serialization helpers for q IPC protocol version 6."""

from typing import Any, Literal, cast

import narwhals as nw

from xqdb._conversion import from_arrow_results, to_arrow_inputs, validate_backend
from xqdb.xqdb import (
    deserialize_ipc_bytes6 as _deserialize_ipc_bytes6,
)
from xqdb.xqdb import (
    deserialize_value6 as _deserialize_value6,
)
from xqdb.xqdb import (
    generate_j6_ipc_msg,
    read_j6_binary_table,
)

_MSG_TYPES = {"async": 0, "sync": 1, "response": 2}


def read_binary6(
    filepath: str, backend: str = "pyarrow", symbol_encoding: str = "strict"
) -> nw.DataFrame:
    """Read and decode a q IPC v6 binary table."""
    return cast(
        "nw.DataFrame",
        from_arrow_results(
            read_j6_binary_table(filepath, symbol_encoding=symbol_encoding),
            backend,
        ),
    )


def deserialize_value6(
    body: bytes,
    *,
    backend: str = "pyarrow",
    symbol_encoding: str = "strict",
    lossless: bool = False,
) -> Any:
    """Deserialize one q IPC value body (without its eight-byte frame header)."""
    backend = validate_backend(backend)
    value = _deserialize_value6(body, symbol_encoding=symbol_encoding, lossless=lossless)
    return from_arrow_results(value, backend)


def deserialize_ipc_bytes6(
    frame: bytes,
    *,
    backend: str = "pyarrow",
    symbol_encoding: str = "strict",
    lossless: bool = False,
) -> tuple[Literal["async", "sync", "response"], Any]:
    """Deserialize one complete q IPC v6 frame."""
    backend = validate_backend(backend)
    message_type, value = _deserialize_ipc_bytes6(
        frame, symbol_encoding=symbol_encoding, lossless=lossless
    )
    return message_type, from_arrow_results(value, backend)


def serialize_as_ipc_bytes6(
    msg_type: Literal["async", "sync", "response"],
    enable_compression: bool,
    any: object,
) -> bytes:
    """Serialize a Python value as a complete q IPC v6 frame."""
    try:
        wire_type = _MSG_TYPES[msg_type]
    except KeyError:
        message = f"expected 'async', 'sync', or 'response' msg_type, got {msg_type!r}"
        raise ValueError(message) from None
    return generate_j6_ipc_msg(wire_type, enable_compression, to_arrow_inputs(any))


__all__ = [
    "deserialize_ipc_bytes6",
    "deserialize_value6",
    "read_binary6",
    "serialize_as_ipc_bytes6",
]
