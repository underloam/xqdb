"""Public Python API for xqdb."""

from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:

    class XqdbQOperator:
        """Represent a q operator token."""

        PLUS: ClassVar["XqdbQOperator"]

        def __init__(self, name: str) -> None:
            """Create an operator token from its q name."""
            ...

        @property
        def name(self) -> str:
            """Return the q operator name."""
            ...

    class XqdbQLambda:
        """Represent q lambda source and its serialized context."""

        def __init__(self, source: str, context: str = "") -> None:
            """Create a q lambda from source and optional context."""
            ...

        @property
        def source(self) -> str:
            """Return the q source text."""
            ...

        @property
        def context(self) -> str:
            """Return the serialized q context."""
            ...

    class XqdbQValue:
        """Represent an immutable, lossless q IPC value body."""

        def __init__(self, body: bytes) -> None:
            """Create a q value from its serialized IPC body."""
            ...

        @staticmethod
        def atom(kind: str | int, value: object) -> "XqdbQValue":
            """Create a q atom of the requested type."""
            ...

        @staticmethod
        def list(values: object) -> "XqdbQValue":
            """Create a q list from native values."""
            ...

        @staticmethod
        def dictionary(keys: object, values: object) -> "XqdbQValue":
            """Create a q dictionary from native keys and values."""
            ...

        @staticmethod
        def native(value: object) -> "XqdbQValue":
            """Convert a native Python value to a lossless q value."""
            ...

        @property
        def body(self) -> bytes:
            """Return the serialized q IPC value body."""
            ...

        @property
        def type_code(self) -> int:
            """Return the signed q type code."""
            ...

        @property
        def len(self) -> int:
            """Return the q value's logical length."""
            ...

        @property
        def is_table(self) -> bool:
            """Return whether the q value is a table."""
            ...

        def __bytes__(self) -> bytes:
            """Return the serialized q IPC value body."""
            ...

        def __len__(self) -> int:
            """Return the q value's logical length."""
            ...
else:
    from xqdb.xqdb import XqdbQLambda, XqdbQOperator, XqdbQValue

from xqdb.exceptions import XqdbAuthError, XqdbError, XqdbIOError
from xqdb.q import Q
from xqdb.util import (
    deserialize_ipc_bytes6,
    deserialize_value6,
    read_binary6,
    serialize_as_ipc_bytes6,
)

__all__ = [
    "Q",
    "XqdbAuthError",
    "XqdbError",
    "XqdbIOError",
    "XqdbQLambda",
    "XqdbQOperator",
    "XqdbQValue",
    "deserialize_ipc_bytes6",
    "deserialize_value6",
    "read_binary6",
    "serialize_as_ipc_bytes6",
]
