from __future__ import annotations

from datetime import date, datetime, time, timedelta
from functools import cache

import narwhals as nw

from xqdb.xqdb import ArrowSeries, ArrowTable, XqdbQValue

_MAX_CONVERSION_DEPTH = 64
_CYCLIC_CONTAINER_MESSAGE = "cyclic Python containers cannot be converted to q"
_LAZY_FRAME_MESSAGE = "lazy dataframe inputs are not supported"
_SUPPORTED_OUTPUT_BACKENDS = frozenset(
    {
        nw.Implementation.PANDAS,
        nw.Implementation.PYARROW,
        nw.Implementation.POLARS,
    }
)


class _SeriesInput:
    __slots__ = ("_frame",)
    __xqdb_series__ = True

    def __init__(self, series: nw.Series) -> None:
        self._frame = series.to_frame()

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        return self._frame.__arrow_c_stream__(requested_schema=requested_schema)


def _as_eager_frame(value: object) -> nw.DataFrame | nw.Series | None:
    converted = nw.from_native(
        value,
        pass_through=True,
        eager_only=True,
        allow_series=True,
    )
    if isinstance(converted, nw.LazyFrame):
        raise TypeError(_LAZY_FRAME_MESSAGE)
    if isinstance(converted, (nw.DataFrame, nw.Series)):
        return converted
    if converted is value:
        unrestricted = nw.from_native(
            value,
            pass_through=True,
            allow_series=True,
        )
        if isinstance(unrestricted, nw.LazyFrame):
            raise TypeError(_LAZY_FRAME_MESSAGE)
    return None


def to_arrow_inputs(value: object) -> object:
    return _to_arrow_inputs(value, 0, set())


def _convert_container(
    value: dict[object, object] | list[object] | tuple[object, ...],
    depth: int,
    active: set[int],
) -> object:
    identity = id(value)
    if identity in active:
        raise ValueError(_CYCLIC_CONTAINER_MESSAGE)
    active.add(identity)
    try:
        if isinstance(value, dict):
            return {key: _to_arrow_inputs(item, depth + 1, active) for key, item in value.items()}
        if isinstance(value, list):
            return [_to_arrow_inputs(item, depth + 1, active) for item in value]
        return tuple(_to_arrow_inputs(item, depth + 1, active) for item in value)
    finally:
        active.remove(identity)


def _to_arrow_inputs(value: object, depth: int, active: set[int]) -> object:
    if depth > _MAX_CONVERSION_DEPTH:
        message = f"Python value nesting exceeds {_MAX_CONVERSION_DEPTH} levels"
        raise ValueError(message)

    if isinstance(
        value,
        (
            type(None),
            bool,
            int,
            float,
            str,
            bytes,
            date,
            datetime,
            time,
            timedelta,
            XqdbQValue,
        ),
    ):
        return value

    if isinstance(value, (dict, list, tuple)):
        return _convert_container(value, depth, active)

    frame = _as_eager_frame(value)
    if isinstance(frame, nw.Series):
        return _SeriesInput(frame)
    if isinstance(frame, nw.DataFrame):
        return frame
    return value


@cache
def validate_backend(backend: str) -> str:
    implementation = nw.Implementation.from_backend(backend)
    if implementation not in _SUPPORTED_OUTPUT_BACKENDS:
        supported = ", ".join(sorted(item.value for item in _SUPPORTED_OUTPUT_BACKENDS))
        message = f"unsupported dataframe backend: {backend!r}; expected one of {supported}"
        raise ValueError(message)
    implementation.to_native_namespace()
    return backend


def _from_arrow(value: object, backend: str) -> nw.DataFrame:
    return nw.from_arrow(value, backend=validate_backend(backend))


def from_arrow_results(value: object, backend: str) -> object:
    if isinstance(value, ArrowTable):
        return _from_arrow(value, backend)
    if isinstance(value, ArrowSeries):
        frame = _from_arrow(value, backend)
        return frame.get_column(value.name)
    if isinstance(value, tuple):
        return tuple(from_arrow_results(item, backend) for item in value)
    if isinstance(value, list):
        return [from_arrow_results(item, backend) for item in value]
    if isinstance(value, dict):
        return {key: from_arrow_results(item, backend) for key, item in value.items()}
    return value
