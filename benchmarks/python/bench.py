"""Python q IPC client comparison against a single fixed KDB-X fixture.

Subjects are measured round-robin with a rotating start position so machine and
server drift is shared instead of accumulating on one subject.

Fidelity is a preflight, not an afterthought:

* `scalar` is a value-exact latency floor, so a subject that cannot return the
  exact value is excluded from it.
* `read.<table>` decodes an identical server byte stream into each library's
  documented frame type, so every subject that can decode it is ranked. The
  timed validation checks shape, not content fidelity. `roundTripStates`
  separately labels the decode-then-encode check as `identical`, `differs`,
  `resized`, or `unverified`; an encoder failure is never reported as decode
  loss.
* `send.<table>` is only comparable when the subject's own decoded frame
  re-encodes to a q-identical value of the same canonical size; otherwise the
  subject would be timed on different work, so it is listed in `unsupported`
  with no samples, throughput, or ratio.

int64 and nanosecond-timestamp exactness are reported by the preflight rather
than timed: they are correctness claims, and timing a wrong decode against a
right one would compare different work.

The preflight runs in one subprocess per subject because a subject can abort the
interpreter rather than raise: kola 2.5.1 panics in its Rust serializer when
asked to send a frame with list columns. Subprocess probing keeps a fatal
subject from taking the whole comparison with it and pins the failure to the
exact operation that caused it.

PyKX is deliberately absent: the licence covering its bundled q runtime forbids
making performance comparisons available to third parties.
"""

from __future__ import annotations

import argparse
import contextlib
import gc
import getpass
import hashlib
import importlib
import ipaddress
import json
import os
import platform
import random
import re
import shutil
import socket
import statistics
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from importlib import metadata
from pathlib import Path
from typing import TYPE_CHECKING, TypeAlias, TypedDict

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from typing import TextIO

SCHEMA_VERSION = 2
REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_PATHS = (
    "Cargo.toml",
    "Cargo.lock",
    "rust-toolchain.toml",
    "Taskfile.yml",
    "crates/xqdb/Cargo.toml",
    "crates/xqdb/src",
    "pyproject.toml",
    "py-xqdb/Cargo.toml",
    "py-xqdb/rust-toolchain.toml",
    "py-xqdb/src",
    "py-xqdb/xqdb",
    "testing/kdb",
    "benchmarks/python/bench.py",
)
TABLES = {"trade": 14, "wide": 64, "depth": 5}
SCALAR_EXPRESSION = "6f*7f"
SCALAR_EXPECTED = 42.0
LONG_EXPRESSION = "9007199254740993j"
LONG_EXPECTED = 9007199254740993
TIMESTAMP_EXPRESSION = "2024.01.02D03:04:05.123456789"
TIMESTAMP_EXPECTED_NS = 1704164645123456789
COUNT_LAMBDA = "{[x]count x}"
CANONICAL_BYTES_LAMBDA = "{[x]count -8!x}"
FRAME_DIMENSIONS = 2
MINUTES_DISPLAY_THRESHOLD_SECONDS = 90
NATIVE_SUFFIXES = frozenset({".so", ".pyd", ".dll", ".dylib"})
MINIMUM_NEEDLE_LENGTH = 3
# Windows drive and UNC paths (segments may contain spaces), then POSIX absolute
# paths with at least two segments so q expressions and ratios are left alone.
_ABSOLUTE_PATH = re.compile(
    r"(?<![A-Za-z0-9])[A-Za-z]:[\\/](?:[^\\/:*?\"<>|\r\n]+[\\/])*[^\\/:*?\"<>|\r\n]*"
    r"|\\\\[^\\/:*?\"<>|\r\n\s]+(?:\\[^\\/:*?\"<>|\r\n]+)+"
    r"|(?<![\w.])/(?:[^/\s:'\"|<>]+/)+[^/\s:'\"|<>]*"
)


def redact_paths(text: str) -> str:
    """Replace absolute filesystem paths, which embed user and system names."""
    return _ABSOLUTE_PATH.sub("<path>", text)


def connection_label(host: str) -> str:
    """Classify the fixture endpoint without recording it."""
    if host == "localhost":
        return "loopback"
    try:
        return "loopback" if ipaddress.ip_address(host).is_loopback else "network"
    except ValueError:
        return "network"


def machine_identifiers() -> dict[str, str]:
    """Return this machine's identifying strings; none may appear in a report.

    `platform.platform()` already embeds the OS release, so the bare release and
    version strings are left out: on macOS they are plain version numbers that
    can collide with a package version.
    """
    needles = {
        "hostname": socket.gethostname(),
        "home": str(Path.home()),
        "cpu": platform.processor(),
        "machine": platform.machine(),
        "system": platform.system(),
        "platform": platform.platform(),
    }
    with contextlib.suppress(OSError):
        needles["user"] = getpass.getuser()
    return {name: value for name, value in needles.items() if len(value) >= MINIMUM_NEEDLE_LENGTH}


def assert_no_machine_identifiers(text: str) -> None:
    """Refuse report text that names this machine, its user, or an absolute path."""
    match = _ABSOLUTE_PATH.search(text)
    if match:
        message = f"report contains an absolute path: {match.group(0)!r}"
        raise RuntimeError(message)
    for name, needle in machine_identifiers().items():
        pattern = rf"(?<![A-Za-z0-9]){re.escape(needle)}(?![A-Za-z0-9])"
        if re.search(pattern, text, re.IGNORECASE):
            message = f"report contains the local {name}: {needle!r}"
            raise RuntimeError(message)


class _OperationOutcome(TypedDict):
    value: object | None
    error: str | None


_Operation: TypeAlias = Callable[[], object]
_ResultCheck: TypeAlias = Callable[[str, object], None]


def _git_executable() -> str:
    executable = shutil.which("git")
    if executable is None:
        message = "cannot record benchmark source provenance: git is not on PATH"
        raise RuntimeError(message)
    return executable


def git_text(*arguments: str) -> str:
    """Run a read-only Git command and return its trimmed standard output."""
    completed = subprocess.run(  # noqa: S603 - fixed executable and argument list
        [_git_executable(), "-C", str(REPO_ROOT), *arguments],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode:
        detail = completed.stderr.strip() or f"exit code {completed.returncode}"
        message = f"cannot record benchmark source provenance: git {arguments[0]}: {detail}"
        raise RuntimeError(message)
    return completed.stdout.strip()


def source_provenance(version: str) -> dict[str, object]:
    """Describe and content-hash the source state relevant to the benchmark."""
    revision = git_text("rev-parse", "HEAD")
    describe = git_text("describe", "--always", "--long", "--dirty", "--tags")
    dirty = bool(git_text("status", "--porcelain=v1", "--untracked-files=all"))
    listed = subprocess.run(  # noqa: S603 - fixed executable and argument list
        [
            _git_executable(),
            "-C",
            str(REPO_ROOT),
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
            "--",
            *SOURCE_PATHS,
        ],
        capture_output=True,
        check=False,
    )
    if listed.returncode:
        detail = listed.stderr.decode(errors="replace").strip()
        detail = detail or f"exit code {listed.returncode}"
        message = f"cannot enumerate benchmark source content: {detail}"
        raise RuntimeError(message)

    digest = hashlib.sha256()
    encoded_paths = sorted(path for path in listed.stdout.split(b"\0") if path)
    for encoded_path in encoded_paths:
        path = REPO_ROOT / os.fsdecode(encoded_path)
        digest.update(len(encoded_path).to_bytes(8, "big"))
        digest.update(encoded_path)
        if path.exists():
            content = path.read_bytes()
            digest.update(b"\1")
            digest.update(len(content).to_bytes(8, "big"))
            digest.update(content)
        else:
            digest.update(b"\0")

    return {
        "revision": revision,
        "version": version,
        "describe": describe,
        "dirty": dirty,
        "identity": (
            "dirty working-tree source state identified by revision plus contentDigest; "
            "this does not prove which source built the loaded artifacts"
            if dirty
            else "clean working-tree source state; this does not itself prove artifact build inputs"
        ),
        "contentDigest": {
            "algorithm": "sha256",
            "framing": (
                "sorted git pathname bytes framed by uint64-be length, then pathname, "
                "one-byte presence, and for present files uint64-be content length plus content"
            ),
            "value": digest.hexdigest(),
            "fileCount": len(encoded_paths),
            "pathspecs": list(SOURCE_PATHS),
            "includesTrackedAndUntrackedFiles": True,
        },
    }


def python_artifact_provenance() -> dict[str, object]:
    """Hash the loaded xqdb Python package and native extension.

    Native file names carry platform tags and install locations carry user
    names, so native artifacts are identified by digest only and the package
    location is reduced to whether it sits inside this repository.
    """
    package = importlib.import_module("xqdb")
    native = importlib.import_module("xqdb.xqdb")
    package_root = Path(package.__file__).resolve().parent
    native_path = Path(native.__file__).resolve()
    runtime_suffixes = NATIVE_SUFFIXES | {".py"}
    files = sorted(
        (
            path
            for path in package_root.rglob("*")
            if path.is_file() and path.suffix.lower() in runtime_suffixes
        ),
        key=lambda path: path.relative_to(package_root).as_posix(),
    )
    if native_path not in files:
        message = "loaded native extension is outside the xqdb package"
        raise RuntimeError(message)

    aggregate = hashlib.sha256()
    entries = []
    for path in files:
        relative = path.relative_to(package_root).as_posix()
        encoded_path = relative.encode()
        content = path.read_bytes()
        content_digest = hashlib.sha256(content).hexdigest()
        aggregate.update(len(encoded_path).to_bytes(8, "big"))
        aggregate.update(encoded_path)
        aggregate.update(len(content).to_bytes(8, "big"))
        aggregate.update(content)
        entry: dict[str, object] = {"bytes": len(content), "sha256": content_digest}
        if path.suffix.lower() in NATIVE_SUFFIXES:
            entry["kind"] = "native"
        else:
            entry["kind"] = "python"
            entry["path"] = relative
        entries.append(entry)

    native_content = native_path.read_bytes()
    return {
        "packageVersion": metadata.version("xqdb"),
        "pythonPackage": {
            "installedInsideRepository": package_root.is_relative_to(REPO_ROOT),
            "contentDigest": {
                "algorithm": "sha256",
                "framing": (
                    "sorted package-relative UTF-8 path framed by uint64-be length, then path, "
                    "uint64-be content length, then content; native file names take part in "
                    "the digest but are not listed because they carry platform tags"
                ),
                "value": aggregate.hexdigest(),
            },
            "files": entries,
        },
        "loadedNativeExtension": {
            "bytes": len(native_content),
            "sha256": hashlib.sha256(native_content).hexdigest(),
        },
    }


# ── subjects ────────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class _SubjectConfig:
    subject_id: str
    package: str
    frame: str
    implementation: str
    representation: str
    notes: tuple[str, ...] = ()


class Subject:
    """One measured client, normalised to a single small protocol."""

    def __init__(self, config: _SubjectConfig) -> None:
        """Load the fixed descriptive metadata for a benchmark subject."""
        self.id = config.subject_id
        self.package = config.package
        self.version = metadata.version(config.package)
        self.frame = config.frame
        self.implementation = config.implementation
        self.representation = config.representation
        self.notes = list(config.notes)
        self.connection = None

    def connect(self) -> None:
        """Open the subject's q IPC connection."""
        raise NotImplementedError

    def close(self) -> None:
        """Close the subject's q IPC connection."""
        raise NotImplementedError

    def eval(self, expression: str) -> object:
        """Evaluate a q expression synchronously."""
        raise NotImplementedError

    def apply(self, lambda_text: str, value: object) -> object:
        """Apply a q lambda synchronously to one value."""
        raise NotImplementedError

    def read(self, table: str) -> object:
        """Decode a named fixture table."""
        return self.eval(table)

    def shape_of(self, value: object) -> dict[str, int] | None:
        """Return frame dimensions when exposed by the competitor API."""
        shape = getattr(value, "shape", None)
        if isinstance(shape, tuple) and len(shape) == FRAME_DIMENSIONS:
            return {"rows": int(shape[0]), "columns": int(shape[1])}
        return None

    def describe(self) -> dict[str, object]:
        """Return the stable machine-readable subject description."""
        return {
            "id": self.id,
            "package": self.package,
            "version": self.version,
            "frame": self.frame,
            "implementation": self.implementation,
            "representation": self.representation,
            "notes": self.notes,
        }


class XqdbSubject(Subject):
    """xqdb subject configured for one supported frame backend."""

    def __init__(self, backend: str, host: str, port: int) -> None:
        """Configure an xqdb benchmark subject."""
        super().__init__(
            _SubjectConfig(
                subject_id=f"xqdb-{backend}",
                package="xqdb",
                frame=backend,
                implementation="Rust core via PyO3, Arrow C Stream decode",
                representation=f"narwhals.DataFrame over a {backend} frame",
            )
        )
        self.backend = backend
        self.host = host
        self.port = port

    def connect(self) -> None:
        """Connect to q through xqdb."""
        import xqdb  # noqa: PLC0415 - competitor imports stay isolated by subject

        self.connection = xqdb.Q(self.host, self.port, timeout=120, backend=self.backend)
        self.connection.connect()

    def close(self) -> None:
        """Disconnect the xqdb client if it was opened."""
        if self.connection is not None:
            self.connection.disconnect()

    def eval(self, expression: str) -> object:
        """Evaluate a q expression through xqdb."""
        return self.connection.sync(expression)

    def apply(self, lambda_text: str, value: object) -> object:
        """Apply a q lambda through xqdb."""
        return self.connection.sync(lambda_text, value)


class KolaSubject(Subject):
    """Kola subject using its fixed Polars representation."""

    def __init__(self, host: str, port: int) -> None:
        """Configure the Kola benchmark subject."""
        super().__init__(
            _SubjectConfig(
                subject_id="kola",
                package="kola",
                frame="polars",
                implementation="Rust core via PyO3, Polars decode",
                representation="polars.DataFrame",
            )
        )
        self.host = host
        self.port = port

    def connect(self) -> None:
        """Connect to q through Kola."""
        import kola  # noqa: PLC0415 - competitor imports stay isolated by subject

        self.connection = kola.Q(self.host, self.port)
        self.connection.connect()

    def close(self) -> None:
        """Disconnect the Kola client if it was opened."""
        if self.connection is not None:
            self.connection.disconnect()

    def eval(self, expression: str) -> object:
        """Evaluate a q expression through Kola."""
        return self.connection.sync(expression)

    def apply(self, lambda_text: str, value: object) -> object:
        """Apply a q lambda through Kola."""
        return self.connection.sync(lambda_text, value)


class QconnectSubject(Subject):
    """qconnect subject using its fixed pandas representation."""

    def __init__(self, host: str, port: int) -> None:
        """Configure the qconnect benchmark subject."""
        super().__init__(
            _SubjectConfig(
                subject_id="qconnect",
                package="qconnect",
                frame="pandas",
                implementation="pure Python codec over numpy (maintained qPython fork)",
                representation="pandas.DataFrame",
                notes=(
                    (
                        "TLS is disabled for parity with the other subjects; "
                        "qconnect enables it by default"
                    ),
                    "requests is imported at module scope but is not declared as a dependency",
                ),
            )
        )
        self.host = host
        self.port = port

    def connect(self) -> None:
        """Connect to q through qconnect."""
        from qconnect import qconnection  # noqa: PLC0415 - isolate competitor import

        self.connection = qconnection.QConnection(
            host=self.host, port=self.port, pandas=True, tls_enabled=False
        )
        self.connection.open()

    def close(self) -> None:
        """Close the qconnect client if it was opened."""
        if self.connection is not None:
            self.connection.close()

    def eval(self, expression: str) -> object:
        """Evaluate a q expression through qconnect."""
        return self.connection.sendSync(expression)

    def apply(self, lambda_text: str, value: object) -> object:
        """Apply a q lambda through qconnect."""
        return self.connection.sendSync(lambda_text, value)


_SubjectBuilder: TypeAlias = Callable[[str, int], Subject]
SUBJECT_BUILDERS: dict[str, _SubjectBuilder] = {
    "xqdb-pyarrow": lambda host, port: XqdbSubject("pyarrow", host, port),
    "xqdb-polars": lambda host, port: XqdbSubject("polars", host, port),
    "xqdb-pandas": lambda host, port: XqdbSubject("pandas", host, port),
    "kola": KolaSubject,
    "qconnect": QconnectSubject,
}
# The first entry is the reference subject every ratio is taken against.
SUBJECT_IDS = list(SUBJECT_BUILDERS)


# ── statistics ──────────────────────────────────────────────────────────────


def nearest_rank(ascending: Sequence[int], fraction: float) -> int:
    """Return the nearest-rank sample for a fraction in an ordered series."""
    rank = round(fraction * (len(ascending) - 1))
    return ascending[max(0, min(len(ascending) - 1, rank))]


def metrics(
    samples_ns: Sequence[int],
    payload_bytes: int | None,
    *,
    keep_samples: bool,
) -> dict[str, object]:
    """Summarize raw nanosecond samples without changing the report schema."""
    ascending = sorted(samples_ns)
    median_ms = statistics.median(ascending) / 1_000_000
    report: dict[str, object] = {
        "iterations": len(samples_ns),
        "minMs": ascending[0] / 1_000_000,
        "medianMs": median_ms,
        "meanMs": statistics.fmean(samples_ns) / 1_000_000,
        "p90Ms": nearest_rank(ascending, 0.90) / 1_000_000,
        "p99Ms": nearest_rank(ascending, 0.99) / 1_000_000,
        "maxMs": ascending[-1] / 1_000_000,
    }
    if payload_bytes is not None:
        report["payloadBytes"] = payload_bytes
        report["medianMibPerSecond"] = payload_bytes / (median_ms / 1000) / 2**20
    if keep_samples:
        report["samplesMs"] = [sample / 1_000_000 for sample in samples_ns]
    return report


def rotate(items: Sequence[str], offset: int) -> list[str]:
    """Rotate an untimed order cyclically so each subject leads in turn."""
    shift = offset % len(items)
    return [*items[shift:], *items[:shift]]


def shuffled(items: Sequence[str], seed: int, round_index: int) -> list[str]:
    """Return a deterministic but independently shuffled per-round order.

    A cyclic rotation is not good enough here. Rotating by one preserves the
    adjacency relation, so every subject keeps the same predecessor in every
    round, and a subject that follows an expensive neighbour pays for it in
    every sample. Reshuffling varies predecessors as well as positions.
    """
    order = list(items)
    random.Random(f"{seed}:{round_index}").shuffle(order)
    return order


# ── progress ────────────────────────────────────────────────────────────────


class Progress:
    """Progress on stderr so stdout stays the summary and the probe protocol.

    A full run is minutes long and a single subject can hold a round for over a
    second, so silence is indistinguishable from a hang. A terminal gets a
    rewritten line per round; a captured log gets one line every few seconds so
    it stays readable.
    """

    THROTTLE_SECONDS = 3.0

    def __init__(self, stream: TextIO = sys.stderr) -> None:
        """Track progress state for a text stream."""
        self.stream = stream
        self.interactive = stream.isatty()
        self.pending = False
        self.last_step = 0.0

    def _write(self, text: str, *, transient: bool) -> None:
        if transient and self.interactive:
            self.stream.write("\r" + text.ljust(96)[:96])
            self.pending = True
        else:
            if self.pending:
                self.stream.write("\n")
                self.pending = False
            self.stream.write(text + "\n")
        self.stream.flush()

    def line(self, text: str) -> None:
        """Write a persistent progress line."""
        self.last_step = 0.0
        self._write(text, transient=False)

    def step(self, text: str, *, force: bool = False) -> None:
        """Write a throttled progress step, replacing it on interactive streams."""
        now = time.monotonic()
        if not self.interactive and not force and now - self.last_step < self.THROTTLE_SECONDS:
            return
        self.last_step = now
        self._write(text, transient=True)


def format_duration(seconds: float) -> str:
    """Format short benchmark durations for progress output."""
    if seconds < MINUTES_DISPLAY_THRESHOLD_SECONDS:
        return f"{seconds:.0f}s"
    return f"{int(seconds) // 60}m{int(seconds) % 60:02d}s"


# ── measurement ─────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class _RunSpec:
    warmups: int
    iterations: int
    seed: int
    label: str


def run_operation(
    ids: Sequence[str],
    operations: Mapping[str, _Operation],
    spec: _RunSpec,
    *,
    check: _ResultCheck | None = None,
    progress: Progress | None = None,
) -> tuple[dict[str, list[int]], list[list[str]]]:
    """Measure one operation in independently shuffled subject rounds."""
    for round_index in range(spec.warmups):
        if progress is not None:
            progress.step(f"{spec.label} warmup {round_index + 1}/{spec.warmups}")
        for subject_id in shuffled(ids, spec.seed, -1 - round_index):
            operations[subject_id]()

    started_run = time.perf_counter()

    samples: dict[str, list[int]] = {subject_id: [] for subject_id in ids}
    order = []
    for round_index in range(spec.iterations):
        sequence = shuffled(ids, spec.seed, round_index)
        order.append(sequence)
        for subject_id in sequence:
            started = time.perf_counter_ns()
            value = operations[subject_id]()
            elapsed = time.perf_counter_ns() - started
            if check is not None:
                check(subject_id, value)
            samples[subject_id].append(elapsed)
            # CPython frees on rebind, so leaving `value` bound would make the
            # next subject's timed interval pay for destroying this subject's
            # frame. Release it here, between the timers.
            del value
        if progress is not None:
            elapsed = time.perf_counter() - started_run
            remaining = elapsed / (round_index + 1) * (spec.iterations - round_index - 1)
            progress.step(
                f"{spec.label} {round_index + 1}/{spec.iterations} rounds, "
                f"{format_duration(elapsed)} elapsed, {format_duration(remaining)} left",
                force=round_index in (0, spec.iterations - 1),
            )
    return samples, order


def retained_memory(count: int, operation: _Operation) -> dict[str, object]:
    """Measure process RSS while retaining a fixed number of decoded results."""
    import psutil  # noqa: PLC0415 - optional benchmark dependency is lazily loaded

    process = psutil.Process()
    gc.collect()
    before = process.memory_info().rss
    retained = [operation() for _ in range(count)]
    gc.collect()
    after = process.memory_info().rss
    retained.clear()
    gc.collect()
    return {"retainedResults": count, "deltaBytes": {"rss": after - before}}


# ── preflight, run in a subprocess per subject ──────────────────────────────


def outcome(operation: _Operation) -> _OperationOutcome:
    """Capture a third-party operation's value or its diagnostic failure."""
    try:
        return {"value": operation(), "error": None}
    except Exception as error:  # noqa: BLE001 - the failure mode is the result
        return {"value": None, "error": redact_paths(f"{type(error).__name__}: {error}")}


def timestamp_nanoseconds(value: object) -> int | None:
    """Return best-effort epoch nanoseconds for any subject's temporal type."""
    raw = getattr(value, "raw", value)  # qconnect wraps numpy datetime64 in QTemporal
    if getattr(raw, "dtype", None) is not None and hasattr(raw, "astype"):
        return int(raw.astype("datetime64[ns]").astype("int64"))
    if hasattr(raw, "timestamp"):  # datetime: microsecond resolution at best
        return round(raw.timestamp() * 1_000_000) * 1_000
    if isinstance(raw, int):
        return raw
    return None


def emit(event: Mapping[str, object]) -> None:
    """Emit and flush one JSON-lines preflight event."""
    sys.stdout.write(json.dumps(event) + "\n")
    sys.stdout.flush()


def probe_subject(subject_id: str, host: str, port: int) -> None:
    """Emit stage boundaries and completed checks so a fatal abort is attributable."""
    emit({"event": "stage", "phase": "preflight", "operation": "construct"})
    subject = SUBJECT_BUILDERS[subject_id](host, port)
    emit({"event": "stage", "phase": "preflight", "operation": "connect"})
    subject.connect()

    emit({"event": "stage", "phase": "preflight", "operation": "scalar"})
    scalar = outcome(lambda: subject.eval(SCALAR_EXPRESSION))
    emit({"event": "stage", "phase": "preflight", "operation": "int64"})
    long_value = outcome(lambda: subject.eval(LONG_EXPRESSION))
    emit({"event": "stage", "phase": "preflight", "operation": "nanosecondTimestamp"})
    timestamp = outcome(lambda: subject.eval(TIMESTAMP_EXPRESSION))
    decoded_nanos = None if timestamp["error"] else timestamp_nanoseconds(timestamp["value"])
    emit(
        {
            "event": "scalars",
            "version": subject.version,
            "scalarExact": scalar["error"] is None and float(scalar["value"]) == SCALAR_EXPECTED,
            "scalarDetail": scalar["error"] or repr(scalar["value"]),
            "int64Exact": long_value["error"] is None and int(long_value["value"]) == LONG_EXPECTED,
            "int64Detail": long_value["error"] or str(int(long_value["value"])),
            "nanosecondTimestamp": (
                "rejected"
                if timestamp["error"]
                else "exact"
                if decoded_nanos == TIMESTAMP_EXPECTED_NS
                else "lossy"
            ),
            "nanosecondTimestampDetail": timestamp["error"] or str(decoded_nanos),
        }
    )

    canonical = {}
    for table in TABLES:
        emit(
            {
                "event": "stage",
                "phase": "fixture",
                "operation": "canonicalBytes",
                "table": table,
            }
        )
        canonical[table] = int(subject.eval(f"count -8!{table}"))
    for table, columns in TABLES.items():
        emit({"event": "stage", "phase": "read", "operation": "decode", "table": table})
        read = outcome(lambda table=table: subject.read(table))
        if read["error"]:
            emit({"event": "read", "table": table, "readable": False, "detail": read["error"]})
            continue
        shape = subject.shape_of(read["value"])
        emit(
            {
                "event": "read",
                "table": table,
                "readable": True,
                "shape": shape,
                "shapeMatchesFixture": shape is not None and shape["columns"] == columns,
            }
        )

        value = read["value"]
        emit({"event": "stage", "phase": "send", "operation": "identity", "table": table})
        identical = outcome(lambda t=table, v=value: subject.apply(f"{{[x]{t}~x}}", v))
        if identical["error"]:
            emit(
                {
                    "event": "send",
                    "table": table,
                    "sendComparable": False,
                    "roundTrip": "unverified",
                    "detail": f"encoder rejected the decoded frame: {identical['error']}",
                }
            )
            continue
        emit(
            {
                "event": "stage",
                "phase": "send",
                "operation": "canonicalBytes",
                "table": table,
            }
        )
        reencoded_bytes = int(subject.apply(CANONICAL_BYTES_LAMBDA, value))
        emit({"event": "stage", "phase": "send", "operation": "rowCount", "table": table})
        reencoded_rows = int(subject.apply(COUNT_LAMBDA, value))
        comparable = bool(identical["value"]) and reencoded_bytes == canonical[table]
        detail = None
        if not bool(identical["value"]):
            detail = "decoded frame does not re-encode to a q-identical value"
        elif reencoded_bytes != canonical[table]:
            detail = f"re-encoded canonical size {reencoded_bytes} != fixture {canonical[table]}"
        emit(
            {
                "event": "send",
                "table": table,
                "sendComparable": comparable,
                "roundTrip": (
                    "identical"
                    if comparable
                    else "differs"
                    if not bool(identical["value"])
                    else "resized"
                ),
                "reencodesToIdenticalQValue": bool(identical["value"]),
                "reencodedCanonicalBytes": reencoded_bytes,
                "reencodedRows": reencoded_rows,
                "detail": detail,
            }
        )

    emit({"event": "stage", "phase": "preflight", "operation": "teardown"})
    subject.close()
    emit({"event": "done"})


def _parse_probe_events(stdout: str) -> list[dict[str, object]]:
    events = []
    for raw_line in stdout.splitlines():
        line = raw_line.strip()
        if line.startswith("{"):
            events.append(json.loads(line))
    return events


def _fold_probe_events(
    report: dict[str, object],
    events: Sequence[dict[str, object]],
) -> dict[str, object] | None:
    last_stage = None
    for event in events:
        event_type = event["event"]
        if event_type == "stage":
            last_stage = {key: value for key, value in event.items() if key != "event"}
        elif event_type == "scalars":
            report.update({key: value for key, value in event.items() if key != "event"})
        elif event_type == "read":
            entry = report["tables"][event["table"]]
            entry["readable"] = event["readable"]
            entry["shape"] = event.get("shape")
            entry["shapeMatchesFixture"] = event.get("shapeMatchesFixture", False)
            if not event["readable"]:
                entry["readExcludedBecause"] = event["detail"]
                entry["sendExcludedBecause"] = "the frame could not be decoded"
        elif event_type == "send":
            entry = report["tables"][event["table"]]
            entry["sendComparable"] = event["sendComparable"]
            entry["roundTrip"] = event.get("roundTrip", "unverified")
            entry["reencodesToIdenticalQValue"] = event.get(
                "reencodesToIdenticalQValue",
                False,
            )
            entry["reencodedCanonicalBytes"] = event.get("reencodedCanonicalBytes")
            entry["reencodedRows"] = event.get("reencodedRows")
            if not event["sendComparable"]:
                entry["sendExcludedBecause"] = event["detail"]
    return last_stage


def _mark_probe_abort(
    report: dict[str, object],
    last_stage: dict[str, object] | None,
    fatal: str,
) -> None:
    pending = [(phase, table) for table in TABLES for phase in ("read", "send")]
    phase = None if last_stage is None else last_stage["phase"]
    table = None if last_stage is None else last_stage.get("table")
    operation = None if last_stage is None else last_stage["operation"]
    location = ".".join(str(part) for part in (phase, table, operation) if part is not None)
    failure = f"probe aborted during {location or 'an unknown preflight stage'}: {fatal}"
    current = (phase, table)

    if current in pending:
        start = pending.index(current)
        for index, (pending_phase, pending_table) in enumerate(pending):
            if index < start:
                continue
            entry = report["tables"][pending_table]
            is_current = index == start
            not_reached = f"not reached: {failure}"
            if pending_phase == "read":
                entry["readable"] = False
                entry["readExcludedBecause"] = failure if is_current else not_reached
                entry["sendExcludedBecause"] = not_reached
            else:
                entry["sendExcludedBecause"] = failure if is_current else not_reached
            entry["sendComparable"] = False
            entry["roundTrip"] = "unverified"
    elif phase != "preflight" or operation != "teardown":
        not_reached = f"not reached: {failure}"
        for entry in report["tables"].values():
            entry["readable"] = False
            entry["readExcludedBecause"] = not_reached
            entry["sendComparable"] = False
            entry["roundTrip"] = "unverified"
            entry["sendExcludedBecause"] = not_reached


def discover(subject_id: str, host: str, port: int) -> dict[str, object]:
    """Run the preflight for one subject out-of-process and fold it into a report."""
    completed = subprocess.run(  # noqa: S603 - fixed argv, no shell
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--probe",
            subject_id,
            "--host",
            host,
            "--port",
            str(port),
        ],
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    events = _parse_probe_events(completed.stdout)
    completed_probe = any(event["event"] == "done" for event in events)
    report: dict[str, object] = {
        "tables": {
            table: {"readable": False, "sendComparable": False, "roundTrip": "unverified"}
            for table in TABLES
        },
        "probe": {
            "exitCode": completed.returncode,
            "completed": completed_probe,
        },
    }
    fatal = None
    if not completed_probe:
        tail = redact_paths((completed.stderr or "").strip()).splitlines()
        fatal = " | ".join(tail[-4:]) or f"probe exited with code {completed.returncode}"
        report["probe"]["fatal"] = fatal

    last_stage = _fold_probe_events(report, events)
    report["probe"]["lastStage"] = last_stage
    if fatal is not None:
        _mark_probe_abort(report, last_stage, fatal)
    return report


# ── cli ─────────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    """Parse and validate benchmark and internal probe options."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--host",
        default=os.environ.get("XQDB_TEST_Q_HOST", "127.0.0.1"),
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("XQDB_TEST_Q_PORT", "1801")),
    )
    parser.add_argument(
        "--warmups",
        type=int,
        default=int(os.environ.get("XQDB_BENCH_WARMUPS", "3")),
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=int(os.environ.get("XQDB_BENCH_ITERATIONS", "50")),
    )
    parser.add_argument(
        "--memory-results",
        type=int,
        default=int(os.environ.get("XQDB_BENCH_MEMORY_RESULTS", "5")),
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=int(os.environ.get("XQDB_BENCH_SEED", "42")),
        help="seed for the deterministic per-round subject order",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--samples",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="retain raw durations and per-round subject order (default: enabled)",
    )
    parser.add_argument(
        "--probe",
        choices=SUBJECT_IDS,
        help="internal: emit the preflight for one subject",
    )
    args = parser.parse_args()
    if args.warmups < 0:
        parser.error("--warmups must be non-negative")
    if args.iterations < 1:
        parser.error("--iterations must be positive")
    if args.memory_results < 1:
        parser.error("--memory-results must be positive")
    return args


ROUND_TRIP_UNPROVEN_PREFIXES = (
    "encoder rejected the decoded frame",
    "aborted the interpreter",
    "not reached",
    "the frame could not be decoded",
)


def round_trip_state(table_entry: Mapping[str, object]) -> str:
    """Return identical, differs, resized, or unverified for a table round trip.

    `differs` requires q to have actually compared the two values. A bare
    `reencodesToIdenticalQValue is False` cannot carry that on its own: it is
    also the initial value when the encoder raised or aborted before any
    comparison happened, and claiming a difference there would assert something
    that was never measured. `roundTrip` is recorded by the preflight; the
    derivation below is the compatibility path for reports written without it.
    """
    recorded = table_entry.get("roundTrip")
    if recorded is not None:
        return recorded
    if table_entry.get("sendComparable"):
        return "identical"
    reason = table_entry.get("sendExcludedBecause") or ""
    if reason.startswith(ROUND_TRIP_UNPROVEN_PREFIXES):
        return "unverified"
    if table_entry.get("reencodesToIdenticalQValue") is False:
        return "differs"
    if table_entry.get("reencodesToIdenticalQValue") is True:
        return "resized"
    return "unverified"


def fastest_subject(entry: Mapping[str, object]) -> str | None:
    """Return the lowest-median subject among those ranked for an operation."""
    ranked = {
        subject_id: measured["medianMs"]
        for subject_id, measured in entry["subjects"].items()
        if measured is not None
    }
    return min(ranked, key=ranked.get) if ranked else None


def _latency_summary_lines(
    report: Mapping[str, object],
    subject_ids: Sequence[str],
) -> list[str]:
    lines = []
    for name, entry in report["operations"].items():
        winner = fastest_subject(entry)
        cells = []
        for subject_id in subject_ids:
            measured = entry["subjects"].get(subject_id)
            if measured is None:
                cells.append("n/a".rjust(17))
                continue
            star = " *" if subject_id == winner else "  "
            cells.append(f"{measured['medianMs']:.3f}{star}".rjust(17))
        lines.append(name.ljust(13) + "".join(cells))
    return lines


def _ratio_summary_lines(
    report: Mapping[str, object],
    subject_ids: Sequence[str],
    reference: str,
) -> list[str]:
    lines = []
    for name, entry in report["operations"].items():
        parts = []
        for subject_id in subject_ids:
            if subject_id == reference:
                continue
            ratio = entry["medianRatioVsReference"].get(subject_id)
            peer = entry["medianRatioVsSameFrameXqdb"].get(subject_id)
            if ratio is None:
                parts.append(f"{subject_id}=excluded")
            elif peer is None or subject_id.startswith("xqdb-"):
                parts.append(f"{subject_id}={ratio:.2f}x")
            else:
                parts.append(f"{subject_id}={ratio:.2f}x ({peer:.2f}x)")
        lines.append(f"  {name.ljust(12)} " + "  ".join(parts))
    return lines


def _fidelity_summary_lines(
    report: Mapping[str, object],
    subject_ids: Sequence[str],
) -> list[str]:
    lines = []
    for subject_id in subject_ids:
        entry = report["fidelity"][subject_id]
        tables = " ".join(
            f"{table}={round_trip_state(value)}" for table, value in entry["tables"].items()
        )
        lines.append(
            f"  {subject_id.ljust(14)} "
            f"int64={'exact' if entry.get('int64Exact') else 'lossy'} "
            f"nanoseconds={entry.get('nanosecondTimestamp', 'unknown')} {tables}"
        )
    return lines


def _reported_round_trip_states(
    report: Mapping[str, object],
    name: str,
    entry: Mapping[str, object],
) -> Mapping[str, object]:
    states = entry.get("roundTripStates")
    if states is not None:
        return states
    table = name.split(".", 1)[1] if "." in name else None
    return {
        subject_id: (
            round_trip_state(report["fidelity"][subject_id]["tables"][table])
            if table is not None
            else "unverified"
        )
        for subject_id in entry.get("roundTripUnverifiedSubjects", [])
    }


def _exclusion_summary_lines(report: Mapping[str, object]) -> list[str]:
    lines = []
    for name, entry in report["operations"].items():
        states = _reported_round_trip_states(report, name, entry)
        reasons = entry.get(
            "roundTripReasons",
            entry.get("roundTripUnverifiedReasons", {}),
        )
        for subject_id, state in states.items():
            if state == "identical":
                continue
            label = {
                "differs": "decode-then-encode q round trip differs from the fixture",
                "resized": "q-identical round trip has a different canonical byte count",
                "unverified": "decode-then-encode q round trip unverified",
            }.get(state, f"decode-then-encode q round trip has unknown state {state}")
            reason = reasons.get(subject_id, "unknown")
            lines.append(f"  {name}: {subject_id} {label} - {reason}")
        for excluded in entry.get("unsupported", []):
            subject = excluded["subject"]
            lines.append(f"  excluded from {name}: {subject} - {excluded['reason']}")
    return lines


def render_summary(report: Mapping[str, object]) -> str:
    """Render the stable human-readable summary for a benchmark report."""
    subject_ids = [subject["id"] for subject in report["subjects"]]
    reference = report["method"]["referenceSubject"]
    header = (
        f"suite=python rows={report['fixture']['rows']} "
        f"q={report['fixture']['qVersion']} "
        f"python={report['runtime']['python']} "
        f"iterations={report['method']['iterationsPerSubjectPerOperation']}"
    )
    columns = "operation".ljust(13) + "".join(
        f"{subject_id} ms".rjust(17) for subject_id in subject_ids
    )
    lines = [header, "", columns]
    lines.extend(_latency_summary_lines(report, subject_ids))
    lines.append("* = fastest measured client for that operation")
    lines.extend(
        [
            "",
            f"median vs {reference}; (x) = vs the xqdb subject returning the same frame type",
        ]
    )
    lines.extend(_ratio_summary_lines(report, subject_ids, reference))
    lines.extend(
        [
            "",
            "atom fidelity and table decode-then-encode round trips (`~` in q)",
        ]
    )
    lines.extend(_fidelity_summary_lines(report, subject_ids))
    lines.extend(_exclusion_summary_lines(report))
    return "\n".join(lines) + "\n"


_OperationBuilder: TypeAlias = Callable[[Subject], _Operation]


def _require(condition: object, message: str) -> None:
    if not condition:
        raise AssertionError(message)


@dataclass(frozen=True, slots=True)
class _BenchmarkSettings:
    warmups: int
    iterations: int
    seed: int
    keep_samples: bool


@dataclass(frozen=True, slots=True)
class _OperationSpec:
    name: str
    subject_ids: Sequence[str]
    payload_bytes: int | None
    build: _OperationBuilder
    check: _ResultCheck | None = None
    extra: Mapping[str, object] | None = None


@dataclass(slots=True)
class _MeasurementRecorder:
    by_id: Mapping[str, Subject]
    reference_id: str
    same_frame_reference: Mapping[str, str]
    settings: _BenchmarkSettings
    progress: Progress
    total_operations: int
    operations: dict[str, object] = field(default_factory=dict, init=False)
    order: dict[str, list[list[str]]] = field(default_factory=dict, init=False)
    index: int = field(default=0, init=False)

    def record(self, spec: _OperationSpec) -> None:
        """Measure and record one operation without widening its timed interval."""
        self.index += 1
        label = (
            f"[bench {self.index}/{self.total_operations}] "
            f"{spec.name} ({len(spec.subject_ids)} subjects)"
        )
        self.progress.step(label)
        started_label = time.perf_counter()
        samples, sequence = run_operation(
            spec.subject_ids,
            {subject_id: spec.build(self.by_id[subject_id]) for subject_id in spec.subject_ids},
            _RunSpec(
                warmups=self.settings.warmups,
                iterations=self.settings.iterations,
                seed=self.settings.seed,
                label=label,
            ),
            check=spec.check,
            progress=self.progress,
        )
        per_subject = {
            subject_id: metrics(
                samples[subject_id],
                spec.payload_bytes,
                keep_samples=self.settings.keep_samples,
            )
            for subject_id in spec.subject_ids
        }
        reference_median = per_subject[self.reference_id]["medianMs"]
        self.operations[spec.name] = {
            "payloadBytes": spec.payload_bytes,
            "subjects": per_subject,
            "medianRatioVsReference": {
                subject_id: per_subject[subject_id]["medianMs"] / reference_median
                for subject_id in spec.subject_ids
            },
            "medianRatioVsSameFrameXqdb": {
                subject_id: per_subject[subject_id]["medianMs"]
                / per_subject[self.same_frame_reference[subject_id]]["medianMs"]
                for subject_id in spec.subject_ids
                if self.same_frame_reference[subject_id] in per_subject
            },
            **(spec.extra or {}),
        }
        if self.settings.keep_samples:
            self.order[spec.name] = sequence
        medians = "  ".join(
            f"{subject_id}={per_subject[subject_id]['medianMs']:.2f}ms"
            for subject_id in spec.subject_ids
        )
        elapsed = format_duration(time.perf_counter() - started_label)
        self.progress.line(f"{label} done in {elapsed}: {medians}")


def _load_preflight(host: str, port: int, progress: Progress) -> dict[str, object]:
    fidelity = {}
    subject_count = len(SUBJECT_IDS)
    for index, subject_id in enumerate(SUBJECT_IDS, start=1):
        progress.step(f"[preflight {index}/{subject_count}] {subject_id}")
        fidelity[subject_id] = discover(subject_id, host, port)
        probe = fidelity[subject_id]["probe"]
        if not probe["completed"]:
            progress.line(
                f"[preflight {index}/{subject_count}] {subject_id}: aborted, capabilities reduced"
            )
    progress.line(f"[preflight] {subject_count} subjects probed")
    return fidelity


def _same_frame_references(
    subjects: Sequence[Subject],
    reference: Subject,
) -> dict[str, str]:
    # Kola returns Polars and qconnect returns pandas, so compare each with the
    # xqdb subject that materialises the same frame type.
    return {
        subject.id: next(
            other.id
            for other in subjects
            if other.package == reference.package and other.frame == subject.frame
        )
        for subject in subjects
    }


def _load_fixture(
    host: str,
    reference: Subject,
) -> dict[str, object]:
    fixture: dict[str, object] = {
        "connection": connection_label(host),
        "qVersion": int(reference.eval(".z.K")),
        "rows": int(reference.eval(".xqdb.rows")),
        "seed": int(reference.eval(".xqdb.seed")),
        "tables": {},
    }
    for table, columns in TABLES.items():
        fixture["tables"][table] = {
            "columns": columns,
            "canonicalBytes": int(reference.eval(f"count -8!{table}")),
        }
    return fixture


def _verify_fixture(
    subjects: Sequence[Subject],
    fidelity: Mapping[str, object],
    fixture: Mapping[str, object],
) -> None:
    # Every subject must agree on what it is talking to before anything is timed.
    for subject in subjects:
        _require(
            int(subject.eval(".xqdb.rows")) == fixture["rows"],
            f"{subject.id}: row mismatch",
        )
        _require(
            int(subject.eval(".z.K")) == fixture["qVersion"],
            f"{subject.id}: q mismatch",
        )
        _require(
            int(subject.eval(".xqdb.seed")) == fixture["seed"],
            f"{subject.id}: seed mismatch",
        )
    for subject_id, entry in fidelity.items():
        for table, table_entry in entry["tables"].items():
            if table_entry.get("shape") is not None:
                _require(
                    table_entry["shape"]["rows"] == fixture["rows"],
                    f"{subject_id}: {table} preflight saw a different fixture",
                )


def _scalar_operation(subject: Subject) -> _Operation:
    return lambda: subject.eval(SCALAR_EXPRESSION)


def _read_operation_builder(table: str) -> _OperationBuilder:
    def _build(subject: Subject) -> _Operation:
        return lambda: subject.read(table)

    return _build


def _send_operation_builder(frames: Mapping[str, object]) -> _OperationBuilder:
    def _build(subject: Subject) -> _Operation:
        return lambda: subject.apply(COUNT_LAMBDA, frames[subject.id])

    return _build


def _read_result_check(
    by_id: Mapping[str, Subject],
    expected_rows: int,
    table: str,
) -> _ResultCheck:
    def _check(subject_id: str, value: object) -> None:
        shape = by_id[subject_id].shape_of(value)
        matches = shape is not None and shape["rows"] == expected_rows
        _require(matches, f"{subject_id}: {table} row count mismatch")

    return _check


def _send_result_check(expected_rows: int, table: str) -> _ResultCheck:
    def _check(subject_id: str, value: object) -> None:
        _require(
            int(value) == expected_rows,
            f"{subject_id}: {table} send count mismatch",
        )

    return _check


def _record_scalar(
    recorder: _MeasurementRecorder,
    subject_ids: Sequence[str],
    fidelity: Mapping[str, object],
) -> None:
    scalar_ids = [
        subject_id for subject_id in subject_ids if fidelity[subject_id].get("scalarExact")
    ]
    _require(
        recorder.reference_id in scalar_ids,
        "reference subject failed the scalar preflight",
    )
    recorder.record(
        _OperationSpec(
            name="scalar",
            subject_ids=scalar_ids,
            payload_bytes=None,
            build=_scalar_operation,
            extra={
                "unsupported": [
                    {
                        "subject": subject_id,
                        "reason": (
                            "scalar decode is not exact: "
                            f"{fidelity[subject_id].get('scalarDetail')}"
                        ),
                    }
                    for subject_id in subject_ids
                    if subject_id not in scalar_ids
                ]
            },
        )
    )


def _read_operation_metadata(
    subject_ids: Sequence[str],
    readable: Sequence[str],
    table_fidelity: Mapping[str, object],
    round_trip_states: Mapping[str, str],
) -> dict[str, object]:
    round_trip_reasons = {
        subject_id: table_fidelity[subject_id].get("sendExcludedBecause", "unknown")
        for subject_id in readable
        if round_trip_states[subject_id] != "identical"
    }
    return {
        "readValidation": "decoded row count only; table content fidelity is not inferred",
        "roundTripStates": round_trip_states,
        "roundTripReasons": round_trip_reasons,
        "roundTripUnverifiedSubjects": [
            subject_id for subject_id in readable if round_trip_states[subject_id] == "unverified"
        ],
        "roundTripUnverifiedReasons": {
            subject_id: round_trip_reasons[subject_id]
            for subject_id in readable
            if round_trip_states[subject_id] == "unverified"
        },
        "unsupported": [
            {
                "subject": subject_id,
                "reason": table_fidelity[subject_id].get("readExcludedBecause", "unknown"),
            }
            for subject_id in subject_ids
            if subject_id not in readable
        ],
    }


def _record_table_operations(
    recorder: _MeasurementRecorder,
    table: str,
    fixture: Mapping[str, object],
    fidelity: Mapping[str, object],
    subject_ids: Sequence[str],
) -> None:
    payload_bytes = fixture["tables"][table]["canonicalBytes"]
    table_fidelity = {
        subject_id: fidelity[subject_id]["tables"][table] for subject_id in subject_ids
    }
    readable = [subject_id for subject_id in subject_ids if table_fidelity[subject_id]["readable"]]
    _require(
        recorder.reference_id in readable,
        f"{table}: reference subject cannot decode the table",
    )
    round_trip_states = {
        subject_id: round_trip_state(table_fidelity[subject_id]) for subject_id in readable
    }
    recorder.record(
        _OperationSpec(
            name=f"read.{table}",
            subject_ids=readable,
            payload_bytes=payload_bytes,
            build=_read_operation_builder(table),
            check=_read_result_check(
                recorder.by_id,
                fixture["rows"],
                table,
            ),
            extra=_read_operation_metadata(
                subject_ids,
                readable,
                table_fidelity,
                round_trip_states,
            ),
        )
    )

    sendable = [
        subject_id for subject_id in subject_ids if table_fidelity[subject_id]["sendComparable"]
    ]
    _require(
        recorder.reference_id in sendable,
        f"{table}: reference subject cannot send comparably",
    )
    decoded = {subject_id: recorder.by_id[subject_id].read(table) for subject_id in sendable}
    recorder.record(
        _OperationSpec(
            name=f"send.{table}",
            subject_ids=sendable,
            payload_bytes=payload_bytes,
            build=_send_operation_builder(decoded),
            check=_send_result_check(fixture["rows"], table),
            extra={
                "unsupported": [
                    {
                        "subject": subject_id,
                        "reason": table_fidelity[subject_id].get(
                            "sendExcludedBecause",
                            "unknown",
                        ),
                    }
                    for subject_id in subject_ids
                    if subject_id not in sendable
                ]
            },
        )
    )


def _measure_memory(
    subject_ids: Sequence[str],
    fidelity: Mapping[str, object],
    by_id: Mapping[str, Subject],
    count: int,
    progress: Progress,
) -> dict[str, object]:
    memory = {}
    for table_index, table in enumerate(TABLES):
        memory[table] = {}
        readable = [
            subject_id
            for subject_id in rotate(subject_ids, table_index)
            if fidelity[subject_id]["tables"][table]["readable"]
        ]
        for subject_id in readable:
            progress.step(f"[memory {table_index + 1}/{len(TABLES)}] {table} {subject_id}")
            memory[table][subject_id] = retained_memory(
                count,
                lambda subject_id=subject_id, table=table: by_id[subject_id].read(table),
            )
    progress.line(f"[memory] {len(TABLES)} tables probed")
    return memory


@dataclass(frozen=True, slots=True)
class _ReportInputs:
    args: argparse.Namespace
    source: Mapping[str, object]
    artifacts: Mapping[str, object]
    fixture: Mapping[str, object]
    reference: Subject
    same_frame_reference: Mapping[str, str]
    subjects: Sequence[Subject]
    fidelity: Mapping[str, object]
    operations: Mapping[str, object]
    memory: Mapping[str, object]
    order: Mapping[str, list[list[str]]]


def _build_report(inputs: _ReportInputs) -> dict[str, object]:
    args = inputs.args
    return {
        "schemaVersion": SCHEMA_VERSION,
        "suite": "python",
        "generatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "provenance": {
            "sourceState": inputs.source,
            "loadedArtifacts": inputs.artifacts,
            "buildProvenance": {
                "status": "not-proven",
                "statement": (
                    "source-state and loaded-artifact digests are recorded independently; "
                    "the harness does not claim an attested build link between them"
                ),
            },
        },
        "fixture": inputs.fixture,
        "runtime": {
            "python": platform.python_version(),
            "packages": {
                name: metadata.version(name)
                for name in ("narwhals", "pyarrow", "pandas", "polars", "numpy")
            },
        },
        "method": {
            "warmupsPerSubjectPerOperation": args.warmups,
            "iterationsPerSubjectPerOperation": args.iterations,
            "retainedResultsPerMemoryProbe": args.memory_results,
            "clock": "time.perf_counter_ns",
            "referenceSubject": inputs.reference.id,
            "sameFrameReference": inputs.same_frame_reference,
            "orderSeed": args.seed,
            "scheduling": (
                "one request in flight per subject; every round runs each subject once in a "
                "deterministic order reshuffled from `orderSeed`, so positions and "
                "predecessors both vary; a cyclic rotation would pin every subject behind the "
                "same neighbour in every round"
            ),
            "percentiles": "nearest-rank on raw durations",
            "rawSamplesRetained": args.samples,
            "auditData": (
                "raw samplesMs and per-round subject order are retained by default; "
                "--no-samples is an explicit local-only size optimization"
            ),
            "payloadBytes": (
                "server-side `count -8!table` for the fixture table: a logical payload size "
                "shared by all subjects, not observed wire bytes"
            ),
            "throughput": "payloadBytes divided by the median duration",
            "memory": (
                "process-wide RSS delta around retained decoded frames with forced GC at both "
                "snapshots; a noisy diagnostic, not a library footprint"
            ),
            "preflight": (
                "one subprocess per subject flushes JSON stage boundaries before operations "
                "and result events after completed checks, so an interpreter abort is pinned "
                "to the operation that killed it without ending the comparison"
            ),
            "scalarComparability": (
                "value-exact latency floor; a subject that cannot return the exact value is "
                "listed in `unsupported` instead of being ranked"
            ),
            "readComparability": (
                "every subject that can decode the table is ranked because the server sends "
                "identical bytes to all of them; timed validation proves the decoded row count, "
                "not table content fidelity. Exact int64 and nanosecond atom probes are recorded "
                "in `fidelity`; `roundTripStates` separately distinguishes identical, differs, "
                "resized, and unverified decode-then-encode outcomes without assigning an "
                "encoder failure to the decoder"
            ),
            "sendComparability": (
                "sends are compared only for subjects whose decoded frame re-encodes to a "
                "q-identical value of the same canonical size; others are listed in "
                "`unsupported` with no samples, throughput, or ratio"
            ),
            "untimedCorrectnessChecks": (
                "int64 and nanosecond-timestamp exactness are reported by the preflight, not "
                "timed: timing a wrong decode against a right one would compare different work"
            ),
            "excludedSubjects": {
                "pykx": (
                    "the licence covering PyKX's bundled q runtime forbids making performance "
                    "comparisons available to third parties"
                ),
                "qpython and qpython3": (
                    "both dereference numpy aliases removed in numpy 2, so they require "
                    "numpy<1.20 and Python<=3.9; qconnect is the maintained fork measured here"
                ),
                "pyq": "embeds Python inside q rather than acting as a client",
            },
        },
        "subjects": [subject.describe() for subject in inputs.subjects],
        "fidelity": inputs.fidelity,
        "operations": inputs.operations,
        "memory": inputs.memory,
        **({"order": inputs.order} if args.samples else {}),
    }


def _close_subject(subject: Subject) -> None:
    try:
        subject.close()
    except Exception as error:  # noqa: BLE001 - teardown must not mask a real failure
        print(f"warning: {subject.id} teardown failed: {error}")


def _run_benchmark(args: argparse.Namespace) -> dict[str, object]:
    source = source_provenance(metadata.version("xqdb"))
    artifacts = python_artifact_provenance()
    _require(
        artifacts["packageVersion"] == source["version"],
        "xqdb distribution version mismatch",
    )

    progress = Progress()
    fidelity = _load_preflight(args.host, args.port, progress)
    subjects = [SUBJECT_BUILDERS[subject_id](args.host, args.port) for subject_id in SUBJECT_IDS]
    by_id = {subject.id: subject for subject in subjects}
    subject_ids = [subject.id for subject in subjects]
    reference = subjects[0]
    _require(
        reference.version == source["version"],
        "loaded xqdb version changed during preflight",
    )
    same_frame_reference = _same_frame_references(subjects, reference)

    try:
        for subject in subjects:
            subject.connect()

        fixture = _load_fixture(args.host, reference)
        _verify_fixture(subjects, fidelity, fixture)
        recorder = _MeasurementRecorder(
            by_id=by_id,
            reference_id=reference.id,
            same_frame_reference=same_frame_reference,
            settings=_BenchmarkSettings(
                warmups=args.warmups,
                iterations=args.iterations,
                seed=args.seed,
                keep_samples=args.samples,
            ),
            progress=progress,
            total_operations=1 + 2 * len(TABLES),
        )
        _record_scalar(recorder, subject_ids, fidelity)
        for table in TABLES:
            _record_table_operations(
                recorder,
                table,
                fixture,
                fidelity,
                subject_ids,
            )
        memory = _measure_memory(
            subject_ids,
            fidelity,
            by_id,
            args.memory_results,
            progress,
        )

        finished_source = source_provenance(reference.version)
        finished_artifacts = python_artifact_provenance()
        _require(
            finished_source == source,
            "relevant source or repository state changed during the run",
        )
        _require(
            finished_artifacts == artifacts,
            "loaded XQDB artifacts changed during the run",
        )
        report = _build_report(
            _ReportInputs(
                args=args,
                source=source,
                artifacts=artifacts,
                fixture=fixture,
                reference=reference,
                same_frame_reference=same_frame_reference,
                subjects=subjects,
                fidelity=fidelity,
                operations=recorder.operations,
                memory=memory,
                order=recorder.order,
            )
        )
    finally:
        for subject in subjects:
            _close_subject(subject)
    return report


def main() -> None:
    """Run an isolated probe or the complete Python benchmark CLI."""
    args = parse_args()
    if args.probe is not None:
        probe_subject(args.probe, args.host, args.port)
        return

    report = _run_benchmark(args)
    output = args.output or (
        Path(__file__).resolve().parent.parent
        / "results"
        / (
            f"python-{'.'.join(platform.python_version_tuple()[:2])}"
            f"-{report['fixture']['rows']}rows.json"
        )
    )
    text = json.dumps(report, indent=2) + "\n"
    assert_no_machine_identifiers(text)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(text, encoding="utf-8", newline="\n")
    print(render_summary(report))
    print(f"wrote {output}")


if __name__ == "__main__":
    main()
