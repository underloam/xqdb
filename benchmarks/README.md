# XQDB client benchmarks

Two harnesses compare XQDB against every q IPC client we can legally and
technically measure, on one fixed fixture server:

| Suite   | Entry point                  | Subjects                                                       |
| ------- | ---------------------------- | -------------------------------------------------------------- |
| Node.js | `benchmarks/node/bench.mjs`  | `@xbbg/xqdb`, `jkdb@1.4.0`, `node-q@2.7.0`                     |
| Python  | `benchmarks/python/bench.py` | `xqdb` (pyarrow/polars/pandas), `kola@2.5.1`, `qconnect@0.1.6` |

Every number is a client-side measurement against a fixed server. Nothing here
is a claim about kdb+ or KDB-X performance.

## Running

Both suites need the fixture container from `testing/kdb`, which requires
`KX_BEARER_TOKEN` and `KDB_LICENSE_B64` (see `testing/kdb/.env`).

```bash
task benchmark-node-podman     # builds js-xqdb, starts the fixture, runs, stops
task benchmark-python-podman
```

The Podman tasks install and build client/benchmark dependencies before fixture
startup and run client commands with the credential variables unset. That is
defense in depth against accidental inheritance, not a sandbox for untrusted
code or dependencies: all commands share one user, benchmark code can control
Podman and send arbitrary q expressions, and q must be able to read its mounted
license while it runs.

Against an already-running fixture:

```bash
cd benchmarks/node && npm install && node --expose-gc bench.mjs
python benchmarks/python/bench.py
```

Flags (both suites): `--host`, `--port`, `--warmups`, `--iterations`,
`--memory-results`, `--seed`, `--output`, `--samples`, `--no-samples`.
Environment equivalents: `XQDB_TEST_Q_HOST`, `XQDB_TEST_Q_PORT`,
`XQDB_BENCH_WARMUPS`, `XQDB_BENCH_ITERATIONS`,
`XQDB_BENCH_MEMORY_RESULTS`, `XQDB_BENCH_SEED`.

Each run prints a summary table and writes a JSON report to
`benchmarks/results/`. Reports retain every raw duration in `samplesMs` and the
per-round subject `order` by default. `--no-samples` is an explicit size
optimization for local exploration; do not publish or check in a report made
with it.

Run from a Git checkout: provenance capture is mandatory, and the harness
fails instead of emitting a report with an unknown revision or source state.

## Fixture

`testing/kdb` builds KDB-X 5.0.20260723 into a container and loads
`testing/kdb/init.q`, which seeds `\S 42` and publishes `.xqdb.rows`,
`.xqdb.seed`, and three tables:

| Table   | Columns | Shape stress                                            |
| ------- | ------- | ------------------------------------------------------- |
| `trade` | 14      | narrow: symbol, timestamp, long, char vector, 10 floats |
| `wide`  | 64      | column count                                            |
| `depth` | 5       | nested: `ask` and `bid` are a 5-float list per row      |

Row count comes from `XQDB_Q_ROWS` (benchmarks default to `100000`). Before
timing, every subject is asked for `.xqdb.rows`, `.z.K`, and `.xqdb.seed`, and
the run aborts unless all subjects agree.

## Operations

| Operation      | Work                                                                  |
| -------------- | --------------------------------------------------------------------- |
| `scalar`       | `6f*7f` round trip: the latency floor                                 |
| `read.<table>` | query and materialise the client's own frame type; validate row count |
| `send.<table>` | send the client's own decoded frame to `{[x]count x}`                 |

`payloadBytes` is the server's `count -8!<table>`: one logical payload size
shared by all subjects, not observed wire bytes. Throughput is that size divided
by the median duration.

## Method

- **Order.** Each round runs every subject once, in an order reshuffled from
  `--seed`. A cyclic rotation is not sufficient: rotating by one preserves the
  adjacency relation, so every subject keeps the same predecessor in every round.
  That was measurable — `xqdb-pyarrow` sat behind `qconnect`'s one-second
  `depth` decode in every sample and reported 26.7 ms for a 2.8 ms operation.
- **Timing.** `process.hrtime.bigint` / `time.perf_counter_ns` around one
  request, one in flight per subject. Percentiles are nearest-rank on raw
  durations.
- **Validation.** Every measured result is checked after its timed interval.
- **Memory.** RSS/heap delta around a set of retained decoded frames with a
  forced GC at both snapshots. This is a noisy process-wide diagnostic affected
  by the runtime, allocator, garbage collector, and prior work—not a rigorous
  library footprint estimate.
- **Python preflight isolation.** The Python preflight runs one subprocess per
  subject and flushes a JSON stage boundary before each operation plus a result
  event after each completed check. A subject can abort the interpreter instead
  of raising (see kola below); the last flushed stage pins the failure without
  taking down the whole comparison.

### Comparability gates

Speed is only reported where the subjects are doing the same work:

- `scalar` is value-exact. A subject that cannot return the exact value is
  listed in `unsupported`, not ranked.
- `read.<table>` is ranked for every subject that can decode the table, because
  the server sends all of them identical bytes. Its timed validation proves the
  decoded row count, not table content fidelity. Exact int64 and nanosecond atom
  probes appear in `fidelity`; `roundTripStates` separately labels the
  decode-then-encode result as `identical`, `differs`, `resized`, or
  `unverified`. An encoder failure is `unverified`, never attributed to the
  decoder.
- `send.<table>` is ranked only where the subject's decoded frame re-encodes to
  a value that q reports as `~`-identical to the fixture **and** of the same
  `count -8!x` size. Otherwise the subject would be timed encoding a different
  value, so it is listed in `unsupported` with no samples, throughput, or ratio.
- int64 and nanosecond-timestamp exactness are preflight facts, never timed.
  Timing a wrong decode against a right one compares different work.

Python reports two ratios because the subjects return different frame types:
`medianRatioVsReference` against `xqdb-pyarrow`, and
`medianRatioVsSameFrameXqdb` against the XQDB backend that materialises the same
frame type as the subject (Polars for kola, pandas for qconnect).

## Fidelity findings

These come out of the preflight, and they are the reason the gates exist.

- **`node-q@2.7.0`** decodes int64 to double (`9007199254740993` reads back as
  `…992`) and timestamps to millisecond `Date`. No decoded table re-encodes to a
  q-identical value, so it is excluded from every `send`. It is measured with
  `flipTables:false`, its fastest documented mode and the one whose shape is
  closest to the other subjects; `long2number:false` plus `nanos2date:false`
  restore exactness but roughly double decode time.
- **`jkdb@1.4.0`** needs `includeNanosecond:true` for sub-millisecond
  timestamps, which turns temporal values into text that its own encoder then
  rejects. The suite therefore keeps a second connection for the temporal
  fidelity check and measures timed operations on the default
  `includeNanosecond:false` connection. JSON and console output label the exact
  timestamp result as a separate-mode probe, not fidelity of the timed mode.
- **`kola@2.5.1`** panics in its Rust serializer and aborts the process when
  asked to send a frame with list columns:
  `range end index 800040 out of range for slice of length 800000` at
  `crates/kola/src/serde6.rs:1852`, at the 100,000-row fixture. It is excluded
  from `send.depth`, and because no comparison ever ran its `depth` round trip
  is reported as `unverified` rather than `differs`; the `depth` read itself is
  unaffected. kola also rounds nanosecond timestamps to microseconds silently.
- **`xqdb`** raises `ValueError` rather than truncate a sub-microsecond
  timestamp atom into a Python `datetime`, so its `nanosecondTimestamp` is
  `rejected` rather than `lossy`.

## Provenance of saved reports

Every new report records three distinct provenance layers:

- `provenance.sourceState` captures the full Git revision, installed package
  version, `git describe`, repository dirty state, and a deterministic SHA-256
  digest over the relevant tracked and untracked core, binding, fixture,
  manifest/lock, Taskfile, and harness sources.
- `provenance.loadedArtifacts` hashes what the benchmark actually executes.
  Python records every installed XQDB Python source file by path and hash, and
  every native file plus the loaded extension binary by hash alone. Node
  records the built `dist` tree, generated native loader and package manifest,
  plus the exact loaded `.node` binary by hash alone.
- `provenance.buildProvenance.status` is `not-proven`: the source-state and
  loaded-artifact digests identify each side independently, but this local
  harness has no attestation proving that one was built from the other.

The harness captures source and artifact state before preflight, recomputes
both after all probes and measurements, and aborts if either changed during the
run. A dirty report is performance evidence for its loaded-artifact hashes,
with the source tree recorded as context; it must not be described as
performance of the nearest release tag or as a proven build of the recorded
source digest. Raw samples and per-round order are required for checked-in
reports so summary statistics and scheduling can be audited.

## What reports leave out

Checked-in reports are public, so they describe the software, not the machine.
A report never contains the CPU model, operating-system name or build, machine
architecture, host name, user name, the fixture host or port, or any absolute
filesystem path. The `runtime` block holds only interpreter and package
versions; `fixture.connection` says `loopback` or `network` instead of naming
an endpoint; native artifacts are identified by digest because their file
names carry platform tags; and output file names carry only the suite,
interpreter version, and row count.

Both harnesses enforce this. Text captured from subjects (exception messages
and the stderr tail of a crashed preflight probe) has absolute paths replaced
with `<path>`, and before a report is written its JSON is scanned for this
machine's own identifiers (host name, user name, home directory, CPU model,
architecture, OS family and release) and for absolute paths; a hit aborts the
run instead of writing the report.

## Subjects we do not measure

| Candidate             | Why not                                                                                                                                                                           |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pykx`                | The licence covering its bundled q runtime forbids making performance comparisons available to third parties.                                                                     |
| `qpython`, `qpython3` | Both dereference numpy aliases removed in numpy 2 (`np.string_`, `np.bool`), so they need numpy<1.20 and Python<=3.9. `qconnect` is the maintained fork, measured in their place. |
| `pyq`                 | Embeds Python inside q rather than acting as a client.                                                                                                                            |
| npm `kx`, `qnode`     | Unrelated packages that happen to own the names.                                                                                                                                  |
