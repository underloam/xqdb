# XQDB

A high-performance connector to kdb+/q with Python (Narwhals/Arrow) and Node.js (TypeScript) bindings.

XQDB is independent and not affiliated with or endorsed by KX. kdb+ is a trademark of KX.

## Overview

**XQDB** provides high-performance connectivity between Python and Node.js applications and kdb+/q processes. The core is written in Rust, with Python bindings built on PyO3, Narwhals, and the Arrow C Stream interface, plus Node.js bindings built on napi-rs.

### Features

- Little-endian kdb+ IPC v6 requests, responses, and separately queued async notifications
- Backend-independent eager DataFrame and Series exchange through Arrow
- Immutable `XqdbQValue` values for lossless typed atoms, nulls, infinities, dictionaries, and supported q function forms
- TLS hostname verification, additional trusted CAs, and client certificates
- Connection-only retries with exponential backoff; queries are never automatically replayed after an ambiguous I/O failure
- Out-of-band socket cancellation, connection lifecycle helpers, and bounded query-result batches
- Configurable compression, phase timeouts, inbound frame limits, notification capacity, and Node queue/snapshot limits
- Direct q binary table reads and validated in-memory IPC/value serialization and decoding

### Protocol and storage boundaries

| Surface             | Supported                                                                                                                                                                                                          | Boundary                                                                                                                                           |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| IPC                 | Little-endian v6 framing, standard q IPC compression, sync responses, and async notifications                                                                                                                      | Big-endian frames and unsupported message/type/opcode forms are rejected; this is not a claim of every q protocol feature                          |
| Native values       | Host scalars, eager Arrow-backed tables/series, and supported containers                                                                                                                                           | Native conversion can reject values it cannot represent faithfully; timestamp infinities and duplicate dictionary keys are not silently normalized |
| Lossless values     | Validated q value-body bytes, including standard typed atoms/vectors, mixed lists, arbitrary/duplicate dictionary keys, keyed/sorted tables, lambdas, primitives, projections, compositions, and derived functions | Preserves supported wire values rather than coercing them through a host dictionary or datetime; validation still rejects unsupported forms        |
| In-memory helpers   | Complete IPC messages or a single value body, with exact consumption and compressed-input validation                                                                                                               | Trailing data, malformed backreferences, and inconsistent lengths fail rather than being ignored                                                   |
| On-disk tables      | Regular q binary table files, uncompressed or Kxzip LZ4 algorithm 4; power-of-two logical blocks from 4 KiB through 4 MiB                                                                                          | No HDB discovery, splayed/partitioned database assembly, enum resolution, or other Kxzip algorithms                                                |
| Bounded consumption | Python `iter_batches()` and Node `batches()` request explicit `(offset; count)` pages                                                                                                                              | Each page is a separate query, not a server snapshot or a streaming decoder for one oversized reply                                                |
| Cancellation        | Active attached-socket I/O and retry backoff can be interrupted; idle cancellation preserves the connection                                                                                                        | An in-progress system DNS/TCP call must return first. The connect timeout bounds TCP establishment, not system DNS resolution                      |

Retries mean additional connection attempts: `0` tries once, `N` tries at most
`N + 1` times. Both bindings wait 1, 2, 4, 8, 16, then 32 seconds before retries,
capped at 32 seconds. A query that might already have executed is never retried
automatically.

Node admission bounds pending command count and native queued input snapshots.
These are not total-process heap limits: active results, JavaScript/Arrow
encoding temporaries, and third-party allocators retain their own memory
behavior. Checked native buffer growth is not a process-wide no-abort guarantee.

### Unreleased changes

- Node preserves typed empty Arrow vectors, including q symbol vectors, and
  retains dictionary values across independently encoded chunks and slices.
- Both bindings use a 30-second default socket timeout, accept fractions, and
  let explicit zero disable a timeout. Units remain Python seconds and Node
  milliseconds. **Python callers relying on the previous unlimited default must
  pass `timeout=0`.** Granular omissions inherit the default; all values are
  bounded to 24 hours.
- Windows cancellation wakes pending socket I/O even when the peer remains open
  and the read timeout is disabled.
- Nested numeric decoding reuses its owned payload as Arrow storage, avoiding a
  second full-size typed allocation while preserving values and nulls.
- Repository-wide Ruff, type-aware Oxlint, and Oxfmt checks cover bindings, tests,
  release scripts, and benchmarks on every push and pull request.
- Tables of at least 256 KiB are decoded column by column while the rest of
  the response is still arriving, so decode time overlaps transfer time
  instead of following it.
- Symbol columns decode straight into a categorical column through a
  per-column interner instead of a string column cast row by row through the
  shared category map; the send path resolves each distinct category once.
- Nested char columns are decoded in one pass into a string-view column.
- Frame buffers are retained between requests (released above 256 MiB), both
  bindings allocate through mimalloc, and on Windows the socket receive buffer
  is 4 MiB; together these keep repeated table reads off the page allocator and
  the 64 KiB default receive window.
- Benchmark reports and harnesses no longer record the CPU model, operating
  system, architecture, host name, user name, fixture endpoint, or any absolute
  path, and a run aborts rather than write a report containing this machine's
  identifiers.

## Project Structure

| Directory            | Description                              |
| -------------------- | ---------------------------------------- |
| `crates/xqdb`        | Core Rust library (connector, IPC serde) |
| `py-xqdb`            | Python bindings (PyO3 + Narwhals)        |
| `js-xqdb`            | Node.js and TypeScript package           |
| `bindings/napi-xqdb` | Shared napi-rs native binding layer      |

## Installation

### Python

**Requirements**: Python ≥ 3.10 and < 3.15, Narwhals ≥ 2.10, PyArrow ≥ 20.0.0; pandas and Polars are optional backend packages

Install the published package:

```bash
python -m pip install xqdb
```

To build the Python package from source with setuptools-rust:

```bash
python -m pip install -e .
```

### Node.js

**Requirements**: Node.js ≥ 20

Install the published package:

```bash
npm install @xbbg/xqdb
```

To build the Node.js package from source for development, use Node.js ≥ 24.12:

```bash
cd js-xqdb
npm install
npm run build
```

## Development checks

The published Node.js package supports Node.js ≥ 20; development tooling requires
Node.js ≥ 24.12. Linting and formatting do not require q, a kdb+ license, or a
native Rust build.

From a new checkout, install the pinned tools and benchmark client types:

```bash
uv venv --python 3.12
uv pip install --python .venv --group lint
npm ci --prefix js-xqdb --no-audit --no-fund
npm ci --prefix benchmarks/node --ignore-scripts --no-audit --no-fund
```

Skip `uv venv` if `.venv` already exists. Benchmark dependencies are required for
type-aware checks even when no benchmark is run; install scripts are unnecessary
for this type-only setup.

Run the same checks locally:

```bash
uv run --no-project --python .venv ruff check .
uv run --no-project --python .venv ruff format --check .
npm --prefix js-xqdb run check
```

`check` builds the TypeScript facade, runs type-aware Oxlint across all maintained
JavaScript/TypeScript, checks repository formatting with Oxfmt, and type-checks
the source, tests, release scripts, and Node benchmark. Ruff enables all rules
with documented compatibility exceptions. Oxlint treats enabled diagnostics as
errors and rejects unused suppression directives. Generated artifacts are
excluded; maintained source is not blanket-excluded.

To apply fixes, run Ruff with `check --fix` followed by `format`, and use
`npm --prefix js-xqdb run lint:fix` followed by
`npm --prefix js-xqdb run format`.

After the setup above, `task lint` runs both language gates. Pixi users can run
`pixi run -e js js-install` to install both Node dependency sets, then
`pixi run -e js js-check`; `pixi run check-python` uses the existing `.venv`.
Checks do not reinstall dependencies.

## Quick Start

### Python

```python
import narwhals as nw
import xqdb

# Query with PyArrow backend (default)
conn = xqdb.Q("localhost", 1800, backend="pyarrow")

# Query
result = conn.sync("select from trade where date=last date")

# Extract native DataFrame: PyArrow, pandas, or Polars
df = nw.to_native(result)

# Send data (Narwhals or native eager DataFrame)
conn.sync("upsert", "table", df)

conn.disconnect()
```

### Node.js

```ts
import { Q } from "@xbbg/xqdb";

const conn = await Q.connect({
  host: "localhost",
  port: 1800,
});

try {
  const result = await conn.sync("select from trade where date=last date");
  await conn.asyn("upsert", "table", ["AAPL", 10n]);
  console.log(result);
} finally {
  await conn.disconnect();
}
```

## Benchmarks

Every q IPC client that can be legally and technically measured, against one
fixed KDB-X 5.0 fixture: 100,000 rows, seed 42, 50 measured rounds per subject
per operation, subject order reshuffled every round. `trade` is 14 columns,
`wide` is 64 columns, `depth` has two nested 5-float list columns. Throughput
divides the server's `count -8!table` by the median duration.

These are client-side measurements against a fixed server, not a claim about
kdb+ or KDB-X performance. Full methodology, fidelity matrix, and raw reports:
[`benchmarks/README.md`](benchmarks/README.md).

### Node.js — Node 26.3.0

★ marks the fastest measured client for that operation.

| Operation         | XQDB                    | jkdb 1.4.0        | node-q 2.7.0      |
| ----------------- | ----------------------- | ----------------- | ----------------- |
| `read trade`      | ★ **14.2 ms** 741 MiB/s | 158.6 ms (11.2x)  | 163.3 ms (11.5x)  |
| `read wide`       | ★ **54.2 ms** 897 MiB/s | 2645.4 ms (48.8x) | 2694.5 ms (49.7x) |
| `read depth`      | ★ **15.2 ms** 708 MiB/s | 176.6 ms (11.6x)  | 169.8 ms (11.2x)  |
| `send trade`      | ★ **18.8 ms** 558 MiB/s | 42.0 ms (2.2x)    | not comparable    |
| `send wide`       | ★ **65.5 ms** 743 MiB/s | 116.9 ms (1.8x)   | not comparable    |
| `send depth`      | ★ **19.4 ms** 556 MiB/s | 48.8 ms (2.5x)    | not comparable    |
| scalar round trip | 0.313 ms                | ★ 0.306 ms        | 0.319 ms          |

XQDB is fastest on every table operation. jkdb takes the scalar round trip by
7 microseconds, which is the latency floor of a single request rather than a
codec difference.

### Python — CPython 3.12.13

Ratios are against XQDB's PyArrow backend. kola returns Polars and qconnect
returns pandas, so the report also carries a ratio against the XQDB backend
that materialises the same frame type: against XQDB's Polars backend, kola is
1.6x slower on `read trade`, 2.9x on `read wide`, 1.4x on `read depth`, 1.3x
on `send trade` and 1.2x on `send wide`. XQDB's PyArrow backend is fastest on
every table operation.

| Operation         | XQDB pyarrow             | XQDB polars | XQDB pandas | kola 2.5.1        | qconnect 0.1.6   |
| ----------------- | ------------------------ | ----------- | ----------- | ----------------- | ---------------- |
| `read trade`      | ★ **8.6 ms** 1217 MiB/s  | 9.4 ms      | 11.1 ms     | 14.9 ms (1.7x)    | 89.4 ms (10.4x)  |
| `read wide`       | ★ **29.6 ms** 1641 MiB/s | 30.4 ms     | 34.1 ms     | 86.9 ms (2.9x)    | 178.0 ms (6.0x)  |
| `read depth`      | ★ **10.0 ms** 1080 MiB/s | 10.8 ms     | 26.5 ms     | 15.0 ms (1.5x)    | 9549.0 ms (957x) |
| `send trade`      | ★ **15.4 ms** 683 MiB/s  | 15.8 ms     | 18.2 ms     | 20.8 ms (1.4x)    | 59.0 ms (3.8x)   |
| `send wide`       | ★ **52.3 ms** 929 MiB/s  | 52.9 ms     | 59.2 ms     | 63.0 ms (1.2x)    | 139.2 ms (2.7x)  |
| `send depth`      | ★ **16.1 ms** 670 MiB/s  | 16.3 ms     | 43.8 ms     | aborts, see below | 1934.6 ms (120x) |
| scalar round trip | 0.294 ms                 | 0.291 ms    | 0.290 ms    | ★ 0.286 ms        | 0.312 ms         |

kola takes the scalar round trip by 8 microseconds, again the single-request
latency floor: XQDB's three backends share one code path for a scalar and land
within 4 microseconds of each other.

### Correctness, measured alongside speed

Speed is only ranked where subjects do the same work. Each subject's decoded
value is sent back to q and compared with `~` before anything is timed.

- XQDB and jkdb round-trip all three tables to a q-identical value. XQDB raises
  rather than truncate a sub-microsecond timestamp atom into a Python
  `datetime`; kola rounds it to microseconds silently.
- `node-q` decodes int64 to double (`9007199254740993` reads back as `…992`) and
  timestamps to millisecond `Date`, and no decoded table re-encodes to a
  q-identical value, so it is excluded from every `send` rather than credited
  with encoding a different value.
- `kola@2.5.1` panics in its Rust serializer and aborts the process when sending
  a frame with list columns (`crates/kola/src/serde6.rs:1852`), so it is
  excluded from `send depth`; its `depth` read is unaffected.
- Not measured: `pykx` (its licence forbids publishing performance
  comparisons), `qpython`/`qpython3` (require numpy<1.20 and Python<=3.9;
  `qconnect` is the maintained fork measured in their place), and `pyq` (embeds
  Python inside q rather than acting as a client).

## Documentation

- [Python API Reference](py-xqdb/README.md) — Comprehensive API documentation and type mapping for Python/Narwhals bindings
- [Node.js API Reference](js-xqdb/README.md) — Comprehensive API documentation and value mapping for Node.js/TypeScript bindings

## License

XQDB is licensed under the [BSD-3-Clause](LICENSE) permissive open-source license, which permits use in proprietary and commercial applications.
