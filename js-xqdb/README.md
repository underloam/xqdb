# XQDB — Node.js Bindings

XQDB is independent and not affiliated with or endorsed by KX. kdb+ is a trademark of KX.

`@xbbg/xqdb` is the ESM Node.js binding for XQDB's q IPC client. It exposes a strict TypeScript facade, keeps q tables and typed vectors columnar with Apache Arrow, and runs connection work on a dedicated native worker instead of the JavaScript event loop.

## Installation

**Requirements**: Node.js ≥ 20

Install the published package:

```bash
npm install @xbbg/xqdb
```

The install selects the matching optional native package for Windows x64, Linux x64 with glibc 2.28 or newer, or macOS arm64.

To build from source for development, use Node.js ≥ 24.12, install a Rust toolchain, and run:

```bash
# From the js-xqdb directory
npm install
npm run build
```

`npm run build:native` calls the napi-rs v3 CLI with `bindings/napi-xqdb/Cargo.toml` and generates the internal CommonJS `native.cjs` loader, `native.d.ts`, and local `.node` artifact in this package. The ESM public facade loads that CommonJS shim synchronously through `createRequire`, so the first call can reserve its ordered native FIFO ticket before inspecting user values. The public entry point does not export the generated native declarations.

Repository linting, formatting, and type-checking also require the benchmark client
types. Follow the root [development checks](../README.md#development-checks) setup,
then run `npm run check` from this directory. The published package's Node.js ≥ 20
runtime requirement is unchanged.

## Connect and query

```ts
import { Q } from "@xbbg/xqdb";

const q = await Q.connect({
  host: "localhost",
  port: 1800,
  user: "user",
  password: "password",
  tls: true,
  timeout: 30_000,
  retries: 2,
});

try {
  const result = await q.sync("select from trade");
  await q.asyn("insert", "trade", ["AAPL", 10n]);
  console.log(result);
} finally {
  await q.disconnect();
}
```

q IPC authentication sends credentials in cleartext when TLS is disabled. Enable `tls` for credentialed connections unless another trusted transport already protects the socket.

`connect()`, `disconnect()`, `sync()`, `asyn()`, and `receive()` all return promises. `sync()` is a synchronous q IPC request/response transaction, not a synchronous JavaScript function. Calls on one `Q` reserve ordered placeholders in its bounded native FIFO before returning, including calls made reentrantly by argument getters, so complete IPC transactions cannot interleave. `queueCapacity` controls the pending-command limit, defaults to 8, and must be from 1 through 1024. `sync()` and `asyn()` reserve capacity before inspecting or copying arguments, including before JavaScript Arrow IPC serialization. Admission never waits on the JavaScript thread: a full queue, an argument snapshot above `maxArgumentBytes` (64 MiB by default), or aggregate native queued snapshots (including expression bytes) above `maxQueuedBytes` (512 MiB by default) fail with `XQDB_BACKPRESSURE`. `maxQueuedBytes` limits native-owned queued expression and value snapshots; JavaScript encoding temporaries, completed results, and third-party allocator overhead are outside that quota, so it is not a total-heap ceiling. Native permits are connector-specific and single-use, and all reservations are released on conversion failure, native failure, cancellation, disconnect, or object cleanup.

The default socket timeout is 30,000 milliseconds. Every timeout must be finite and non-negative and cannot exceed 24 hours (86,400,000 milliseconds). `timeout` is the fallback for connect, read, and write operations; fractional milliseconds are preserved rather than rounded up to whole seconds. Set `connectTimeout`, `readTimeout`, or `writeTimeout` to override one phase; omission inherits `timeout`, and an explicit `0` disables that timeout. Positive values below one nanosecond are clamped to one nanosecond, with actual socket timing subject to operating-system resolution. Python exposes the same policy in seconds, with a default of 30. These are TCP connection and socket I/O limits, not whole-query deadlines: queueing and system DNS resolution are not bounded by them.

`retries` is the number of additional attempts made by an explicit `connect()` after an IO failure. An actual retry waits 1, 2, 4, 8, 16, then 32 seconds; further retry waits remain capped at 32 seconds. All attempts occupy one FIFO transaction; authentication failures and cancellation are not retried. Query methods establish the native connection automatically when needed. `compression` is `"auto"` by default, `"on"` to request compression for payloads at least `compressionThreshold` bytes, or `"off"` to disable it. `maxMessageBytes` sets an optional ceiling for the total uncompressed inbound IPC frame, including its header. `maxPendingNotifications` bounds buffered unsolicited async messages; overflow fails and closes the connection rather than dropping a notification.

q stores symbols and strings as raw bytes and never validates them, while Arrow string columns must be valid UTF-8. With the default `symbolEncoding: "strict"`, a result carrying a stray Latin-1 or binary byte in a symbol, symbol column, string column, char column, or lambda fails with `XQDB_CONVERSION` naming the offending value. Opt in with `symbolEncoding: "lossy"` to decode such text with each maximal invalid sequence replaced by `"\uFFFD"`, so every other value in the result survives intact. Valid text decodes identically under both policies, and q error messages always surface with replacement characters rather than being hidden behind a decoding failure.

Native-owned adapter buffers, argument snapshots, frame reads, decompression targets, and owned conversion buffers use checked sizes and fallible growth so allocation failure can become a JavaScript error. This is deliberately narrower than a process-wide no-abort guarantee: V8, Apache Arrow, Polars, and other third-party internals retain their own allocation behavior. A declared frame length alone reserves at most 32 MiB; larger reads grow only as bytes arrive. A compressed response whose declared output is unreachable from the bytes received is rejected before allocating that output, because q IPC decompression expands its input by at most 121×.

`disconnect()` is idempotent. A `Q` can reconnect after disconnect:

```ts
await q.disconnect();
await q.connect();
const value = await q.sync("42");
```

Always call `disconnect()` in `finally` or an equivalent cleanup hook instead of relying on garbage collection.

Cancellation leaves queued commands intact and is a no-op while the connection is idle.

`cancel()` is an out-of-band interruption rather than a command queued behind the connection's FIFO. It promptly interrupts active connected-socket IO and retry waits. If an operating-system DNS resolution or TCP establishment call is already in progress before a connected socket is available to the abort handle, cancellation is observed after that call returns. `connectTimeout` bounds only TCP establishment; it does not bound system DNS resolution. The cancellation is still remembered, no connection retry follows it, and the active operation rejects with `XQDB_IO`. Reconnect and restore any server-side subscription state before reusing the `Q`.

### Operators and lambdas

Pass q primitives and arbitrary lambdas as first-class arguments:

```ts
import { XqdbQLambda, XqdbQOperator } from "@xbbg/xqdb";

await q.sync("{[op;a;b] .[op;(a;b)]}", XqdbQOperator.PLUS, 1, 2);
await q.sync("{[op;a;b] .[op;(a;b)]}", new XqdbQLambda("{x+y}"), 1, 2);

const scoped = new XqdbQLambda("{x+y}", "analytics");
```

`new XqdbQOperator(name)` accepts supported q primitive names such as `"+"`; it does not expose wire opcodes. `new XqdbQLambda(source, context = "")` preserves its source text, requires a brace-delimited UTF-8 body (optionally prefixed with `k)`), rejects NUL bytes in both fields, and rejects context values beginning with `"."`. The context `"analytics"` represents q namespace `.analytics` because the wire context omits the leading dot. Lambda source is executable q code: construct it only from trusted input.

## Value mapping

### JavaScript to q

| JavaScript input          | q value                                                               |
| ------------------------- | --------------------------------------------------------------------- |
| `null`                    | generic null                                                          |
| `boolean`                 | boolean                                                               |
| `number`                  | float                                                                 |
| `bigint`                  | long                                                                  |
| `string`                  | symbol; embedded NUL is rejected                                      |
| `Buffer` or `Uint8Array`  | char vector, preserving arbitrary bytes                               |
| ordinary array            | mixed list                                                            |
| plain string-keyed object | dictionary; keys cannot contain NUL; `{}` becomes ``(`symbol$())!()`` |
| Apache Arrow `Vector`     | typed series/list through Arrow IPC                                   |
| Apache Arrow `Table`      | table through Arrow IPC                                               |
| `XqdbTimestamp`           | timestamp as Unix-epoch nanoseconds                                   |
| `XqdbDate`                | date in `YYYY-MM-DD` form                                             |
| `XqdbTime`                | millisecond-aligned nanoseconds since midnight                        |
| `XqdbTimespan`            | signed nanosecond duration                                            |
| `XqdbQOperator`           | supported primitive operator                                          |
| `XqdbQLambda`             | lambda source and q context                                           |
| `XqdbQValue`              | its already-validated exact q value body                              |

Arrow `Utf8` vectors map to q lists of char vectors (strings), while
`Dictionary<Utf8, Int32>` vectors map to q symbol vectors. This distinction also
holds for typed empty vectors; an ordinary `[]` remains a q mixed list.

```ts
import { Dictionary, Int32, Utf8, vectorFromArray } from "apache-arrow";

const emptySymbols = vectorFromArray([], new Dictionary(new Utf8(), new Int32()));
await q.sync("{x}", emptySymbols); // sends `symbol$(), without a placeholder value
```

### q to JavaScript

| q value                                              | JavaScript output                                                           |
| ---------------------------------------------------- | --------------------------------------------------------------------------- |
| boolean and safe-width numeric atoms                 | `boolean` or `number`                                                       |
| long                                                 | `bigint`                                                                    |
| symbol, string, or GUID                              | `string`                                                                    |
| char atom                                            | byte value as `number`                                                      |
| char vector                                          | `Buffer`                                                                    |
| timestamp                                            | `XqdbTimestamp` with a `bigint` nanosecond payload                          |
| date                                                 | `XqdbDate`                                                                  |
| time                                                 | `XqdbTime` with a `bigint` nanosecond payload                               |
| timespan                                             | `XqdbTimespan` with a `bigint` nanosecond payload                           |
| primitive operator                                   | `XqdbQOperator`                                                             |
| lambda                                               | `XqdbQLambda`                                                               |
| typed list                                           | Apache Arrow `Vector`                                                       |
| mixed list                                           | array                                                                       |
| dictionary                                           | plain string-keyed object; an empty dictionary such as `()!()` becomes `{}` |
| table                                                | Apache Arrow `Table`                                                        |
| any successfully decoded value with `lossless: true` | immutable `XqdbQValue`                                                      |

### Lossless values

Convenient mode intentionally maps q values into ergonomic JavaScript and Arrow objects, so it cannot preserve distinctions such as duplicate dictionary keys, typed null and infinity sentinels, every temporal kind, or non-table keyed dictionaries. Use `lossless: true` when exact structure matters:

```ts
import { Q, XqdbQValue } from "@xbbg/xqdb";

const exactQ = await Q.connect({ host: "localhost", port: 1800, lossless: true });
const exact = await exactQ.sync("`a`a!1 2");
console.log(exact.typeCode, exact.length, exact.isTable);
const body = exact.toBytes(); // defensive copy; no eight-byte IPC frame header

const nullTimestamp = await XqdbQValue.timestamp(-(1n << 63n));
await exactQ.sync("{x~0Np}", nullTimestamp);
const validated = await XqdbQValue.fromBytes(body);
await exactQ.disconnect();
```

Every `XqdbQValue` is created through asynchronous native validation and can be supplied directly to `sync()`, `asyn()`, `serializeAsIpcBytes6()`, `XqdbQValue.list()`, or `XqdbQValue.dictionary()` without a convenient-mode round trip. `XqdbQValue.from()` converts a supported convenient input to exact bytes. The typed atom factories cover boolean, GUID, byte, short, int, long, real, float, char, symbol, timestamp, month, date, datetime, timespan, minute, second, and time; temporal factory arguments are raw q epoch units and preserve sentinel bit patterns.

The temporal wrappers prevent nanosecond values from being rounded through JavaScript `number` or `Date`:

```ts
import { XqdbTime, XqdbTimespan, XqdbTimestamp } from "@xbbg/xqdb";

const timestamp = new XqdbTimestamp(1_725_000_000_000_000_001n);
const noon = new XqdbTime(43_200_000_000_000n);
const oneNanosecondAgo = new XqdbTimespan(-1n);
```

Tables and typed lists cross the native boundary as Arrow IPC streams and are materialized as Arrow `Table` and `Vector` objects. They are not expanded into row objects. The transfer is columnar, but this package does not claim zero-copy transfer across the N-API boundary.

Top-level `Buffer` and `Uint8Array` values remain lossless for arbitrary bytes. q char data inside Arrow table or nested columns must be valid UTF-8; invalid bytes return a conversion error instead of being replaced or panicking. Because a q char atom is one byte while each Arrow string cell must be valid UTF-8, direct char-atom columns are limited to ASCII. Use a top-level byte value when arbitrary-byte round trips are required.

## Bounded table batches

`batches()` is an async generator over a caller-supplied q paging function. XQDB sends `offset` and `requestedRows` as the first two long arguments, followed by at most six caller arguments. The function must return exactly one table with no more than the requested number of rows:

```ts
for await (const batch of q.batches("{[offset;n] (offset;n) sublist trade}", 65_536)) {
  consume(batch);
}
```

Iteration stops on an empty or short table. In convenient mode each item is an Arrow `Table`; in lossless mode it is an `XqdbQValue` whose `isTable` is true. The paging function owns dataset stability—capture or identify the desired server-side snapshot there when concurrent mutation matters. `batches()` creates no hidden server cursor and never rewrites the q expression.

## Binary helpers

These helpers are asynchronous because native parsing and serialization run away from the JavaScript event loop:

```ts
import {
  deserializeIpcBytes6,
  deserializeValue6,
  readBinary6,
  serializeAsIpcBytes6,
} from "@xbbg/xqdb";

const table = await readBinary6("trade.bin");
const legacy = await readBinary6("legacy.bin", { symbolEncoding: "lossy" });
const message = await serializeAsIpcBytes6("sync", true, table);
const decodedMessage = await deserializeIpcBytes6(message);
const valueFrame = await serializeAsIpcBytes6("sync", false, 42n);
const exactBody = await deserializeValue6(valueFrame.subarray(8), { lossless: true });
```

`readBinary6()` resolves to an Arrow `Table` and accepts the same `symbolEncoding` policy as `Q`. `serializeAsIpcBytes6()` resolves to a `Buffer` containing one complete q IPC message. `deserializeValue6()` validates and decodes one complete q value body; `deserializeIpcBytes6()` validates a complete frame and returns both `messageType` and `value`. Both decoders snapshot their input before asynchronous work and reject truncation, trailing bytes, malformed compression metadata, and invalid value bodies. Pass `{ lossless: true }` to retain exact value bytes.

`readBinary6()` accepts regular local files that fit in available memory and rejects Windows UNC/device paths. It imposes no fixed size limit: a Kxzip-compressed file is rejected only when its declared decompressed size exceeds what its own LZ4 blocks can hold, which is the smaller of each block's uncompressed size and 255x the compressed bytes actually present.

## Subscriptions

A pending `receive()` occupies its q connection until a message arrives or the socket timeout expires. A receive timeout fails with `XQDB_IO`, and the core closes that socket; callers must reconnect and resubscribe before receiving again. Use a dedicated `Q` for subscriptions and another for ordinary queries:

```ts
import { XqdbIOError, Q } from "@xbbg/xqdb";

const queries = await Q.connect(options);
const subscription = await Q.connect(options);

try {
  await subscription.asyn(".u.sub", "trade", "");
  for (;;) {
    try {
      const update = await subscription.receive();
      consume(update);
    } catch (error) {
      if (!(error instanceof XqdbIOError) || error.code !== "XQDB_IO") {
        throw error;
      }
      await subscription.connect();
      await subscription.asyn(".u.sub", "trade", "");
    }
  }
} finally {
  await Promise.allSettled([subscription.disconnect(), queries.disconnect()]);
}
```

Alternatively, set `timeout` above the maximum expected quiet interval, up to the 24-hour limit. The first release intentionally has no connection pool or separate subscription engine.

## Errors

Native failures become stable public errors:

- `XqdbIOError` for `XQDB_IO` transport and connection failures
- `XqdbAuthError` for `XQDB_AUTH` authentication failures
- `XqdbError` with code `XQDB_BACKPRESSURE` when a connection's native FIFO is full
- `XqdbError` for server, conversion, unsupported-value, internal, and other generic native failures

Every error has a stable `code`, its original native text in `nativeMessage`, and the native payload or rejected exception in `cause`. Failure to resolve a local or installed addon is a `XqdbIOError` with code `XQDB_NATIVE_LOAD` and remediation in its message.

## Supported native targets

| Platform    | Native package              | Requirement         |
| ----------- | --------------------------- | ------------------- |
| Windows x64 | `@xbbg/xqdb-win32-x64-msvc` | Microsoft x64 ABI   |
| Linux x64   | `@xbbg/xqdb-linux-x64-gnu`  | glibc 2.28 or newer |
| macOS arm64 | `@xbbg/xqdb-darwin-arm64`   | Apple Silicon       |

Other operating-system, CPU, and libc combinations are unsupported in the initial release. A generated napi-rs loader reports an unsupported target or a missing optional binary instead of silently falling back to a different build.

## TLS certificate behavior

`tls: true` encrypts the connection and verifies the server certificate and hostname against the operating system's trusted certificate store. `tlsCa` supplies additional PEM trust roots and `tlsServerName` overrides the verified DNS name when it differs from `host`. Mutual TLS requires both `tlsCert` and `tlsKey` as PEM strings. Supplying custom TLS fields while `tls` is disabled, supplying only one client-credential field, or presenting an invalid, expired, untrusted, or hostname-mismatched certificate fails closed.

## License

XQDB is licensed under the [BSD-3-Clause](../LICENSE) permissive open-source license, which permits use in proprietary and commercial applications.
