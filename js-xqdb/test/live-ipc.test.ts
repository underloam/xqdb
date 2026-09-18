import { afterEach, describe, expect, it } from "vitest";
import {
  Dictionary,
  Int32,
  Table,
  Utf8,
  Vector,
  tableFromArrays,
  vectorFromArray,
} from "apache-arrow";
import type { DataType, TypeMap } from "apache-arrow";
import { once } from "node:events";
import { createRequire } from "node:module";
import { createServer } from "node:net";
import type { AddressInfo } from "node:net";

import {
  Q,
  XqdbIOError,
  XqdbQLambda,
  XqdbQOperator,
  XqdbQValue,
  XqdbTimestamp,
  deserializeIpcBytes6,
  deserializeValue6,
  serializeAsIpcBytes6,
} from "../dist/index.js";

const liveEnabled = process.env.XQDB_TEST_Q_EXTERNAL === "1";
const host = process.env.XQDB_TEST_Q_HOST ?? "127.0.0.1";
const port = Number(process.env.XQDB_TEST_Q_PORT ?? "1801");
const expectedRows = Number(process.env.XQDB_Q_ROWS ?? "10000");
const expectedTimestampNanoseconds =
  BigInt(Date.UTC(2024, 0, 2, 3, 4, 5, 123)) * 1_000_000n + 456_789n;
const connections: Q[] = [];
const require = createRequire(import.meta.url);

interface RawNativeResult {
  readonly ok: boolean;
  readonly error?: { readonly code: string; readonly message: string };
  readonly value?: {
    readonly tag: string;
    readonly bytesValue?: Uint8Array;
  };
}

interface RawNativeConnector {
  reserve(): { readonly ok: boolean; readonly permit?: object };
  release(permit: object): RawNativeResult;
  sync(permit: object, expression: string, args: unknown[]): Promise<RawNativeResult>;
  disconnect(permit: object): Promise<RawNativeResult>;
}

interface RawNativeModule {
  readonly NativeConnector: new (options: Record<string, unknown>) => RawNativeConnector;
  qValueAtom(kind: number, payload: Uint8Array): Promise<RawNativeResult>;
}
function isRawNativeModule(value: unknown): value is RawNativeModule {
  return (
    typeof value === "object" &&
    value !== null &&
    "NativeConnector" in value &&
    typeof value.NativeConnector === "function" &&
    "qValueAtom" in value &&
    typeof value.qValueAtom === "function"
  );
}

function rawNativeModule(): RawNativeModule {
  const value: unknown = require("../native.cjs");
  if (!isRawNativeModule(value)) {
    throw new TypeError("Generated native module omitted the expected raw test surface");
  }
  return value;
}

function isArrowTable(value: unknown): value is Table<TypeMap> {
  return value instanceof Table;
}

function isArrowVector(value: unknown): value is Vector<DataType> {
  return value instanceof Vector;
}

function requiredChild<T extends TypeMap, Name extends keyof T & string>(
  table: Table<T>,
  name: Name,
): Vector<T[Name]> {
  const child = table.getChild(name);
  if (child === null) {
    throw new Error(`Arrow table omitted its ${name} column`);
  }
  return child;
}

function requiredServerPort(address: AddressInfo | string | null): number {
  if (typeof address !== "object" || address === null) {
    throw new Error("Server did not expose a TCP address");
  }
  return address.port;
}

async function cancelOrReject(q: Q, reject: (reason?: unknown) => void): Promise<void> {
  try {
    await q.cancel();
  } catch (error) {
    reject(error);
  }
}

async function settlementOutcome(promise: Promise<unknown>): Promise<"settled"> {
  try {
    await promise;
  } catch {
    // This probe intentionally maps either settlement state to one race outcome.
  }
  return "settled";
}

type QueueProbeOutcome =
  | { readonly kind: "settled" }
  | { readonly error: unknown; readonly kind: "rejected" };

async function queueProbeOutcome(candidate: Promise<unknown>): Promise<QueueProbeOutcome> {
  try {
    await candidate;
    return { kind: "settled" };
  } catch (error) {
    return { error, kind: "rejected" };
  }
}

function trackedQ(): Q {
  const q = new Q({ host, port });
  connections.push(q);
  return q;
}

async function trackedConnectedQ(): Promise<Q> {
  const q = await Q.connect({ host, port });
  connections.push(q);
  return q;
}

describe.runIf(liveEnabled)("live q IPC", () => {
  afterEach(async () => {
    const active = connections.splice(0);
    await Promise.allSettled(active.map((q) => q.cancel()));
    const results = await Promise.allSettled(active.map((q) => q.disconnect()));
    const failed = results.find((result) => result.status === "rejected");
    if (failed?.status === "rejected") {
      const reason: unknown = failed.reason;
      if (reason instanceof Error) {
        throw reason;
      }
      throw new Error(String(reason), { cause: reason });
    }
  });

  it("connects explicitly, auto-connects, disconnects, and reconnects", async () => {
    expect(Number.isInteger(port) && port > 0 && port <= 65_535).toBe(true);
    expect(Number.isInteger(expectedRows) && expectedRows > 0).toBe(true);

    const explicit = trackedQ();
    await explicit.connect();
    await expect(explicit.sync("1b")).resolves.toBe(true);

    const staticConnection = await trackedConnectedQ();
    await expect(staticConnection.sync("1f+1f")).resolves.toBe(2);

    const automatic = trackedQ();
    await expect(automatic.sync("6f*7f")).resolves.toBe(42);
    await automatic.disconnect();
    await expect(automatic.sync("7f*6f")).resolves.toBe(42);
  });

  it("preserves scalar, BigInt, and non-millisecond timestamp values", async () => {
    const q = await trackedConnectedQ();

    await expect(q.sync("6f*7f")).resolves.toBe(42);
    await expect(q.sync("9007199254740993j")).resolves.toBe(9_007_199_254_740_993n);
    for (const boundary of [-(1n << 63n), (1n << 63n) - 1n]) {
      await expect(q.sync("{x}", boundary)).resolves.toBe(boundary);
    }

    const timestamp = await q.sync("2024.01.02D03:04:05.123456789");
    expect(timestamp).toBeInstanceOf(XqdbTimestamp);
    if (!(timestamp instanceof XqdbTimestamp)) {
      throw new Error("q timestamp did not decode as XqdbTimestamp");
    }
    expect(timestamp.nanoseconds).toBe(expectedTimestampNanoseconds);

    const roundTripped = await q.sync("{x}", new XqdbTimestamp(expectedTimestampNanoseconds));
    expect(roundTripped).toStrictEqual(new XqdbTimestamp(expectedTimestampNanoseconds));

    const preEpoch = new XqdbTimestamp(-1n);
    await expect(q.sync("{x}", preEpoch)).resolves.toStrictEqual(preEpoch);
  });

  it("round-trips arbitrary char-vector bytes", async () => {
    const q = trackedQ();
    const bytes = new Uint8Array([0, 1, 2, 127, 128, 254, 255]);

    const result = await q.sync("{x}", bytes);

    expect(Buffer.isBuffer(result)).toBe(true);
    expect(result).toStrictEqual(Buffer.from(bytes));
  });

  it("preserves Arrow string, symbol, and empty mixed-list semantics", async () => {
    const q = await trackedConnectedQ();
    const describeVector = "{(type x;count x)}";
    const symbolType = new Dictionary(new Utf8(), new Int32());
    const emptyStrings = vectorFromArray([], new Utf8());
    const emptySymbols = vectorFromArray([], symbolType);

    await expect(q.sync(describeVector, emptyStrings)).resolves.toStrictEqual([0, 0n]);
    const stringFrame = await serializeAsIpcBytes6("sync", false, emptyStrings);
    expect(stringFrame.subarray(8)).toStrictEqual(Buffer.from([0, 0, 0, 0, 0, 0]));

    await expect(q.sync(describeVector, emptySymbols)).resolves.toStrictEqual([11, 0n]);
    const emptyFrame = await serializeAsIpcBytes6("sync", false, emptySymbols);
    expect(emptyFrame.subarray(8)).toStrictEqual(Buffer.from([11, 0, 0, 0, 0, 0]));

    const emptyMixed: [] = [];
    await expect(q.sync(describeVector, emptyMixed)).resolves.toStrictEqual([0, 0n]);
    const mixedFrame = await serializeAsIpcBytes6("sync", false, emptyMixed);
    expect(mixedFrame.subarray(8)).toStrictEqual(Buffer.from([0, 0, 0, 0, 0, 0]));

    const populatedStrings = vectorFromArray(["AAPL", null, "MSFT"], new Utf8());
    await expect(q.sync('{x~("AAPL";"";"MSFT")}', populatedStrings)).resolves.toBe(true);

    const populatedSymbols = vectorFromArray(["AAPL", null, "MSFT"], symbolType);
    const populatedDescription = await q.sync("{(type x;count x;x)}", populatedSymbols);
    if (!Array.isArray(populatedDescription) || !isArrowVector(populatedDescription[2])) {
      throw new Error("q did not return the described symbol vector");
    }
    expect(populatedDescription.slice(0, 2)).toStrictEqual([11, 3n]);
    expect([...populatedDescription[2]]).toStrictEqual(["AAPL", "", "MSFT"]);

    const decodedEmpty = await q.sync("`symbol$()");
    expect(decodedEmpty).toBeInstanceOf(Vector);
    if (!isArrowVector(decodedEmpty)) {
      throw new Error("q did not decode the empty symbol vector as Arrow");
    }
    const decodedFrame = await serializeAsIpcBytes6("sync", false, decodedEmpty);
    expect(decodedFrame.subarray(8)).toStrictEqual(Buffer.from([11, 0, 0, 0, 0, 0]));
    const decodedAgain = await deserializeIpcBytes6(decodedFrame);
    expect(decodedAgain.messageType).toBe("sync");
    await expect(q.sync(describeVector, decodedAgain.value)).resolves.toStrictEqual([11, 0n]);
  });

  it("round-trips q operators and lambdas as first-class values", async () => {
    const q = trackedQ();
    const expression = "{[op;a;b] .[op;(a;b)]}";

    await expect(q.sync(expression, XqdbQOperator.PLUS, 1, 2)).resolves.toBe(3);
    await expect(q.sync(expression, new XqdbQLambda("{x+y}"), 1, 2)).resolves.toBe(3);

    const operator = await q.sync("+");
    expect(operator).toBeInstanceOf(XqdbQOperator);
    if (!(operator instanceof XqdbQOperator)) {
      throw new Error("q operator did not decode as XqdbQOperator");
    }
    expect(operator.name).toBe("+");
    await expect(q.sync(expression, operator, 1, 2)).resolves.toBe(3);

    const lambda = await q.sync("{x+y}");
    if (!(lambda instanceof XqdbQLambda)) {
      throw new Error("q lambda did not decode as XqdbQLambda");
    }
    expect(lambda.source).toBe("{x+y}");
    expect(lambda.context).toBe("");
    await expect(q.sync(expression, lambda, 1, 2)).resolves.toBe(3);

    await expect(serializeAsIpcBytes6("sync", false, XqdbQOperator.PLUS)).resolves.toStrictEqual(
      Buffer.from([1, 1, 0, 0, 10, 0, 0, 0, 102, 1]),
    );
    await expect(
      serializeAsIpcBytes6("sync", false, new XqdbQLambda("{x+y}")),
    ).resolves.toStrictEqual(
      Buffer.from([1, 1, 0, 0, 21, 0, 0, 0, 100, 0, 10, 0, 5, 0, 0, 0, 123, 120, 43, 121, 125]),
    );
  });

  it("round-trips empty dictionaries as (`symbol$())!()", async () => {
    const q = await trackedConnectedQ();
    await expect(q.sync("()!()")).resolves.toStrictEqual({});
    await expect(q.sync("(`symbol$())!()")).resolves.toStrictEqual({});
    await expect(q.sync("0#`a`b!1 2")).resolves.toStrictEqual({});
    await expect(q.sync("{x}", {})).resolves.toStrictEqual({});
    await expect(q.sync("{x~(`symbol$())!()}", {})).resolves.toBe(true);
    await expect(q.sync("{x}", { nested: {} })).resolves.toStrictEqual({ nested: {} });
    await expect(serializeAsIpcBytes6("sync", false, {})).resolves.toStrictEqual(
      Buffer.from([1, 1, 0, 0, 21, 0, 0, 0, 99, 11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
    );
  });

  it("reads and q-acknowledges an Arrow table send", async () => {
    const q = await trackedConnectedQ();
    const fixtureRows = await q.sync(".xqdb.rows");
    expect(fixtureRows).toBe(BigInt(expectedRows));

    const table = await q.sync("trade");
    expect(table).toBeInstanceOf(Table);
    if (!isArrowTable(table)) {
      throw new Error("trade did not decode as an Arrow Table");
    }
    expect(table.numRows).toBe(expectedRows);
    expect(table.schema.fields.map((field) => field.name)).toStrictEqual([
      "sym",
      "time",
      "volume",
      "cond",
      "ask0",
      "ask1",
      "ask2",
      "ask3",
      "ask4",
      "bid0",
      "bid1",
      "bid2",
      "bid3",
      "bid4",
    ]);

    await expect(q.sync("{x~trade}", table)).resolves.toBe(true);
    await expect(q.sync("{count x}", table)).resolves.toBe(BigInt(expectedRows));
    const depth = await q.sync("depth");
    expect(depth).toBeInstanceOf(Table);
    await expect(q.sync("{x~depth}", depth)).resolves.toBe(true);
    const serialized = await serializeAsIpcBytes6("sync", false, table);
    expect(Buffer.isBuffer(serialized)).toBe(true);
    expect(serialized.byteLength).toBeGreaterThan(8);
  });

  it("submits concurrent calls to one connection in FIFO order", async () => {
    const q = await trackedConnectedQ();
    await q.sync(".xqdb.nodeFifo:0#0j");

    const submitted = Array.from({ length: 8 }, (_, index) =>
      q.sync("{.xqdb.nodeFifo,:x;x}", BigInt(index)),
    );
    await expect(Promise.all(submitted)).resolves.toStrictEqual(
      Array.from({ length: 8 }, (_, index) => BigInt(index)),
    );

    const observed = await q.sync(".xqdb.nodeFifo");
    expect(observed).toBeInstanceOf(Vector);
    if (!isArrowVector(observed)) {
      throw new Error("FIFO probe did not return an Arrow Vector");
    }
    expect([...observed]).toStrictEqual(Array.from({ length: 8 }, (_, index) => BigInt(index)));
  });

  it("keeps the event loop live while q handles a slow request", async () => {
    const q = await trackedConnectedQ();
    let eventLoopTicks = 0;
    const interval = setInterval(() => {
      eventLoopTicks += 1;
    }, 25);

    // The external q process must stay busy long enough to observe Node's real event loop;
    // Fake timers cannot exercise whether the native IPC worker blocks that loop.
    let result: unknown;
    try {
      result = await q.sync('system "sleep 1";42f');
    } finally {
      clearInterval(interval);
    }

    expect(result).toBe(42);
    expect(eventLoopTicks).toBeGreaterThanOrEqual(5);
  });

  it("receives a q message after an async send on a dedicated connection", async () => {
    const dedicated = await trackedConnectedQ();

    await dedicated.asyn("{neg[.z.w] x}", 4242n);
    await expect(dedicated.receive()).resolves.toBe(4242n);
  });

  it("round-trips exact lossless values and typed temporal nulls", async () => {
    const lossless = new Q({ host, lossless: true, port });
    connections.push(lossless);
    const convenient = await trackedConnectedQ();
    await expect(convenient.sync("`a`a!1 2")).rejects.toMatchObject({
      code: "XQDB_CONVERSION",
    });
    await expect(convenient.sync("0Np")).resolves.toBeNull();
    await expect(convenient.sync("-0Wp")).rejects.toMatchObject({
      code: "XQDB_CONVERSION",
    });

    const duplicateKeys = await lossless.sync("`a`a!1 2");
    expect(duplicateKeys).toBeInstanceOf(XqdbQValue);
    if (!(duplicateKeys instanceof XqdbQValue)) {
      throw new Error("lossless dictionary did not return XqdbQValue");
    }
    expect(duplicateKeys.typeCode).toBe(99);
    expect(duplicateKeys).toHaveLength(2);
    expect(duplicateKeys.isTable).toBe(false);
    await expect(convenient.sync("{x~(`a`a!1 2)}", duplicateKeys)).resolves.toBe(true);

    const nullTimestamp = await XqdbQValue.timestamp(-(1n << 63n));
    await expect(convenient.sync("{x~0Np}", nullTimestamp)).resolves.toBe(true);
  });

  it("decodes real native frames, exact bodies, compression, and malformed input", async () => {
    for (const messageType of ["async", "sync", "response"] as const) {
      const frame = await serializeAsIpcBytes6(messageType, false, 42n);
      const body = frame.subarray(8);
      await expect(deserializeValue6(body)).resolves.toBe(42n);
      const exact = await deserializeValue6(body, { lossless: true });
      expect(exact).toBeInstanceOf(XqdbQValue);
      if (!(exact instanceof XqdbQValue)) {
        throw new Error("lossless body decoder did not return XqdbQValue");
      }
      expect(exact.typeCode).toBe(-7);
      expect(exact).toHaveLength(1);
      expect(exact.toBytes()).toStrictEqual(body);
      await expect(deserializeIpcBytes6(frame)).resolves.toStrictEqual({
        messageType,
        value: 42n,
      });
    }

    const valid = await serializeAsIpcBytes6("sync", false, 42n);
    const validBody = valid.subarray(8);
    const trailingByte = Buffer.from([0]);
    const validBodyWithTrailingByte = Buffer.concat([validBody, trailingByte]);
    await expect(deserializeValue6(validBody.subarray(0, -1))).rejects.toMatchObject({
      code: "XQDB_CONVERSION",
    });
    await expect(deserializeValue6(validBodyWithTrailingByte)).rejects.toMatchObject({
      code: "XQDB_CONVERSION",
    });
    await expect(deserializeIpcBytes6(valid.subarray(0, -1))).rejects.toMatchObject({
      code: "XQDB_CONVERSION",
    });
    const validWithTrailingByte = Buffer.concat([valid, trailingByte]);
    await expect(deserializeIpcBytes6(validWithTrailingByte)).rejects.toMatchObject({
      code: "XQDB_CONVERSION",
    });
    const invalidEndiannessMarker = Buffer.from(valid);
    invalidEndiannessMarker[0] = 2;
    await expect(deserializeIpcBytes6(invalidEndiannessMarker)).rejects.toMatchObject({
      code: "XQDB_CONVERSION",
    });
    const invalidKind = Buffer.from(valid);
    invalidKind[1] = 3;
    await expect(deserializeIpcBytes6(invalidKind)).rejects.toMatchObject({
      code: "XQDB_CONVERSION",
    });
    const invalidLength = Buffer.from(valid);
    invalidLength.writeUInt32LE(valid.length + 1, 4);
    await expect(deserializeIpcBytes6(invalidLength)).rejects.toMatchObject({
      code: "XQDB_CONVERSION",
    });

    const repetitive = Buffer.alloc(128 * 1024, 65);
    const compressed = await serializeAsIpcBytes6("response", true, repetitive);
    expect(compressed[2]).toBe(1);
    await expect(deserializeIpcBytes6(compressed)).resolves.toStrictEqual({
      messageType: "response",
      value: repetitive,
    });
    await expect(
      deserializeIpcBytes6(Uint8Array.of(1, 2, 1, 0, 15, 0, 0, 0, 14, 0, 0, 0, 1, 0, 4)),
    ).rejects.toMatchObject({ code: "XQDB_CONVERSION" });
  });

  it("rejects lossy raw N-API numeric aliases and oversized BigInts", async () => {
    const native = rawNativeModule();
    expect(() => new native.NativeConnector({ host, port: port + 0.5 })).toThrow(/port/u);
    expect(() => new native.NativeConnector({ host, port, queueCapacity: 1025 })).toThrow(
      /queueCapacity/u,
    );
    await expect(native.qValueAtom(1.9, Uint8Array.of(1))).resolves.toMatchObject({
      error: { code: "XQDB_CONVERSION" },
      ok: false,
    });

    const connector = new native.NativeConnector({ host, port });
    const long = await XqdbQValue.long(1n);
    const admission = connector.reserve();
    if (!admission.ok || admission.permit === undefined) {
      throw new Error("raw native numeric probe was not admitted");
    }
    const result = await connector.sync(admission.permit, "{x}", [
      {
        bytesValue: long.toBytes(),
        isTable: false,
        length: 1,
        tag: "qvalue",
        typeCode: -7.9,
      },
    ]);
    expect(result).toMatchObject({
      error: { code: "XQDB_CONVERSION" },
      ok: false,
    });
    connector.release(admission.permit);

    const disconnectAdmission = connector.reserve();
    if (!disconnectAdmission.ok || disconnectAdmission.permit === undefined) {
      throw new Error("raw native disconnect probe was not admitted");
    }
    await connector.disconnect(disconnectAdmission.permit);
    connector.release(disconnectAdmission.permit);

    const q = await trackedConnectedQ();
    await expect(q.sync("{x}", 1n << 1_000_000n)).rejects.toMatchObject({
      code: "XQDB_CONVERSION",
    });
  });

  it("enforces aggregate raw N-API snapshot budgets and rolls back the queue", async () => {
    const native = rawNativeModule();
    const snapshotBudget = 16 * 1024;
    const payload = Buffer.alloc(9 * 1024, 7);
    const connector = new native.NativeConnector({
      host,
      maxArgumentBytes: snapshotBudget,
      maxQueuedBytes: snapshotBudget,
      port,
      queueCapacity: 1,
    });
    let boundaryPayloadRead = false;
    const boundaryArgument = Object.defineProperties(
      {},
      {
        bytesValue: {
          enumerable: true,
          get() {
            boundaryPayloadRead = true;
            return payload;
          },
        },
        tag: { enumerable: true, value: "bytes" },
      },
    );
    let laterTagRead = false;
    const laterArgument = Object.defineProperty({}, "tag", {
      enumerable: true,
      get() {
        laterTagRead = true;
        return "null";
      },
    });
    const admission = connector.reserve();
    if (!admission.ok || admission.permit === undefined) {
      throw new Error("raw native aggregate snapshot probe was not admitted");
    }
    await expect(
      connector.sync(admission.permit, "{[x;y;z] x}", [
        { bytesValue: payload, tag: "bytes" },
        boundaryArgument,
        laterArgument,
      ]),
    ).resolves.toMatchObject({
      error: { code: "XQDB_BACKPRESSURE" },
      ok: false,
    });
    expect(boundaryPayloadRead).toBe(true);
    expect(laterTagRead).toBe(false);
    expect(connector.release(admission.permit).ok).toBe(true);

    const recovery = connector.reserve();
    if (!recovery.ok || recovery.permit === undefined) {
      throw new Error("aggregate snapshot failure did not release queue capacity");
    }
    const recoveryResult = await connector.sync(recovery.permit, "{x}", [
      { bytesValue: payload, tag: "bytes" },
    ]);
    expect(recoveryResult).toMatchObject({
      ok: true,
      value: { bytesValue: payload, tag: "bytes" },
    });
    expect(connector.release(recovery.permit).ok).toBe(true);

    const disconnect = connector.reserve();
    if (!disconnect.ok || disconnect.permit === undefined) {
      throw new Error("raw native aggregate snapshot cleanup was not admitted");
    }
    await expect(connector.disconnect(disconnect.permit)).resolves.toMatchObject({ ok: true });
    expect(connector.release(disconnect.permit).ok).toBe(true);
  });

  it("builds every exact atom plus list, dictionary, and native-value QValues", async () => {
    const atoms = await Promise.all([
      XqdbQValue.boolean(true),
      XqdbQValue.guid(Uint8Array.from({ length: 16 }, (_, index) => index)),
      XqdbQValue.byte(255),
      XqdbQValue.short(-2),
      XqdbQValue.int(-3),
      XqdbQValue.long(-4n),
      XqdbQValue.real(1.5),
      XqdbQValue.float(-2.5),
      XqdbQValue.char(128),
      XqdbQValue.symbol("trade"),
      XqdbQValue.timestamp(-(1n << 63n)),
      XqdbQValue.month(-12),
      XqdbQValue.date(9),
      XqdbQValue.datetime(Number.POSITIVE_INFINITY),
      XqdbQValue.timespan((1n << 63n) - 1n),
      XqdbQValue.minute(-1),
      XqdbQValue.second(2),
      XqdbQValue.time(3),
    ]);
    expect(atoms.map((value) => value.toBytes()[0])).toStrictEqual([
      255, 254, 252, 251, 250, 249, 248, 247, 246, 245, 244, 243, 242, 241, 240, 239, 238, 237,
    ]);
    expect(atoms[9].toBytes()).toStrictEqual(Buffer.from([245, 116, 114, 97, 100, 101, 0]));
    expect(atoms[10].toBytes()).toStrictEqual(Buffer.from([244, 0, 0, 0, 0, 0, 0, 0, 128]));
    expect(atoms[11].toBytes()).toStrictEqual(Buffer.from([243, 244, 255, 255, 255]));

    const positiveInfinity = await XqdbQValue.real(Number.POSITIVE_INFINITY);
    const negativeInfinity = await XqdbQValue.real(Number.NEGATIVE_INFINITY);
    const notANumber = await XqdbQValue.real(Number.NaN);
    expect(positiveInfinity.toBytes().readFloatLE(1)).toBe(Number.POSITIVE_INFINITY);
    expect(negativeInfinity.toBytes().readFloatLE(1)).toBe(Number.NEGATIVE_INFINITY);
    expect(Number.isNaN(notANumber.toBytes().readFloatLE(1))).toBe(true);

    const list = await XqdbQValue.list([atoms[5], atoms[10]]);
    const dictionary = await XqdbQValue.dictionary(list, list);
    const native = await XqdbQValue.from(42n);
    expect(list.toBytes()[0]).toBe(0);
    expect(dictionary.toBytes()[0]).toBe(99);
    await expect(deserializeValue6(native.toBytes())).resolves.toBe(42n);
  });

  it("snapshots mutable byte and Arrow inputs before returning to the caller", async () => {
    const q = await trackedConnectedQ();
    const initial = trackedQ();
    const initialBytes = Uint8Array.of(9);
    const initialQuery = initial.sync("{x}", initialBytes);
    initialBytes[0] = 10;
    await expect(initialQuery).resolves.toStrictEqual(Buffer.from([9]));

    const queryBytes = Uint8Array.of(1);
    const query = q.sync("{x}", queryBytes);
    queryBytes[0] = 2;
    await expect(query).resolves.toStrictEqual(Buffer.from([1]));
    const sharedBytes = new Uint8Array(new SharedArrayBuffer(1));
    sharedBytes[0] = 5;
    const sharedQuery = q.sync("{x}", sharedBytes);
    sharedBytes[0] = 6;
    await expect(sharedQuery).resolves.toStrictEqual(Buffer.from([5]));
    const table = tableFromArrays({ value: Int32Array.of(7) });
    const tableQuery = q.sync("{x}", table);
    requiredChild(table, "value").set(0, 8);
    const tableResult = await tableQuery;
    expect(tableResult).toBeInstanceOf(Table);
    if (!isArrowTable(tableResult)) {
      throw new Error("table snapshot probe did not return a table");
    }
    expect([...requiredChild(tableResult, "value")]).toStrictEqual([7]);

    const helperBytes = Uint8Array.of(3);
    const serialized = serializeAsIpcBytes6("sync", false, helperBytes);
    helperBytes[0] = 4;
    const decoded = await deserializeIpcBytes6(await serialized);
    expect(decoded.value).toStrictEqual(Buffer.from([3]));
    const helperTable = tableFromArrays({ value: Int32Array.of(11) });
    const helperSerialization = serializeAsIpcBytes6("sync", false, helperTable);
    requiredChild(helperTable, "value").set(0, 12);
    const helperResult = await deserializeIpcBytes6(await helperSerialization);
    const helperDecoded = helperResult.value;
    expect(helperDecoded).toBeInstanceOf(Table);
    if (!isArrowTable(helperDecoded)) {
      throw new Error("helper Arrow snapshot probe did not return a table");
    }
    expect([...requiredChild(helperDecoded, "value")]).toStrictEqual([11]);

    const factoryTable = tableFromArrays({ value: Int32Array.of(13) });
    const factoryQValue = XqdbQValue.from(factoryTable);
    requiredChild(factoryTable, "value").set(0, 14);
    const resolvedFactoryQValue = await factoryQValue;
    const factoryDecoded = await deserializeValue6(resolvedFactoryQValue.toBytes());
    expect(factoryDecoded).toBeInstanceOf(Table);
    if (!isArrowTable(factoryDecoded)) {
      throw new Error("factory Arrow snapshot probe did not return a table");
    }
    expect([...requiredChild(factoryDecoded, "value")]).toStrictEqual([13]);

    const body = Uint8Array.of(0xf9, 5, 0, 0, 0, 0, 0, 0, 0);
    const qvalue = XqdbQValue.fromBytes(body);
    body[1] = 6;
    const resolvedQValue = await qvalue;
    expect(resolvedQValue.toBytes()).toStrictEqual(Buffer.from([0xf9, 5, 0, 0, 0, 0, 0, 0, 0]));
  });

  it("preserves wire FIFO for queries submitted reentrantly by argument getters", async () => {
    const q = await trackedConnectedQ();
    await q.sync("xqdbNodeReentrant:0#0j");
    let nested: Promise<void> | undefined;
    const argument = Object.defineProperty({}, "trigger", {
      enumerable: true,
      get() {
        nested = q.asyn("xqdbNodeReentrant,:2j");
        return 1;
      },
    });

    await q.asyn("{[x] xqdbNodeReentrant,:1j}", argument);
    await nested;
    const observed = await q.sync("xqdbNodeReentrant");
    expect(observed).toBeInstanceOf(Vector);
    if (!isArrowVector(observed)) {
      throw new Error("reentrant ordering probe did not return a long vector");
    }
    expect([...observed]).toStrictEqual([1n, 2n]);

    await q.sync("xqdbNodeReentrant:0#0j");
    const sentinel = new Error("nested conversion failed");
    let failed: Promise<void> | undefined;
    let third: Promise<void> | undefined;
    const failingArgument = Object.defineProperty({}, "trigger", {
      enumerable: true,
      get() {
        third = q.asyn("xqdbNodeReentrant,:3j");
        throw sentinel;
      },
    });
    const outerArgument = Object.defineProperty({}, "trigger", {
      enumerable: true,
      get() {
        const failure = q.asyn("{[x] xqdbNodeReentrant,:2j}", failingArgument);
        failed = failure;
        void Promise.allSettled([failure]);
        return 1;
      },
    });
    await q.asyn("{[x] xqdbNodeReentrant,:1j}", outerArgument);
    if (failed === undefined || third === undefined) {
      throw new Error("nested ordering probes were not submitted");
    }
    await expect(failed).rejects.toBe(sentinel);
    await third;
    const afterFailure = await q.sync("xqdbNodeReentrant");
    expect(afterFailure).toBeInstanceOf(Vector);
    if (!isArrowVector(afterFailure)) {
      throw new Error("failed reentrant ordering probe did not return a long vector");
    }
    expect([...afterFailure]).toStrictEqual([1n, 3n]);
  });

  it("rejects a full real native queue before getters without discarding queued work", async () => {
    const q = await Q.connect({ host, port, queueCapacity: 1 });
    connections.push(q);
    const receive = q.receive();
    let queued: Promise<unknown> | undefined;
    let queuedOutcome: PromiseSettledResult<unknown> | undefined;
    try {
      for (let attempt = 0; attempt < 256 && queued === undefined; attempt += 1) {
        const candidate = q.sync("1+1");
        const outcome = await Promise.race([
          queueProbeOutcome(candidate),
          new Promise<{ readonly kind: "pending" }>((resolve) => {
            setImmediate(() => {
              resolve({ kind: "pending" });
            });
          }),
        ]);
        if (outcome.kind === "pending") {
          queued = candidate;
        } else if (
          outcome.kind !== "rejected" ||
          typeof outcome.error !== "object" ||
          outcome.error === null ||
          !("code" in outcome.error) ||
          outcome.error.code !== "XQDB_BACKPRESSURE"
        ) {
          throw new Error("queue activation probe settled unexpectedly");
        }
      }
      if (queued === undefined) {
        throw new Error("native receive did not enter its blocking read");
      }
      let touched = false;
      const argument = new Proxy(
        {},
        {
          ownKeys() {
            touched = true;
            throw new Error("full-queue argument getter must remain untouched");
          },
        },
      );

      await expect(q.sync("{x}", argument)).rejects.toMatchObject({
        code: "XQDB_BACKPRESSURE",
      });
      expect(touched).toBe(false);
    } finally {
      await q.cancel();
      const outcomes = await Promise.allSettled(
        queued === undefined ? [receive] : [receive, queued],
      );
      const [, queuedResult] = outcomes;
      queuedOutcome = queuedResult;
    }
    await expect(receive).rejects.toMatchObject({ code: "XQDB_IO" });
    if (queuedOutcome === undefined) {
      throw new Error("queued operation outcome was not captured");
    }
    if (queuedOutcome.status === "fulfilled") {
      expect(queuedOutcome.value).toBe(2n);
    } else {
      const reason: unknown = queuedOutcome.reason;
      expect(reason).toBeInstanceOf(XqdbIOError);
      expect(reason).toMatchObject({ code: "XQDB_IO" });
      expect(reason).not.toMatchObject({
        nativeMessage: "Connector operation was aborted",
      });
    }
    await q.connect();
    await expect(q.sync("1+1")).resolves.toBe(2n);
  });

  it("interrupts an active receive promptly with out-of-band cancel", async () => {
    const dedicated = await trackedConnectedQ();
    const pending = dedicated.receive();
    const started = performance.now();
    let cancellation: NodeJS.Timeout | undefined;
    let timeout: NodeJS.Timeout | undefined;
    const deadline = new Promise<never>((_resolve, reject) => {
      // Native worker/socket timing cannot be advanced with JavaScript fake timers.
      // Poll until the real receive settles; cancellation before activation is a no-op.
      cancellation = setInterval(() => {
        void cancelOrReject(dedicated, reject);
      }, 5);
      timeout = setTimeout(() => {
        reject(new Error("active receive was not cancelled"));
      }, 1000);
    });
    try {
      await expect(Promise.race([pending, deadline])).rejects.toMatchObject({ code: "XQDB_IO" });
      expect(performance.now() - started).toBeLessThan(1000);
    } finally {
      clearInterval(cancellation);
      clearTimeout(timeout);
    }
  });

  it("pages stable table batches with (offset;n) sublist semantics", async () => {
    const q = await trackedConnectedQ();
    const rowCounts: number[] = [];
    for await (const batch of q.batches("{[offset;n] (offset;n) sublist 7#trade}", 3)) {
      if (!isArrowTable(batch)) {
        throw new Error("Expected a convenient Arrow table batch");
      }
      rowCounts.push(batch.numRows);
    }
    expect(rowCounts).toStrictEqual([3, 3, 1]);

    const exact = await Q.connect({ host, lossless: true, port });
    connections.push(exact);
    const exactRowCounts: number[] = [];
    for await (const batch of exact.batches("{[offset;n] (offset;n) sublist 7#trade}", 3)) {
      if (!(batch instanceof XqdbQValue) || !batch.isTable) {
        throw new Error("Expected a lossless table QValue batch");
      }
      exactRowCounts.push(batch.length);
    }
    expect(exactRowCounts).toStrictEqual([3, 3, 1]);

    const dictionary = exact.batches("{[offset;n] `a`b!1 2}");
    await expect(dictionary.next()).rejects.toMatchObject({ code: "XQDB_CONVERSION" });
  });

  it("maps q evaluation failures to a stable server error", async () => {
    const q = await trackedConnectedQ();
    await expect(q.sync("1+`a")).rejects.toMatchObject({
      code: "XQDB_SERVER",
      name: "XqdbError",
    });
  });

  it("maps a stable refused connection to XqdbIOError", async () => {
    const portProbe = createServer();
    const listening = once(portProbe, "listening");
    portProbe.listen(0, "127.0.0.1");
    await listening;
    const closedPort = requiredServerPort(portProbe.address());
    const closed = once(portProbe, "close");
    portProbe.close();
    await closed;

    const q = new Q({ host: "127.0.0.1", port: closedPort, timeout: 1000 });
    connections.push(q);
    await expect(q.connect()).rejects.toMatchObject({
      code: "XQDB_IO",
      name: "XqdbIOError",
    });
    await expect(q.connect()).rejects.toBeInstanceOf(XqdbIOError);
  });
});

describe("offline Arrow wire encoding", () => {
  it("serializes and round-trips empty q symbols without changing mixed-list semantics", async () => {
    const symbolBody = Buffer.from([11, 0, 0, 0, 0, 0]);
    const symbols = vectorFromArray([], new Dictionary(new Utf8(), new Int32()));
    const serializedSymbols = await serializeAsIpcBytes6("sync", false, symbols);
    expect(serializedSymbols.subarray(8)).toStrictEqual(symbolBody);
    const decoded = await deserializeValue6(symbolBody);
    const reserializedSymbols = await serializeAsIpcBytes6("sync", false, decoded);
    expect(reserializedSymbols.subarray(8)).toStrictEqual(symbolBody);
    for (const mixed of [vectorFromArray([], new Utf8()), []]) {
      const serializedMixed = await serializeAsIpcBytes6("sync", false, mixed);
      expect(serializedMixed.subarray(8)).toStrictEqual(Buffer.from([0, 0, 0, 0, 0, 0]));
    }
  });

  it("serializes sliced symbol chunks with replacement dictionaries", async () => {
    const first = vectorFromArray(
      ["ignored", "AAPL", null],
      new Dictionary(new Utf8(), new Int32()),
    );
    const second = vectorFromArray(["MSFT", "ignored"], new Dictionary(new Utf8(), new Int32()));
    const combined = new Vector([...first.data, ...second.data]);
    const frame = await serializeAsIpcBytes6("sync", false, combined.slice(1, 4));
    const expectedHeader = Buffer.from([11, 0, 3, 0, 0, 0]);
    const expectedValues = Buffer.from("AAPL\0\0MSFT\0");
    expect(frame.subarray(8)).toStrictEqual(Buffer.concat([expectedHeader, expectedValues]));
  });

  it("serializes zero-row tables with typed symbol and numeric columns", async () => {
    const numberVector = vectorFromArray([], new Int32());
    const symbolVector = vectorFromArray([], new Dictionary(new Utf8(), new Int32()));
    const table = new Table({
      sym: symbolVector,
      n: numberVector,
    });
    const tableHeader = Buffer.from([98, 0, 99, 11, 0, 2, 0, 0, 0]);
    const tableColumns = Buffer.from("sym\0n\0");
    const tableBodies = Buffer.from([0, 0, 2, 0, 0, 0, 11, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0]);
    const expected = Buffer.concat([tableHeader, tableColumns, tableBodies]);
    const frame = await serializeAsIpcBytes6("sync", false, table);
    expect(frame.subarray(8)).toStrictEqual(expected);
    const decoded = await deserializeIpcBytes6(frame);
    const reserialized = await serializeAsIpcBytes6("sync", false, decoded.value);
    expect(reserialized.subarray(8)).toStrictEqual(expected);
  });
});

async function openSilentQServer(): Promise<{
  readonly port: number;
  close(): Promise<void>;
}> {
  let connectionError: Error | undefined;
  const server = createServer((socket) => {
    let authenticated = false;
    socket.on("error", (error) => {
      if (!authenticated) {
        connectionError ??= error;
      }
    });
    socket.on("data", function authenticate(credentials) {
      if (credentials.includes(0)) {
        socket.off("data", authenticate);
        authenticated = true;
        socket.write(Uint8Array.of(6));
      }
    });
  });
  server.on("error", (error) => {
    connectionError ??= error;
  });
  const listening = once(server, "listening");
  server.listen(0, "127.0.0.1");
  await listening;
  const serverPort = requiredServerPort(server.address());

  async function close(): Promise<void> {
    const closed = once(server, "close");
    server.close();
    await closed;
    if (connectionError !== undefined) {
      throw connectionError;
    }
  }

  return {
    close,
    port: serverPort,
  };
}

describe("offline socket timeout policy", () => {
  it.each([
    ["inherited fractional default", { timeout: 100.5 }],
    ["granular override", { readTimeout: 100.5, timeout: 2000 }],
  ])("honors the %s on a silent authenticated socket", async (_name, options) => {
    const server = await openSilentQServer();
    const q = new Q({ host: "127.0.0.1", port: server.port, ...options });
    try {
      await q.connect();
      const started = performance.now();
      await expect(q.receive()).rejects.toMatchObject({ code: "XQDB_IO" });
      const elapsed = performance.now() - started;
      expect(elapsed).toBeGreaterThan(25);
      expect(elapsed).toBeLessThan(800);
    } finally {
      try {
        await q.cancel();
      } finally {
        try {
          await q.disconnect();
        } finally {
          await server.close();
        }
      }
    }
  });

  it.each([
    ["default", { timeout: 0 }],
    ["granular", { readTimeout: 0, timeout: 100 }],
  ])("lets an explicit zero %s timeout disable the socket limit", async (_name, options) => {
    const server = await openSilentQServer();
    const q = new Q({
      host: "127.0.0.1",
      port: server.port,
      ...options,
    });
    let observationTimer: NodeJS.Timeout | undefined;
    try {
      await q.connect();
      const receive = q.receive();
      // The timeout lives in the native worker/socket, so JavaScript fake timers cannot drive it.
      const outcome = await Promise.race([
        settlementOutcome(receive),
        new Promise<"pending">((resolve) => {
          observationTimer = setTimeout(() => {
            resolve("pending");
          }, 350);
        }),
      ]);
      expect(outcome).toBe("pending");
      await q.cancel();
      await expect(receive).rejects.toMatchObject({ code: "XQDB_IO" });
    } finally {
      clearTimeout(observationTimer);
      try {
        await q.cancel();
      } finally {
        try {
          await q.disconnect();
        } finally {
          await server.close();
        }
      }
    }
  });
});
