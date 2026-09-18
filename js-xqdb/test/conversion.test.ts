import { describe, expect, it } from "vitest";
import {
  DataType,
  Dictionary,
  Int32,
  Table,
  Utf8,
  Vector,
  tableFromArrays,
  tableToIPC,
  vectorFromArray,
} from "apache-arrow";
import type { TypeMap } from "apache-arrow";
import { Buffer } from "node:buffer";

import { ensureNativeSuccess, normalizeInput, normalizeOutput } from "../src/conversion.js";
import { XqdbError } from "../src/errors.js";
import type { NativeValue } from "../src/native-contract.js";
import {
  XqdbDate,
  XqdbQLambda,
  XqdbQOperator,
  XqdbTime,
  XqdbTimespan,
  XqdbTimestamp,
} from "../src/types.js";
import type { XqdbInput } from "../src/types.js";

// @ts-expect-error Private fields keep XqdbQOperator nominal.
const structurallyForgedOperator: XqdbQOperator = { name: "+" };
void structurallyForgedOperator;
// @ts-expect-error Private fields keep XqdbQLambda nominal.
const structurallyForgedLambda: XqdbQLambda = { context: "", source: "{x}" };
void structurallyForgedLambda;
function isArrowTable(value: unknown): value is Table<TypeMap> {
  return value instanceof Table;
}

function isArrowVector(value: unknown): value is Vector<DataType> {
  return value instanceof Vector;
}

function expectConversionError(operation: () => unknown): void {
  try {
    operation();
  } catch (error) {
    expect(error).toBeInstanceOf(XqdbError);
    if (!(error instanceof XqdbError)) {
      throw error;
    }
    expect(error.code).toBe("XQDB_CONVERSION");
    return;
  }
  throw new Error("Expected XQDB_CONVERSION");
}

describe("public value conversion", () => {
  it("normalizes scalar, list, dictionary, bigint, and arbitrary byte inputs", () => {
    const bytes = Uint8Array.of(0, 128, 255);

    expect(normalizeInput("trade")).toStrictEqual({ stringValue: "trade", tag: "symbol" });
    expect(normalizeInput(9_007_199_254_740_993n)).toStrictEqual({
      bigintValue: 9_007_199_254_740_993n,
      tag: "i64",
    });
    const normalizedBytes = normalizeInput(bytes);
    expect(normalizedBytes.tag).toBe("bytes");
    expect(Buffer.isBuffer(normalizedBytes.bytesValue)).toBe(true);
    expect(normalizedBytes.bytesValue).toStrictEqual(Buffer.from(bytes));
    expect(normalizeInput([true, 1n, "x"])).toStrictEqual({
      items: [
        { tag: "boolean", boolValue: true },
        { tag: "i64", bigintValue: 1n },
        { tag: "symbol", stringValue: "x" },
      ],
      tag: "list",
    });
    expect(normalizeInput({ sym: "AAPL", size: 10n })).toStrictEqual({
      entries: [
        { key: "sym", value: { tag: "symbol", stringValue: "AAPL" } },
        { key: "size", value: { tag: "i64", bigintValue: 10n } },
      ],
      tag: "dictionary",
    });
    expect(normalizeInput({})).toStrictEqual({ entries: [], tag: "dictionary" });
  });

  it("copies mutable Uint8Array inputs, including SharedArrayBuffer views", () => {
    const bytes = Uint8Array.of(1);
    const normalized = normalizeInput(bytes);
    bytes[0] = 2;
    expect(normalized.bytesValue).toStrictEqual(Buffer.from([1]));

    const shared = new Uint8Array(new SharedArrayBuffer(1));
    shared[0] = 3;
    const normalizedShared = normalizeInput(shared);
    shared[0] = 4;
    expect(normalizedShared.bytesValue).toStrictEqual(Buffer.from([3]));
  });

  it("round-trips precise temporal wrapper payloads", () => {
    const values = [
      new XqdbTimestamp(1_725_000_000_000_000_001n),
      new XqdbTimestamp(-1n),
      new XqdbDate("2026-08-22"),
      new XqdbTime(43_200_000_000_000n),
      new XqdbTimespan(-1n),
    ] as const;

    for (const value of values) {
      expect(normalizeOutput(normalizeInput(value))).toStrictEqual(value);
    }
  });

  it("round-trips q operator and lambda wrapper contracts", () => {
    expect(XqdbQOperator.PLUS.name).toBe("+");
    for (const name of ["+:", "setenv", "'", "/", "\\"]) {
      expect(new XqdbQOperator(name).name).toBe(name);
    }
    expect(normalizeInput(XqdbQOperator.PLUS)).toStrictEqual({
      stringValue: "+",
      tag: "operator",
    });

    const root = new XqdbQLambda("{x+y}");
    const contextual = new XqdbQLambda(" {x+y} ", "ctx");
    const kDialect = new XqdbQLambda(" k){x+y} ");
    expect(normalizeInput(root)).toStrictEqual({
      context: "",
      stringValue: "{x+y}",
      tag: "lambda",
    });
    expect(normalizeInput(contextual)).toStrictEqual({
      context: "ctx",
      stringValue: " {x+y} ",
      tag: "lambda",
    });
    expect(normalizeInput(kDialect)).toStrictEqual({
      context: "",
      stringValue: " k){x+y} ",
      tag: "lambda",
    });
    expect(normalizeOutput({ stringValue: "+", tag: "operator" })).toStrictEqual(
      XqdbQOperator.PLUS,
    );
    expect(
      normalizeOutput({
        context: "ctx",
        stringValue: " {x+y} ",
        tag: "lambda",
      }),
    ).toStrictEqual(contextual);
  });

  it("rejects invalid constructors and malformed native function envelopes", () => {
    const invalidConstructors: (() => unknown)[] = [
      () => new XqdbQOperator("plus"),
      () => new XqdbQOperator("+\0"),
      () =>
        // @ts-expect-error Runtime validation must reject non-string operator names.
        new XqdbQOperator(1),
      () => new XqdbQLambda("x+y"),
      () => new XqdbQLambda("k)x+y"),
      () => new XqdbQLambda("{x\0+y}"),
      () => new XqdbQLambda("{x+y}", "bad\0context"),
      () => new XqdbQLambda("{x+y}", ".ctx"),
      () =>
        // @ts-expect-error Runtime validation must reject non-string lambda sources.
        new XqdbQLambda(1),
      () =>
        // @ts-expect-error Runtime validation must reject non-string lambda contexts.
        new XqdbQLambda("{x+y}", 1),
    ];
    for (const construct of invalidConstructors) {
      expectConversionError(construct);
    }

    const invalidEnvelopes: NativeValue[] = [
      { stringValue: "plus", tag: "operator" },
      { context: "", stringValue: "x+y", tag: "lambda" },
      { context: "bad\0context", stringValue: "{x+y}", tag: "lambda" },
      { context: ".ctx", stringValue: "{x+y}", tag: "lambda" },
      { stringValue: "{x+y}", tag: "lambda" },
    ];
    for (const envelope of invalidEnvelopes) {
      expectConversionError(() => normalizeOutput(envelope));
    }
  });

  it("freezes callable values and defensively rejects forged instances", () => {
    expect(XqdbQOperator.PLUS).toBe(XqdbQOperator.PLUS);
    expect(Object.isFrozen(XqdbQOperator.PLUS)).toBe(true);
    const lambda = new XqdbQLambda("{x+y}");
    expect(Object.isFrozen(lambda)).toBe(true);
    expect(Reflect.set(XqdbQOperator, "PLUS", new XqdbQOperator("-"))).toBe(false);
    expect(() => Object.defineProperty(XqdbQOperator.PLUS, "name", { value: "-" })).toThrow(
      TypeError,
    );
    expect(() => Object.defineProperty(lambda, "context", { value: "ctx" })).toThrow(TypeError);

    const forgedOperator: unknown = Object.create(XqdbQOperator.prototype);
    const forgedLambda: unknown = Object.create(XqdbQLambda.prototype);
    if (!(forgedOperator instanceof XqdbQOperator)) {
      throw new TypeError("Forged operator did not retain its prototype");
    }
    if (!(forgedLambda instanceof XqdbQLambda)) {
      throw new TypeError("Forged lambda did not retain its prototype");
    }
    for (const value of [forgedOperator, forgedLambda]) {
      expectConversionError(() => normalizeInput(value));
    }
  });

  it("rejects invalid temporal wrappers as stable conversion errors", () => {
    expectConversionError(() => normalizeInput(new XqdbDate("22 August 2026")));
    expectConversionError(() => normalizeInput(new XqdbTime(86_400_000_000_000n)));
    expectConversionError(() => normalizeInput(new XqdbTime(1n)));
  });

  it("rejects unsupported runtime inputs as stable conversion errors", () => {
    expectConversionError(() => {
      // @ts-expect-error Runtime validation must reject undefined input.
      normalizeInput();
    });
    expectConversionError(() => {
      // @ts-expect-error Runtime validation must reject symbol input.
      normalizeInput(Symbol("trade"));
    });
  });

  it("rejects embedded NUL symbols and dictionary keys recursively", () => {
    expectConversionError(() => normalizeInput({ outer: ["bad\0symbol"] }));
    expectConversionError(() => normalizeInput({ outer: [{ "bad\0key": 1n }] }));
  });

  it("rejects over-depth and cyclic arrays and dictionaries", () => {
    let deepArray: XqdbInput = null;
    let deepDictionary: XqdbInput = null;
    for (let depth = 0; depth < 65; depth += 1) {
      deepArray = [deepArray];
      deepDictionary = { value: deepDictionary };
    }
    expectConversionError(() => normalizeInput(deepArray));
    expectConversionError(() => normalizeInput(deepDictionary));

    const cyclicArray: XqdbInput[] = [];
    cyclicArray.push(cyclicArray);
    const cyclicDictionary: Record<string, XqdbInput> = {};
    cyclicDictionary.self = cyclicDictionary;
    expectConversionError(() => normalizeInput(cyclicArray));
    expectConversionError(() => normalizeInput(cyclicDictionary));
  });

  it("keeps tables and vectors columnar through Arrow IPC", () => {
    const table = tableFromArrays({ price: Float64Array.of(10.5, 11.25) });
    const tableInput = normalizeInput(table);

    const decodedTable = normalizeOutput(tableInput);
    expect(decodedTable).toBeInstanceOf(Table);
    if (!isArrowTable(decodedTable)) {
      throw new Error("Expected an Arrow Table");
    }
    const price = decodedTable.getChild("price");
    if (price === null) {
      throw new Error("Expected the Arrow Table to contain a price column");
    }
    expect([...price]).toStrictEqual([10.5, 11.25]);

    const vector = table.getChild("price");
    if (vector === null) {
      throw new Error("Expected the source Arrow Table to contain a price column");
    }
    const vectorInput = normalizeInput(vector);
    const decodedVector = normalizeOutput(vectorInput);
    if (!isArrowVector(decodedVector)) {
      throw new Error("Expected an Arrow Vector");
    }
    expect([...decodedVector]).toStrictEqual([10.5, 11.25]);
  });

  it("preserves typed empty vector schemas through Arrow IPC conversion", () => {
    const vectors = [
      vectorFromArray([], new Utf8()),
      vectorFromArray([], new Int32()),
      vectorFromArray([], new Dictionary(new Utf8(), new Int32())),
    ];

    for (const vector of vectors) {
      const decoded = normalizeOutput(normalizeInput(vector));
      expect(decoded).toBeInstanceOf(Vector);
      if (!isArrowVector(decoded)) {
        throw new Error("Expected an Arrow Vector");
      }
      expect(decoded).toHaveLength(0);
      expect(decoded.type.typeId).toBe(vector.type.typeId);
      if (DataType.isInt(vector.type)) {
        if (!DataType.isInt(decoded.type)) {
          throw new Error("Expected the decoded integer type");
        }
        expect(decoded.type.bitWidth).toBe(vector.type.bitWidth);
        expect(decoded.type.isSigned).toBe(vector.type.isSigned);
      }
      if (DataType.isDictionary(vector.type)) {
        if (!DataType.isDictionary(decoded.type)) {
          throw new Error("Expected the decoded dictionary type");
        }
        expect(DataType.isUtf8(decoded.type.dictionary)).toBe(true);
        expect(decoded.type.indices.bitWidth).toBe(vector.type.indices.bitWidth);
        expect(decoded.type.indices.isSigned).toBe(vector.type.indices.isSigned);
      }
    }
  });

  it("preserves sliced dictionary chunks with independently encoded dictionaries", () => {
    const first = vectorFromArray(
      ["ignored", "AAPL", null],
      new Dictionary(new Utf8(), new Int32()),
    );
    const second = vectorFromArray(["MSFT", "ignored"], new Dictionary(new Utf8(), new Int32()));
    const combined = new Vector([...first.data, ...second.data]);
    const sliced = combined.slice(1, 4);
    const decoded = normalizeOutput(normalizeInput(sliced));
    if (!isArrowVector(decoded)) {
      throw new Error("Expected an Arrow Vector");
    }
    expect(decoded.type).toBeInstanceOf(Dictionary);
    expect([...decoded]).toStrictEqual(["AAPL", null, "MSFT"]);
  });

  it("decodes bigint, bytes, dictionaries, and one-column series without row materialization", () => {
    expect(normalizeOutput({ bigintValue: 9_007_199_254_740_993n, tag: "i64" })).toBe(
      9_007_199_254_740_993n,
    );

    const source = Uint8Array.of(0, 128, 255);
    const output = normalizeOutput({ bytesValue: source, tag: "bytes" });
    expect(Buffer.isBuffer(output)).toBe(true);
    expect(output).toStrictEqual(Buffer.from(source));

    const dictionary = normalizeOutput({
      entries: [{ key: "__proto__", value: { tag: "symbol", stringValue: "safe" } }],
      tag: "dictionary",
    });
    if (typeof dictionary !== "object" || dictionary === null) {
      throw new Error("Expected a decoded dictionary object");
    }
    expect(Object.getOwnPropertyDescriptor(dictionary, "__proto__")?.value).toBe("safe");
    expect(normalizeOutput({ entries: [], tag: "dictionary" })).toStrictEqual({});

    const vector = tableFromArrays({ value: Int32Array.of(1, 2, 3) }).getChild("value");
    if (vector === null) {
      throw new Error("Expected the source Arrow Table to contain a value column");
    }
    const ipc = tableToIPC(new Table({ value: vector }), "stream");
    const decoded = normalizeOutput({ bytesValue: ipc, tag: "series" });
    expect(decoded).toBeInstanceOf(Vector);
    if (!isArrowVector(decoded)) {
      throw new Error("Expected an Arrow Vector");
    }
    expect([...decoded]).toStrictEqual([1, 2, 3]);
  });

  it("rejects malformed native envelopes without hiding the error", () => {
    expectConversionError(() => normalizeOutput({ tag: "i64" }));
    expect(() => {
      ensureNativeSuccess({ ok: false });
    }).toThrow(XqdbError);
  });
});
