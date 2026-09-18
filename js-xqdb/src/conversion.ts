import {
  Field,
  RecordBatch,
  Schema,
  Struct,
  Table,
  Vector,
  makeData,
  tableFromIPC,
  tableToIPC,
} from "apache-arrow";
import type { Data, DataType, TypeMap } from "apache-arrow";
import { Buffer } from "node:buffer";

import { conversionError, mapNativeError } from "./errors.js";
import type { NativeEntry, NativeResult, NativeValue } from "./native-contract.js";
import { XqdbQValue, qValueFromNative, validatedQValueNative } from "./qvalue.js";
import {
  XqdbDate,
  XqdbQLambda,
  XqdbQOperator,
  XqdbTime,
  XqdbTimespan,
  XqdbTimestamp,
  validatedQLambdaParts,
  validatedQOperatorName,
} from "./types.js";
import type { XqdbInput, XqdbValue } from "./types.js";

const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/u;
const MAX_VALUE_DEPTH = 64;
const NANOSECONDS_PER_DAY = 86_400_000_000_000n;
const NANOSECONDS_PER_MILLISECOND = 1_000_000n;

function isInputList(value: XqdbInput): value is readonly XqdbInput[] {
  return Array.isArray(value);
}

function isInputArrowTable(value: XqdbInput): value is Table {
  return value instanceof Table;
}
export function isArrowTable(value: unknown): value is Table<TypeMap> {
  return value instanceof Table;
}

function isArrowVector(value: unknown): value is Vector<DataType> {
  return value instanceof Vector;
}

function requiredVectorData(value: Vector<DataType>): Data {
  const [data] = value.data;
  if (data === undefined) {
    throw conversionError("Arrow vector omitted its data chunk");
  }
  return data;
}

function vectorTable<T extends DataType>(value: Vector<T>): Table<{ value: T }> {
  // Keep typed empty chunks and use the logical vector schema for every batch: independently
  // Encoded dictionary chunks may have different IDs while still belonging to one vector.
  const schema = new Schema<{ value: T }>([new Field("value", value.type, true)]);
  const type = new Struct<{ value: T }>(schema.fields);
  const batches = value.data.map(
    (data) =>
      new RecordBatch(
        schema,
        makeData({ children: [data], length: data.length, nullCount: 0, type }),
      ),
  );
  return new Table(schema, batches);
}

function normalizeEmptyTable<T extends TypeMap>(table: Table<T>): Table<T> {
  if (
    table.numRows !== 0 ||
    !table.batches.some((batch) => batch.data.children.some((child) => isArrowVector(child)))
  ) {
    return table;
  }
  // Arrow's zero-row object constructor can leave Vectors where a RecordBatch expects Data.
  // Unwrap only that representation; populated and already-normalized tables stay untouched.
  return new Table(
    table.schema,
    table.batches.map(
      (batch) =>
        new RecordBatch(
          batch.schema,
          makeData({
            children: batch.data.children.map((child) =>
              isArrowVector(child) ? requiredVectorData(child) : child,
            ),
            length: 0,
            nullCount: 0,
            type: batch.data.type,
          }),
          batch.metadata,
        ),
    ),
  );
}

function arrowBytes(value: Table | Vector<DataType>): Buffer;
function arrowBytes(value: Table<TypeMap> | Vector<DataType>): Buffer {
  try {
    const table = isArrowTable(value) ? normalizeEmptyTable(value) : vectorTable(value);
    const bytes = tableToIPC(table, "stream");
    return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  } catch (error) {
    throw conversionError("Unable to serialize the Arrow value as an IPC stream", error);
  }
}

function normalizeDictionary(
  value: Readonly<Record<string, XqdbInput>>,
  depth: number,
  active: WeakSet<object>,
): NativeValue {
  const entries: NativeEntry[] = Object.entries(value).map(([key, entryValue]) => {
    if (key.includes("\0")) {
      throw conversionError("q dictionary keys cannot contain NUL bytes");
    }
    return {
      key,
      value: normalizeInputAtDepth(entryValue, depth + 1, active),
    };
  });
  return { entries, tag: "dictionary" };
}

function normalizeInputAtDepth(
  value: XqdbInput,
  depth: number,
  active: WeakSet<object>,
): NativeValue {
  if (depth > MAX_VALUE_DEPTH) {
    throw conversionError(`Input nesting exceeds ${MAX_VALUE_DEPTH} levels`);
  }
  if (value === null) {
    return { tag: "null" };
  }
  if (typeof value === "boolean") {
    return { boolValue: value, tag: "boolean" };
  }
  if (typeof value === "number") {
    return { numberValue: value, tag: "f64" };
  }
  if (typeof value === "bigint") {
    return { bigintValue: value, tag: "i64" };
  }
  if (typeof value === "string") {
    if (value.includes("\0")) {
      throw conversionError("q symbols cannot contain NUL bytes");
    }
    return { stringValue: value, tag: "symbol" };
  }
  if (value instanceof XqdbQValue) {
    return validatedQValueNative(value);
  }
  if (value instanceof Uint8Array) {
    return {
      bytesValue: Buffer.from(value),
      tag: "bytes",
    };
  }
  if (value instanceof XqdbQOperator) {
    return { stringValue: validatedQOperatorName(value), tag: "operator" };
  }
  if (value instanceof XqdbQLambda) {
    const { source, context } = validatedQLambdaParts(value);
    return {
      context,
      stringValue: source,
      tag: "lambda",
    };
  }
  if (value instanceof XqdbTimestamp) {
    if (typeof value.nanoseconds !== "bigint") {
      throw conversionError("XqdbTimestamp.nanoseconds must be a bigint");
    }
    return { bigintValue: value.nanoseconds, tag: "timestamp" };
  }
  if (value instanceof XqdbDate) {
    if (typeof value.value !== "string" || !ISO_DATE.test(value.value)) {
      throw conversionError("XqdbDate.value must use YYYY-MM-DD form");
    }
    return { stringValue: value.value, tag: "date" };
  }
  if (value instanceof XqdbTime) {
    if (typeof value.nanoseconds !== "bigint") {
      throw conversionError("XqdbTime.nanoseconds must be a bigint");
    }
    if (value.nanoseconds < 0n || value.nanoseconds >= NANOSECONDS_PER_DAY) {
      throw conversionError("XqdbTime.nanoseconds must be within one day");
    }
    if (value.nanoseconds % NANOSECONDS_PER_MILLISECOND !== 0n) {
      throw conversionError("XqdbTime.nanoseconds must use millisecond precision");
    }
    return { bigintValue: value.nanoseconds, tag: "time" };
  }
  if (value instanceof XqdbTimespan) {
    if (typeof value.nanoseconds !== "bigint") {
      throw conversionError("XqdbTimespan.nanoseconds must be a bigint");
    }
    return { bigintValue: value.nanoseconds, tag: "timespan" };
  }
  if (isInputArrowTable(value)) {
    return {
      bytesValue: arrowBytes(value),
      tag: "table",
    };
  }
  if (isArrowVector(value)) {
    return {
      bytesValue: arrowBytes(value),
      tag: "series",
    };
  }
  if (isInputList(value)) {
    if (active.has(value)) {
      throw conversionError("Cyclic arrays and dictionaries cannot map to q values");
    }
    active.add(value);
    try {
      return {
        items: value.map((item) => normalizeInputAtDepth(item, depth + 1, active)),
        tag: "list",
      };
    } finally {
      active.delete(value);
    }
  }
  if (typeof value !== "object") {
    throw conversionError(`Unsupported JavaScript input type: ${typeof value}`);
  }

  const prototype = Reflect.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) {
    throw conversionError("Only plain string-keyed objects can map to q dictionaries");
  }
  if (active.has(value)) {
    throw conversionError("Cyclic arrays and dictionaries cannot map to q values");
  }
  active.add(value);
  try {
    return normalizeDictionary(value, depth, active);
  } finally {
    active.delete(value);
  }
}

export function normalizeInput(value: XqdbInput): NativeValue {
  return normalizeInputAtDepth(value, 0, new WeakSet());
}

function requiredBoolean(value: NativeValue): boolean {
  if (typeof value.boolValue !== "boolean") {
    throw conversionError(`Native ${value.tag} value omitted boolValue`);
  }
  return value.boolValue;
}

function requiredNumber(value: NativeValue): number {
  if (typeof value.numberValue !== "number") {
    throw conversionError(`Native ${value.tag} value omitted numberValue`);
  }
  return value.numberValue;
}

function requiredBigInt(value: NativeValue): bigint {
  if (typeof value.bigintValue !== "bigint") {
    throw conversionError(`Native ${value.tag} value omitted bigintValue`);
  }
  return value.bigintValue;
}

function requiredString(value: NativeValue): string {
  if (typeof value.stringValue !== "string") {
    throw conversionError(`Native ${value.tag} value omitted stringValue`);
  }
  return value.stringValue;
}

function requiredContext(value: NativeValue): string {
  if (typeof value.context !== "string") {
    throw conversionError(`Native ${value.tag} value omitted context`);
  }
  return value.context;
}

function requiredBytes(value: NativeValue): Uint8Array {
  if (!(value.bytesValue instanceof Uint8Array)) {
    throw conversionError(`Native ${value.tag} value omitted bytesValue`);
  }
  return value.bytesValue;
}

function decodeArrowTable(value: NativeValue): Table<TypeMap> {
  try {
    return tableFromIPC<TypeMap>(requiredBytes(value));
  } catch (error) {
    throw conversionError(`Unable to decode native ${value.tag} IPC stream`, error);
  }
}

function decodeDictionary(value: NativeValue): Readonly<Record<string, XqdbValue>> {
  if (!Array.isArray(value.entries)) {
    throw conversionError("Native dictionary value omitted entries");
  }
  const output: Record<string, XqdbValue> = {};
  for (const entry of value.entries) {
    Object.defineProperty(output, entry.key, {
      configurable: true,
      enumerable: true,
      value: normalizeOutput(entry.value),
      writable: true,
    });
  }
  return output;
}

export function normalizeOutput(value: NativeValue): XqdbValue {
  switch (value.tag) {
    case "null": {
      return null;
    }
    case "boolean": {
      return requiredBoolean(value);
    }
    case "u8":
    case "i16":
    case "i32":
    case "f32":
    case "f64":
    case "char": {
      return requiredNumber(value);
    }
    case "i64": {
      return requiredBigInt(value);
    }
    case "guid":
    case "symbol":
    case "string": {
      return requiredString(value);
    }
    case "operator": {
      return new XqdbQOperator(requiredString(value));
    }
    case "lambda": {
      return new XqdbQLambda(requiredString(value), requiredContext(value));
    }
    case "bytes": {
      const bytes = requiredBytes(value);
      return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    }
    case "timestamp": {
      return new XqdbTimestamp(requiredBigInt(value));
    }
    case "date": {
      return new XqdbDate(requiredString(value));
    }
    case "time": {
      return new XqdbTime(requiredBigInt(value));
    }
    case "timespan": {
      return new XqdbTimespan(requiredBigInt(value));
    }
    case "list": {
      if (!Array.isArray(value.items)) {
        throw conversionError("Native list value omitted items");
      }
      return value.items.map((item) => normalizeOutput(item));
    }
    case "dictionary": {
      return decodeDictionary(value);
    }
    case "table": {
      return decodeArrowTable(value);
    }
    case "series": {
      const vector = decodeArrowTable(value).getChildAt<DataType>(0);
      if (vector === null) {
        throw conversionError("Native series IPC stream contained no column");
      }
      return vector;
    }
    case "qvalue": {
      return qValueFromNative(value);
    }
    default: {
      throw conversionError(`Unsupported native value tag: ${value.tag}`);
    }
  }
}

export function ensureNativeSuccess(result: NativeResult): void {
  if (result.ok) {
    return;
  }
  if (result.error === undefined) {
    throw conversionError("Native operation failed without an error payload");
  }
  throw mapNativeError(result.error);
}

export function unwrapNativeValue(result: NativeResult): XqdbValue {
  ensureNativeSuccess(result);
  if (result.value === undefined) {
    throw conversionError("Native operation succeeded without a value payload");
  }
  return normalizeOutput(result.value);
}
