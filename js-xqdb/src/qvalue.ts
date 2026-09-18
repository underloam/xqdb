import { Buffer } from "node:buffer";
import { normalizeInput } from "./conversion.js";

import { XqdbError, conversionError, mapNativeError, rejectionToIOError } from "./errors.js";
import { loadNativeBinding } from "./native-loader.js";
import type { NativeModule, NativeResult, NativeValue } from "./native-contract.js";
import type { XqdbInput } from "./types.js";

interface QValueState {
  readonly body: Buffer;
  readonly typeCode: number;
  readonly length: number;
  readonly isTable: boolean;
}

const states = new WeakMap<XqdbQValue, QValueState>();

const CONSTRUCTION_TOKEN = Symbol("XqdbQValue construction token");
const I64_SIGN_BIT_INDEX = 63n;
const I64_SIGN_BIT = 1n << I64_SIGN_BIT_INDEX;
const MIN_I64 = -I64_SIGN_BIT;
const MAX_I64 = I64_SIGN_BIT - 1n;
const INT16_BYTE_WIDTH = 2;
const INT32_BYTE_WIDTH = 4;
const INT64_BYTE_WIDTH = 8;
const MIN_I16 = -32_768;
const MAX_I16 = 32_767;
const MIN_I32 = -2_147_483_648;
const MAX_I32 = 2_147_483_647;
const MAX_U8 = 255;
const RESERVED_ATOM_TYPE_CODE = 3;
const MAX_ATOM_TYPE_CODE = 19;
const Q_BYTE_TYPE_CODE = 4;
const Q_SHORT_TYPE_CODE = 5;
const Q_INT_TYPE_CODE = 6;
const Q_LONG_TYPE_CODE = 7;
const Q_REAL_TYPE_CODE = 8;
const Q_FLOAT_TYPE_CODE = 9;
const Q_CHAR_TYPE_CODE = 10;
const Q_SYMBOL_TYPE_CODE = 11;
const Q_TIMESTAMP_TYPE_CODE = 12;
const Q_MONTH_TYPE_CODE = 13;
const Q_DATE_TYPE_CODE = 14;
const Q_DATETIME_TYPE_CODE = 15;
const Q_TIMESPAN_TYPE_CODE = 16;
const Q_MINUTE_TYPE_CODE = 17;
const Q_SECOND_TYPE_CODE = 18;
const Q_TIME_TYPE_CODE = 19;
const encoder = new TextEncoder();

async function invoke(
  operation: (binding: NativeModule) => Promise<NativeResult>,
): Promise<XqdbQValue> {
  let result: NativeResult;
  try {
    result = await operation(loadNativeBinding());
  } catch (error) {
    if (error instanceof XqdbError) {
      throw error;
    }
    throw rejectionToIOError(error);
  }
  if (!result.ok) {
    if (result.error === undefined) {
      throw conversionError("Native QValue operation failed without an error payload");
    }
    throw mapNativeError(result.error);
  }
  if (result.value === undefined) {
    throw conversionError("Native QValue operation succeeded without a value payload");
  }
  return qValueFromNative(result.value);
}

function checkedInteger(value: number, range: "uint8" | "int16" | "int32", name: string): number {
  let minimum = MIN_I32;
  let maximum = MAX_I32;
  if (range === "uint8") {
    minimum = 0;
    maximum = MAX_U8;
  } else if (range === "int16") {
    minimum = MIN_I16;
    maximum = MAX_I16;
  }
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw conversionError(`${name} must be a safe integer from ${minimum} through ${maximum}`);
  }
  return value;
}

function signedPayload(
  value: number,
  bytes: typeof INT16_BYTE_WIDTH | typeof INT32_BYTE_WIDTH,
): Buffer {
  const payload = Buffer.alloc(bytes);
  if (bytes === INT16_BYTE_WIDTH) {
    payload.writeInt16LE(value);
  } else {
    payload.writeInt32LE(value);
  }
  return payload;
}

function bigintPayload(value: bigint, name: string): Buffer {
  if (typeof value !== "bigint" || value < MIN_I64 || value > MAX_I64) {
    throw conversionError(`${name} must fit in a signed 64-bit q value`);
  }
  const payload = Buffer.alloc(INT64_BYTE_WIDTH);
  payload.writeBigInt64LE(value);
  return payload;
}

function floatPayload(
  value: number,
  bytes: typeof INT32_BYTE_WIDTH | typeof INT64_BYTE_WIDTH,
  name: string,
): Buffer {
  if (typeof value !== "number") {
    throw conversionError(`${name} must be a number`);
  }
  const payload = Buffer.alloc(bytes);
  if (bytes === INT32_BYTE_WIDTH) {
    payload.writeFloatLE(value);
  } else {
    payload.writeDoubleLE(value);
  }
  return payload;
}

/**
 * Immutable, validated bytes for one complete q IPC value body (without the eight-byte frame
 * header). Use the asynchronous factories so Rust validates every body before an instance exists.
 */
export class XqdbQValue {
  constructor(token: typeof CONSTRUCTION_TOKEN, state: QValueState) {
    if (token !== CONSTRUCTION_TOKEN) {
      throw conversionError("XqdbQValue instances must be created by a validated factory");
    }
    states.set(this, state);
    Object.freeze(this);
  }

  public get typeCode(): number {
    return requiredState(this).typeCode;
  }

  public get length(): number {
    return requiredState(this).length;
  }

  public get isTable(): boolean {
    return requiredState(this).isTable;
  }

  /** Returns a defensive copy, so the validated body cannot be mutated through the public API. */
  public toBytes(): Buffer {
    return Buffer.from(requiredState(this).body);
  }

  public static async fromBytes(bytes: Uint8Array): Promise<XqdbQValue> {
    if (!(bytes instanceof Uint8Array)) {
      throw conversionError("XqdbQValue.fromBytes requires a Uint8Array");
    }
    const snapshot = Buffer.from(bytes);
    return invoke((binding) => binding.qValueFromBytes(snapshot));
  }

  /** Constructs an atom from q kind 1..19 (excluding 3) and its exact raw payload. */
  public static async atom(kind: number, payload: Uint8Array): Promise<XqdbQValue> {
    if (
      !Number.isInteger(kind) ||
      kind < 1 ||
      kind > MAX_ATOM_TYPE_CODE ||
      kind === RESERVED_ATOM_TYPE_CODE
    ) {
      throw conversionError("q atom kind must be 1..19 excluding reserved kind 3");
    }
    if (!(payload instanceof Uint8Array)) {
      throw conversionError("XqdbQValue.atom payload must be a Uint8Array");
    }
    const snapshot = Buffer.from(payload);
    return invoke((binding) => binding.qValueAtom(kind, snapshot));
  }

  public static async list(values: readonly XqdbQValue[]): Promise<XqdbQValue> {
    const nativeValues = values.map((value) => validatedQValueNative(value));
    const result = await invoke((binding) => binding.qValueList(nativeValues));
    return result;
  }

  public static async dictionary(keys: XqdbQValue, values: XqdbQValue): Promise<XqdbQValue> {
    const nativeKeys = validatedQValueNative(keys);
    const nativeValues = validatedQValueNative(values);
    const result = await invoke((binding) => binding.qValueDictionary(nativeKeys, nativeValues));
    return result;
  }

  /** Converts any supported convenient input to its exact serialized q value body. */
  public static async from(value: XqdbInput): Promise<XqdbQValue> {
    if (value instanceof XqdbQValue) {
      requiredState(value);
      return value;
    }
    const native = normalizeInput(value);
    return invoke((binding) => binding.qValueFromNative(native));
  }

  public static async boolean(value: boolean): Promise<XqdbQValue> {
    if (typeof value !== "boolean") {
      throw conversionError("q boolean value must be a boolean");
    }
    return this.atom(1, Uint8Array.of(value ? 1 : 0));
  }

  public static async guid(value: Uint8Array): Promise<XqdbQValue> {
    return this.atom(2, value);
  }

  public static async byte(value: number): Promise<XqdbQValue> {
    return this.atom(
      Q_BYTE_TYPE_CODE,
      Uint8Array.of(checkedInteger(value, "uint8", "q byte value")),
    );
  }

  public static async short(value: number): Promise<XqdbQValue> {
    return this.atom(
      Q_SHORT_TYPE_CODE,
      signedPayload(checkedInteger(value, "int16", "q short value"), INT16_BYTE_WIDTH),
    );
  }

  public static async int(value: number): Promise<XqdbQValue> {
    return this.atom(
      Q_INT_TYPE_CODE,
      signedPayload(checkedInteger(value, "int32", "q int value"), INT32_BYTE_WIDTH),
    );
  }

  public static async long(value: bigint): Promise<XqdbQValue> {
    return this.atom(Q_LONG_TYPE_CODE, bigintPayload(value, "q long value"));
  }

  public static async real(value: number): Promise<XqdbQValue> {
    if (typeof value !== "number") {
      throw conversionError("q real value must be a number");
    }
    if (Number.isFinite(value) && !Number.isFinite(Math.fround(value))) {
      throw conversionError("q real value is finite but outside the finite 32-bit float range");
    }
    return this.atom(Q_REAL_TYPE_CODE, floatPayload(value, INT32_BYTE_WIDTH, "q real value"));
  }

  public static async float(value: number): Promise<XqdbQValue> {
    return this.atom(Q_FLOAT_TYPE_CODE, floatPayload(value, INT64_BYTE_WIDTH, "q float value"));
  }

  public static async char(value: number): Promise<XqdbQValue> {
    return this.atom(
      Q_CHAR_TYPE_CODE,
      Uint8Array.of(checkedInteger(value, "uint8", "q char value")),
    );
  }

  public static async symbol(value: string): Promise<XqdbQValue> {
    if (typeof value !== "string" || value.includes("\0")) {
      throw conversionError("q symbol value must be a NUL-free string");
    }
    const encoded = encoder.encode(value);
    const payload = Buffer.alloc(encoded.byteLength + 1);
    payload.set(encoded);
    return this.atom(Q_SYMBOL_TYPE_CODE, payload);
  }

  /** Raw q nanoseconds since 2000.01.01, including typed-null/infinity sentinels. */
  public static async timestamp(rawNanoseconds: bigint): Promise<XqdbQValue> {
    return this.atom(Q_TIMESTAMP_TYPE_CODE, bigintPayload(rawNanoseconds, "q timestamp raw value"));
  }

  /** Raw q month count since 2000.01. */
  public static async month(rawMonths: number): Promise<XqdbQValue> {
    return this.atom(
      Q_MONTH_TYPE_CODE,
      signedPayload(checkedInteger(rawMonths, "int32", "q month raw value"), INT32_BYTE_WIDTH),
    );
  }

  /** Raw q day count since 2000.01.01. */
  public static async date(rawDays: number): Promise<XqdbQValue> {
    return this.atom(
      Q_DATE_TYPE_CODE,
      signedPayload(checkedInteger(rawDays, "int32", "q date raw value"), INT32_BYTE_WIDTH),
    );
  }

  /** Raw q floating-point day count since 2000.01.01. */
  public static async datetime(rawDays: number): Promise<XqdbQValue> {
    return this.atom(
      Q_DATETIME_TYPE_CODE,
      floatPayload(rawDays, INT64_BYTE_WIDTH, "q datetime raw value"),
    );
  }

  public static async timespan(rawNanoseconds: bigint): Promise<XqdbQValue> {
    return this.atom(Q_TIMESPAN_TYPE_CODE, bigintPayload(rawNanoseconds, "q timespan raw value"));
  }

  public static async minute(rawMinutes: number): Promise<XqdbQValue> {
    return this.atom(
      Q_MINUTE_TYPE_CODE,
      signedPayload(checkedInteger(rawMinutes, "int32", "q minute raw value"), INT32_BYTE_WIDTH),
    );
  }

  public static async second(rawSeconds: number): Promise<XqdbQValue> {
    return this.atom(
      Q_SECOND_TYPE_CODE,
      signedPayload(checkedInteger(rawSeconds, "int32", "q second raw value"), INT32_BYTE_WIDTH),
    );
  }

  public static async time(rawMilliseconds: number): Promise<XqdbQValue> {
    return this.atom(
      Q_TIME_TYPE_CODE,
      signedPayload(checkedInteger(rawMilliseconds, "int32", "q time raw value"), INT32_BYTE_WIDTH),
    );
  }
}

function requiredState(value: XqdbQValue): QValueState {
  const state = states.get(value);
  if (state === undefined) {
    throw conversionError("Invalid XqdbQValue instance");
  }
  return state;
}

export function validatedQValueNative(value: XqdbQValue): NativeValue {
  const state = requiredState(value);
  return {
    bytesValue: state.body,
    isTable: state.isTable,
    length: state.length,
    tag: "qvalue",
    typeCode: state.typeCode,
  };
}

export function qValueFromNative(value: NativeValue): XqdbQValue {
  if (
    value.tag !== "qvalue" ||
    !(value.bytesValue instanceof Uint8Array) ||
    typeof value.typeCode !== "number" ||
    !Number.isInteger(value.typeCode) ||
    typeof value.length !== "number" ||
    !Number.isSafeInteger(value.length) ||
    value.length < 0 ||
    typeof value.isTable !== "boolean"
  ) {
    throw conversionError("Native qvalue omitted validated bytes or metadata");
  }
  const instance = new XqdbQValue(CONSTRUCTION_TOKEN, {
    body: Buffer.from(value.bytesValue),
    isTable: value.isTable,
    length: value.length,
    typeCode: value.typeCode,
  });
  return instance;
}
