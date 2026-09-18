import type { DataType, Table, Vector } from "apache-arrow";
import type { Buffer } from "node:buffer";
import { conversionError } from "./errors.js";
import type { XqdbQValue } from "./qvalue.js";

/**
 * How q text that is not valid UTF-8 is decoded. q stores symbols and strings as raw bytes, so a
 * result carrying stray Latin-1 or binary bytes is either refused as a whole (`"strict"`, the
 * default) or transcoded with each invalid sequence replaced by U+FFFD (`"lossy"`).
 */
export type XqdbSymbolEncoding = "strict" | "lossy";

export type XqdbCompression = "auto" | "on" | "off";

export interface QOptions {
  readonly host: string;
  readonly port: number;
  readonly user?: string;
  readonly password?: string;
  readonly tls?: boolean;
  /**
   * Default socket timeout in milliseconds; 0 disables it; defaults to 30,000;
   * maximum 86,400,000.
   */
  readonly timeout?: number;
  /** Additional IO connection attempts, delayed 1, 2, 4, 8, 16, then at most 32 seconds. */
  readonly retries?: number;
  /** Decoding policy for symbols and strings that are not valid UTF-8; defaults to `"strict"`. */
  readonly symbolEncoding?: XqdbSymbolEncoding;
  /** Return immutable, byte-exact XqdbQValue instances instead of convenient values. */
  readonly lossless?: boolean;
  readonly compression?: XqdbCompression;
  readonly compressionThreshold?: number;
  /**
   * Phase timeouts in milliseconds; omitted values inherit `timeout`; 0 disables;
   * maximum 86,400,000.
   */
  readonly connectTimeout?: number;
  readonly readTimeout?: number;
  readonly writeTimeout?: number;
  readonly maxMessageBytes?: number;
  readonly maxPendingNotifications?: number;
  readonly tlsCa?: string;
  readonly tlsCert?: string;
  readonly tlsKey?: string;
  readonly tlsServerName?: string;
  /** Maximum pending native commands for one connection; defaults to 8. */
  readonly queueCapacity?: number;
  /** Maximum bytes in one admitted native argument snapshot; defaults to 64 MiB. */
  readonly maxArgumentBytes?: number;
  /** Maximum aggregate bytes in native-owned queued expression/value snapshots; defaults to 512 MiB. */
  readonly maxQueuedBytes?: number;
}

export function validatedSymbolEncoding(
  value: unknown,
  owner: string,
): XqdbSymbolEncoding | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (value === "strict" || value === "lossy") {
    return value;
  }
  throw new RangeError(`${owner}.symbolEncoding must be "strict" or "lossy"`);
}

export type XqdbMessageType = "async" | "sync" | "response";

export class XqdbTimestamp {
  public readonly kind = "timestamp";

  public constructor(public readonly nanoseconds: bigint) {}
}

export class XqdbDate {
  public readonly kind = "date";

  /** An ISO calendar date in YYYY-MM-DD form. */
  public constructor(public readonly value: string) {}
}

export class XqdbTime {
  public readonly kind = "time";

  /** Millisecond-aligned nanoseconds since midnight; q time has millisecond precision. */
  public constructor(public readonly nanoseconds: bigint) {}
}

export class XqdbTimespan {
  public readonly kind = "timespan";

  /** Signed duration in nanoseconds. */
  public constructor(public readonly nanoseconds: bigint) {}
}

const SUPPORTED_Q_OPERATOR_NAMES: Readonly<Record<string, true>> = Object.freeze({
  "!": true,
  "!:": true,
  "#": true,
  "#:": true,
  $: true,
  "$:": true,
  "%": true,
  "%:": true,
  "&": true,
  "&:": true,
  "'": true,
  "*": true,
  "*:": true,
  "+": true,
  "+:": true,
  ",": true,
  ",:": true,
  "-": true,
  "-:": true,
  ".": true,
  ".:": true,
  "/": true,
  "0:": true,
  "0::": true,
  "1:": true,
  "1::": true,
  "2:": true,
  "2::": true,
  ":": true,
  "<": true,
  "<:": true,
  "=": true,
  "=:": true,
  ">": true,
  ">:": true,
  "?": true,
  "?:": true,
  "@": true,
  "@:": true,
  "\\": true,
  "^": true,
  "^:": true,
  _: true,
  "_:": true,
  abs: true,
  acos: true,
  asin: true,
  atan: true,
  avg: true,
  bin: true,
  cos: true,
  div: true,
  enlist: true,
  exit: true,
  exp: true,
  getenv: true,
  in: true,
  insert: true,
  last: true,
  like: true,
  log: true,
  max: true,
  min: true,
  prd: true,
  setenv: true,
  sin: true,
  sqrt: true,
  ss: true,
  sum: true,
  tan: true,
  wavg: true,
  within: true,
  wsum: true,
  xexp: true,
  "|": true,
  "|:": true,
  "~": true,
  "~:": true,
});

function validateQOperatorName(name: unknown): string {
  if (typeof name !== "string") {
    throw conversionError("XqdbQOperator.name must be a string");
  }
  if (name.includes("\0")) {
    throw conversionError("XqdbQOperator.name cannot contain NUL bytes");
  }
  if (!Object.hasOwn(SUPPORTED_Q_OPERATOR_NAMES, name)) {
    throw conversionError(`Unsupported q primitive operator name: ${JSON.stringify(name)}`);
  }
  return name;
}

function validateQLambdaParts(
  source: unknown,
  context: unknown,
): { readonly source: string; readonly context: string } {
  if (typeof source !== "string") {
    throw conversionError("XqdbQLambda.source must be a string");
  }
  if (typeof context !== "string") {
    throw conversionError("XqdbQLambda.context must be a string");
  }
  if (source.includes("\0")) {
    throw conversionError("XqdbQLambda.source cannot contain NUL bytes");
  }
  if (context.includes("\0")) {
    throw conversionError("XqdbQLambda.context cannot contain NUL bytes");
  }
  if (context !== "" && context.startsWith(".")) {
    throw conversionError("q lambda context omits the leading dot");
  }
  const trimmed = source.trim();
  const lambdaSource = trimmed.startsWith("k)") ? trimmed.slice(2) : trimmed;
  if (!lambdaSource.startsWith("{") || !lambdaSource.endsWith("}")) {
    throw conversionError("XqdbQLambda.source must be brace-delimited");
  }
  return { context, source };
}

export class XqdbQOperator {
  readonly #name: string;

  public static get PLUS(): XqdbQOperator {
    return XQDB_Q_PLUS;
  }

  public constructor(name: string) {
    this.#name = validateQOperatorName(name);
    Object.freeze(this);
  }

  public get name(): string {
    return this.#name;
  }
}

const XQDB_Q_PLUS = new XqdbQOperator("+");

export class XqdbQLambda {
  readonly #source: string;
  readonly #context: string;

  public constructor(source: string, context = "") {
    const validated = validateQLambdaParts(source, context);
    this.#source = validated.source;
    this.#context = validated.context;
    Object.freeze(this);
  }

  public get source(): string {
    return this.#source;
  }

  public get context(): string {
    return this.#context;
  }
}

export function validatedQOperatorName(value: XqdbQOperator): string {
  let name: unknown;
  try {
    ({ name } = value);
  } catch (error) {
    throw conversionError("Invalid XqdbQOperator instance", error);
  }
  return validateQOperatorName(name);
}

export function validatedQLambdaParts(value: XqdbQLambda): {
  readonly source: string;
  readonly context: string;
} {
  let context: unknown;
  let source: unknown;
  try {
    ({ context, source } = value);
  } catch (error) {
    throw conversionError("Invalid XqdbQLambda instance", error);
  }
  return validateQLambdaParts(source, context);
}

type XqdbInputArray = readonly XqdbInput[];

interface XqdbInputRecord {
  readonly [key: string]: XqdbInput;
}

type XqdbValueArray = readonly XqdbValue[];

interface XqdbValueRecord {
  readonly [key: string]: XqdbValue;
}

export type XqdbInput =
  | null
  | boolean
  | number
  | bigint
  | string
  | Uint8Array
  | XqdbTimestamp
  | XqdbDate
  | XqdbTime
  | XqdbTimespan
  | XqdbQOperator
  | XqdbQLambda
  | Table
  | XqdbQValue
  | Vector<DataType>
  | XqdbInputArray
  | XqdbInputRecord;

export type XqdbValue =
  | null
  | boolean
  | number
  | bigint
  | string
  | Buffer
  | XqdbTimestamp
  | XqdbDate
  | XqdbTime
  | XqdbTimespan
  | XqdbQOperator
  | XqdbQLambda
  | Table
  | XqdbQValue
  | Vector<DataType>
  | XqdbValueArray
  | XqdbValueRecord;
