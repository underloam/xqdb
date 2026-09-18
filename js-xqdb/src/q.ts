import type { Table, TypeMap } from "apache-arrow";
import { Buffer } from "node:buffer";

import {
  ensureNativeSuccess,
  isArrowTable,
  normalizeInput,
  unwrapNativeValue,
} from "./conversion.js";
import { XqdbError, conversionError, mapNativeError, rejectionToIOError } from "./errors.js";
import { loadNativeBinding } from "./native-loader.js";
import type {
  NativeAdmission,
  NativeConnector,
  NativeOptions,
  NativePermit,
  NativeResult,
  NativeValue,
} from "./native-contract.js";
import { XqdbQValue } from "./qvalue.js";
import { validatedSymbolEncoding } from "./types.js";
import type { QOptions, XqdbInput, XqdbValue } from "./types.js";

const DEFAULT_QUEUE_CAPACITY = 8;
const DEFAULT_TIMEOUT_MILLISECONDS = 30_000;
const MAX_QUEUE_CAPACITY = 1024;
const MAX_TIMEOUT_MILLISECONDS = 86_400_000;
const DEFAULT_MAX_ARGUMENT_BYTES = 67_108_864;
const DEFAULT_MAX_QUEUED_BYTES = 536_870_912;
const MAX_EXPRESSION_BYTES = 67_108_864;
const MAX_PORT = 65_535;
const DEFAULT_COMPRESSION_THRESHOLD = 10_000_000;
const DEFAULT_MAX_PENDING_NOTIFICATIONS = 1024;
const MAX_Q_ARGUMENTS = 8;
const DEFAULT_BATCH_SIZE = 65_536;
const MAX_BATCH_ARGUMENTS = 6;

interface NormalizedOptions {
  readonly native: NativeOptions;
  readonly retries: number;
}

function checkedTimeout(value: unknown, name: string): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (
    typeof value !== "number" ||
    !Number.isFinite(value) ||
    value < 0 ||
    value > MAX_TIMEOUT_MILLISECONDS
  ) {
    throw new RangeError(
      `QOptions.${name} must be a non-negative number of milliseconds no greater than 86400000`,
    );
  }
  return value;
}

function checkedPositiveInteger(value: unknown, name: string, defaultValue: number): number {
  const candidate = value ?? defaultValue;
  if (typeof candidate !== "number" || !Number.isSafeInteger(candidate) || candidate <= 0) {
    throw new RangeError(`QOptions.${name} must be a positive safe integer`);
  }
  return candidate;
}

function checkedNonNegativeInteger(value: unknown, name: string, defaultValue: number): number {
  const candidate = value ?? defaultValue;
  if (typeof candidate !== "number" || !Number.isSafeInteger(candidate) || candidate < 0) {
    throw new RangeError(`QOptions.${name} must be a non-negative safe integer`);
  }
  return candidate;
}

function optionalString(value: unknown, name: string): string | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (typeof value !== "string") {
    throw new TypeError(`QOptions.${name} must be a string`);
  }
  return value;
}

function optionalBoolean(value: unknown, name: string): boolean | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (typeof value !== "boolean") {
    throw new TypeError(`QOptions.${name} must be a boolean`);
  }
  return value;
}

function normalizeOptions(options: QOptions): NormalizedOptions {
  const { host } = options;
  if (typeof host !== "string") {
    throw new TypeError("QOptions.host must be a string");
  }
  const { port } = options;
  if (!Number.isInteger(port) || port < 1 || port > MAX_PORT) {
    throw new RangeError("QOptions.port must be an integer from 1 through 65535");
  }
  const { timeout } = options;
  const timeoutMilliseconds = checkedTimeout(timeout, "timeout") ?? DEFAULT_TIMEOUT_MILLISECONDS;
  const retriesInput = options.retries;
  if (retriesInput !== undefined && (!Number.isSafeInteger(retriesInput) || retriesInput < 0)) {
    throw new RangeError("QOptions.retries must be a non-negative safe integer");
  }
  const retries = retriesInput ?? 0;
  const symbolEncodingInput = options.symbolEncoding;
  const symbolEncoding = validatedSymbolEncoding(symbolEncodingInput, "QOptions");
  const { compression } = options;
  if (
    compression !== undefined &&
    compression !== "auto" &&
    compression !== "on" &&
    compression !== "off"
  ) {
    throw new RangeError('QOptions.compression must be "auto", "on", or "off"');
  }
  const compressionThresholdInput = options.compressionThreshold;
  const compressionThreshold = checkedNonNegativeInteger(
    compressionThresholdInput,
    "compressionThreshold",
    DEFAULT_COMPRESSION_THRESHOLD,
  );
  const { connectTimeout } = options;
  const connectTimeoutMilliseconds = checkedTimeout(connectTimeout, "connectTimeout");
  const { readTimeout } = options;
  const readTimeoutMilliseconds = checkedTimeout(readTimeout, "readTimeout");
  const { writeTimeout } = options;
  const writeTimeoutMilliseconds = checkedTimeout(writeTimeout, "writeTimeout");
  const maxMessageBytesInput = options.maxMessageBytes;
  const maxMessageBytes =
    maxMessageBytesInput === undefined
      ? undefined
      : checkedPositiveInteger(maxMessageBytesInput, "maxMessageBytes", 1);
  const maxPendingNotificationsInput = options.maxPendingNotifications;
  const maxPendingNotifications = checkedPositiveInteger(
    maxPendingNotificationsInput,
    "maxPendingNotifications",
    DEFAULT_MAX_PENDING_NOTIFICATIONS,
  );
  const queueCapacityInput = options.queueCapacity;
  const queueCapacity = checkedPositiveInteger(
    queueCapacityInput,
    "queueCapacity",
    DEFAULT_QUEUE_CAPACITY,
  );
  if (queueCapacity > MAX_QUEUE_CAPACITY) {
    throw new RangeError(`QOptions.queueCapacity must be no greater than ${MAX_QUEUE_CAPACITY}`);
  }
  const maxArgumentBytesInput = options.maxArgumentBytes;
  const maxArgumentBytes = checkedPositiveInteger(
    maxArgumentBytesInput,
    "maxArgumentBytes",
    DEFAULT_MAX_ARGUMENT_BYTES,
  );
  const maxQueuedBytesInput = options.maxQueuedBytes;
  const maxQueuedBytes = checkedPositiveInteger(
    maxQueuedBytesInput,
    "maxQueuedBytes",
    DEFAULT_MAX_QUEUED_BYTES,
  );
  const lossless = optionalBoolean(options.lossless, "lossless");
  const tls = optionalBoolean(options.tls, "tls");
  const tlsCert = optionalString(options.tlsCert, "tlsCert");
  const tlsKey = optionalString(options.tlsKey, "tlsKey");
  const tlsCa = optionalString(options.tlsCa, "tlsCa");
  const tlsServerName = optionalString(options.tlsServerName, "tlsServerName");
  const user = optionalString(options.user, "user");
  const password = optionalString(options.password, "password");
  if ((tlsCert === undefined) !== (tlsKey === undefined)) {
    throw new RangeError("QOptions.tlsCert and tlsKey must be supplied together");
  }
  if (
    tls !== true &&
    (tlsCa !== undefined || tlsCert !== undefined || tlsServerName !== undefined)
  ) {
    throw new RangeError("QOptions TLS credentials and server name require tls: true");
  }

  const native: NativeOptions = {
    host,
    port,
    ...(user === undefined ? {} : { user }),
    ...(password === undefined ? {} : { password }),
    ...(tls === undefined ? {} : { tls }),
    timeoutMilliseconds,
    ...(symbolEncoding === undefined ? {} : { symbolEncoding }),
    ...(lossless === undefined ? {} : { lossless }),
    ...(compression === undefined ? {} : { compression }),
    compressionThreshold,
    ...(connectTimeoutMilliseconds === undefined ? {} : { connectTimeoutMilliseconds }),
    ...(readTimeoutMilliseconds === undefined ? {} : { readTimeoutMilliseconds }),
    ...(writeTimeoutMilliseconds === undefined ? {} : { writeTimeoutMilliseconds }),
    ...(maxMessageBytes === undefined ? {} : { maxMessageBytes }),
    maxPendingNotifications,
    ...(tlsCa === undefined ? {} : { tlsCa }),
    ...(tlsCert === undefined ? {} : { tlsCert }),
    ...(tlsKey === undefined ? {} : { tlsKey }),
    ...(tlsServerName === undefined ? {} : { tlsServerName }),
    queueCapacity,
    maxArgumentBytes,
    maxQueuedBytes,
  };
  return { native, retries };
}

function admittedPermit(admission: NativeAdmission): NativePermit {
  if (!admission.ok) {
    if (admission.error === undefined) {
      throw conversionError("Native queue admission failed without an error payload");
    }
    throw mapNativeError(admission.error);
  }
  if (admission.permit === undefined) {
    throw conversionError("Native queue admission succeeded without a permit");
  }
  return admission.permit;
}

function isNativeInvalidArgument(cause: unknown): boolean {
  return (
    cause instanceof TypeError ||
    cause instanceof RangeError ||
    (typeof cause === "object" && cause !== null && "code" in cause && cause.code === "InvalidArg")
  );
}

export class Q {
  readonly #nativeOptions: NativeOptions;
  readonly #retries: number;
  #connectorInstance: NativeConnector | undefined;
  #connectorFailure: XqdbError | undefined;

  public constructor(options: QOptions) {
    const normalized = normalizeOptions(options);
    this.#nativeOptions = normalized.native;
    this.#retries = normalized.retries;
  }

  public static async connect(options: QOptions): Promise<Q> {
    const q = new Q(options);
    await q.connect();
    return q;
  }

  #connector(): NativeConnector {
    if (this.#connectorFailure !== undefined) {
      throw this.#connectorFailure;
    }
    if (this.#connectorInstance !== undefined) {
      return this.#connectorInstance;
    }
    try {
      const binding = loadNativeBinding();
      try {
        this.#connectorInstance = new binding.NativeConnector(this.#nativeOptions);
        return this.#connectorInstance;
      } catch (error) {
        if (isNativeInvalidArgument(error)) {
          const message = error instanceof Error ? error.message : String(error);
          throw conversionError(message, error);
        }
        throw error;
      }
    } catch (error) {
      if (error instanceof XqdbError && error.code === "XQDB_CONVERSION") {
        throw error;
      }
      const failure = error instanceof XqdbError ? error : rejectionToIOError(error);
      this.#connectorFailure = failure;
      throw failure;
    }
  }

  async #invoke(
    operation: (connector: NativeConnector, permit: NativePermit) => Promise<NativeResult>,
  ): Promise<NativeResult> {
    const connector = this.#connector();
    const permit = admittedPermit(connector.reserve());
    try {
      try {
        return await operation(connector, permit);
      } catch (error) {
        if (error instanceof XqdbError) {
          throw error;
        }
        throw rejectionToIOError(error);
      }
    } finally {
      connector.release(permit);
    }
  }

  async #execute(
    mode: "sync" | "asyn",
    expression: string,
    args: readonly XqdbInput[],
  ): Promise<NativeResult> {
    if (typeof expression !== "string") {
      throw new TypeError("q expression must be a string");
    }
    if (Buffer.byteLength(expression, "utf8") > MAX_EXPRESSION_BYTES) {
      throw conversionError(`q expression exceeds its ${MAX_EXPRESSION_BYTES} byte limit`);
    }
    if (args.length > MAX_Q_ARGUMENTS) {
      throw conversionError(`Too many arguments (${MAX_Q_ARGUMENTS} max)`);
    }

    const connector = this.#connector();
    const permit = admittedPermit(connector.reserve());
    try {
      // The native permit is already an ordered FIFO placeholder before conversion invokes
      // User getters or snapshots mutable byte/Arrow inputs.
      const nativeArgs: NativeValue[] = args.map((argument) => normalizeInput(argument));
      let nativeOperation: Promise<NativeResult>;
      try {
        nativeOperation = connector[mode](permit, expression, nativeArgs);
      } catch (error) {
        if (error instanceof XqdbError) {
          throw error;
        }
        throw rejectionToIOError(error);
      }
      try {
        return await nativeOperation;
      } catch (error) {
        if (error instanceof XqdbError) {
          throw error;
        }
        throw rejectionToIOError(error);
      }
    } finally {
      connector.release(permit);
    }
  }

  public async connect(): Promise<void> {
    const result = await this.#invoke((connector, permit) =>
      connector.connect(permit, this.#retries),
    );
    ensureNativeSuccess(result);
  }

  public async disconnect(): Promise<void> {
    if (this.#connectorInstance === undefined) {
      return;
    }
    const result = await this.#invoke((connector, permit) => connector.disconnect(permit));
    ensureNativeSuccess(result);
  }

  /** Cancels socket IO/retry waits out of band; connectTimeout bounds TCP setup, not DNS. */
  public async cancel(): Promise<void> {
    if (this.#connectorInstance === undefined) {
      return;
    }
    const result = await Promise.resolve(this.#connectorInstance.cancel());
    ensureNativeSuccess(result);
  }

  public async sync(expression: string, ...args: readonly XqdbInput[]): Promise<XqdbValue> {
    return unwrapNativeValue(await this.#execute("sync", expression, args));
  }

  public async asyn(expression: string, ...args: readonly XqdbInput[]): Promise<void> {
    ensureNativeSuccess(await this.#execute("asyn", expression, args));
  }

  public async receive(): Promise<XqdbValue> {
    return unwrapNativeValue(await this.#invoke((connector, permit) => connector.receive(permit)));
  }

  /**
   * Pages a caller-supplied q function `(offset; requestedRows; ...args)`. The function owns stable
   * dataset/snapshot semantics; this iterator creates no server cursor and never rewrites a query.
   */
  public async *batches(
    expression: string,
    batchSize = DEFAULT_BATCH_SIZE,
    ...args: readonly XqdbInput[]
  ): AsyncGenerator<Table<TypeMap> | XqdbQValue, void, undefined> {
    if (!Number.isSafeInteger(batchSize) || batchSize <= 0) {
      throw new RangeError("batchSize must be a positive safe integer");
    }
    if (args.length > MAX_BATCH_ARGUMENTS) {
      throw new RangeError(`batches accepts at most ${MAX_BATCH_ARGUMENTS} additional arguments`);
    }

    let offset = 0n;
    for (;;) {
      const value = await this.sync(expression, offset, BigInt(batchSize), ...args);
      let rows: number;
      if (isArrowTable(value)) {
        rows = value.numRows;
      } else if (value instanceof XqdbQValue && value.isTable) {
        rows = value.length;
      } else {
        throw conversionError("batches paging function must return exactly one q table");
      }
      if (rows > batchSize) {
        throw conversionError(
          `batches paging function returned ${rows} rows, exceeding requested ${batchSize}`,
        );
      }
      if (rows === 0) {
        return;
      }
      yield value;
      offset += BigInt(rows);
      if (rows < batchSize) {
        return;
      }
    }
  }
}
