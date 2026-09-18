import type { Table, TypeMap } from "apache-arrow";
import { Buffer } from "node:buffer";

import {
  ensureNativeSuccess,
  isArrowTable,
  normalizeInput,
  unwrapNativeValue,
} from "./conversion.js";
import { XqdbError, conversionError, rejectionToIOError } from "./errors.js";
import { loadNativeBinding } from "./native-loader.js";
import type { NativeModule, NativeResult } from "./native-contract.js";
import { validatedSymbolEncoding } from "./types.js";
import type { XqdbInput, XqdbMessageType, XqdbSymbolEncoding, XqdbValue } from "./types.js";

export interface ReadBinary6Options {
  /** Decoding policy for symbols and strings that are not valid UTF-8; defaults to `"strict"`. */
  readonly symbolEncoding?: XqdbSymbolEncoding;
}

export interface Deserialize6Options {
  /** Decoding policy for convenient values; ignored by lossless bodies after validation. */
  readonly symbolEncoding?: XqdbSymbolEncoding;
  /** Return an immutable XqdbQValue retaining the exact q body. */
  readonly lossless?: boolean;
}

export interface DeserializedIpc6 {
  readonly messageType: XqdbMessageType;
  readonly value: XqdbValue;
}

async function invokeNativeHelper(
  operation: (binding: NativeModule) => Promise<NativeResult>,
): Promise<NativeResult> {
  try {
    const binding = loadNativeBinding();
    return await operation(binding);
  } catch (error) {
    if (error instanceof XqdbError) {
      throw error;
    }
    throw rejectionToIOError(error);
  }
}

export async function readBinary6(
  path: string,
  options: ReadBinary6Options = {},
): Promise<Table<TypeMap>> {
  if (typeof path !== "string") {
    throw new TypeError("readBinary6 path must be a string");
  }
  const symbolEncoding = validatedSymbolEncoding(options.symbolEncoding, "ReadBinary6Options");
  const result = await invokeNativeHelper((binding) => binding.readBinary6(path, symbolEncoding));
  const value = unwrapNativeValue(result);
  if (!isArrowTable(value)) {
    throw conversionError("readBinary6 returned a native value that was not a table");
  }
  return value;
}

export async function serializeAsIpcBytes6(
  messageType: XqdbMessageType,
  compress: boolean,
  value: XqdbInput,
): Promise<Buffer> {
  if (messageType !== "async" && messageType !== "sync" && messageType !== "response") {
    throw new RangeError('messageType must be "async", "sync", or "response"');
  }
  if (typeof compress !== "boolean") {
    throw new TypeError("compress must be a boolean");
  }
  const nativeValue = normalizeInput(value);
  const result = await invokeNativeHelper((binding) =>
    binding.serializeAsIpcBytes6(messageType, compress, nativeValue),
  );
  const bytes = unwrapNativeValue(result);
  if (!Buffer.isBuffer(bytes)) {
    throw conversionError("serializeAsIpcBytes6 returned a native value that was not bytes");
  }
  return bytes;
}

export async function deserializeValue6(
  body: Uint8Array,
  options: Deserialize6Options = {},
): Promise<XqdbValue> {
  if (!(body instanceof Uint8Array)) {
    throw new TypeError("deserializeValue6 body must be a Uint8Array");
  }
  const symbolEncoding = validatedSymbolEncoding(options.symbolEncoding, "Deserialize6Options");
  const { lossless } = options;
  if (lossless !== undefined && typeof lossless !== "boolean") {
    throw new TypeError("Deserialize6Options.lossless must be a boolean");
  }
  const snapshot = Buffer.from(body);
  const result = await invokeNativeHelper((binding) =>
    binding.deserializeValue6(snapshot, symbolEncoding, lossless),
  );
  return unwrapNativeValue(result);
}

export async function deserializeIpcBytes6(
  frame: Uint8Array,
  options: Deserialize6Options = {},
): Promise<DeserializedIpc6> {
  if (!(frame instanceof Uint8Array)) {
    throw new TypeError("deserializeIpcBytes6 frame must be a Uint8Array");
  }
  const symbolEncoding = validatedSymbolEncoding(options.symbolEncoding, "Deserialize6Options");
  const { lossless } = options;
  if (lossless !== undefined && typeof lossless !== "boolean") {
    throw new TypeError("Deserialize6Options.lossless must be a boolean");
  }
  const snapshot = Buffer.from(frame);
  const result = await invokeNativeHelper((binding) =>
    binding.deserializeIpcBytes6(snapshot, symbolEncoding, lossless),
  );
  ensureNativeSuccess(result);
  if (
    result.messageType !== "async" &&
    result.messageType !== "sync" &&
    result.messageType !== "response"
  ) {
    throw conversionError("Native IPC decoder omitted a valid messageType");
  }
  return {
    messageType: result.messageType,
    value: unwrapNativeValue(result),
  };
}
