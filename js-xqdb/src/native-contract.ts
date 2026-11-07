interface NativeConnectionOptions {
  readonly user?: string;
  readonly password?: string;
  readonly timeoutMilliseconds?: number;
  readonly symbolEncoding?: string;
  readonly lossless?: boolean;
  readonly compression?: string;
  readonly compressionThreshold?: number;
  readonly connectTimeoutMilliseconds?: number;
  readonly readTimeoutMilliseconds?: number;
  readonly writeTimeoutMilliseconds?: number;
  readonly maxMessageBytes?: number;
  readonly maxPendingNotifications?: number;
  readonly queueCapacity?: number;
  readonly maxArgumentBytes?: number;
  readonly maxQueuedBytes?: number;
}

export interface NativeTcpEndpoint {
  readonly host: string;
  readonly port: number;
  readonly unixSocket?: never;
  readonly tls?: boolean;
  readonly tlsCa?: string;
  readonly tlsCert?: string;
  readonly tlsKey?: string;
  readonly tlsServerName?: string;
}

/** The addon independently rejects `unixSocket` beside TCP endpoint or TLS fields. */
export interface NativeUnixSocketEndpoint {
  readonly unixSocket: string;
  readonly host?: never;
  readonly port?: never;
  readonly tls?: never;
  readonly tlsCa?: never;
  readonly tlsCert?: never;
  readonly tlsKey?: never;
  readonly tlsServerName?: never;
}

export type NativeEndpoint = NativeTcpEndpoint | NativeUnixSocketEndpoint;

export type NativeOptions = NativeConnectionOptions & NativeEndpoint;

export interface NativeEntry {
  readonly key: string;
  readonly value: NativeValue;
}

export interface NativeValue {
  readonly tag: string;
  readonly boolValue?: boolean;
  readonly numberValue?: number;
  readonly bigintValue?: bigint;
  readonly stringValue?: string;
  readonly context?: string;
  readonly bytesValue?: Uint8Array;
  readonly items?: NativeValue[];
  readonly entries?: NativeEntry[];
  readonly typeCode?: number;
  readonly length?: number;
  readonly isTable?: boolean;
}

export interface NativeError {
  readonly code: string;
  readonly message: string;
}

export interface NativeResult {
  readonly ok: boolean;
  readonly value?: NativeValue;
  readonly error?: NativeError;
  readonly messageType?: string;
}

export type NativePermit = object;

export interface NativeAdmission {
  readonly ok: boolean;
  readonly permit?: NativePermit;
  readonly error?: NativeError;
}

export interface NativeConnector {
  reserve(): NativeAdmission;
  release(permit: NativePermit): NativeResult;
  cancel(): NativeResult;
  connect(permit: NativePermit, retries: number): Promise<NativeResult>;
  disconnect(permit: NativePermit): Promise<NativeResult>;
  sync(permit: NativePermit, expression: string, args: NativeValue[]): Promise<NativeResult>;
  asyn(permit: NativePermit, expression: string, args: NativeValue[]): Promise<NativeResult>;
  receive(permit: NativePermit): Promise<NativeResult>;
}

export type NativeConnectorConstructor = new (options: NativeOptions) => NativeConnector;

export interface NativeModule {
  readonly NativeConnector: NativeConnectorConstructor;
  readBinary6(path: string, symbolEncoding?: string): Promise<NativeResult>;
  serializeAsIpcBytes6(
    messageType: "async" | "sync" | "response",
    compress: boolean,
    value: NativeValue,
  ): Promise<NativeResult>;
  deserializeValue6(
    body: Uint8Array,
    symbolEncoding?: string,
    lossless?: boolean,
  ): Promise<NativeResult>;
  deserializeIpcBytes6(
    frame: Uint8Array,
    symbolEncoding?: string,
    lossless?: boolean,
  ): Promise<NativeResult>;
  qValueFromBytes(bytes: Uint8Array): Promise<NativeResult>;
  qValueAtom(kind: number, payload: Uint8Array): Promise<NativeResult>;
  qValueList(values: NativeValue[]): Promise<NativeResult>;
  qValueDictionary(keys: NativeValue, values: NativeValue): Promise<NativeResult>;
  qValueFromNative(value: NativeValue): Promise<NativeResult>;
}
