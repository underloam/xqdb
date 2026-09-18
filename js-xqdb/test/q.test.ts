import { beforeEach, describe, expect, it, vi } from "vitest";
import { XqdbIOError } from "../src/errors.js";
import { Q } from "../src/q.js";

import type {
  NativeAdmission,
  NativeConnector,
  NativeModule,
  NativeOptions,
  NativePermit,
  NativeResult,
  NativeValue,
} from "../src/native-contract.js";
import type { XqdbInput } from "../src/types.js";

const loaderMock = vi.hoisted(() => ({
  loadNativeBinding: vi.fn<() => NativeModule>(),
}));

vi.mock(import("../src/native-loader.js"), () => ({
  loadNativeBinding: loaderMock.loadNativeBinding,
}));

let connectResults: NativeResult[];
let syncResultPromise: Promise<NativeResult> | undefined;

class MockNativeConnector implements NativeConnector {
  public reserve(): NativeAdmission {
    return { ok: true, permit: {} };
  }

  public release(_permit: NativePermit): NativeResult {
    return { ok: true };
  }

  public cancel(): NativeResult {
    return { ok: true };
  }

  public connect(_permit: NativePermit, _retries: number): Promise<NativeResult> {
    return Promise.resolve(connectResults.shift() ?? { ok: true });
  }

  public disconnect(_permit: NativePermit): Promise<NativeResult> {
    return Promise.resolve({ ok: true });
  }

  public sync(
    _permit: NativePermit,
    _expression: string,
    _args: NativeValue[],
  ): Promise<NativeResult> {
    if (syncResultPromise !== undefined) {
      return syncResultPromise;
    }
    return Promise.resolve({
      ok: true,
      value: { bigintValue: 42n, tag: "i64" },
    });
  }

  public asyn(
    _permit: NativePermit,
    _expression: string,
    _args: NativeValue[],
  ): Promise<NativeResult> {
    return Promise.resolve({ ok: true });
  }

  public receive(_permit: NativePermit): Promise<NativeResult> {
    return Promise.resolve({ ok: true, value: { stringValue: "update", tag: "symbol" } });
  }
}

function nativeModule(): NativeModule {
  return {
    NativeConnector: MockNativeConnector,
    deserializeIpcBytes6: () => Promise.resolve({ ok: true }),
    deserializeValue6: () => Promise.resolve({ ok: true }),
    qValueAtom: () => Promise.resolve({ ok: true }),
    qValueDictionary: () => Promise.resolve({ ok: true }),
    qValueFromBytes: () => Promise.resolve({ ok: true }),
    qValueFromNative: () => Promise.resolve({ ok: true }),
    qValueList: () => Promise.resolve({ ok: true }),
    readBinary6: () => Promise.resolve({ ok: true }),
    serializeAsIpcBytes6: () => Promise.resolve({ ok: true }),
  };
}

describe("q native delegation", () => {
  beforeEach(() => {
    connectResults = [];
    syncResultPromise = undefined;
    loaderMock.loadNativeBinding.mockReset();
    loaderMock.loadNativeBinding.mockReturnValue(nativeModule());
  });

  it("rejects invalid timeouts, capacities, and unpaired client credentials", () => {
    // @ts-expect-error Runtime validation must reject non-string hosts.
    expect(() => new Q({ host: 42, port: 1800 })).toThrow(TypeError);

    for (const options of [
      { timeout: -Number.MIN_VALUE },
      { timeout: 86_400_001 },
      { timeout: Number.NaN },
      { timeout: Number.POSITIVE_INFINITY },
      { connectTimeout: -1 },
      { readTimeout: 86_400_001 },
      { writeTimeout: Number.NaN },
      { queueCapacity: 0 },
      { queueCapacity: 1025 },
      { maxArgumentBytes: Number.MAX_SAFE_INTEGER + 1 },
      { maxQueuedBytes: 1.5 },
      { tlsCert: "cert" },
      { tlsKey: "key" },
      { tls: false, tlsCa: "ca" },
    ]) {
      expect(() => new Q({ host: "localhost", port: 1800, ...options })).toThrow(RangeError);
    }
  });

  it("reads each option once without crossing an earlier validation gate", async () => {
    let portTouched = false;
    const invalidHost = {
      host: 42,
      get port(): number {
        portTouched = true;
        return 1800;
      },
    };
    // @ts-expect-error Runtime validation must reject non-string hosts.
    expect(() => new Q(invalidHost)).toThrow(TypeError);
    expect(portTouched).toBe(false);

    let retryReads = 0;
    const options = {
      host: "localhost",
      port: 1800,
      get retries(): number {
        retryReads += 1;
        return retryReads === 1 ? 0 : -1;
      },
    };
    await new Q(options).connect();
    expect(retryReads).toBe(1);
  });

  it("reports cyclic argument conversion as XQDB_CONVERSION", async () => {
    const cyclic: XqdbInput[] = [];
    cyclic.push(cyclic);
    const q = new Q({ host: "127.0.0.1", port: 1800 });

    await expect(q.sync("{x}", cyclic)).rejects.toMatchObject({
      code: "XQDB_CONVERSION",
    });
  });

  it("preserves caller conversion exceptions", async () => {
    const q = await Q.connect({ host: "127.0.0.1", port: 1800 });
    const sentinel = new Error("getter sentinel");
    const argument = new Proxy(
      {},
      {
        ownKeys() {
          throw sentinel;
        },
      },
    );

    await expect(q.sync("{x}", argument)).rejects.toBe(sentinel);
  });

  it("rejects more than eight arguments before normalizing any argument", async () => {
    const q = await Q.connect({ host: "127.0.0.1", port: 1800 });
    let touched = false;
    const argument = new Proxy(
      {},
      {
        ownKeys() {
          touched = true;
          return [];
        },
      },
    );

    await expect(
      q.sync("tooMany", ...Array.from({ length: 9 }, () => argument)),
    ).rejects.toMatchObject({
      code: "XQDB_CONVERSION",
    });
    expect(touched).toBe(false);
  });

  it("rejects invalid expressions before normalizing arguments", async () => {
    const q = new Q({ host: "127.0.0.1", port: 1800 });
    let touched = false;
    const argument = new Proxy(
      {},
      {
        ownKeys() {
          touched = true;
          return [];
        },
      },
    );

    // @ts-expect-error Runtime validation must reject non-string expressions.
    await expect(q.sync(42, argument)).rejects.toBeInstanceOf(TypeError);
    expect(touched).toBe(false);
  });

  it("caches terminal native-loader failures before later argument conversion", async () => {
    loaderMock.loadNativeBinding.mockImplementation(() => {
      throw new Error("missing addon");
    });
    let reads = 0;
    const argument = {
      get value(): number {
        reads += 1;
        return 1;
      },
    };
    const q = new Q({ host: "127.0.0.1", port: 1800 });

    await expect(q.sync("{x}", argument)).rejects.toBeInstanceOf(XqdbIOError);
    await expect(q.sync("{x}", argument)).rejects.toBeInstanceOf(XqdbIOError);
    expect(reads).toBe(0);
  });

  it("does not load the addon when disconnecting or cancelling an unused instance", async () => {
    const q = new Q({ host: "localhost", port: 1800 });
    await q.disconnect();
    await q.cancel();
    expect(loaderMock.loadNativeBinding).not.toHaveBeenCalled();
  });

  it("maps authentication failures without involving JS retry policy", async () => {
    connectResults = [{ error: { code: "XQDB_AUTH", message: "access denied" }, ok: false }];
    await expect(new Q({ host: "localhost", port: 1800 }).connect()).rejects.toMatchObject({
      code: "XQDB_AUTH",
    });
  });

  it("maps permanent native-loader failures", async () => {
    loaderMock.loadNativeBinding.mockImplementation(() => {
      throw new XqdbIOError("XQDB_NATIVE_LOAD", "missing addon");
    });
    await expect(
      new Q({ host: "localhost", port: 1800, retries: 5 }).connect(),
    ).rejects.toMatchObject({ code: "XQDB_NATIVE_LOAD" });
  });

  it("preserves and does not cache native InvalidArg constructor failures", async () => {
    let constructions = 0;
    class InvalidConnector extends MockNativeConnector {
      public constructor(_options: NativeOptions) {
        super();
        constructions += 1;
        throw Object.assign(new Error("invalid TLS server name"), { code: "InvalidArg" });
      }
    }
    loaderMock.loadNativeBinding.mockReturnValue({
      ...nativeModule(),
      NativeConnector: InvalidConnector,
    });
    const q = new Q({ host: "localhost", port: 1800, retries: 100 });

    await expect(q.connect()).rejects.toMatchObject({ code: "XQDB_CONVERSION" });
    await expect(q.connect()).rejects.toMatchObject({ code: "XQDB_CONVERSION" });
    expect(constructions).toBe(2);
    expect(loaderMock.loadNativeBinding).toHaveBeenCalledTimes(2);
  });

  it("preserves rejected native calls as IO errors", async () => {
    syncResultPromise = Promise.reject(new Error("worker stopped"));
    const q = new Q({ host: "localhost", port: 1800 });
    await expect(q.sync("1+1")).rejects.toBeInstanceOf(XqdbIOError);
  });
});
