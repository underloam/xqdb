import { beforeEach, describe, expect, it, vi } from "vitest";
import { XqdbQValue } from "../src/qvalue.js";
import type {
  NativeAdmission,
  NativeConnector,
  NativeModule,
  NativePermit,
  NativeResult,
} from "../src/native-contract.js";

type QValueNativeBinding = Pick<NativeModule, "qValueFromBytes">;

const loaderMock = vi.hoisted(() => ({
  loadNativeBinding: vi.fn<() => NativeModule>(),
}));

vi.mock(import("../src/native-loader.js"), () => ({
  loadNativeBinding: loaderMock.loadNativeBinding,
}));
function unexpectedNativeCall(): never {
  throw new Error("Unexpected native operation in QValue test");
}

class UnexpectedNativeConnector implements NativeConnector {
  public reserve(): NativeAdmission {
    return unexpectedNativeCall();
  }

  public release(_permit: NativePermit): NativeResult {
    return unexpectedNativeCall();
  }

  public cancel(): NativeResult {
    return unexpectedNativeCall();
  }

  public connect(): Promise<NativeResult> {
    return unexpectedNativeCall();
  }

  public disconnect(): Promise<NativeResult> {
    return unexpectedNativeCall();
  }

  public sync(): Promise<NativeResult> {
    return unexpectedNativeCall();
  }

  public asyn(): Promise<NativeResult> {
    return unexpectedNativeCall();
  }

  public receive(): Promise<NativeResult> {
    return unexpectedNativeCall();
  }
}

function qValueNativeModule(binding: QValueNativeBinding): NativeModule {
  return {
    NativeConnector: UnexpectedNativeConnector,
    deserializeIpcBytes6: unexpectedNativeCall,
    deserializeValue6: unexpectedNativeCall,
    qValueAtom: unexpectedNativeCall,
    qValueDictionary: unexpectedNativeCall,
    qValueFromBytes: binding.qValueFromBytes,
    qValueFromNative: unexpectedNativeCall,
    qValueList: unexpectedNativeCall,
    readBinary6: unexpectedNativeCall,
    serializeAsIpcBytes6: unexpectedNativeCall,
  };
}

describe("lossless q values", () => {
  beforeEach(() => {
    loaderMock.loadNativeBinding.mockReset();
  });

  it("reports preprocessing failures as Promise rejections rather than synchronous throws", async () => {
    const invalidFactories: (() => Promise<XqdbQValue>)[] = [
      () => XqdbQValue.byte(256),
      () => XqdbQValue.short(32_768),
      () => XqdbQValue.int(2_147_483_648),
      () => XqdbQValue.long(1n << 63n),
      () => XqdbQValue.real(1e100),
      () =>
        // @ts-expect-error Runtime validation must reject non-number inputs.
        XqdbQValue.float("not a number"),
      () => XqdbQValue.char(256),
      () => XqdbQValue.timestamp(1n << 63n),
      () => XqdbQValue.month(2_147_483_648),
      () => XqdbQValue.date(2_147_483_648),
      () =>
        // @ts-expect-error Runtime validation must reject non-number inputs.
        XqdbQValue.datetime("not a number"),
      () => XqdbQValue.timespan(1n << 63n),
      () => XqdbQValue.minute(2_147_483_648),
      () => XqdbQValue.second(2_147_483_648),
      () => XqdbQValue.time(2_147_483_648),
    ];

    for (const factory of invalidFactories) {
      const captured: { promise?: Promise<XqdbQValue> } = {};
      expect(() => {
        captured.promise = factory();
      }).not.toThrow();
      if (captured.promise === undefined) {
        throw new Error("XqdbQValue factory did not return a promise");
      }
      await expect(captured.promise).rejects.toMatchObject({
        code: "XQDB_CONVERSION",
      });
    }
  });

  it("surfaces native structural validation errors and rejects forged instances", async () => {
    loaderMock.loadNativeBinding.mockReturnValue(
      qValueNativeModule({
        qValueFromBytes: () =>
          Promise.resolve({
            error: { code: "XQDB_CONVERSION", message: "truncated q body" },
            ok: false,
          }),
      }),
    );
    await expect(XqdbQValue.fromBytes(Uint8Array.of(249))).rejects.toMatchObject({
      code: "XQDB_CONVERSION",
    });
    const forged: unknown = Object.create(XqdbQValue.prototype);
    if (!(forged instanceof XqdbQValue)) {
      throw new TypeError("Forged value did not retain the XqdbQValue prototype");
    }
    try {
      Reflect.construct(XqdbQValue, [{}]);
      throw new Error("Expected reflected construction to fail");
    } catch (error) {
      expect(error).toMatchObject({ code: "XQDB_CONVERSION" });
    }
    await expect(XqdbQValue.from(forged)).rejects.toMatchObject({
      code: "XQDB_CONVERSION",
    });
  });
});
