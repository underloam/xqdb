import { Table, tableFromArrays, tableToIPC } from "apache-arrow";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { readBinary6, serializeAsIpcBytes6 } from "../src/helpers.js";
import type {
  NativeAdmission,
  NativeConnector,
  NativeModule,
  NativePermit,
  NativeResult,
} from "../src/native-contract.js";

type HelperNativeBinding = Pick<NativeModule, "readBinary6" | "serializeAsIpcBytes6">;

const loaderMock = vi.hoisted(() => ({
  loadNativeBinding: vi.fn<() => NativeModule>(),
}));

vi.mock(import("../src/native-loader.js"), () => ({
  loadNativeBinding: loaderMock.loadNativeBinding,
}));
function unexpectedNativeCall(): never {
  throw new Error("Unexpected native operation in helper test");
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

function helperNativeModule(helpers: HelperNativeBinding): NativeModule {
  return {
    NativeConnector: UnexpectedNativeConnector,
    deserializeIpcBytes6: unexpectedNativeCall,
    deserializeValue6: unexpectedNativeCall,
    qValueAtom: unexpectedNativeCall,
    qValueDictionary: unexpectedNativeCall,
    qValueFromBytes: unexpectedNativeCall,
    qValueFromNative: unexpectedNativeCall,
    qValueList: unexpectedNativeCall,
    readBinary6: helpers.readBinary6,
    serializeAsIpcBytes6: helpers.serializeAsIpcBytes6,
  };
}

describe("binary helpers", () => {
  beforeEach(() => {
    loaderMock.loadNativeBinding.mockReset();
  });

  it("materializes readBinary6 output as an Arrow Table", async () => {
    const table = tableFromArrays({ size: BigInt64Array.of(10n, 20n) });
    loaderMock.loadNativeBinding.mockReturnValue(
      helperNativeModule({
        readBinary6: () =>
          Promise.resolve({
            ok: true,
            value: { bytesValue: tableToIPC(table, "stream"), tag: "table" },
          }),
        serializeAsIpcBytes6: () => Promise.resolve({ ok: true }),
      }),
    );

    const decoded = await readBinary6("trade.bin");

    expect(decoded).toBeInstanceOf(Table);
    const size = decoded.getChild("size");
    if (size === null) {
      throw new Error("Decoded table omitted its size column");
    }
    expect([...size]).toStrictEqual([10n, 20n]);
  });

  it("validates symbolEncoding before loading the native addon", async () => {
    // @ts-expect-error Runtime validation must reject unsupported encodings.
    await expect(readBinary6("trade.bin", { symbolEncoding: "latin1" })).rejects.toThrow(
      RangeError,
    );
    expect(loaderMock.loadNativeBinding).not.toHaveBeenCalled();
  });

  it("validates frame options before touching the value or native loader", async () => {
    let touched = false;
    const value = new Proxy(
      {},
      {
        ownKeys() {
          touched = true;
          throw new Error("value must not be normalized");
        },
      },
    );

    // @ts-expect-error Runtime validation must reject unsupported message types.
    await expect(serializeAsIpcBytes6("invalid", true, value)).rejects.toBeInstanceOf(RangeError);
    // @ts-expect-error Runtime validation must reject non-boolean compression flags.
    await expect(serializeAsIpcBytes6("sync", "yes", value)).rejects.toBeInstanceOf(TypeError);
    expect(touched).toBe(false);
    expect(loaderMock.loadNativeBinding).not.toHaveBeenCalled();
  });
});
