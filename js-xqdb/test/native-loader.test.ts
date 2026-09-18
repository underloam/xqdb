import { describe, expect, it } from "vitest";

import { XqdbIOError } from "../src/errors.js";
import { loadNativeBinding } from "../src/native-loader.js";

function NativeConnector(): never {
  throw new Error("NativeConnector is not exercised by loader surface tests");
}

function generatedSurface(): object {
  return {
    NativeConnector,
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

describe("generated native loader integration", () => {
  it("loads the generated napi-rs export surface synchronously", () => {
    const moduleValue = generatedSurface();
    expect(loadNativeBinding(() => moduleValue)).toBe(moduleValue);
  });

  it("reports missing local or optional native binaries clearly", () => {
    const cause = new Error("Unsupported platform or native binary not found");

    try {
      loadNativeBinding(() => {
        throw cause;
      });
      throw new Error("Expected the loader to fail");
    } catch (error) {
      expect(error).toBeInstanceOf(XqdbIOError);
      if (!(error instanceof XqdbIOError)) {
        throw error;
      }
      expect(error).toMatchObject({ cause, code: "XQDB_NATIVE_LOAD" });
      expect(error.nativeMessage).toContain("npm run build:native");
      expect(error.nativeMessage).toContain("optional dependencies enabled");
      expect(error.nativeMessage).toContain(cause.message);
    }
  });

  it("rejects a generated loader with an incompatible declaration surface", () => {
    try {
      loadNativeBinding(() => ({ NativeConnector }));
      throw new Error("Expected incompatible exports to fail");
    } catch (error) {
      expect(error).toBeInstanceOf(XqdbIOError);
      if (!(error instanceof XqdbIOError)) {
        throw error;
      }
      expect(error.code).toBe("XQDB_NATIVE_LOAD");
      expect(error.cause).toBeInstanceOf(TypeError);
      if (!(error.cause instanceof TypeError)) {
        throw error.cause;
      }
      expect(error.cause.message).toBe(
        "The generated loader did not expose the expected native exports",
      );
    }
  });
});
