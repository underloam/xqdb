import { describe, expect, it } from "vitest";

import {
  XqdbAuthError,
  XqdbError,
  XqdbIOError,
  mapNativeError,
  rejectionToIOError,
} from "../src/errors.js";

describe("stable public errors", () => {
  it("maps native IO and authentication codes to stable subclasses", () => {
    const ioPayload = { code: "XQDB_IO", message: "connection reset" };
    const authPayload = { code: "XQDB_AUTH", message: "access denied" };

    const ioError = mapNativeError(ioPayload);
    expect(ioError).toBeInstanceOf(XqdbIOError);
    expect(ioError).toMatchObject({
      cause: ioPayload,
      code: "XQDB_IO",
      name: "XqdbIOError",
      nativeMessage: "connection reset",
    });

    const authError = mapNativeError(authPayload);
    expect(authError).toBeInstanceOf(XqdbAuthError);
    expect(authError).toMatchObject({
      cause: authPayload,
      code: "XQDB_AUTH",
      name: "XqdbAuthError",
      nativeMessage: "access denied",
    });
  });

  it.each(["XQDB_SERVER", "XQDB_CONVERSION", "XQDB_UNSUPPORTED", "XQDB_ERROR"])(
    "keeps native code %s on XqdbError",
    (code) => {
      const payload = { code, message: "native detail" };
      const error = mapNativeError(payload);
      expect(error).toBeInstanceOf(XqdbError);
      expect(error).not.toBeInstanceOf(XqdbIOError);
      expect(error).toMatchObject({ cause: payload, code, nativeMessage: "native detail" });
    },
  );

  it("preserves rejected native exceptions as causes", () => {
    const cause = new Error("worker channel closed");
    const error = rejectionToIOError(cause);

    expect(error).toBeInstanceOf(XqdbIOError);
    expect(error).toMatchObject({
      cause,
      code: "XQDB_IO",
      nativeMessage: "worker channel closed",
    });
  });
});
