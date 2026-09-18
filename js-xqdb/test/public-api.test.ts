import { describe, expect, it } from "vitest";

import * as publicApi from "../src/index.js";

describe("public facade surface", () => {
  it("exports only the facade runtime and keeps native internals private", () => {
    const names = Object.keys(publicApi);
    names.sort();
    expect(names).toStrictEqual([
      "Q",
      "XqdbAuthError",
      "XqdbDate",
      "XqdbError",
      "XqdbIOError",
      "XqdbQLambda",
      "XqdbQOperator",
      "XqdbQValue",
      "XqdbTime",
      "XqdbTimespan",
      "XqdbTimestamp",
      "deserializeIpcBytes6",
      "deserializeValue6",
      "readBinary6",
      "serializeAsIpcBytes6",
    ]);
  });
});
