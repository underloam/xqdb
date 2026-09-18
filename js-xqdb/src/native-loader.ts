import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

import { XqdbIOError } from "./errors.js";
import type { NativeModule } from "./native-contract.js";

export type NativeImporter = () => unknown;

const require = createRequire(import.meta.url);
const generatedNativePath = fileURLToPath(new URL("../native.cjs", import.meta.url));

function importGeneratedNative(): unknown {
  return require(generatedNativePath);
}

function isNativeModule(value: unknown): value is NativeModule {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  return (
    "NativeConnector" in value &&
    typeof value.NativeConnector === "function" &&
    "readBinary6" in value &&
    typeof value.readBinary6 === "function" &&
    "serializeAsIpcBytes6" in value &&
    typeof value.serializeAsIpcBytes6 === "function" &&
    "deserializeValue6" in value &&
    typeof value.deserializeValue6 === "function" &&
    "deserializeIpcBytes6" in value &&
    typeof value.deserializeIpcBytes6 === "function" &&
    "qValueFromBytes" in value &&
    typeof value.qValueFromBytes === "function" &&
    "qValueAtom" in value &&
    typeof value.qValueAtom === "function" &&
    "qValueList" in value &&
    typeof value.qValueList === "function" &&
    "qValueDictionary" in value &&
    typeof value.qValueDictionary === "function" &&
    "qValueFromNative" in value &&
    typeof value.qValueFromNative === "function"
  );
}

export function loadNativeBinding(importer: NativeImporter = importGeneratedNative): NativeModule {
  try {
    const moduleValue = importer();
    if (!isNativeModule(moduleValue)) {
      throw new TypeError("The generated loader did not expose the expected native exports");
    }
    return moduleValue;
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    throw new XqdbIOError(
      "XQDB_NATIVE_LOAD",
      "Unable to load the xqdb native addon. For a local checkout run " +
        "`npm run build:native`; for an installed package, reinstall with optional " +
        `dependencies enabled. Loader detail: ${detail}`,
      { cause: error },
    );
  }
}
