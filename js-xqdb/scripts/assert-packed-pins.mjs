// Asserts that a packed root tarball declares every platform package at an exact
// Version. This is the gate that makes pack-time injection safe: the publish job
// Declares `needs: [source, assemble]`, so failing here prevents a root package
// Shipping without its optionalDependencies.
//
// Usage: node scripts/assert-packed-pins.mjs <tarball> <version>

import { execFileSync } from "node:child_process";
import { readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const TAR_EXTRACT_MAX_BYTES = 8_388_608;
const [tarball, version] = process.argv.slice(2);
if (tarball === undefined || tarball === "" || version === undefined || version === "") {
  throw new Error("usage: assert-packed-pins.mjs <tarball> <version>");
}

/**
 * @param {unknown} value
 * @returns {value is Record<string, unknown>}
 */
function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

const packageDir = dirname(dirname(fileURLToPath(import.meta.url)));
const expected = readdirSync(join(packageDir, "npm"), { withFileTypes: true })
  .filter((entry) => entry.isDirectory())
  .map((entry) => `@xbbg/xqdb-${entry.name}`);
expected.sort();

if (expected.length === 0) {
  throw new Error("npm/ declares no platform packages");
}

const raw = execFileSync("tar", ["-xzOf", tarball, "package/package.json"], {
  encoding: "utf8",
  maxBuffer: TAR_EXTRACT_MAX_BYTES,
});
/** @type {unknown} */
const manifest = JSON.parse(raw);
const packedVersion = isRecord(manifest) ? manifest.version : undefined;
if (packedVersion !== version) {
  throw new Error(`packed version ${String(packedVersion)} is not ${version}`);
}

const declaredDependencies = isRecord(manifest) ? manifest.optionalDependencies : undefined;
const actual = isRecord(declaredDependencies) ? declaredDependencies : {};
const actualNames = Object.keys(actual);
actualNames.sort();
if (
  actualNames.length !== expected.length ||
  actualNames.some((name, index) => name !== expected[index])
) {
  throw new Error(
    `packed optionalDependencies ${JSON.stringify(actualNames)} does not match ${JSON.stringify(expected)}`,
  );
}

for (const name of expected) {
  if (actual[name] !== version) {
    throw new Error(`packed ${name} is ${String(actual[name])}, expected exactly ${version}`);
  }
}

process.stdout.write(`packed tarball pins ${expected.length} platform packages at ${version}\n`);
