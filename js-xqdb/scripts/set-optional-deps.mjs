// Writes the exact platform pins into package.json immediately before `npm pack`.
//
// The committed manifest deliberately carries no `optionalDependencies`: the pins
// Name a version the registry does not serve until the natives publish, which makes
// The committed tree — and therefore the release tag — fail `npm ci`. Injecting at
// Pack time keeps the tag installable while still shipping exact pins to consumers.
//
// This is not `napi prepublish`: that command validates that every npm/<target>/
// Directory already contains its .node file, which is untrue in the assemble job,
// Where only the packed platform tarballs are present.
//
// Safety rests on the assertion that runs after `npm pack`. The publish job declares
// `needs: [source, assemble]`, so if either this script or that assertion fails, no
// Publish happens.

import { readFile, readdir, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const JSON_INDENT = 2;
const packageDir = dirname(dirname(fileURLToPath(import.meta.url)));
const rootPath = join(packageDir, "package.json");
const npmDir = join(packageDir, "npm");

/**
 * @param {unknown} value
 * @returns {value is Record<string, unknown>}
 */
function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/**
 * @param {string} path
 * @returns {Promise<unknown>}
 */
async function readJson(path) {
  const text = await readFile(path, "utf8");
  /** @type {unknown} */
  const value = JSON.parse(text);
  return value;
}

const root = await readJson(rootPath);
if (
  !isRecord(root) ||
  typeof root.name !== "string" ||
  typeof root.version !== "string" ||
  !isRecord(root.napi) ||
  !Array.isArray(root.napi.targets) ||
  root.napi.targets.length === 0
) {
  throw new Error("package.json is missing name, version, or napi.targets");
}
const expectedCount = root.napi.targets.length;

const directoryEntries = await readdir(npmDir, { withFileTypes: true });
const entries = directoryEntries.filter((entry) => entry.isDirectory()).map((entry) => entry.name);
entries.sort();

if (entries.length !== expectedCount) {
  throw new Error(
    `npm/ has ${entries.length} platform directories but napi.targets declares ${expectedCount}`,
  );
}

/** @type {Record<string, string>} */
const optionalDependencies = {};
for (const entry of entries) {
  const manifest = await readJson(join(npmDir, entry, "package.json"));
  if (
    !isRecord(manifest) ||
    typeof manifest.name !== "string" ||
    !manifest.name.startsWith(`${root.name}-`)
  ) {
    throw new Error(
      `npm/${entry} name ${isRecord(manifest) ? String(manifest.name) : String(manifest)} is not a ${root.name} platform package`,
    );
  }
  if (manifest.version !== root.version) {
    throw new Error(
      `npm/${entry} version ${String(manifest.version)} does not match root version ${root.version}`,
    );
  }
  optionalDependencies[manifest.name] = root.version;
}

root.optionalDependencies = optionalDependencies;
await writeFile(rootPath, `${JSON.stringify(root, null, JSON_INDENT)}\n`);

const pins = Object.entries(optionalDependencies)
  .map(([name, version]) => `${name}@${version}`)
  .join(", ");
process.stdout.write(`set optionalDependencies: ${pins}\n`);
