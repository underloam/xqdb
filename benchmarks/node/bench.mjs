// Node.js q IPC client comparison against a single fixed KDB-X fixture.
//
// Subjects are measured round-robin with a rotating start position so machine
// And server drift is shared instead of accumulating on one subject. Every
// Subject is configured in its fastest documented mode.
//
// Fidelity is a preflight, not an afterthought:
//
//   * `scalar` is a value-exact latency floor, so a subject that cannot return
//     The exact value is excluded from it.
//   * `read.<table>` decodes an identical server byte stream into each
//     Library's documented representation, so every subject is ranked. Timed
//     Validation checks shape, not content fidelity. `roundTripStates`
//     Separately labels the decode-then-encode check as `identical`, `differs`,
//     `resized`, or `unverified`; an encoder failure is never reported as
//     Decode loss.
//   * `send.<table>` is only comparable when the subject's own decoded value
//     Re-encodes to a q-identical value of the same canonical size; otherwise
//     The subject would be timed on different work, so it is listed in
//     `unsupported` with no samples, throughput, or ratio.
//
// Int64 and nanosecond-timestamp exactness are reported by the preflight rather
// Than timed: they are correctness claims, and timing a wrong decode against a
// Right one would compare different work.

import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { mkdir, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { cpus, homedir, hostname, platform as osPlatform, release, userInfo } from "node:os";
import { dirname, relative, resolve, sep } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { promisify } from "node:util";

import { Q, XqdbTimestamp } from "../../js-xqdb/dist/index.js";
/**
 * @typedef {"identical" | "differs" | "resized" | "unverified"} RoundTripState
 * @typedef {{ columns: number, rows: number }} Shape
 * @typedef {{ name: string, version: string }} PackageIdentity
 * @typedef {{
 *   host: string,
 *   iterations: number,
 *   memoryResults: number,
 *   output: string | undefined,
 *   port: number,
 *   samples: boolean,
 *   seed: number,
 *   warmups: number,
 * }} BenchmarkOptions
 * @typedef {{ host: string, port: number }} ConnectionOptions
 * @typedef {{
 *   apply: (lambda: string, value: unknown) => Promise<unknown>,
 *   close: () => Promise<void>,
 *   config: Record<string, boolean>,
 *   eval: (expression: string) => Promise<unknown>,
 *   id: string,
 *   implementation: string,
 *   longOf: (value: unknown) => bigint | null | Promise<bigint | null>,
 *   notes: string[],
 *   package: string,
 *   read: (table: string) => Promise<unknown>,
 *   representation: string,
 *   shapeOf: (value: unknown) => Shape | null,
 *   timestampNanosOf: (value: unknown) => bigint | null | Promise<bigint | null>,
 *   timestampProbeMode: string,
 *   version: string,
 * }} Subject
 * @typedef {(options: ConnectionOptions) => Promise<Subject>} SubjectFactory
 * @typedef {{
 *   bytes: number,
 *   path: string,
 *   sha256: string,
 * }} ArtifactFile
 * @typedef {{
 *   contentDigest: { algorithm: string, framing: string, value: string },
 *   files: ArtifactFile[],
 * }} ArtifactTree
 * @typedef {{
 *   javascriptBuild: ArtifactTree,
 *   loadedNativeAddon: { bytes: number, sha256: string },
 *   packageVersion: string,
 * }} ArtifactProvenance
 * @typedef {{
 *   contentDigest: {
 *     algorithm: string,
 *     fileCount: number,
 *     framing: string,
 *     includesTrackedAndUntrackedFiles: boolean,
 *     pathspecs: string[],
 *     value: string,
 *   },
 *   describe: string,
 *   dirty: boolean,
 *   identity: string,
 *   revision: string,
 *   version: string,
 * }} SourceProvenance
 * @typedef {{ canonicalBytes: number, columns: number }} FixtureTable
 * @typedef {{
 *   connection: "loopback" | "network",
 *   qVersion: number,
 *   rows: number,
 *   seed: number,
 *   tables: Record<string, FixtureTable>,
 * }} Fixture
 * @typedef {{
 *   canonicalBytesMatchFixture: boolean,
 *   encodeError?: string,
 *   reencodedCanonicalBytes?: number,
 *   reencodedRows?: number,
 *   reencodesToIdenticalQValue: boolean,
 *   roundTrip?: RoundTripState,
 *   sendComparable?: boolean,
 *   sendExcludedBecause?: string,
 *   shape: Shape | null,
 *   shapeMatchesFixture: boolean,
 * }} TableFidelity
 * @typedef {{
 *   int64Decoded: string | null,
 *   int64Exact: boolean,
 *   nanosecondTimestampDecoded: string | null,
 *   nanosecondTimestampExact: boolean,
 *   nanosecondTimestampMode: string,
 *   scalarDetail: string,
 *   scalarExact: boolean,
 *   tables: Record<string, TableFidelity>,
 * }} FidelityReport
 * @typedef {{
 *   iterations: number,
 *   maxMs: number,
 *   meanMs: number,
 *   medianMibPerSecond?: number,
 *   medianMs: number,
 *   minMs: number,
 *   p90Ms: number,
 *   p99Ms: number,
 *   payloadBytes?: number,
 *   samplesMs?: number[],
 * }} OperationMetrics
 * @typedef {{
 *   medianRatioVsXqdb: Record<string, number>,
 *   payloadBytes: number | null,
 *   readValidation?: string,
 *   roundTripReasons?: Record<string, string | undefined>,
 *   roundTripStates?: Record<string, RoundTripState>,
 *   roundTripUnverifiedReasons?: Record<string, string | undefined>,
 *   roundTripUnverifiedSubjects?: string[],
 *   subjects: Record<string, OperationMetrics>,
 *   unsupported?: Array<{ reason: string | undefined, subject: string }>,
 * }} OperationReport
 * @typedef {{
 *   fidelity: Record<string, FidelityReport>,
 *   fixture: Fixture,
 *   generatedAt: string,
 *   memory: Record<string, Record<string, MemoryReport>>,
 *   method: { iterationsPerSubjectPerOperation: number } & Record<string, unknown>,
 *   operations: Record<string, OperationReport>,
 *   order?: Record<string, string[][]>,
 *   provenance: Record<string, unknown>,
 *   runtime: { arrow: string, node: string },
 *   schemaVersion: number,
 *   subjects: SubjectSummary[],
 *   suite: string,
 * }} BenchmarkReport
 * @typedef {{
 *   config: Record<string, boolean>,
 *   id: string,
 *   implementation: string,
 *   notes: string[],
 *   package: string,
 *   representation: string,
 *   version: string,
 * }} SubjectSummary
 * @typedef {{
 *   arrayBuffers: number,
 *   external: number,
 *   heapUsed: number,
 *   rss: number,
 * }} MemorySnapshot
 * @typedef {{ deltaBytes: MemorySnapshot, retainedResults: number }} MemoryReport
 * @typedef {{
 *   interactive: boolean,
 *   lastStep: number,
 *   line: (text: string) => void,
 *   pending: boolean,
 *   step: (text: string, force?: boolean) => void,
 *   write: (text: string, transient: boolean) => void,
 * }} ProgressReporter
 * @typedef {{
 *   build: (subject: Subject) => () => Promise<unknown>,
 *   check?: (id: string, value: unknown) => void,
 *   extra: Pick<
 *     OperationReport,
 *     "readValidation" | "roundTripReasons" | "roundTripStates" |
 *       "roundTripUnverifiedReasons" | "roundTripUnverifiedSubjects" | "unsupported"
 *   >,
 *   ids: string[],
 *   name: string,
 *   payloadBytes: number | null,
 * }} RecordOperationOptions
 * @typedef {import("../../js-xqdb/dist/index.js").XqdbInput} XqdbInput
 * @typedef {(statement: string) => Promise<unknown>} NodeQQuery
 * @typedef {(statement: string, value: unknown) => Promise<unknown>} NodeQApply
 */

const BYTES_PER_KIBIBYTE = 1024;
// Absolute paths: Windows drive and UNC paths (segments may contain spaces), then
// POSIX paths with at least two segments so q expressions and ratios are left alone.
const ABSOLUTE_PATH_PATTERN =
  /(?<![A-Za-z0-9])[A-Za-z]:[\\/](?:[^\\/:*?"<>|\r\n]+[\\/])*[^\\/:*?"<>|\r\n]*|\\\\[^\\/:*?"<>|\s]+(?:\\[^\\/:*?"<>|\r\n]+)+|(?<![\w.])\/(?:[^/\s:'"|<>]+\/)+[^/\s:'"|<>]*/gu;
const BYTES_PER_MEBIBYTE = BYTES_PER_KIBIBYTE ** 2;
const DEFAULT_ITERATIONS = 50;
const DEFAULT_MEMORY_RESULTS = 5;
const DEFAULT_PORT = 1801;
const DEFAULT_SEED = 42;
const DEFAULT_WARMUPS = 3;
const DURATION_COMPACT_THRESHOLD_SECONDS = 90;
const GIT_MAX_BUFFER_MEBIBYTES = 10;
const GIT_MAX_BUFFER_BYTES = GIT_MAX_BUFFER_MEBIBYTES * BYTES_PER_MEBIBYTE;
const JSON_INDENT = 2;
const LENGTH_PREFIX_BYTES = 8;
const MINIMUM_NEEDLE_LENGTH = 3;
const MILLISECONDS_PER_SECOND = 1000;
const MULBERRY_INCREMENT = 0x6d_2b_79_f5;
const MULBERRY_SHIFT_A = 15;
const MULBERRY_SHIFT_B = 7;
const MULBERRY_SHIFT_C = 14;
const MULBERRY_MULTIPLIER = 61;
const NANOSECONDS_PER_MILLISECOND = 1_000_000n;
const NANOSECONDS_PER_MILLISECOND_NUMBER = 1e6;
const NANOSECONDS_PER_SECOND_NUMBER = 1e9;
const PACKAGE_MANIFEST_SEARCH_LIMIT = 8;
const PERCENTILE_90 = 0.9;
const PERCENTILE_99 = 0.99;
const PRNG_UINT32_RANGE = 4_294_967_296;
const PROGRESS_LINE_WIDTH = 96;
const SCALAR_EXPECTED = 42;
const SECONDS_PER_MINUTE = 60;
const SHUFFLE_SEED_FACTOR = 1_000_003;
const SUMMARY_CLIENT_COLUMN_WIDTH = 16;
const SUMMARY_FIDELITY_ID_WIDTH = 7;
const SUMMARY_OPERATION_COLUMN_WIDTH = 13;
const SUMMARY_RATIO_COLUMN_WIDTH = 18;
const SUMMARY_RATIO_DECIMAL_PLACES = 2;
const SUMMARY_TIME_DECIMAL_PLACES = 3;
const TIMESTAMP_FRACTION_DIGITS = 9;

/**
 * @param {unknown} value
 * @returns {value is Record<PropertyKey, unknown>}
 */
function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/**
 * @param {unknown} value
 * @returns {value is string[]}
 */
function isStringArray(value) {
  return Array.isArray(value) && value.every((entry) => typeof entry === "string");
}

/**
 * @template T
 * @param {readonly T[]} values
 * @param {number} index
 * @param {string} owner
 * @returns {T}
 */
function requiredArrayEntry(values, index, owner) {
  const value = values[index];
  if (value === undefined) {
    throw new RangeError(`${owner} is missing index ${index}`);
  }
  return value;
}

/**
 * @template T
 * @param {Record<string, T>} values
 * @param {string} key
 * @param {string} owner
 * @returns {T}
 */
function requiredRecordEntry(values, key, owner) {
  const value = values[key];
  if (value === undefined) {
    throw new Error(`${owner} is missing ${key}`);
  }
  return value;
}

/**
 * @param {unknown} value
 * @returns {value is { length: number }}
 */
function hasNumberLength(value) {
  if (typeof value === "string") {
    return true;
  }
  return (
    value !== null &&
    (typeof value === "object" || typeof value === "function") &&
    "length" in value &&
    typeof value.length === "number"
  );
}

/**
 * @param {unknown} value
 * @param {string} owner
 * @returns {PackageIdentity}
 */
function packageIdentity(value, owner) {
  if (!isRecord(value) || typeof value.name !== "string" || typeof value.version !== "string") {
    throw new TypeError(`${owner} is missing a string name or version`);
  }
  return { name: value.name, version: value.version };
}

/**
 * @param {string} text
 * @param {string} owner
 * @returns {PackageIdentity}
 */
function parsePackageIdentity(text, owner) {
  /** @type {unknown} */
  const value = JSON.parse(text);
  return packageIdentity(value, owner);
}

/**
 * @param {string} specifier
 * @param {NodeJS.Require} resolver
 * @returns {PackageIdentity}
 */
function requiredPackageIdentity(specifier, resolver) {
  /** @type {unknown} */
  const value = resolver(specifier);
  return packageIdentity(value, specifier);
}

/**
 * @param {unknown} error
 * @returns {string}
 */
function errorMessage(error) {
  return redactPaths(error instanceof Error ? error.message : String(error));
}

/**
 * Replace absolute filesystem paths, which embed user and system names.
 * @param {string} text
 * @returns {string}
 */
function redactPaths(text) {
  return text.replaceAll(ABSOLUTE_PATH_PATTERN, "<path>");
}

/**
 * Classify the fixture endpoint without recording it.
 * @param {string} host
 * @returns {"loopback" | "network"}
 */
function connectionLabel(host) {
  const bare = host.toLowerCase().replaceAll(/^\[|\]$/gu, "");
  return bare === "localhost" ||
    bare === "::1" ||
    bare.startsWith("127.") ||
    bare.startsWith("::ffff:127.")
    ? "loopback"
    : "network";
}

/**
 * This machine's identifying strings; none may appear in a report. `release()`
 * is left out: on macOS it is a bare version number that can collide with a
 * package version, and the OS family is already covered by `platform()`.
 * @returns {Record<string, string>}
 */
function machineIdentifiers() {
  /** @type {Record<string, string>} */
  const needles = {
    arch: process.arch,
    cpu: cpus()[0]?.model ?? "",
    home: homedir(),
    hostname: hostname(),
    platform: osPlatform(),
    release: release(),
  };
  try {
    needles.user = userInfo().username;
  } catch {
    // No account name available; the remaining needles still apply.
  }
  return Object.fromEntries(
    Object.entries(needles).filter(
      ([name, needle]) =>
        needle.trim().length >= MINIMUM_NEEDLE_LENGTH &&
        (name !== "release" || /[a-z]/iu.test(needle)),
    ),
  );
}

/**
 * Refuse report text that names this machine, its user, or an absolute path.
 * @param {string} text
 */
function assertNoMachineIdentifiers(text) {
  const [path] = text.match(ABSOLUTE_PATH_PATTERN) ?? [];
  if (path !== undefined) {
    throw new Error(`report contains an absolute path: ${JSON.stringify(path)}`);
  }
  for (const [name, needle] of Object.entries(machineIdentifiers())) {
    const escaped = needle.trim().replaceAll(/[$()*+.?[\\\]^{|}]/gu, String.raw`\$&`);
    if (new RegExp(`(?<![A-Za-z0-9])${escaped}(?![A-Za-z0-9])`, "iu").test(text)) {
      throw new Error(`report contains the local ${name}: ${JSON.stringify(needle)}`);
    }
  }
}

/**
 * @param {unknown} error
 * @returns {string}
 */
function commandErrorDetail(error) {
  if (isRecord(error)) {
    const { stderr } = error;
    if (typeof stderr === "string" || Buffer.isBuffer(stderr)) {
      const detail = stderr.toString().trim();
      if (detail.length > 0) {
        return detail;
      }
    }
  }
  return errorMessage(error);
}

/**
 * @param {unknown} value
 * @returns {value is { NativeConnector: (...arguments_: never[]) => unknown }}
 */
function hasNativeConnector(value) {
  return (
    value !== null &&
    (typeof value === "object" || typeof value === "function") &&
    "NativeConnector" in value &&
    typeof value.NativeConnector === "function"
  );
}

/**
 * @param {bigint} value
 * @returns {number}
 */
function toMilliseconds(value) {
  return Number(value) / NANOSECONDS_PER_MILLISECOND_NUMBER;
}

/**
 * @param {unknown} value
 * @returns {value is { QConnection: typeof import("jkdb").QConnection }}
 */
function isJkdbModule(value) {
  return isRecord(value) && typeof value.QConnection === "function";
}

/**
 * @param {unknown} value
 * @returns {value is { connect: typeof import("node-q").connect }}
 */
function isNodeQModule(value) {
  return isRecord(value) && typeof value.connect === "function";
}

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(HERE, "../..");
const SOURCE_PATHS = [
  "Cargo.toml",
  "Cargo.lock",
  "rust-toolchain.toml",
  "Taskfile.yml",
  "crates/xqdb/Cargo.toml",
  "crates/xqdb/src",
  "bindings/napi-xqdb/Cargo.toml",
  "bindings/napi-xqdb/build.rs",
  "bindings/napi-xqdb/src",
  "js-xqdb/package.json",
  "js-xqdb/package-lock.json",
  "js-xqdb/tsconfig.json",
  "js-xqdb/tsconfig.build.json",
  "js-xqdb/src",
  "js-xqdb/scripts",
  "js-xqdb/native.cjs",
  "js-xqdb/native.d.ts",
  "testing/kdb",
  "benchmarks/node/package.json",
  "benchmarks/node/package-lock.json",
  "benchmarks/node/bench.mjs",
];
const require = createRequire(import.meta.url);
// Resolve from js-xqdb so reported versions are the ones its dist actually loads.
const packageRequire = createRequire(resolve(HERE, "../../js-xqdb/package.json"));

// `require("pkg/package.json")` fails for packages with a restrictive exports
// Map (apache-arrow), so resolve the entry point and walk up to its manifest.
/**
 * @param {string} specifier
 * @param {NodeJS.Require} resolver
 * @returns {string}
 */
function packageVersion(specifier, resolver) {
  let directory = dirname(resolver.resolve(specifier));
  for (let depth = 0; depth < PACKAGE_MANIFEST_SEARCH_LIMIT; depth += 1) {
    const candidate = resolve(directory, "package.json");
    if (existsSync(candidate)) {
      const manifest = parsePackageIdentity(readFileSync(candidate, "utf8"), candidate);
      if (manifest.name === specifier) {
        return manifest.version;
      }
    }
    const parent = dirname(directory);
    if (parent === directory) {
      break;
    }
    directory = parent;
  }
  throw new Error(`could not determine installed version of ${specifier}`);
}

/**
 * @param {...string} arguments_
 * @returns {string}
 */
function gitText(...arguments_) {
  try {
    return execFileSync("git", ["-C", REPO_ROOT, ...arguments_], {
      encoding: "utf8",
      maxBuffer: GIT_MAX_BUFFER_BYTES,
    }).trim();
  } catch (error) {
    const detail = commandErrorDetail(error);
    throw new Error(`cannot record benchmark source provenance: git ${arguments_[0]}: ${detail}`, {
      cause: error,
    });
  }
}

/**
 * @param {string} version
 * @returns {SourceProvenance}
 */
function sourceProvenance(version) {
  const revision = gitText("rev-parse", "HEAD");
  const describe = gitText("describe", "--always", "--long", "--dirty", "--tags");
  const dirty = gitText("status", "--porcelain=v1", "--untracked-files=all").length > 0;
  /** @type {Buffer} */
  let listed;
  try {
    listed = execFileSync(
      "git",
      [
        "-C",
        REPO_ROOT,
        "ls-files",
        "-z",
        "--cached",
        "--others",
        "--exclude-standard",
        "--",
        ...SOURCE_PATHS,
      ],
      { maxBuffer: GIT_MAX_BUFFER_BYTES },
    );
  } catch (error) {
    const detail = commandErrorDetail(error);
    throw new Error(`cannot enumerate benchmark source content: ${detail}`, { cause: error });
  }

  const paths = listed
    .toString("utf8")
    .split("\0")
    .filter((path) => path.length > 0);
  paths.sort();
  const digest = createHash("sha256");
  /**
   * @param {number} length
   */
  const updateLength = (length) => {
    const encoded = Buffer.allocUnsafe(LENGTH_PREFIX_BYTES);
    encoded.writeBigUInt64BE(BigInt(length));
    digest.update(encoded);
  };
  for (const path of paths) {
    const encodedPath = Buffer.from(path);
    updateLength(encodedPath.length);
    digest.update(encodedPath);
    const absolute = resolve(REPO_ROOT, path);
    if (existsSync(absolute)) {
      const content = readFileSync(absolute);
      digest.update(Buffer.of(1));
      updateLength(content.length);
      digest.update(content);
    } else {
      digest.update(Buffer.of(0));
    }
  }

  return {
    contentDigest: {
      algorithm: "sha256",
      fileCount: paths.length,
      framing:
        "sorted git pathname bytes framed by uint64-be length, then pathname, one-byte presence, and for present files uint64-be content length plus content",
      includesTrackedAndUntrackedFiles: true,
      pathspecs: SOURCE_PATHS,
      value: digest.digest("hex"),
    },
    describe,
    dirty,
    identity: dirty
      ? "dirty working-tree source state identified by revision plus contentDigest; this does not prove which source built the loaded artifacts"
      : "clean working-tree source state; this does not itself prove artifact build inputs",
    revision,
    version,
  };
}

/**
 * @param {string} left
 * @param {string} right
 * @returns {number}
 */
function compareText(left, right) {
  if (left < right) {
    return -1;
  }
  if (left > right) {
    return 1;
  }
  return 0;
}

/**
 * @param {string} path
 * @param {Set<string>} files
 */
function collectArtifactFiles(path, files) {
  const status = statSync(path);
  if (status.isFile()) {
    files.add(resolve(path));
    return;
  }
  if (!status.isDirectory()) {
    throw new Error(`unsupported artifact path: ${path}`);
  }
  const directoryEntries = readdirSync(path, { withFileTypes: true });
  directoryEntries.sort((left, right) => compareText(left.name, right.name));
  for (const entry of directoryEntries) {
    collectArtifactFiles(resolve(path, entry.name), files);
  }
}

/**
 * @param {string} root
 * @param {string[]} paths
 * @returns {ArtifactTree}
 */
function artifactTree(root, paths) {
  /** @type {Set<string>} */
  const files = new Set();
  for (const path of paths) {
    if (!existsSync(path)) {
      throw new Error(`required XQDB runtime artifact is missing: ${path}`);
    }
    collectArtifactFiles(path, files);
  }
  /**
   * @param {string} path
   * @returns {string}
   */
  const artifactPath = (path) => relative(root, path).split(sep).join("/");
  const ordered = [...files];
  ordered.sort((left, right) => compareText(artifactPath(left), artifactPath(right)));
  const aggregate = createHash("sha256");
  const entries = ordered.map((path) => {
    const relativePath = artifactPath(path);
    const encodedPath = Buffer.from(relativePath);
    const content = readFileSync(path);
    const pathLength = Buffer.allocUnsafe(LENGTH_PREFIX_BYTES);
    pathLength.writeBigUInt64BE(BigInt(encodedPath.length));
    const contentLength = Buffer.allocUnsafe(LENGTH_PREFIX_BYTES);
    contentLength.writeBigUInt64BE(BigInt(content.length));
    aggregate.update(pathLength);
    aggregate.update(encodedPath);
    aggregate.update(contentLength);
    aggregate.update(content);
    return {
      bytes: content.length,
      path: relativePath,
      sha256: createHash("sha256").update(content).digest("hex"),
    };
  });
  return {
    contentDigest: {
      algorithm: "sha256",
      framing:
        "sorted package-relative UTF-8 path framed by uint64-be length, then path, uint64-be content length, then content",
      value: aggregate.digest("hex"),
    },
    files: entries,
  };
}

/**
 * @param {string} version
 * @returns {ArtifactProvenance}
 */
function nodeArtifactProvenance(version) {
  const packageRoot = resolve(REPO_ROOT, "js-xqdb");
  const nativeMatches = Object.entries(require.cache ?? {}).filter(([path, moduleRecord]) => {
    /** @type {unknown} */
    const exported = moduleRecord?.exports;
    return (
      path.endsWith(".node") && path.toLowerCase().includes("xqdb") && hasNativeConnector(exported)
    );
  });
  if (nativeMatches.length !== 1) {
    throw new Error(`expected one loaded XQDB native addon, found ${nativeMatches.length}`);
  }
  const nativeMatch = requiredArrayEntry(nativeMatches, 0, "loaded XQDB native addon");
  const [nativePath] = nativeMatch;
  const nativeContent = readFileSync(nativePath);
  return {
    javascriptBuild: artifactTree(packageRoot, [
      resolve(packageRoot, "dist"),
      resolve(packageRoot, "native.cjs"),
      resolve(packageRoot, "package.json"),
    ]),
    loadedNativeAddon: {
      bytes: nativeContent.length,
      sha256: createHash("sha256").update(nativeContent).digest("hex"),
    },
    packageVersion: version,
  };
}

const TABLES = {
  trade: 14,
  wide: 64,
  depth: 5,
};
const SCALAR_EXPRESSION = "6f*7f";
const LONG_EXPRESSION = "9007199254740993j";
const LONG_EXPECTED = 9_007_199_254_740_993n;
const TIMESTAMP_EXPECTED_NS = 1_704_164_645_123_456_789n;
const TIMESTAMP_EXPRESSION = "2024.01.02D03:04:05.123456789";
const CANONICAL_BYTES_LAMBDA = "{[x]count -8!x}";
const COUNT_LAMBDA = "{[x]count x}";

// ── options ──────────────────────────────────────────────────────────────────

/**
 * @param {string[]} argv
 * @returns {BenchmarkOptions}
 */
function parseOptions(argv) {
  /** @type {BenchmarkOptions} */
  const options = {
    host: process.env.XQDB_TEST_Q_HOST ?? "127.0.0.1",
    iterations: Number(process.env.XQDB_BENCH_ITERATIONS ?? DEFAULT_ITERATIONS),
    memoryResults: Number(process.env.XQDB_BENCH_MEMORY_RESULTS ?? DEFAULT_MEMORY_RESULTS),
    output: undefined,
    port: Number(process.env.XQDB_TEST_Q_PORT ?? DEFAULT_PORT),
    samples: true,
    seed: Number(process.env.XQDB_BENCH_SEED ?? DEFAULT_SEED),
    warmups: Number(process.env.XQDB_BENCH_WARMUPS ?? DEFAULT_WARMUPS),
  };
  /** @type {Record<string, "iterations" | "memoryResults" | "port" | "seed" | "warmups">} */
  const numeric = {
    "--iterations": "iterations",
    "--memory-results": "memoryResults",
    "--port": "port",
    "--seed": "seed",
    "--warmups": "warmups",
  };
  for (let index = 0; index < argv.length; index += 1) {
    const flag = requiredArrayEntry(argv, index, "argument list");
    if (flag === "--samples" || flag === "--no-samples") {
      options.samples = flag === "--samples";
    } else {
      const value = argv[index + 1];
      if (value === undefined) {
        throw new Error(`${flag} requires a value`);
      }
      index += 1;
      if (flag === "--host") {
        options.host = value;
      } else if (flag === "--output") {
        options.output = value;
      } else {
        const numericName = numeric[flag];
        if (numericName === undefined) {
          throw new Error(`unknown flag: ${flag}`);
        }
        options[numericName] = Number(value);
      }
    }
  }
  if (!Number.isInteger(options.port) || options.port < 1) {
    throw new Error("--port must be a positive integer");
  }
  if (!Number.isInteger(options.warmups) || options.warmups < 0) {
    throw new Error("--warmups must be non-negative");
  }
  if (!Number.isInteger(options.iterations) || options.iterations < 1) {
    throw new Error("--iterations must be positive");
  }
  if (!Number.isInteger(options.memoryResults) || options.memoryResults < 1) {
    throw new Error("--memory-results must be positive");
  }
  if (!Number.isInteger(options.seed)) {
    throw new TypeError("--seed must be an integer");
  }
  return options;
}

// ── subjects ─────────────────────────────────────────────────────────────────

/**
 * @param {ConnectionOptions} options
 * @returns {Promise<Subject>}
 */
async function createXqdb({ host, port }) {
  const manifest = requiredPackageIdentity("./package.json", packageRequire);
  const connection = await Q.connect({ host, port });
  return {
    apply: (lambda, value) => {
      // Each subject sends its own decoded frame; the client validates inputs before IPC.
      // oxlint-disable typescript/no-unsafe-type-assertion
      const frame =
        /** @type {XqdbInput} */
        (value);
      // oxlint-enable typescript/no-unsafe-type-assertion
      return connection.sync(lambda, frame);
    },
    close: () => connection.disconnect(),
    config: {},
    eval: (expression) => connection.sync(expression),
    id: "xqdb",
    implementation: "Rust core via napi-rs, Arrow C Stream decode",
    longOf: (value) => (typeof value === "bigint" ? value : null),
    notes: [],
    package: manifest.name,
    read: (table) => connection.sync(table),
    representation: "apache-arrow Table",
    shapeOf: (value) => {
      if (
        !isRecord(value) ||
        typeof value.numRows !== "number" ||
        !isRecord(value.schema) ||
        !Array.isArray(value.schema.fields)
      ) {
        return null;
      }
      return { rows: value.numRows, columns: value.schema.fields.length };
    },
    timestampNanosOf: (value) => (value instanceof XqdbTimestamp ? value.nanoseconds : null),
    timestampProbeMode: "timed configuration",
    version: manifest.version,
  };
}

/**
 * @param {ConnectionOptions} options
 * @returns {Promise<Subject>}
 */
async function createJkdb({ host, port }) {
  const manifest = requiredPackageIdentity("jkdb/package.json", require);
  /** @type {unknown} */
  const jkdbModule = require("jkdb");
  if (!isJkdbModule(jkdbModule)) {
    throw new TypeError("jkdb does not export QConnection");
  }
  const { QConnection } = jkdbModule;
  const primary = new QConnection({ host, port, useBigInt: true });
  await primary.connectAsync();
  // Jkdb decodes sub-millisecond timestamps only with includeNanosecond, which
  // Switches temporal values to text and makes its encoder reject them. Keep it
  // On a second connection so the temporal fidelity claim stays measurable
  // Without changing the representation used for the timed operations.
  const nanosecond = new QConnection({ host, includeNanosecond: true, port, useBigInt: true });
  await nanosecond.connectAsync();
  return {
    apply: (lambda, value) =>
      /** @type {Promise<unknown>} */
      (primary.syncAsync([lambda, value])),
    close: async () => {
      await primary.closeAsync();
      await nanosecond.closeAsync();
    },
    config: { includeNanosecond: false, useBigInt: true },
    eval: (expression) =>
      /** @type {Promise<unknown>} */
      (primary.syncAsync(expression)),
    id: "jkdb",
    implementation: "Rust core via napi-rs, plain JS objects",
    longOf: (value) => (typeof value === "bigint" ? value : null),
    notes: [
      "sub-millisecond timestamps need includeNanosecond:true, measured here on a second connection; in that mode temporal values become text and the encoder rejects them, so tables cannot be sent back",
    ],
    package: manifest.name,
    read: (table) =>
      /** @type {Promise<unknown>} */
      (primary.syncAsync(table)),
    representation: 'column-oriented object with Symbol.for("meta") schema',
    shapeOf: (value) => {
      if (!isRecord(value)) {
        return null;
      }
      const meta = value[Symbol.for("meta")];
      if (!isRecord(meta) || !isStringArray(meta.c)) {
        return null;
      }
      const [firstColumn] = meta.c;
      const firstValue = firstColumn === undefined ? undefined : value[firstColumn];
      return {
        rows: hasNumberLength(firstValue) ? firstValue.length : 0,
        columns: meta.c.length,
      };
    },
    timestampNanosOf: async () => {
      /** @type {unknown} */
      const text = await nanosecond.syncAsync(TIMESTAMP_EXPRESSION);
      if (typeof text !== "string") {
        return null;
      }
      const [seconds, fraction = ""] = text.split(".");
      const epochMs = BigInt(Date.parse(`${seconds}Z`));
      return (
        epochMs * NANOSECONDS_PER_MILLISECOND +
        BigInt(fraction.padEnd(TIMESTAMP_FRACTION_DIGITS, "0").slice(0, TIMESTAMP_FRACTION_DIGITS))
      );
    },
    timestampProbeMode:
      "separate includeNanosecond:true connection; timed configuration uses includeNanosecond:false",
    version: manifest.version,
  };
}

/**
 * @param {ConnectionOptions} options
 * @returns {Promise<Subject>}
 */
async function createNodeQ({ host, port }) {
  const manifest = requiredPackageIdentity("node-q/package.json", require);
  /** @type {unknown} */
  const nodeq = require("node-q");
  if (!isNodeQModule(nodeq)) {
    throw new TypeError("node-q does not export connect");
  }
  /**
   * @param {import("node-q").ConnectionParameters} parameters
   * @param {import("node-q").AsyncValueCallback<import("node-q").Connection>} callback
   * @returns {void}
   */
  const connectCallback = (parameters, callback) => {
    nodeq.connect(parameters, callback);
  };
  const connect = promisify(connectCallback);
  // FlipTables:false keeps node-q column-oriented, which is both its fastest
  // Decode path and the shape closest to the other subjects. long2number and
  // Nanos2date keep their defaults because the lossless alternatives allocate a
  // Wrapper object per element and roughly double decode time.
  const connection = await connect({ flipTables: false, host, port });
  if (connection === undefined) {
    throw new Error("node-q connect completed without a connection");
  }
  /**
   * @param {string} statement
   * @param {import("node-q").AsyncValueCallback<unknown>} callback
   * @returns {void}
   */
  const queryCallback = (statement, callback) => {
    connection.k(statement, callback);
  };
  /**
   * @param {string} statement
   * @param {unknown} value
   * @param {import("node-q").AsyncValueCallback<unknown>} callback
   * @returns {void}
   */
  const applyCallback = (statement, value, callback) => {
    connection.k(statement, value, callback);
  };
  /** @type {NodeQQuery} */
  const k = promisify(queryCallback);
  /** @type {NodeQApply} */
  const apply = promisify(applyCallback);
  /** @type {() => Promise<void>} */
  const close = () =>
    new Promise((resolveClose) => {
      connection.close(resolveClose);
    });
  return {
    apply,
    close,
    config: { flipTables: false },
    eval: (expression) => k(expression),
    id: "node-q",
    implementation: "pure JavaScript codec",
    longOf: (value) => (typeof value === "number" && Number.isFinite(value) ? BigInt(value) : null),
    notes: [
      "int64 decodes to double and loses precision beyond 2^53; timestamps decode to millisecond Date",
      "long2number:false and nanos2date:false restore exactness with Long wrappers but roughly double decode time",
    ],
    package: manifest.name,
    read: (table) => k(table),
    representation: "column-oriented object (flipTables:false)",
    shapeOf: (value) => {
      if (!isRecord(value)) {
        return null;
      }
      const columns = Object.keys(value);
      const [firstColumn] = columns;
      const firstValue = firstColumn === undefined ? undefined : value[firstColumn];
      return {
        rows: hasNumberLength(firstValue) ? firstValue.length : 0,
        columns: columns.length,
      };
    },
    timestampNanosOf: (value) =>
      value instanceof Date ? BigInt(value.getTime()) * NANOSECONDS_PER_MILLISECOND : null,
    timestampProbeMode: "timed configuration",
    version: manifest.version,
  };
}

/** @type {SubjectFactory[]} */
const SUBJECT_FACTORIES = [createXqdb, createJkdb, createNodeQ];

// ── statistics ───────────────────────────────────────────────────────────────

/**
 * @param {bigint} left
 * @param {bigint} right
 * @returns {number}
 */
function compareBigInts(left, right) {
  if (left < right) {
    return -1;
  }
  if (left > right) {
    return 1;
  }
  return 0;
}

/**
 * @param {bigint[]} ascending
 * @param {number} fraction
 * @returns {bigint}
 */
function nearestRank(ascending, fraction) {
  const rank = Math.round(fraction * (ascending.length - 1));
  return requiredArrayEntry(
    ascending,
    Math.max(0, Math.min(ascending.length - 1, rank)),
    "ranked samples",
  );
}

/**
 * @param {bigint[]} samplesNs
 * @param {number | null} payloadBytes
 * @param {boolean} keepSamples
 * @returns {OperationMetrics}
 */
function metrics(samplesNs, payloadBytes, keepSamples) {
  if (samplesNs.length === 0) {
    throw new RangeError("cannot compute metrics without samples");
  }
  const ascending = [...samplesNs];
  ascending.sort(compareBigInts);
  const total = samplesNs.reduce((sum, value) => sum + value, 0n);
  const middle = Math.floor(ascending.length / 2);
  const median =
    ascending.length % 2 === 1
      ? toMilliseconds(requiredArrayEntry(ascending, middle, "median samples"))
      : (toMilliseconds(requiredArrayEntry(ascending, middle - 1, "median samples")) +
          toMilliseconds(requiredArrayEntry(ascending, middle, "median samples"))) /
        2;
  /** @type {OperationMetrics} */
  const report = {
    iterations: samplesNs.length,
    maxMs: toMilliseconds(requiredArrayEntry(ascending, ascending.length - 1, "maximum sample")),
    meanMs: Number(total) / samplesNs.length / NANOSECONDS_PER_MILLISECOND_NUMBER,
    medianMs: median,
    minMs: toMilliseconds(requiredArrayEntry(ascending, 0, "minimum sample")),
    p90Ms: toMilliseconds(nearestRank(ascending, PERCENTILE_90)),
    p99Ms: toMilliseconds(nearestRank(ascending, PERCENTILE_99)),
  };
  if (payloadBytes !== null) {
    report.payloadBytes = payloadBytes;
    report.medianMibPerSecond =
      payloadBytes / (median / MILLISECONDS_PER_SECOND) / BYTES_PER_MEBIBYTE;
  }
  if (keepSamples) {
    report.samplesMs = samplesNs.map((value) => toMilliseconds(value));
  }
  return report;
}

/**
 * @param {() => Promise<unknown>} operation
 * @returns {Promise<{ durationNs: bigint, value: unknown }>}
 */
async function timed(operation) {
  const started = process.hrtime.bigint();
  const value = await operation();
  return { durationNs: process.hrtime.bigint() - started, value };
}

/**
 * @template T
 * @param {T[]} list
 * @param {number} offset
 * @returns {T[]}
 */
function rotate(list, offset) {
  // Only for untimed ordering: a cyclic shift keeps neighbours fixed.
  const shift = offset % list.length;
  return [...list.slice(shift), ...list.slice(0, shift)];
}

// Deterministic per-round order. A cyclic rotation is not good enough here:
// Rotating by one preserves the adjacency relation, so every subject keeps the
// Same predecessor in every round and a subject that follows an expensive
// Neighbour pays for it in every sample. Reshuffling varies predecessors too.
/**
 * @param {number} seed
 * @returns {() => number}
 */
function mulberry32(seed) {
  let state = seed >>> 0;
  return () => {
    state = (state + MULBERRY_INCREMENT) >>> 0;
    let value = state;
    value = Math.imul(value ^ (value >>> MULBERRY_SHIFT_A), value | 1);
    value ^= value + Math.imul(value ^ (value >>> MULBERRY_SHIFT_B), value | MULBERRY_MULTIPLIER);
    return ((value ^ (value >>> MULBERRY_SHIFT_C)) >>> 0) / PRNG_UINT32_RANGE;
  };
}

/**
 * @template T
 * @param {T[]} list
 * @param {number} seed
 * @param {number} roundIndex
 * @returns {T[]}
 */
function shuffled(list, seed, roundIndex) {
  const random = mulberry32(Math.imul(seed, SHUFFLE_SEED_FACTOR) + roundIndex);
  const order = [...list];
  for (let index = order.length - 1; index > 0; index -= 1) {
    const swap = Math.floor(random() * (index + 1));
    const current = requiredArrayEntry(order, index, "shuffle order");
    order[index] = requiredArrayEntry(order, swap, "shuffle order");
    order[swap] = current;
  }
  return order;
}

// ── progress ─────────────────────────────────────────────────────────────────

// Progress on stderr so stdout stays the summary. A full run is minutes long
// And a single subject can hold a round for seconds, so silence is
// Indistinguishable from a hang. A terminal gets a rewritten line per round; a
// Captured log gets one line every few seconds so it stays readable.
const PROGRESS_THROTTLE_MS = 3000;
/** @type {ProgressReporter} */
const progress = {
  interactive: process.stderr.isTTY,
  lastStep: 0,
  line(text) {
    this.lastStep = 0;
    this.write(text, false);
  },
  pending: false,
  step(text, force = false) {
    const now = Date.now();
    if (!this.interactive && !force && now - this.lastStep < PROGRESS_THROTTLE_MS) {
      return;
    }
    this.lastStep = now;
    this.write(text, true);
  },
  write(text, transient) {
    if (transient && this.interactive) {
      process.stderr.write(`\r${text.padEnd(PROGRESS_LINE_WIDTH).slice(0, PROGRESS_LINE_WIDTH)}`);
      this.pending = true;
      return;
    }
    if (this.pending) {
      process.stderr.write("\n");
      this.pending = false;
    }
    process.stderr.write(`${text}\n`);
  },
};

/**
 * @param {number} seconds
 * @returns {string}
 */
function formatDuration(seconds) {
  return seconds < DURATION_COMPACT_THRESHOLD_SECONDS
    ? `${seconds.toFixed(0)}s`
    : `${Math.floor(seconds / SECONDS_PER_MINUTE)}m${String(
        Math.floor(seconds) % SECONDS_PER_MINUTE,
      ).padStart(2, "0")}s`;
}

// ── measurement ──────────────────────────────────────────────────────────────

/**
 * @param {{
 *   check?: (id: string, value: unknown) => void,
 *   ids: string[],
 *   iterations: number,
 *   label: string,
 *   operations: Record<string, () => Promise<unknown>>,
 *   seed: number,
 *   warmups: number,
 * }} settings
 * @returns {Promise<{ order: string[][], samples: Record<string, bigint[]> }>}
 */
async function runOperation({ ids, operations, warmups, iterations, seed, check, label }) {
  for (let round = 0; round < warmups; round += 1) {
    progress.step(`${label} warmup ${round + 1}/${warmups}`);
    for (const id of shuffled(ids, seed, -1 - round)) {
      const operation = requiredRecordEntry(operations, id, "benchmark operations");
      await operation();
    }
  }
  /** @type {Record<string, bigint[]>} */
  const samples = Object.fromEntries(ids.map((id) => [id, []]));
  /** @type {string[][]} */
  const order = [];
  const startedRun = process.hrtime.bigint();
  for (let round = 0; round < iterations; round += 1) {
    const sequence = shuffled(ids, seed, round);
    order.push(sequence);
    for (const id of sequence) {
      const operation = requiredRecordEntry(operations, id, "benchmark operations");
      const measured = await timed(operation);
      if (check !== undefined) {
        check(id, measured.value);
      }
      const subjectSamples = requiredRecordEntry(samples, id, "benchmark samples");
      subjectSamples.push(measured.durationNs);
    }
    const elapsed = Number(process.hrtime.bigint() - startedRun) / NANOSECONDS_PER_SECOND_NUMBER;
    progress.step(
      `${label} ${round + 1}/${iterations} rounds, ${formatDuration(elapsed)} elapsed, ${formatDuration(
        (elapsed / (round + 1)) * (iterations - round - 1),
      )} left`,
    );
  }
  return { order, samples };
}

/**
 * @returns {MemorySnapshot}
 */
function memorySnapshot() {
  const usage = process.memoryUsage();
  return {
    arrayBuffers: usage.arrayBuffers,
    external: usage.external,
    heapUsed: usage.heapUsed,
    rss: usage.rss,
  };
}

/**
 * @param {NonNullable<typeof globalThis.gc>} collectGarbage
 * @param {number} count
 * @param {() => Promise<unknown>} operation
 * @returns {Promise<MemoryReport>}
 */
async function retainedMemory(collectGarbage, count, operation) {
  collectGarbage();
  const before = memorySnapshot();
  /** @type {unknown[]} */
  const retained = [];
  for (let index = 0; index < count; index += 1) {
    retained.push(await operation());
  }
  collectGarbage();
  const after = memorySnapshot();
  assert.equal(retained.length, count);
  const deltaBytes = {
    arrayBuffers: after.arrayBuffers - before.arrayBuffers,
    external: after.external - before.external,
    heapUsed: after.heapUsed - before.heapUsed,
    rss: after.rss - before.rss,
  };
  retained.length = 0;
  collectGarbage();
  return { deltaBytes, retainedResults: count };
}

// ── fidelity preflight ───────────────────────────────────────────────────────

/**
 * @param {Subject} subject
 * @param {Fixture} fixture
 * @returns {Promise<FidelityReport>}
 */
async function measureFidelity(subject, fixture) {
  const scalar = await subject.eval(SCALAR_EXPRESSION);
  const decodedLong = await subject.longOf(await subject.eval(LONG_EXPRESSION));
  const decodedNanos = await subject.timestampNanosOf(await subject.eval(TIMESTAMP_EXPRESSION));
  /** @type {FidelityReport} */
  const report = {
    int64Decoded: decodedLong === null ? null : decodedLong.toString(),
    int64Exact: decodedLong === LONG_EXPECTED,
    nanosecondTimestampDecoded: decodedNanos === null ? null : decodedNanos.toString(),
    nanosecondTimestampExact: decodedNanos === TIMESTAMP_EXPECTED_NS,
    nanosecondTimestampMode: subject.timestampProbeMode,
    scalarDetail: String(scalar),
    scalarExact: Number(scalar) === SCALAR_EXPECTED,
    tables: {},
  };
  for (const [table, columns] of Object.entries(TABLES)) {
    const value = await subject.read(table);
    const shape = subject.shapeOf(value);
    /** @type {TableFidelity} */
    const entry = {
      canonicalBytesMatchFixture: false,
      reencodesToIdenticalQValue: false,
      shape,
      shapeMatchesFixture:
        shape !== null && shape.rows === fixture.rows && shape.columns === columns,
    };
    try {
      entry.reencodesToIdenticalQValue = Boolean(await subject.apply(`{[x]${table}~x}`, value));
      entry.reencodedRows = Number(await subject.apply(COUNT_LAMBDA, value));
      entry.reencodedCanonicalBytes = Number(await subject.apply(CANONICAL_BYTES_LAMBDA, value));
      entry.canonicalBytesMatchFixture =
        entry.reencodedCanonicalBytes ===
        requiredRecordEntry(fixture.tables, table, "fixture tables").canonicalBytes;
    } catch (error) {
      entry.encodeError = errorMessage(error);
    }
    // Timing a send is only meaningful when the encoder produces the same q
    // Value of the same size; anything else is different work.
    entry.sendComparable = entry.reencodesToIdenticalQValue && entry.canonicalBytesMatchFixture;
    if (!entry.sendComparable) {
      if (entry.encodeError !== undefined) {
        entry.sendExcludedBecause = `encoder rejected the decoded value: ${entry.encodeError}`;
      } else if (entry.reencodesToIdenticalQValue) {
        entry.sendExcludedBecause = `re-encoded canonical size ${entry.reencodedCanonicalBytes} != fixture ${requiredRecordEntry(fixture.tables, table, "fixture tables").canonicalBytes}`;
      } else {
        entry.sendExcludedBecause = "decoded value does not re-encode to a q-identical value";
      }
    }
    // `differs` requires q to have actually compared the two values. When the
    // Encoder threw, no comparison happened, so the round trip is unproven
    // Rather than failed, and the summary must not claim a value difference.
    if (entry.sendComparable) {
      entry.roundTrip = "identical";
    } else if (entry.encodeError !== undefined) {
      entry.roundTrip = "unverified";
    } else if (entry.reencodesToIdenticalQValue) {
      entry.roundTrip = "resized";
    } else {
      entry.roundTrip = "differs";
    }
    report.tables[table] = entry;
  }
  return report;
}

// ── main ─────────────────────────────────────────────────────────────────────

/**
 * @param {string} _key
 * @param {unknown} value
 * @returns {unknown}
 */
function stringifyBigInt(_key, value) {
  return typeof value === "bigint" ? value.toString() : value;
}

async function main() {
  const collectGarbage = globalThis.gc;
  if (typeof collectGarbage !== "function") {
    throw new TypeError("run with `node --expose-gc` so retained-result memory can be measured");
  }
  const options = parseOptions(process.argv.slice(2));
  const source = sourceProvenance(
    requiredPackageIdentity("./package.json", packageRequire).version,
  );
  /** @type {Subject[]} */
  const subjects = [];
  /** @type {BenchmarkReport} */
  let report;
  try {
    for (const factory of SUBJECT_FACTORIES) {
      subjects.push(await factory(options));
    }
    const reference = requiredArrayEntry(subjects, 0, "benchmark subjects");
    assert.equal(reference.id, "xqdb", "xqdb must be the reference subject");
    assert.equal(reference.version, source.version, "loaded xqdb version changed during setup");
    const artifacts = nodeArtifactProvenance(reference.version);
    const allIds = subjects.map((subject) => subject.id);
    /** @type {Record<string, Subject>} */
    const byId = Object.fromEntries(subjects.map((subject) => [subject.id, subject]));
    /**
     * @param {string} id
     * @returns {Subject}
     */
    const subjectFor = (id) => requiredRecordEntry(byId, id, "benchmark subjects");

    /** @type {Fixture} */
    const fixture = {
      connection: connectionLabel(options.host),
      qVersion: Number(await reference.eval(".z.K")),
      rows: Number(await reference.eval(".xqdb.rows")),
      seed: Number(await reference.eval(".xqdb.seed")),
      tables: {},
    };
    for (const [table, columns] of Object.entries(TABLES)) {
      fixture.tables[table] = {
        canonicalBytes: Number(await reference.eval(`count -8!${table}`)),
        columns,
      };
    }
    // Every subject must agree on what it is talking to before anything is timed.
    for (const subject of subjects) {
      assert.equal(
        Number(await subject.eval(".xqdb.rows")),
        fixture.rows,
        `${subject.id}: fixture row mismatch`,
      );
      assert.equal(
        Number(await subject.eval(".z.K")),
        fixture.qVersion,
        `${subject.id}: q version mismatch`,
      );
      assert.equal(
        Number(await subject.eval(".xqdb.seed")),
        fixture.seed,
        `${subject.id}: fixture seed mismatch`,
      );
    }

    /** @type {Record<string, FidelityReport>} */
    const fidelity = {};
    for (const [index, subject] of subjects.entries()) {
      progress.step(`[preflight ${index + 1}/${subjects.length}] ${subject.id}`);
      fidelity[subject.id] = await measureFidelity(subject, fixture);
    }
    /**
     * @param {string} id
     * @returns {FidelityReport}
     */
    const fidelityFor = (id) => requiredRecordEntry(fidelity, id, "fidelity reports");
    /**
     * @param {string} id
     * @param {string} table
     * @returns {TableFidelity}
     */
    const tableFidelityFor = (id, table) =>
      requiredRecordEntry(fidelityFor(id).tables, table, `${id} table fidelity`);
    progress.line(`[preflight] ${subjects.length} subjects probed`);
    const totalOperations = 1 + 2 * Object.keys(TABLES).length;
    let operationIndex = 0;

    /** @type {Record<string, OperationReport>} */
    const operations = {};
    /** @type {Record<string, string[][]>} */
    const order = {};

    /** @type {(settings: RecordOperationOptions) => Promise<void>} */
    const record = async ({ name, ids, payloadBytes, build, check, extra }) => {
      operationIndex += 1;
      const label = `[bench ${operationIndex}/${totalOperations}] ${name} (${ids.length} subjects)`;
      progress.step(label);
      const startedLabel = process.hrtime.bigint();
      /** @type {Record<string, () => Promise<unknown>>} */
      const subjectOperations = Object.fromEntries(ids.map((id) => [id, build(subjectFor(id))]));
      const run = await runOperation({
        ...(check === undefined ? {} : { check }),
        ids,
        iterations: options.iterations,
        label,
        operations: subjectOperations,
        seed: options.seed,
        warmups: options.warmups,
      });
      /** @type {Record<string, OperationMetrics>} */
      const perSubject = Object.fromEntries(
        ids.map((id) => [
          id,
          metrics(
            requiredRecordEntry(run.samples, id, `${name} samples`),
            payloadBytes,
            options.samples,
          ),
        ]),
      );
      const referenceMedian = requiredRecordEntry(
        perSubject,
        reference.id,
        `${name} metrics`,
      ).medianMs;
      const medianRatioVsXqdb = Object.fromEntries(
        ids.map((id) => [
          id,
          requiredRecordEntry(perSubject, id, `${name} metrics`).medianMs / referenceMedian,
        ]),
      );
      operations[name] = {
        medianRatioVsXqdb,
        payloadBytes,
        subjects: perSubject,
        ...extra,
      };
      if (options.samples) {
        order[name] = run.order;
      }
      const elapsed =
        Number(process.hrtime.bigint() - startedLabel) / NANOSECONDS_PER_SECOND_NUMBER;
      const timingSummary = ids
        .map(
          (id) =>
            `${id}=${requiredRecordEntry(perSubject, id, `${name} metrics`).medianMs.toFixed(SUMMARY_RATIO_DECIMAL_PLACES)}ms`,
        )
        .join("  ");
      progress.line(`${label} done in ${formatDuration(elapsed)}: ${timingSummary}`);
    };

    const scalarIds = allIds.filter((id) => fidelityFor(id).scalarExact);
    assert.ok(scalarIds.includes(reference.id), "reference subject failed the scalar preflight");
    await record({
      build: (subject) => () => subject.eval(SCALAR_EXPRESSION),
      extra: {
        unsupported: allIds
          .filter((id) => !scalarIds.includes(id))
          .map((id) => ({
            reason: `scalar decode is not exact: ${fidelityFor(id).scalarDetail}`,
            subject: id,
          })),
      },
      ids: scalarIds,
      name: "scalar",
      payloadBytes: null,
    });

    for (const table of Object.keys(TABLES)) {
      const fixtureTable = requiredRecordEntry(fixture.tables, table, "fixture tables");
      const payloadBytes = fixtureTable.canonicalBytes;
      /** @type {Record<string, RoundTripState>} */
      const roundTripStates = Object.fromEntries(
        allIds.map((id) => [id, roundTripState(tableFidelityFor(id, table))]),
      );
      /** @type {Record<string, string | undefined>} */
      const roundTripReasons = Object.fromEntries(
        allIds
          .filter(
            (id) =>
              requiredRecordEntry(roundTripStates, id, `${table} round-trip states`) !==
              "identical",
          )
          .map((id) => [id, tableFidelityFor(id, table).sendExcludedBecause]),
      );
      await record({
        build: (subject) => () => subject.read(table),
        check: (id, value) => {
          const shape = subjectFor(id).shapeOf(value);
          assert.equal(shape?.rows, fixture.rows, `${id}: ${table} row count mismatch`);
        },
        extra: {
          readValidation: "decoded row count only; table content fidelity is not inferred",
          roundTripReasons,
          roundTripStates,
          roundTripUnverifiedReasons: Object.fromEntries(
            allIds
              .filter(
                (id) =>
                  requiredRecordEntry(roundTripStates, id, `${table} round-trip states`) ===
                  "unverified",
              )
              .map((id) => [id, roundTripReasons[id]]),
          ),
          roundTripUnverifiedSubjects: allIds.filter(
            (id) =>
              requiredRecordEntry(roundTripStates, id, `${table} round-trip states`) ===
              "unverified",
          ),
        },
        ids: allIds,
        name: `read.${table}`,
        payloadBytes,
      });

      const sendableIds = allIds.filter(
        (id) => tableFidelityFor(id, table).sendComparable === true,
      );
      assert.ok(
        sendableIds.includes(reference.id),
        `${table}: reference subject cannot send comparably`,
      );
      /** @type {Record<string, unknown>} */
      const decoded = {};
      for (const id of sendableIds) {
        decoded[id] = await subjectFor(id).read(table);
      }
      await record({
        build: (subject) => () => subject.apply(COUNT_LAMBDA, decoded[subject.id]),
        check: (id, value) => {
          assert.equal(Number(value), fixture.rows, `${id}: send count mismatch`);
        },
        extra: {
          unsupported: allIds
            .filter((id) => !sendableIds.includes(id))
            .map((id) => ({
              reason: tableFidelityFor(id, table).sendExcludedBecause,
              subject: id,
            })),
        },
        ids: sendableIds,
        name: `send.${table}`,
        payloadBytes,
      });
    }

    /** @type {Record<string, Record<string, MemoryReport>>} */
    const memory = {};
    let tableIndex = 0;
    const tableNames = Object.keys(TABLES);
    for (const table of tableNames) {
      /** @type {Record<string, MemoryReport>} */
      const tableMemory = {};
      memory[table] = tableMemory;
      for (const id of rotate(allIds, tableIndex)) {
        progress.step(`[memory ${tableIndex + 1}/${tableNames.length}] ${table} ${id}`);
        tableMemory[id] = await retainedMemory(collectGarbage, options.memoryResults, () =>
          subjectFor(id).read(table),
        );
      }
      tableIndex += 1;
    }
    progress.line(`[memory] ${tableNames.length} tables probed`);
    assert.deepEqual(
      sourceProvenance(reference.version),
      source,
      "relevant source or repository state changed during the run",
    );
    assert.deepEqual(
      nodeArtifactProvenance(reference.version),
      artifacts,
      "loaded XQDB artifacts changed during the run",
    );

    report = {
      fidelity,
      fixture,
      generatedAt: new Date().toISOString(),
      memory,
      method: {
        auditData:
          "raw samplesMs and per-round subject order are retained by default; --no-samples is an explicit local-only size optimization",
        clock: "process.hrtime.bigint",
        excludedSubjects: {
          "@kxsystems/*": "KX publishes no Node.js q client on npm",
        },
        iterationsPerSubjectPerOperation: options.iterations,
        memory:
          "process-wide RSS/heap delta around retained decoded results with forced GC at both snapshots; a noisy diagnostic, not a library footprint",
        orderSeed: options.seed,
        payloadBytes:
          "server-side `count -8!table` for the fixture table: a logical payload size shared by all subjects, not observed wire bytes",
        percentiles: "nearest-rank on raw durations",
        rawSamplesRetained: options.samples,
        readComparability:
          "every subject decodes an identical server byte stream into its documented representation, so all are ranked; timed validation proves the decoded row count, not table content fidelity. Exact int64 and nanosecond atom probes are recorded in `fidelity`; `roundTripStates` separately distinguishes identical, differs, resized, and unverified decode-then-encode outcomes without assigning an encoder failure to the decoder",
        referenceSubject: reference.id,
        retainedResultsPerMemoryProbe: options.memoryResults,
        scalarComparability:
          "value-exact latency floor; a subject that cannot return the exact value is listed in `unsupported` instead of being ranked",
        scheduling:
          "one request in flight per subject; every round runs each subject once in a deterministic order reshuffled from `orderSeed`, so positions and predecessors both vary; a cyclic rotation would pin every subject behind the same neighbour in every round",
        sendComparability:
          "sends are compared only for subjects whose decoded value re-encodes to a q-identical value of the same canonical size; others are listed in `unsupported` with no samples, throughput, or ratio",
        throughput: "payloadBytes divided by the median duration",
        untimedCorrectnessChecks:
          "int64 and nanosecond-timestamp exactness are reported by the preflight, not timed: timing a wrong decode against a right one would compare different work",
        warmupsPerSubjectPerOperation: options.warmups,
      },
      operations,
      provenance: {
        buildProvenance: {
          statement:
            "source-state and loaded-artifact digests are recorded independently; the harness does not claim an attested build link between them",
          status: "not-proven",
        },
        loadedArtifacts: artifacts,
        sourceState: source,
      },
      runtime: {
        arrow: packageVersion("apache-arrow", packageRequire),
        node: process.version,
      },
      schemaVersion: 2,
      subjects: subjects.map((subject) => ({
        id: subject.id,
        package: subject.package,
        version: subject.version,
        implementation: subject.implementation,
        representation: subject.representation,
        config: subject.config,
        notes: subject.notes,
      })),
      suite: "node",
      ...(options.samples ? { order } : {}),
    };
  } finally {
    await Promise.allSettled(subjects.map((subject) => subject.close()));
  }

  const json = `${JSON.stringify(report, stringifyBigInt, JSON_INDENT)}\n`;
  assertNoMachineIdentifiers(json);
  const nodeMajor = requiredArrayEntry(process.versions.node.split("."), 0, "Node.js version");
  const outputPath = resolve(
    options.output ?? `${HERE}/../results/node-${nodeMajor}-${report.fixture.rows}rows.json`,
  );
  await mkdir(dirname(outputPath), { recursive: true });
  await writeFile(outputPath, json, "utf8");
  process.stdout.write(renderSummary(report));
  process.stdout.write(`\nwrote ${outputPath}\n`);
}

const ROUND_TRIP_UNPROVEN_PREFIXES = [
  "encoder rejected the decoded value",
  "aborted the interpreter",
  "not reached",
  "the frame could not be decoded",
];
/** @type {Record<string, string>} */
const ROUND_TRIP_LABELS = {
  differs: "decode-then-encode q round trip differs from the fixture",
  resized: "q-identical round trip has a different canonical byte count",
  unverified: "decode-then-encode q round trip unverified",
};

// Identical / differs / resized / unverified for one table round trip.
// `roundTrip` is recorded by the preflight; the derivation below is the
// Compatibility path for reports written before that field existed.
/**
 * @param {TableFidelity} tableEntry
 * @returns {RoundTripState}
 */
function roundTripState(tableEntry) {
  if (tableEntry.roundTrip !== undefined) {
    return tableEntry.roundTrip;
  }
  if (tableEntry.sendComparable === true) {
    return "identical";
  }
  const reason = tableEntry.sendExcludedBecause ?? "";
  if (ROUND_TRIP_UNPROVEN_PREFIXES.some((prefix) => reason.startsWith(prefix))) {
    return "unverified";
  }
  if (!tableEntry.reencodesToIdenticalQValue) {
    return "differs";
  }
  if (tableEntry.reencodesToIdenticalQValue) {
    return "resized";
  }
  return "unverified";
}

// Id of the lowest median among the subjects ranked for this operation.
/**
 * @param {OperationReport} entry
 * @returns {string | undefined}
 */
function fastestSubject(entry) {
  /** @type {string | undefined} */
  let winner;
  for (const [id, measured] of Object.entries(entry.subjects)) {
    if (
      winner === undefined ||
      measured.medianMs <
        requiredRecordEntry(entry.subjects, winner, "operation subject metrics").medianMs
    ) {
      winner = id;
    }
  }
  return winner;
}

/**
 * @param {BenchmarkReport} report
 * @returns {string}
 */
function renderSummary(report) {
  const ids = report.subjects.map((subject) => subject.id);
  const lines = [
    `suite=node rows=${report.fixture.rows} q=${report.fixture.qVersion} node=${report.runtime.node} iterations=${report.method.iterationsPerSubjectPerOperation}`,
    "",
    `${"operation".padEnd(SUMMARY_OPERATION_COLUMN_WIDTH)}${ids
      .map((id) => `${id} ms`.padStart(SUMMARY_CLIENT_COLUMN_WIDTH))
      .join("")}${ids
      .slice(1)
      .map((id) => `${id} vs xqdb`.padStart(SUMMARY_RATIO_COLUMN_WIDTH))
      .join("")}`,
  ];
  for (const [name, entry] of Object.entries(report.operations)) {
    const winner = fastestSubject(entry);
    const medians = ids.map((id) => {
      const measured = entry.subjects[id];
      /** @type {string} */
      let value;
      if (measured === undefined) {
        value = "n/a";
      } else {
        const winnerSuffix = id === winner ? " *" : "  ";
        value = `${measured.medianMs.toFixed(SUMMARY_TIME_DECIMAL_PLACES)}${winnerSuffix}`;
      }
      return value.padStart(SUMMARY_CLIENT_COLUMN_WIDTH);
    });
    const ratios = ids.slice(1).map((id) => {
      const ratio = entry.medianRatioVsXqdb[id];
      const value =
        ratio === undefined ? "excluded" : `${ratio.toFixed(SUMMARY_RATIO_DECIMAL_PLACES)}x`;
      return value.padStart(SUMMARY_RATIO_COLUMN_WIDTH);
    });
    lines.push(
      `${name.padEnd(SUMMARY_OPERATION_COLUMN_WIDTH)}${medians.join("")}${ratios.join("")}`,
    );
  }
  lines.push(
    "* = fastest measured client for that operation",
    "",
    "atom fidelity and table decode-then-encode round trips (`~` in q)",
  );
  for (const id of ids) {
    const entry = requiredRecordEntry(report.fidelity, id, "fidelity reports");
    const tables = Object.entries(entry.tables)
      .map(([table, value]) => `${table}=${roundTripState(value)}`)
      .join(" ");
    const nanoseconds = entry.nanosecondTimestampExact
      ? "exact"
      : `lossy(${entry.nanosecondTimestampDecoded})`;
    const nanosecondMode =
      entry.nanosecondTimestampMode === "timed configuration"
        ? ""
        : ` [${entry.nanosecondTimestampMode ?? "unknown probe mode"}]`;
    lines.push(
      `  ${id.padEnd(SUMMARY_FIDELITY_ID_WIDTH)} int64=${entry.int64Exact ? "exact" : `lossy(${entry.int64Decoded})`} nanoseconds=${nanoseconds}${nanosecondMode} ${tables}`,
    );
  }
  for (const [name, entry] of Object.entries(report.operations)) {
    const table = name.includes(".") ? name.slice(name.indexOf(".") + 1) : undefined;
    /** @type {Record<string, RoundTripState>} */
    const states =
      entry.roundTripStates ??
      Object.fromEntries(
        (entry.roundTripUnverifiedSubjects ?? []).map((id) => [
          id,
          table === undefined
            ? "unverified"
            : roundTripState(
                requiredRecordEntry(
                  requiredRecordEntry(report.fidelity, id, "fidelity reports").tables,
                  table,
                  `${id} table fidelity`,
                ),
              ),
        ]),
      );
    const reasons = entry.roundTripReasons ?? entry.roundTripUnverifiedReasons ?? {};
    for (const [id, state] of Object.entries(states)) {
      if (state !== "identical") {
        const label =
          ROUND_TRIP_LABELS[state] ?? `decode-then-encode q round trip has unknown state ${state}`;
        lines.push(`  ${name}: ${id} ${label} - ${reasons[id] ?? "unknown"}`);
      }
    }
    for (const excluded of entry.unsupported ?? []) {
      lines.push(`  excluded from ${name}: ${excluded.subject} - ${excluded.reason}`);
    }
  }
  return `${lines.join("\n")}\n`;
}

export { renderSummary };

// Guarded so the summary renderer can be imported and checked without running
// A benchmark. `process.argv[1]` rather than `import.meta.main`, which needs
// Node 24 and would silently skip the run on older releases.
const entrypoint =
  typeof process.argv[1] === "string" ? pathToFileURL(process.argv[1]).href : undefined;
if (import.meta.url === entrypoint) {
  await main();
}
