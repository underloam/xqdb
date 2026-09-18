import { describe, expect, it } from "vitest";
import { readFile } from "node:fs/promises";

type JsonObject = Readonly<Record<string, unknown>>;

function isJsonObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function requiredJsonObject(value: unknown, name: string): JsonObject {
  if (!isJsonObject(value)) {
    throw new TypeError(`${name} must be a JSON object`);
  }
  return value;
}

async function readPackageMetadata(relativePath: string): Promise<JsonObject> {
  const text = await readFile(new URL(relativePath, import.meta.url), "utf8");
  const parsed: unknown = JSON.parse(text);
  return requiredJsonObject(parsed, relativePath);
}

describe("npm package metadata", () => {
  it("uses the public package name, Node floor, and napi-rs v3 targets", async () => {
    const root = await readPackageMetadata("../package.json");

    expect(root.name).toBe("@xbbg/xqdb");
    expect(root.engines).toStrictEqual({ node: ">=20" });
    expect(Object.keys(requiredJsonObject(root.exports, "package exports"))).toStrictEqual(["."]);
    expect(root.napi).toStrictEqual({
      binaryName: "xqdb",
      targets: ["x86_64-pc-windows-msvc", "x86_64-unknown-linux-gnu", "aarch64-apple-darwin"],
    });
    // Injected at pack time by scripts/set-optional-deps.mjs, deliberately absent from
    // The committed manifest so the release tag stays npm ci-installable while the
    // Platform packages are unpublished.
    expect(root.optionalDependencies).toBeUndefined();
  });

  it("keeps native package versions, licenses, and platform constraints synchronized", async () => {
    const root = await readPackageMetadata("../package.json");
    const windows = await readPackageMetadata("../npm/win32-x64-msvc/package.json");
    const linux = await readPackageMetadata("../npm/linux-x64-gnu/package.json");
    const darwin = await readPackageMetadata("../npm/darwin-arm64/package.json");

    expect(windows).toMatchObject({
      author: "XQDB contributors",

      cpu: ["x64"],
      engines: { node: ">=20" },
      files: ["xqdb.win32-x64-msvc.node", "LICENSE"],
      main: "xqdb.win32-x64-msvc.node",
      name: "@xbbg/xqdb-win32-x64-msvc",
      os: ["win32"],
      repository: { url: "git+https://github.com/underloam/xqdb.git" },
      version: root.version,
    });
    expect(linux).toMatchObject({
      author: "XQDB contributors",

      cpu: ["x64"],
      engines: { node: ">=20" },
      files: ["xqdb.linux-x64-gnu.node", "LICENSE"],
      libc: ["glibc"],
      main: "xqdb.linux-x64-gnu.node",
      name: "@xbbg/xqdb-linux-x64-gnu",
      os: ["linux"],
      repository: { url: "git+https://github.com/underloam/xqdb.git" },
      version: root.version,
    });
    expect(darwin).toMatchObject({
      author: "XQDB contributors",

      cpu: ["arm64"],
      engines: { node: ">=20" },
      files: ["xqdb.darwin-arm64.node", "LICENSE"],
      main: "xqdb.darwin-arm64.node",
      name: "@xbbg/xqdb-darwin-arm64",
      os: ["darwin"],
      repository: { url: "git+https://github.com/underloam/xqdb.git" },
      version: root.version,
    });
  });

  it("keeps package-lock identity and optional dependencies synchronized", async () => {
    const root = await readPackageMetadata("../package.json");
    const lock = await readPackageMetadata("../package-lock.json");
    const packages = requiredJsonObject(lock.packages, "package-lock packages");
    const lockRoot = requiredJsonObject(packages[""], "package-lock root package");

    expect(lock.name).toBe(root.name);
    expect(lock.version).toBe(root.version);
    expect(lockRoot.name).toBe(root.name);
    expect(lockRoot.version).toBe(root.version);
    // Both must omit them: a pin naming an unpublished version makes `npm ci` abort
    // With EUSAGE on a fresh checkout of the release tag.
    expect(lockRoot.optionalDependencies).toBeUndefined();
    expect(root.optionalDependencies).toBeUndefined();
  });
});
