import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";

interface RootPackageMetadata {
  readonly name: string;
  readonly version: string;
  readonly author: string;
  readonly repository: { readonly url: string };
  readonly engines: Readonly<Record<string, string>>;
  readonly optionalDependencies: Readonly<Record<string, string>>;
  readonly exports: Readonly<Record<string, unknown>>;
}

interface PlatformPackageMetadata {
  readonly name: string;
  readonly version: string;
  readonly author: string;
  readonly repository: { readonly url: string };
  readonly main: string;
  readonly files: readonly string[];
  readonly os: readonly string[];
  readonly cpu: readonly string[];
  readonly libc?: readonly string[];
  readonly engines: Readonly<Record<string, string>>;
}

interface PackageLockMetadata {
  readonly name: string;
  readonly version: string;
  readonly packages: Readonly<
    Record<
      string,
      {
        readonly name?: string;
        readonly version?: string;
        readonly optionalDependencies?: Readonly<Record<string, string>>;
      }
    >
  >;
}

async function readPackageMetadata<T>(relativePath: string): Promise<T> {
  const text = await readFile(new URL(relativePath, import.meta.url), "utf8");
  return JSON.parse(text) as T;
}

describe("npm package metadata", () => {
  it("advertises the public package identity, Node floor, and exports", async () => {
    const root = await readPackageMetadata<RootPackageMetadata>("../package.json");

    expect(root.name).toBe("@xbbg/xqdb");
    expect(root.author).toBe("XQDB contributors");
    expect(root.repository.url).toBe("git+https://github.com/underloam/xqdb.git");
    expect(root.engines.node).toBe(">=20");
    expect(Object.keys(root.exports)).toEqual(["."]);
    // Injected at pack time by scripts/set-optional-deps.mjs, deliberately absent from
    // the committed manifest so the release tag stays npm ci-installable while the
    // platform packages are unpublished.
    expect(root.optionalDependencies).toBeUndefined();
  });

  it("keeps native package versions, licenses, and platform constraints synchronized", async () => {
    const root = await readPackageMetadata<RootPackageMetadata>("../package.json");
    const windows = await readPackageMetadata<PlatformPackageMetadata>(
      "../npm/win32-x64-msvc/package.json",
    );
    const linux = await readPackageMetadata<PlatformPackageMetadata>(
      "../npm/linux-x64-gnu/package.json",
    );
    const darwin = await readPackageMetadata<PlatformPackageMetadata>(
      "../npm/darwin-arm64/package.json",
    );

    expect(windows).toMatchObject({
      name: "@xbbg/xqdb-win32-x64-msvc",
      version: root.version,
      author: "XQDB contributors",
      repository: { url: root.repository.url },
      main: "xqdb.win32-x64-msvc.node",
      files: ["xqdb.win32-x64-msvc.node", "LICENSE"],
      os: ["win32"],
      cpu: ["x64"],
      engines: { node: ">=20" },
    });
    expect(linux).toMatchObject({
      name: "@xbbg/xqdb-linux-x64-gnu",
      version: root.version,
      author: "XQDB contributors",
      repository: { url: root.repository.url },
      main: "xqdb.linux-x64-gnu.node",
      files: ["xqdb.linux-x64-gnu.node", "LICENSE"],
      os: ["linux"],
      cpu: ["x64"],
      libc: ["glibc"],
      engines: { node: ">=20" },
    });
    expect(darwin).toMatchObject({
      name: "@xbbg/xqdb-darwin-arm64",
      version: root.version,
      author: "XQDB contributors",
      repository: { url: root.repository.url },
      main: "xqdb.darwin-arm64.node",
      files: ["xqdb.darwin-arm64.node", "LICENSE"],
      os: ["darwin"],
      cpu: ["arm64"],
      engines: { node: ">=20" },
    });

  });

  it("keeps package-lock identity and optional dependencies synchronized", async () => {
    const root = await readPackageMetadata<RootPackageMetadata>("../package.json");
    const lock = await readPackageMetadata<PackageLockMetadata>("../package-lock.json");
    const lockRoot = lock.packages[""];

    expect(lock.name).toBe(root.name);
    expect(lock.version).toBe(root.version);
    expect(lockRoot?.name).toBe(root.name);
    expect(lockRoot?.version).toBe(root.version);
    // Both must omit them: a pin naming an unpublished version makes `npm ci` abort
    // with EUSAGE on a fresh checkout of the release tag.
    expect(lockRoot?.optionalDependencies).toBeUndefined();
    expect(root.optionalDependencies).toBeUndefined();
  });

});
