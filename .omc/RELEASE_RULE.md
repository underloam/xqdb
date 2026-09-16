# Release Rules

<!-- last-analyzed: 2026-09-01T22:30:00Z -->

## Version Sources

Every source below must hold the identical version string, and the tag must be
exactly `v<version>`. `.github/workflows/NPM.yml` job `source` enforces this and
fails the release on any mismatch.

| File                                          | Field                                                          |
| --------------------------------------------- | -------------------------------------------------------------- |
| `Cargo.toml`                                  | `[workspace.package] version`                                  |
| `Cargo.lock`                                  | `version` of packages `xqdb`, `napi-xqdb`, `py-xqdb`            |
| `js-xqdb/package.json`                        | `version` only — see the pin note below                        |
| `js-xqdb/package-lock.json`                   | `version`, `packages[""].version`                              |
| `js-xqdb/npm/win32-x64-msvc/package.json`     | `version`                                                      |
| `js-xqdb/npm/linux-x64-gnu/package.json`      | `version`                                                      |
| `js-xqdb/npm/darwin-arm64/package.json`       | `version`                                                      |

The Python distribution has no version field: `pyproject.toml` sets
`dynamic = ["version"]` and setuptools-scm derives it from the git tag, so
tagging is what versions the wheel.

### Platform pins are injected, not committed

Neither `js-xqdb/package.json` nor `js-xqdb/package-lock.json` may declare
`optionalDependencies`, and the lock must contain no resolved
`node_modules/@xbbg/xqdb-*` entries. The `source` job asserts both.

The reason is that an exact pin names a version the registry does not serve until
the platform packages publish, so any committed pin makes a fresh checkout of the
release tag fail `npm ci` with `EUSAGE ... package.json and package-lock.json are
not in sync`. Removing only the resolved entries does not help; npm then reports
the three natives as `Missing` and `--omit=optional` fails identically. A tag is
immutable, so a post-publish lock refresh cannot repair it.

The `assemble` job therefore runs `npm run set-optional-deps`
(`js-xqdb/scripts/set-optional-deps.mjs`) after `npm run build:ts` and before
`npm pack`. It reads each `npm/*/package.json`, requires every name to be a
`@xbbg/xqdb-` platform package at exactly the root version, and writes the pins.
Immediately after packing, `node scripts/assert-packed-pins.mjs <tarball>
<version>` extracts `package/package.json` from the tarball and fails unless all
three pins are present at exactly that version. `publish` declares
`needs: [source, assemble]`, so neither step can be skipped and a root package
cannot be published without its pins.

Two approaches that do **not** work, so do not retry them:

- `napi prepublish -t npm --skip-optional-publish` validates that every
  `npm/<target>/` already holds its `.node` and aborts with
  `Release package ... is incomplete` otherwise. In `assemble` only the packed
  platform tarballs exist, so it cannot run there.
- `"workspaces": ["npm/*"]` links the platform directories unconditionally, so npm
  enforces their `os`/`cpu` fields and fails on every host with
  `notsup Valid cpu: arm64 / Actual cpu: x64`.

No release automation script exists; the bump is a manual edit across the files
above.

## Release Trigger

- **npm**: push a `v*` tag. `NPM.yml` builds all three native targets, packs,
  smoke-tests, and publishes.
- **PyPI**: manual `workflow_dispatch` of `CI.yml` with `dry-run: false`, from the
  default branch, and only on repository `underloam/xqdb`. A tag push does not
  publish to PyPI. The `source` job requires the exact `v<version>` tag on the
  dispatched commit, so push the tag before dispatching.
- Tags must be pushed to the `xqdb` remote (`underloam/xqdb`). `task tag`
  defaults to that remote. Before overriding `REMOTE`, confirm it points to
  the release repository rather than the upstream project.

### Publish jobs wait for an environment approval

Both publish jobs run in a protected GitHub environment with a required
reviewer: `publish` in `NPM.yml` uses environment `npm`, `release` in `CI.yml`
uses environment `pypi`. Once the build jobs succeed the run sits in `waiting`
until someone approves the deployment — the v0.1.6 npm run waited three days
unnoticed. Approve from the run page or with:

```bash
run_id=$(gh run list --repo underloam/xqdb --workflow NPM.yml --limit 1 --json databaseId --jq '.[0].databaseId')
env_id=$(gh api "repos/underloam/xqdb/actions/runs/$run_id/pending_deployments" --jq '.[0].environment.id')
gh api -X POST "repos/underloam/xqdb/actions/runs/$run_id/pending_deployments" \
  -f "environment_ids[]=$env_id" -f state=approved -f comment="release"
```

The `github-release` job in each workflow runs only after its publish job, so an
unapproved run also leaves no GitHub Release and no attached artifacts.

### Post-publish verification can lag the registry

`publish` verifies the packages by installing them from the registry after
publishing. The 0.1.5 and 0.1.7 publishes both succeeded yet failed that check
because the registry had not propagated within the original 100-second window;
the window is now ten 30-second attempts. If it still fails after a successful
publish, `gh run rerun <run_id> --failed` is safe: `publish_if_missing` compares
the tarball integrity against the registry and skips any byte-identical version,
so the rerun only repeats the verification and then runs `github-release`. The
rerun waits for the `npm` environment approval again.

## Test Gate

From `.github/workflows/Quality.yml`:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --exclude py-xqdb --all-targets --locked
cargo test --workspace --exclude py-xqdb --doc --locked
python -m pytest -q py-xqdb/test
```

Plus `npm test` in `js-xqdb` from `JS.yml`.

`py-xqdb` is excluded from `cargo test` deliberately: it is a `cdylib` and its
test binary cannot resolve the Python DLL. `cargo test --workspace` without the
exclusion fails with `STATUS_DLL_NOT_FOUND`.

Run each gate without a masking pipeline. `cmd | tail` reports `tail`'s exit
status, so a failing suite looks green.

Python tests exercise a live q fixture when `XQDB_TEST_Q_EXTERNAL=1`; otherwise
the live cases skip. `XQDB_Q_ROWS` must match the running fixture.

## Registry / Distribution

| Target | Package                                            | Publisher                        |
| ------ | -------------------------------------------------- | -------------------------------- |
| npm    | `@xbbg/xqdb` plus three platform packages          | `NPM.yml`, on `v*` tag push      |
| PyPI   | `xqdb`                                             | `CI.yml` job `release`, manual   |

## Release Notes Strategy

Conventional Commits, no `CHANGELOG.md`. Tags are annotated with the message
`XQDB <version>`.

Notes live in `docs/release-notes/v<version>.md`, checked in before tagging. The
`github-release` job in `NPM.yml` passes that file to `gh release create
--notes-file` when it exists, and `gh release edit --notes-file` when the release
was pre-created; it falls back to `--generate-notes` only when the file is
absent. That job therefore checks the repository out — without a checkout the
file is not on disk, and the release silently degrades to commit subjects.

Subject lines alone are not sufficient when a release changes observable
behaviour, because `--generate-notes` emits only subjects and never a commit
body. Any release containing a breaking change MUST ship a notes file spelling
out the old behaviour, the new behaviour, and the migration. Draft the routine
part from `git log <prev-tag>..HEAD --no-merges --format="%s"`.

`v0.1.4` carries one such change — Python q timestamp atoms are now naive where
0.1.3 returned UTC-aware values — and its notes file already documents it.

## CI Workflow Files

- `.github/workflows/NPM.yml` — npm release on tag
- `.github/workflows/CI.yml` — PyPI release on manual dispatch
- `.github/workflows/Quality.yml` — Rust and Python gates
- `.github/workflows/JS.yml` — Node gates

## First-Time Setup Gaps

Before the first release after an organization rename, configure the npm
trusted publishers for `@xbbg/xqdb`, `@xbbg/xqdb-win32-x64-msvc`,
`@xbbg/xqdb-linux-x64-gnu`, and `@xbbg/xqdb-darwin-arm64` with owner
`underloam`, repository `xqdb`, workflow `NPM.yml`, and environment `npm`.
Configure the PyPI publisher for `xqdb` with owner `underloam`, repository
`xqdb`, workflow `CI.yml`, and environment `pypi`. Preserve the required
reviewers and other protections on both GitHub environments.

Release workflows exist, build artifacts are gitignored, and tags are in
use (`v0.1.0` through `v0.1.6`). npm has published `0.1.3` through `0.1.5`; PyPI
has `0.1.1` through `0.1.3` — the 0.1.4–0.1.6 Python releases were never
dispatched, so the next PyPI release note must carry the 0.1.4 timestamp
migration.
