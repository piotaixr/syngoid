# syngoid M3a — Distribution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish signed syngoid releases with GoReleaser, and let a release build of syngoid download, verify, cache and push agents for other platforms, with `syngoid fetch` to pre-seed them.

**Architecture:** `internal/trust` verifies minisign-signed `SHA256SUMS` against embedded public keys. `internal/dist` manages verified store directories (the user cache, or `fetch --dir`) and downloads raw agent binaries into them. `bootstrap.Resolver` gains cache and download as its last two steps; `app` gains `fetch`. GoReleaser builds and signs releases; mise pins every tool (with a lockfile) and every CI job runs through mise tasks.

**Tech Stack:** Go 1.27, `golang.org/x/crypto/blake2b` (new direct dependency), `net/http`, GoReleaser 2.19.0, minisign 0.12, mise (lockfile), GitHub Actions (`jdx/mise-action`, `actions/attest-build-provenance`).

**Spec:** `docs/superpowers/specs/2026-10-28-syngoid-m3a-distribution-design.md` (the M3a design note, cited as "design §N") and the main spec `docs/superpowers/specs/2026-10-18-syngoid-design.md` (cited as "spec §N").

## Global Constraints

- Module `github.com/piotaixr/syngoid`; Go through mise (`mise run …` / `mise exec -- go …`).
- `CGO_ENABLED=0`. Release targets (spec §11.1): linux amd64, arm64, arm (v7), riscv64, ppc64le; freebsd amd64, arm64; illumos amd64; darwin amd64, arm64.
- Direct dependencies: the existing five plus `golang.org/x/crypto` (blake2b only). No minisign or HTTP client library.
- Console prefixes exactly `DEBUG: `, `INFO: ` (stdout), `WARNING: `, `CRITICAL ERROR: ` (stderr). Exit codes 0 ok, 1 soft failure, 2 critical/global.
- Asset names exactly: raw agent `syngoid_<ver>_<os>_<arch>`, archive `syngoid_<ver>_<os>_<arch>.tar.gz`, `SHA256SUMS`, `SHA256SUMS.minisig`; `<ver>` has no leading `v`. Trusted comment exactly `syngoid <ver>`.
- Default download template (unchanged): `https://github.com/piotaixr/syngoid/releases/download/v{version}/{file}`.
- User cache `${XDG_CACHE_HOME:-~/.cache}/syngoid/bin/<ver>/`, layout `SHA256SUMS`, `SHA256SUMS.minisig`, `<os>-<arch>/syngoid` (0755).
- Download caps: `SHA256SUMS` and `.minisig` 1 MiB each, agent 256 MiB; 30 s connect and response-header timeouts; schemes `http`/`https` only.
- Dev or dirty builds (`buildinfo.IsDev()`) never use the user cache and never download.
- `go test -race ./...`, `go vet ./...`, `golangci-lint run` (and `--build-tags integration`) clean, i.e. `mise run check`.
- Every commit message ends with a blank line then the `Co-Authored-By:` line from the harness attribution reminder.
- Pushing branches, opening PRs, pushing tags and changing repository settings need the user's go-ahead each time.
- Each task that changes behaviour described in the main spec edits that spec section in the same commit.

## Review Focus

1. **A download cut short, or two runs downloading at once,** must never leave a file that a later run pushes without checking it. Expected: nothing is installed unless fully verified, and `Lookup` re-verifies on every use. → Task 3 `TestDownloadCancelledMidBodyInstallsNothing`, `TestDownloadConcurrentSameDir`, `TestLookupCorruptBinaryRemoved`.
2. **A mirror serving the validly signed `SHA256SUMS` of an older release** (rollback) must be refused end to end, not just in the parser. → Task 3 `TestDownloadRejectsOtherVersion`.
3. **A home without a usable cache** (cron as a system user, `HOME` unset, read-only home, relative `XDG_CACHE_HOME`) must not break syncs whose agents resolve otherwise, and must explain itself when the cache was needed. Expected: a relative `XDG_CACHE_HOME` is ignored per the XDG spec; a missing cache is reported in the resolver error. → Task 3 `TestUserCacheDir`; Task 4 `TestResolveWithoutCache`.
4. **A mistyped `--download-url`** (no `{file}`, `{ver}` instead of `{version}`, `ftp://`) must fail with a message naming the template, before any request. → Task 3 `TestExpandURL`.
5. **GitHub's release downloads redirect** to another host. Expected: redirects are followed and the signature still decides trust. → Task 3 `TestDownloadFollowsRedirect`.

## Branches and PRs

- **PR 1 (Tasks 1–5):** branch `m3a-distribution`, created from the brainstorm branch `claude/milestone-3-brainstorm-6a97de` (it carries the design note and this plan), rebased on `main` before the PR.
- **PR 2 (Tasks 6–8):** branch `m3a-pipeline` from `main` after PR 1 merges (stack it on `m3a-distribution` if PR 1 is still in review). Task 7 uses PR 1's verifier.
- **Task 9:** release steps, outside any PR.

---

## File Structure

```
internal/trust/keys.go             embedded current + next public keys               (Task 1)
internal/trust/minisign.go         PublicKey, Signature, parsers, Verify, VerifyWith  (Task 1)
internal/trust/sums.go             ParseSums                                          (Task 1)
internal/trust/testdata/           golden vectors from the minisign CLI               (Task 1)
scripts/gen-trust-testdata.sh      regenerates the golden vectors                     (Task 1)
internal/trust/trusttest/          test-only signer and release builder               (Task 2)
internal/platform/platform.go      Platform, Supported, Self, Parse (moved from bootstrap) (Task 3)
internal/dist/store.go             Store, UserCacheDir, AssetName, Lookup             (Task 3)
internal/dist/download.go          ExpandURL, Download                                (Task 3)
internal/bootstrap/resolve.go      + Cache, NoDownload, ctx                           (Task 4)
internal/cli/fetch.go              fetch flag set and FetchOptions                    (Task 5)
internal/app/fetch.go              runFetch                                           (Task 5)
mise.toml, mise.lock               tools, lockfile, tasks                             (Tasks 6, 7)
.github/workflows/ci.yml           mise-action jobs, release-check                    (Tasks 6, 7)
.goreleaser.yaml                   builds, archives, checksum, signs, release         (Task 7)
scripts/release-check.sh           throwaway key, signed snapshot, verify             (Task 7)
internal/trust/releasecheck_test.go  verifies dist/ (build tag releasecheck)          (Task 7)
.github/workflows/release.yml      release + verify jobs                              (Task 8)
```

---

### Task 0 (maintainer, before Task 1): signing keys

Ask the user to run, on their machine, and paste the two `.pub` files' second lines (the base64 keys):

```sh
minisign -G -p syngoid-current.pub -s syngoid-current.key
minisign -G -p syngoid-next.pub    -s syngoid-next.key
```

Both with a password. Never ask for or handle the `.key` files or passwords. If the keys are not available yet, Task 1 proceeds with the trusttest-generated keys in tests and `keys.go` holds a clearly named `TODO(maintainer)` placeholder that `TestEmbeddedKeys` fails on, so PR 1 cannot merge without them.

---

### Task 1: `internal/trust` — minisign verification

**Files:**
- Create: `internal/trust/minisign.go`, `internal/trust/sums.go`, `internal/trust/keys.go`, `internal/trust/minisign_test.go`, `internal/trust/sums_test.go`, `internal/trust/fuzz_test.go`, `internal/trust/testdata/*`, `scripts/gen-trust-testdata.sh`
- Modify: `go.mod`, `go.sum` (add `golang.org/x/crypto`)

**Interfaces:**
- Produces:
  - `type PublicKey struct { ID [8]byte; Key ed25519.PublicKey }`; `func (k PublicKey) IDString() string` — `%016X` of the little-endian uint64 of `ID` (minisign's display form).
  - `func ParsePublicKey(text string) (PublicKey, error)` — accepts a whole `.pub` file (untrusted comment line + base64 line) or the bare base64 line. Decoded: `"Ed"` + 8-byte ID + 32-byte key.
  - `type Signature struct { Algorithm string; KeyID [8]byte; Sig []byte; TrustedComment string; GlobalSig []byte }`
  - `func ParseSignature(b []byte) (Signature, error)` — four lines: `untrusted comment: …`; base64 of 2-byte algorithm + 8-byte key ID + 64-byte signature; `trusted comment: <text>`; base64 64-byte global signature. Tolerates `\r\n` and a missing final newline.
  - `func Verify(sums, sig []byte, wantVersion string) error` — `VerifyWith(trustedKeys(), …)`.
  - `func VerifyWith(keys []PublicKey, sums, sig []byte, wantVersion string) error` — checks, in order (design §4.1): key ID known (`unknown signing key <IDString>`), algorithm `ED` (`unsupported signature algorithm "<alg>"`), Ed25519 over `blake2b.Sum512(sums)` (`invalid signature`), global signature over `Sig ‖ TrustedComment` (`invalid trusted comment signature`), `TrustedComment == "syngoid "+wantVersion` (`signed for "<got>", want "syngoid <ver>"`).
  - `func ParseSums(b []byte) (map[string]string, error)` — lines `<64 lowercase hex><two spaces or " *"><name>`; blank lines skipped; malformed line → `SHA256SUMS line <n>: malformed`; duplicate → `SHA256SUMS: duplicate entry <name>`.
  - `type Cleanuper interface { Helper(); Cleanup(func()) }` and `func ReplaceKeysForTesting(tb Cleanuper, keys ...PublicKey)` — swaps the trusted keys, restores them in `Cleanup`. (No `testing` import in production code.)
  - `keys.go`: `var embeddedKeys = []string{currentKey, nextKey}` parsed once by `trustedKeys() []PublicKey` (panics on a malformed constant — a build error in disguise, pinned by `TestEmbeddedKeys`).

- [ ] **Step 1: Write `scripts/gen-trust-testdata.sh` and generate the vectors**

The script needs `minisign` (Task 6 pins it in mise; until then `mise exec minisign@0.12 -- …` or a local install). In a temp dir it creates two passwordless key pairs (`minisign -G -W -p … -s …`), writes `sums.txt` (two lines: a 64-hex digest for `syngoid_1.2.3_linux_amd64` and one for `syngoid_1.2.3_freebsd_amd64`), signs it twice — `sums.txt.minisig` with `-t "syngoid 1.2.3"` and `sums-other-version.txt.minisig` with `-t "syngoid 1.2.2"` — and copies into `internal/trust/testdata/`: `test.pub`, `other.pub`, `sums.txt`, both `.minisig`. Secret keys are discarded. Commit the outputs.

- [ ] **Step 2: Write the failing tests**

`minisign_test.go` (keys loaded from `testdata/test.pub`, verified with `VerifyWith`):
- `TestVerifyGolden`: valid → nil.
- `TestVerifyRejects` table, each asserting the exact error text from the Interfaces block: sums with one byte changed → `invalid signature`; keys `[other.pub]` → `unknown signing key <IDString of test.pub>`; `sums-other-version.txt.minisig` with want `1.2.3` → `signed for "syngoid 1.2.2", want "syngoid 1.2.3"`; the valid signature with its trusted comment line edited to `syngoid 1.2.4` → `invalid trusted comment signature`; algorithm bytes rewritten to `Ed` (re-encoded) → `unsupported signature algorithm "Ed"`.
- `TestParseSignatureMalformed`: three lines, bad base64, wrong decoded length, missing `trusted comment: ` prefix → errors.
- `TestParsePublicKey`: whole file and bare line give the same key; `IDString()` equals the ID minisign printed in the file's untrusted comment (`minisign public key <ID>`).
- `TestEmbeddedKeys`: both embedded keys parse and have different IDs.

`sums_test.go`: `TestParseSums` (golden file → two entries; `" *name"` form accepted; uppercase hex, 63 hex, one space, duplicate → errors with the exact texts).

`fuzz_test.go`: `FuzzParseSignature`, `FuzzParsePublicKey`, `FuzzParseSums` seeded with the testdata files; they must not panic.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `mise exec -- go test ./internal/trust/`
Expected: FAIL (package does not compile).

- [ ] **Step 4: Implement `minisign.go`, `sums.go`, `keys.go`**

`go get golang.org/x/crypto@latest` for `blake2b`. `keys.go` holds the Task 0 keys (or the placeholder described there).

- [ ] **Step 5: Run the tests and a short fuzz**

Run: `mise exec -- go test -race ./internal/trust/ && mise exec -- go test -run '^$' -fuzz FuzzParseSignature -fuzztime 20s ./internal/trust/`
Expected: PASS; fuzz finds nothing.

- [ ] **Step 6: Commit**

`feat(trust): verify minisign-signed SHA256SUMS against embedded keys`

---

### Task 2: `internal/trust/trusttest` — test signer

**Files:**
- Create: `internal/trust/trusttest/trusttest.go`, `internal/trust/trusttest/trusttest_test.go`

**Interfaces:**
- Consumes: Task 1 `trust.PublicKey`, `trust.ReplaceKeysForTesting`, `trust.VerifyWith`, `trust.ParseSignature`.
- Produces:
  - `type Signer struct { /* unexported */ }`
  - `func New(tb testing.TB) *Signer` — fresh Ed25519 key with a random ID, installed as the only trusted key via `ReplaceKeysForTesting`.
  - `func (s *Signer) PublicKey() trust.PublicKey`
  - `func (s *Signer) Sign(sums []byte, trustedComment string) []byte` — minisign `ED` format, same four-line layout as the CLI output.
  - `func (s *Signer) Release(version string, assets map[string][]byte) map[string][]byte` — returns `assets` plus `SHA256SUMS` (sorted lines `<hex>  <name>`) and `SHA256SUMS.minisig` signed with `syngoid <version>`.

- [ ] **Step 1: Write the failing tests**

- `TestSignVerifies`: `Release("1.2.3", {"a": …})` → `trust.Verify(files["SHA256SUMS"], files["SHA256SUMS.minisig"], "1.2.3")` is nil, and `ParseSums` lists `a` with its SHA-256.
- `TestSignMatchesCLIShape`: `ParseSignature` of `Sign` output and of `../testdata/sums.txt.minisig` both succeed, both have `Algorithm == "ED"`, and both untrusted comment lines start with `untrusted comment: `.
- `TestKeysRestored`: sign a release inside a subtest that calls `New`; after the subtest, `trust.Verify` of that release fails with `unknown signing key` (the signer's key was removed again).

- [ ] **Step 2: Run to verify failure** — `mise exec -- go test ./internal/trust/trusttest/` → FAIL (no package).
- [ ] **Step 3: Implement** — signing mirrors Task 1's verification (signature over `blake2b.Sum512(sums)`, global signature over `sig ‖ trustedComment`).
- [ ] **Step 4: Run** — `mise exec -- go test -race ./internal/trust/...` → PASS.
- [ ] **Step 5: Commit** — `test(trust): add a minisign-format test signer`

---

### Task 3: `internal/platform` and `internal/dist`

**Files:**
- Create: `internal/platform/platform.go`, `internal/platform/platform_test.go`, `internal/dist/store.go`, `internal/dist/download.go`, `internal/dist/store_test.go`, `internal/dist/download_test.go`
- Modify: `internal/bootstrap/platform.go` (use `internal/platform`), `internal/bootstrap/platform_test.go`, `internal/bootstrap/resolve_test.go` (`supported` → `platform.Supported()`)

**Interfaces:**
- Consumes: Task 1 `trust.Verify`, `trust.ParseSums`; Task 2 `trusttest.New`, `(*Signer).Release`.
- Produces:
  - `package platform`: `type Platform struct{ OS, Arch string }`; `func (p Platform) String() string` (`os/arch`); `func (p Platform) Dir() string` (`os-arch`); `func Supported() []Platform` (the 10 targets, spec §11.1 order); `func Self() Platform`; `func Parse(s string) (Platform, error)` — `os/arch` that must be supported, else `unsupported platform "<s>"; supported: <list>`.
  - `bootstrap`: `type Platform = platform.Platform`; `Self()` delegates; `ParsePlatform` unchanged in behaviour.
  - `package dist`:
    - `func AssetName(version string, p platform.Platform) string` → `syngoid_<ver>_<os>_<arch>`.
    - `func UserCacheDir(version string) (string, error)` → `$XDG_CACHE_HOME/syngoid/bin/<ver>` when `XDG_CACHE_HOME` is absolute, else `$HOME/.cache/syngoid/bin/<ver>`; error `cannot locate the user cache: HOME is not set` when neither applies.
    - `func ExpandURL(tmpl, version, file string, p platform.Platform) (string, error)` — replaces `{version}`, `{os}`, `{arch}`, `{file}`; errors: no `{file}` → `--download-url "<tmpl>": must contain {file}`; any other `{…}` left → `--download-url "<tmpl>": unknown placeholder {x}`; scheme not `http`/`https` → `--download-url "<tmpl>": scheme must be http or https`.
    - `type Store struct { Dir, Version, URLTemplate string; Client *http.Client; Log *slog.Logger }` — `Client` nil → a client with `Proxy: http.ProxyFromEnvironment`, 30 s dial and response-header timeouts; `Log` nil → discard. Safe for concurrent use (one mutex serialises downloads; verified sums are kept in memory after the first download, so they are fetched once per `Store`).
    - `func (s *Store) Lookup(p platform.Platform) (string, bool)` — design §4.2: returns `Dir/<os>-<arch>/syngoid` when the store's sums and signature verify for `Version` and the binary's SHA-256 matches `AssetName`. Corrupt binary → `WARNING: cached agent for <p> does not match SHA256SUMS, removing it` and removal; invalid sums/signature → `WARNING: <Dir>/SHA256SUMS does not verify (<err>), removing it` and removal of both. Absent files → miss without warning.
    - `func (s *Store) Download(ctx context.Context, p platform.Platform) (string, error)` — design §4.2 steps 1–3. `{os}`/`{arch}` in the sums URLs expand to `p`. Logs `INFO: downloading syngoid agent for <p> from <url>`. Errors: `GET <url>: <status>`, `<url>: larger than <cap> bytes`, `<asset>: SHA-256 does not match SHA256SUMS`, `SHA256SUMS has no entry for <asset>`, and the `trust` errors prefixed `SHA256SUMS from <url>: `. Writes temp files named `.syngoid-dl-*` in the target directories and removes them on every failure.

- [ ] **Step 1: Move `Platform` to `internal/platform`** with its tests (`TestParse`: `linux/arm` ok, `linux/386` and `linux` → error text above; `TestSupportedCount` = 10). Bootstrap keeps compiling through the alias. Run `mise exec -- go test ./internal/platform/ ./internal/bootstrap/` → PASS. Commit `refactor: move Platform to internal/platform`.

- [ ] **Step 2: Write the failing `dist` tests** (an `httptest.Server` serving `(*Signer).Release("1.2.3", {AssetName("1.2.3", p): binary})` with a request counter):
  - `TestAssetName`, `TestUserCacheDir` (absolute XDG used; relative XDG ignored → `$HOME/.cache/…`; both unset → error text).
  - `TestExpandURL`: the default template for `SHA256SUMS` → `https://github.com/piotaixr/syngoid/releases/download/v1.2.3/SHA256SUMS`; the three error cases with exact texts.
  - `TestDownloadInstalls`: returns `Dir/<os>-<arch>/syngoid`, mode 0755, content equal; `SHA256SUMS` and `.minisig` present in `Dir`; INFO line logged; a second `Download` of another platform makes no further sums requests.
  - `TestDownloadRejectsOtherVersion`: server built with `Release("1.2.2", …)`, store `Version` `1.2.3` → error contains `signed for "syngoid 1.2.2", want "syngoid 1.2.3"`; `Dir` contains no files.
  - `TestDownloadBadSignature`: sums served with one byte changed → error, nothing written.
  - `TestDownloadHashMismatch`: binary served with one byte changed → `SHA-256 does not match`, no binary, no `.syngoid-dl-*` left.
  - `TestDownloadHTTPErrors`: 404 and 500 → `GET <url>: 404 Not Found` etc.
  - `TestDownloadSizeCaps`: sums body of 1 MiB + 1 → `larger than 1048576 bytes`.
  - `TestDownloadCancelledMidBodyInstallsNothing`: the handler writes half the binary then blocks; cancel ctx → `context.Canceled`, no binary, no temp file.
  - `TestDownloadConcurrentSameDir`: two `Store` values on one `Dir`, 8 goroutines downloading the same platform → all succeed, the binary verifies, no temp files remain.
  - `TestDownloadFollowsRedirect`: a second server 302-redirects every path to the first → success.
  - `TestDownloadScheme`: template `ftp://x/{file}` → the scheme error, no request.
  - `TestLookup`: miss on empty dir (no warning); hit after `Download`; makes no HTTP request.
  - `TestLookupCorruptBinaryRemoved`: after `Download`, overwrite the binary → miss, warning text, file gone.
  - `TestLookupBadSumsRemoved`: after `Download`, edit `SHA256SUMS` → miss, warning, sums and `.minisig` gone.

- [ ] **Step 3: Run to verify failure** — `mise exec -- go test ./internal/dist/` → FAIL.
- [ ] **Step 4: Implement `store.go` and `download.go`** — every install is temp file + `os.Rename` in the same directory; the binary is renamed last.
- [ ] **Step 5: Run** — `mise exec -- go test -race ./internal/dist/ ./internal/platform/ ./internal/bootstrap/` → PASS.
- [ ] **Step 6: Commit** — `feat(dist): verified agent store, user cache and download`

---

### Task 4: Resolver with cache and download

**Files:**
- Modify: `internal/bootstrap/resolve.go`, `internal/bootstrap/resolve_test.go`, `internal/bootstrap/connect.go:148` (pass `ctx`), `internal/dist/store.go` (`NewUserCache`), `cmd/syngoid/main.go` (`newConnector`), spec §5.3

**Interfaces:**
- Consumes: Task 3 `dist.Store`, `dist.UserCacheDir`, `(*Store).Lookup`, `(*Store).Download`.
- Produces:
  - `Resolver` gains `Cache *dist.Store` (nil → no cache, no download) and `NoDownload bool`; `CacheErr error` (why `Cache` is nil for a release build, reported in the failure).
  - `func (r Resolver) Resolve(ctx context.Context, p Platform) (string, error)`.
  - `func NewUserCache(version, urlTemplate string, log *slog.Logger) (*dist.Store, error)` in `package dist` — `UserCacheDir` + `Store`; used by `main.go` and Task 5.
  - Failure texts (`<p>` = `os/arch`, `<d>` = `os-arch`), with the common tail `T` = `put one at DIR/<d>/syngoid and pass --bin-store=DIR, or install /usr/lib/syngoid/bin/<d>/syngoid`:
    - dev build: `no syngoid agent binary for <p>: T` (unchanged from M2);
    - release, download failed: `no syngoid agent binary for <p>: download failed: <err>; run "syngoid fetch --platform <p>" where downloads work, or T`;
    - release, `--no-download`: `no syngoid agent binary for <p> (--no-download): run "syngoid fetch --platform <p>", or T`;
    - release, no cache: `no syngoid agent binary for <p>: <CacheErr>; T`.

- [ ] **Step 1: Write the failing tests** (release version stamped as in `TestAgentDirName`; `trusttest` server as in Task 3):
  - `TestResolveOrder` keeps its cases (with `ctx`), plus: cache hit beats download (zero requests); stores beat the cache.
  - `TestResolveDownloads`: empty stores, release build → downloaded path; second `Resolve` → zero new requests.
  - `TestResolveNoDownload`: `NoDownload` → the `--no-download` text, zero requests; a cache hit still wins.
  - `TestResolveDevSkipsCache`: `Version = "dev"` with a populated cache → the dev text, zero requests.
  - `TestResolveDownloadFailure`: server returns 404 → the download-failed text containing `GET `.
  - `TestResolveWithoutCache`: `Cache` nil, `CacheErr` `cannot locate the user cache: HOME is not set` → the no-cache text.
- [ ] **Step 2: Run to verify failure** — `mise exec -- go test ./internal/bootstrap/` → FAIL.
- [ ] **Step 3: Implement** `Resolve` per design §4.3 (cache and download only when `!buildinfo.IsDev()`, whatever `Cache` holds) and `dist.NewUserCache`; in `main.go`, for a release build call `dist.NewUserCache(buildinfo.Version, o.DownloadURL, log)` and set `Cache` or `CacheErr`; set `NoDownload: o.NoDownload`. Update the M2 paragraph of spec §5.3 to the design §4.3 order.
- [ ] **Step 4: Run** — `mise run check` → PASS.
- [ ] **Step 5: Commit** — `feat(bootstrap): resolve agents from the user cache and by download`

---

### Task 5: `syngoid fetch`, README, spec

**Files:**
- Create: `internal/cli/fetch.go`, `internal/cli/fetch_test.go`, `internal/app/fetch.go`, `internal/app/fetch_test.go`
- Modify: `internal/cli/options.go` (`Parse` dispatch, `Parsed.Fetch`), `internal/cli/options_test.go` (`fetch` rows of `TestParseSubcommands`/`TestParseSubcommandArity`), `internal/app/app.go` (dispatch, usage text), `internal/app/app_test.go` (remove `TestMainFetchNotYet`), `README.md`, spec §3.7, §11.4, §11.5

**Interfaces:**
- Consumes: Task 3 `platform.Parse`, `platform.Supported`, `dist.Store`; Task 4 `dist.NewUserCache`.
- Produces:
  - `type FetchOptions struct { Platforms []platform.Platform; All bool; Dir, DownloadURL string; Quiet, Debug bool }`; `Parsed.Fetch *FetchOptions`.
  - `Parse`: when `args[0] == "fetch"`, the rest is parsed by its own flag set (`--platform` repeatable, `--all`, `--dir`, `--download-url` default as sync, `--quiet`, `--debug`). Errors: neither or both of `--platform`/`--all` → `fetch needs --platform OS/ARCH or --all`; positional argument → `fetch takes no arguments`; bad platform → the `platform.Parse` error. A `fetch` reached after global flags (`-r fetch`) keeps the existing `fetch takes no options` error.
  - `runFetch(ctx context.Context, env Env, f *cli.FetchOptions) int` in `internal/app/fetch.go`.
  - Usage text gains the line `       syngoid fetch (--platform OS/ARCH ... | --all) [--dir DIR]`.

- [ ] **Step 1: Write the failing tests**
  - `cli`: `TestParseFetch` (`--platform linux/arm64 --platform freebsd/amd64` → two platforms; `--all --dir d --download-url u --quiet`; each error text above).
  - `app` (release version stamped, `XDG_CACHE_HOME` set to a temp dir with `t.Setenv`, `trusttest` server, `--download-url <server>/{file}`):
    - `TestFetchAllDir`: `fetch --all --dir D` → exit 0; `D/<d>/syngoid` for all 10 platforms; `D/SHA256SUMS` and `.minisig` present; sums requested once.
    - `TestFetchIntoUserCache`: `fetch --platform linux/arm64` → file under `$XDG_CACHE_HOME/syngoid/bin/1.2.3/linux-arm64/syngoid`.
    - `TestFetchAlreadyPresent`: second run → `INFO: linux/arm64 already present`, no agent request.
    - `TestFetchPartialFailure`: server lacks `freebsd/amd64` → exit 2, `CRITICAL ERROR:` line naming `freebsd/amd64`, `linux/arm64` still installed.
    - `TestFetchDevBuild`: `Version = "dev"` → exit 2, stderr `CRITICAL ERROR: fetch needs a release build`.
- [ ] **Step 2: Run to verify failure** — `mise exec -- go test ./internal/cli/ ./internal/app/` → FAIL.
- [ ] **Step 3: Implement** — `runFetch` builds a `dist.Store` on `Dir` (or `NewUserCache`), then for each platform `Lookup` (INFO when present) else `Download`; failures are logged as `CRITICAL ERROR: <p>: <err>` and make the exit code 2. `--no-download` does not exist on the fetch flag set.
- [ ] **Step 4: Docs** — README: status line `milestone 3a (distribution)`; a "Agent binaries" subsection: resolution order, user cache path, automatic download for release builds, `--no-download`, `fetch` with `--dir` for air-gapped hosts, and how to check a download by hand (`minisign -Vm SHA256SUMS -P <current key>`, `sha256sum --ignore-missing -c SHA256SUMS`, `gh attestation verify`); the "Not yet available" line keeps only `--otel-endpoint`. Spec §3.7 `fetch` synopsis per design §4.4; §11.4 per design §4.2 (raw binary, no extraction); §11.5 adds the `--dir` layout.
- [ ] **Step 5: Run** — `mise run check` → PASS.
- [ ] **Step 6: Commit** — `feat(app): add syngoid fetch`
- [ ] **Step 7: PR 1** — confirm `keys.go` holds the Task 0 keys (`TestEmbeddedKeys` passes), rebase on `main`, then ask the user before pushing `m3a-distribution` and opening the PR.

---

### Task 6: mise lockfile and CI on mise

**Files:**
- Modify: `mise.toml`, `.github/workflows/ci.yml`, `README.md` (Build section)
- Create: `mise.lock`

**Interfaces:**
- Produces: mise tasks `test-integration` and the two-pass `lint`, used by Task 7 and 8 workflows; `MISE_LOCKED=1` convention.

- [ ] **Step 1: `mise.toml`** — `[tools]` add `goreleaser = "2.19.0"`, `minisign = "0.12"`; `[settings]` `lockfile = true`, `lockfile_platforms = ["linux-x64", "linux-arm64", "macos-x64", "macos-arm64"]`; task `lint` runs `golangci-lint … run` then `… run --build-tags integration`; task `test-integration` = `sudo -E env PATH=$PATH go test -tags integration -v -timeout 45m ./test/integration/...`.
- [ ] **Step 2: Lock** — `mise lock`, then `MISE_LOCKED=1 mise install` succeeds; `mise.lock` lists the three tools for the four platforms. Commit `build: pin goreleaser and minisign with a mise lockfile`.
- [ ] **Step 3: `ci.yml`** — every job: `actions/checkout@v5`, `jdx/mise-action` (current major, `install: true`, cache on), `env: MISE_LOCKED: "1"`. `test`: `mise run vet`, `mise run test`. `lint`: `mise run lint`. `integration`: `scripts/ci/setup-zfs.sh`, then `mise run test-integration` with the same `SYNGOID_IT*` env. The `cross` job stays until Task 7 replaces it.
- [ ] **Step 4: Verify** — `mise run check` locally → PASS; `actionlint` on `ci.yml` if available. CI itself runs when the PR is pushed (Task 8 step).
- [ ] **Step 5: Commit** — `ci: run every job through mise tasks`

---

### Task 7: GoReleaser and `release-check`

**Files:**
- Create: `.goreleaser.yaml`, `scripts/release-check.sh`, `internal/trust/releasecheck_test.go`
- Modify: `mise.toml` (tasks), `.github/workflows/ci.yml` (`cross` → `release-check`), spec §11.1, §11.2, §11.3, §12.1, §14

**Interfaces:**
- Consumes: Task 1 `trust.ParsePublicKey`, `trust.VerifyWith`, `trust.ParseSums`.
- Produces: `dist/` layout used by Task 8: the 10 tar.gz archives, 10 raw binaries, `SHA256SUMS`, `SHA256SUMS.minisig`, `metadata.json`.

- [ ] **Step 1: Write `releasecheck_test.go`** (`//go:build releasecheck`): `TestReleaseCheck` reads `SYNGOID_RELEASECHECK_DIR` (default `../../dist`) and `SYNGOID_RELEASECHECK_PUBKEY` (path, required), takes the version from `metadata.json`'s `version` field, then asserts: `VerifyWith` nil; `ParseSums` has exactly 20 entries; each of the 10 `AssetName`s and 10 `.tar.gz` names is listed and its file hash matches.
- [ ] **Step 2: Write `.goreleaser.yaml`** per design §3.3 (`version: 2`; `snapshot.version_template: "{{ incpatch .Version }}-snapshot"`; ldflags `-s -w` plus the three `-X` stamps; archive `files` with `info.mtime: "{{ .CommitDate }}"`).
- [ ] **Step 3: Write `scripts/release-check.sh`** and tasks — `release-snapshot` = `goreleaser release --snapshot --clean --skip=sign`; `release-check` runs the script: temp dir with trap cleanup, random password, `printf '%s\n%s\n' "$pw" "$pw" | minisign -G -p "$tmp/k.pub" -s "$tmp/k.key"`, then `MINISIGN_KEY_FILE=… MINISIGN_PASSWORD=… goreleaser release --snapshot --clean`, then `SYNGOID_RELEASECHECK_PUBKEY="$tmp/k.pub" go test -tags releasecheck -run TestReleaseCheck ./internal/trust`.
- [ ] **Step 4: Resolve design §7 items, recording outcomes in spec §14**
  1. `mise run release-check </dev/null` (no terminal on stdin; the script pipes the password itself). Expected: PASS. If `minisign` refuses a non-terminal password, **stop and ask the user** (design §7.1).
  2. Run `mise run release-check` twice; `diff` the two `dist/SHA256SUMS` → identical. If not, find the varying artifact (`sha256sum` per file) and fix the config (usually an archive mtime) before continuing.
  3. `SHA256SUMS` lists the raw binaries (covered by `TestReleaseCheck`'s 20 entries).
- [ ] **Step 5: CI** — replace the `cross` job with `release-check` (mise-action, `MISE_LOCKED=1`, `fetch-depth: 0`, `mise run release-check`).
- [ ] **Step 6: Spec** — §11.1 config summary; §11.2 artifacts per design §3.5 (no deb/rpm, no remote-bins tarball, raw binaries added); §11.3 the two keys and the `release` environment; §12.1 adds the golden vectors and release-check; §14 the three outcomes.
- [ ] **Step 7: Commit** — `build: GoReleaser config and release-check`

---

### Task 8: Release workflow

**Files:**
- Create: `.github/workflows/release.yml`
- Modify: spec §1.2 and §13 (packages, remote-bins tarball and nightly BSD/illumos jobs deferred), `README.md` (a short "Releases" note: how a release is cut and approved)

- [ ] **Step 1: Write `release.yml`** per design §3.4:
  - `on: push: tags: ['v*']`; `permissions: contents: read` at top level.
  - `release` job: `environment: release`; job permissions `contents: write`, `id-token: write`, `attestations: write`; checkout `fetch-depth: 0`; mise-action with `MISE_LOCKED=1`; write `${{ secrets.MINISIGN_SECRET_KEY }}` to `$RUNNER_TEMP/minisign.key` with `umask 077`; `mise exec -- goreleaser release --clean` with `MINISIGN_KEY_FILE`, `MINISIGN_PASSWORD`, `GITHUB_TOKEN`; `actions/attest-build-provenance` with `subject-path` covering `dist/syngoid_*` (archives and raw binaries) and `dist/SHA256SUMS`; remove the key file in an `if: always()` step.
  - `verify` job: `needs: release`; mise-action (for `minisign`); `gh release download "$GITHUB_REF_NAME" -p SHA256SUMS -p SHA256SUMS.minisig -p "syngoid_${VER}_linux_amd64"` (`VER` = tag without `v`); `minisign -Vm SHA256SUMS -P <current public key from keys.go>`; `sha256sum --ignore-missing -c SHA256SUMS`; `chmod +x`, then `./syngoid_${VER}_linux_amd64 fetch --all --dir out`; assert 10 files under `out/`.
- [ ] **Step 2: Verify** — `actionlint` clean if available; `mise run check` PASS.
- [ ] **Step 3: Commit** — `ci: release workflow with signing, attestations and a fetch check`
- [ ] **Step 4: PR 2** — ask the user before pushing `m3a-pipeline` and opening the PR; CI (all mise jobs plus `release-check`) must be green.

---

### Task 9: First release (each push only after asking)

- [ ] **Step 1: Repository settings (user)** — environment `release`, required reviewer the maintainer, deployment branches/tags limited to `v*`; secrets `MINISIGN_SECRET_KEY` (contents of `syngoid-current.key`) and `MINISIGN_PASSWORD`; tag protection rule for `v*`. Give the user these steps; do not change settings yourself.
- [ ] **Step 2: `v0.1.0-rc.1`** — after asking, tag `main` and push the tag. The user approves the environment. Expected: the release is a pre-release with 22 assets (10 archives, 10 raw binaries, `SHA256SUMS`, `.minisig`) and attestations; `verify` passes.
- [ ] **Step 3: Manual check (user)** — with the rc binary on a host, sync to a remote of another platform; expected `INFO: downloading syngoid agent for <p> from https://github.com/piotaixr/syngoid/releases/download/v0.1.0-rc.1/syngoid_0.1.0-rc.1_<os>_<arch>` then a successful run; a second run makes no download.
- [ ] **Step 4: `v0.1.0`** — after asking, tag and push. Expected: a full release, `verify` passes. Update the README status line in a follow-up commit if it should name the release.
