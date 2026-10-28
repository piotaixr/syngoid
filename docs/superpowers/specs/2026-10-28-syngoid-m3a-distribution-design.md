# syngoid M3a — Distribution Design

- **Date:** 2026-10-28
- **Status:** Approved in brainstorming, pending written-spec review
- **Main spec:** `docs/superpowers/specs/2026-10-18-syngoid-design.md` (this note refines §3.7, §5.3, §11 and §12–14;
  each PR edits the main spec sections it touches, in the same commit as the code)

## 1. Scope

The M1 roadmap's M3 ("Release & observability") is split:

- **M3a — Distribution (this note):** signed releases, agent download and verification, the initiator cache,
  `syngoid fetch`, CI on mise.
- **M3b — Observability:** §10.2 OTLP log export (`--otel-endpoint`). Separate brainstorm, spec and plan.
- **Deferred (moved to main spec §13):** nightly FreeBSD/OmniOS integration jobs, deb/rpm packages, the
  `syngoid-remote-bins` tarball.

**Done when:** `v0.1.0` is published on GitHub with signed `SHA256SUMS` and attestations; the release workflow's
`verify` job has run `syngoid fetch --all --dir DIR` against it successfully; and on `v0.1.0-rc.1` the maintainer has
run a sync to a remote of another platform that downloaded, verified and pushed its agent.

## 2. Decisions

| # | Decision | Rationale |
|---|---|---|
| A1 | Drop deb/rpm packages from v1. | Not needed now; their only extra value was filling `/usr/lib/syngoid/bin`. |
| A2 | Drop the `syngoid-remote-bins_<ver>.tar.gz` tarball. `fetch --all --dir DIR` replaces it. | Same air-gap seeding with per-file verification; makes a plain GoReleaser config sufficient. |
| A3 | Keep GoReleaser (§11.1), signing through the `minisign` CLI in its `signs:` step. | With A1 and A2 nothing needs glue around it. |
| A4 | Publish raw binaries `syngoid_<ver>_<os>_<arch>` next to the tar.gz archives; agents are downloaded raw. | `SHA256SUMS` covers the exact file that is pushed: no in-process extraction, and a cached agent is re-verified by hashing it. Costs a ~2× larger one-time download per platform. |
| A5 | Two password-protected minisign key pairs, *current* and *next*; both public keys embedded. The current secret key and its password are secrets of an approval-gated `release` environment; the next secret key stays offline. | §11.3 rotation without a bridge release. |
| A6 | Accept only prehashed `ED` signatures. | minisign's default since 0.10; smaller verifier surface. |
| A7 | Download URLs may be `https` or `http`; any other scheme is an error. | Integrity comes from the signature; plain-HTTP intranet mirrors keep working. |
| A8 | Tools pinned in `mise.toml` (`go`, `goreleaser`, `minisign`); every CI job runs through `jdx/mise-action` and mise tasks. | Same versions locally and in CI. |
| A9 | The `cross` CI matrix is replaced by `mise run release-check` on every PR. | Builds the same 10 targets through the real release config, and checks signing and verification too. |
| A10 | First release goes through `v0.1.0-rc.1` (GitHub pre-release), then `v0.1.0`. | Proves download and verification against real GitHub before the real tag. |

## 3. Release pipeline

### 3.1 Keys (one-time, by the maintainer)

```sh
minisign -G -p syngoid-current.pub -s syngoid-current.key
minisign -G -p syngoid-next.pub    -s syngoid-next.key
```

Both are password-protected. The two public keys go into `internal/trust/keys.go` (PR 1). In the repository settings:
environment `release` with the maintainer as required reviewer and deployments limited to tags matching `v*`;
secrets `MINISIGN_SECRET_KEY` (contents of `syngoid-current.key`) and `MINISIGN_PASSWORD`. `syngoid-next.key` is
kept offline.

### 3.2 `mise.toml`

- `[tools]`: `go = "1.27.1"` (unchanged), `goreleaser = "2.19.0"`, `minisign = "0.12"`.
- New tasks, next to `build`, `vet`, `test`, `lint`, `check`:
  - `release-snapshot` — `goreleaser release --snapshot --clean --skip=sign`: every target and both archive sets
    into `dist/`, unsigned.
  - `release-check` — creates a throwaway password-protected key pair in a temp dir, runs the snapshot with signing
    on, then `go test -tags releasecheck ./internal/trust`, which verifies `dist/SHA256SUMS.minisig` and every
    listed hash with syngoid's own verifier (throwaway public key, dist dir and expected version passed through
    environment variables read only by that test).
  - `test-integration` — `sudo -E env PATH=$PATH go test -tags integration -v -timeout 45m ./test/integration/...`.
- `lint` also runs `golangci-lint run --build-tags integration`, as CI does today.

### 3.3 `.goreleaser.yaml` (version 2)

- **builds:** the 10 §11.1 targets (`goarm: "7"`), `env: [CGO_ENABLED=0]`, `flags: [-trimpath]`,
  `mod_timestamp: "{{ .CommitTimestamp }}"`, ldflags stamping `buildinfo.Version={{ .Version }}`,
  `buildinfo.Commit={{ .FullCommit }}`, `buildinfo.Hash={{ .FullCommit }}`. `Dirty` is never set (GoReleaser refuses
  a dirty tree outside snapshots).
- **archives:**
  - `tar.gz` named `syngoid_{{ .Version }}_{{ .Os }}_{{ .Arch }}`, with `LICENSE` and `README.md` given the commit
    time as mtime.
  - `formats: [binary]` named `syngoid_{{ .Version }}_{{ .Os }}_{{ .Arch }}` (the raw agent, A4).
- **checksum:** `name_template: SHA256SUMS`, `algorithm: sha256`, covering both archive sets.
- **signs:** `artifacts: checksum`, `cmd: minisign`,
  `args: [-S, -s, "{{ .Env.MINISIGN_KEY_FILE }}", -m, "${artifact}", -x, "${artifact}.minisig", -t, "syngoid {{ .Version }}"]`,
  `stdin: "{{ .Env.MINISIGN_PASSWORD }}"`. Output: `SHA256SUMS.minisig`.
- **release:** `prerelease: auto` (a `-rc.N` tag is published as a pre-release).

### 3.4 Workflows

- **`ci.yml`** — every job uses `jdx/mise-action`:
  - `test`: `mise run vet`, `mise run test`.
  - `lint`: `mise run lint` (replaces `golangci-lint-action`; still v2.15.0, both tag sets).
  - `integration`: `scripts/ci/setup-zfs.sh`, then `mise run test-integration`.
  - `release-check` (replaces `cross`): `mise run release-check`.
  - `fixtures.yml` is unchanged.
- **`release.yml`** — on push of a `v*` tag:
  - `release` job, `environment: release`; permissions `contents: write`, `id-token: write`, `attestations: write`.
    Checkout with full history, mise-action, write `MINISIGN_SECRET_KEY` to a 0600 file under `$RUNNER_TEMP`
    (`MINISIGN_KEY_FILE`), `goreleaser release --clean` through mise, then `actions/attest-build-provenance` over
    `dist/*.tar.gz`, the raw binaries and `SHA256SUMS`.
  - `verify` job (needs `release`): downloads the published `SHA256SUMS`, `SHA256SUMS.minisig` and
    `syngoid_<ver>_linux_amd64`, checks them with `minisign -V -P <current public key>` and
    `sha256sum --ignore-missing -c`, then runs `syngoid fetch --all --dir out` against the
    release. Fails the workflow if any platform fails.

### 3.5 Release artifacts (replaces main spec §11.2)

- `syngoid_<ver>_<os>_<arch>.tar.gz` (binary, LICENSE, README) — for people.
- `syngoid_<ver>_<os>_<arch>` (raw binary) — for agent downloads.
- `SHA256SUMS`, `SHA256SUMS.minisig` (trusted comment exactly `syngoid <ver>`).
- GitHub artifact attestations (SLSA provenance) for `gh attestation verify`; not checked by syngoid.

## 4. Initiator side

### 4.1 `internal/trust` (cryptography only)

- `keys.go` embeds the current and next public keys (A5). They are a package variable that only
  `ReplaceKeysForTesting(tb testing.TB, keys ...PublicKey)` can change, restoring them in `tb.Cleanup`; there is no
  runtime, flag or environment way to add a key.
- `ParsePublicKey`, `ParseSignature`: the minisign text formats (untrusted comment line, base64 signature, trusted
  comment line, base64 global signature).
- `Verify(sums, sig []byte, wantVersion string) error`, in this order:
  1. the signature's key ID is an embedded key, else `unknown signing key <id>`;
  2. algorithm `ED` (A6), else `unsupported signature algorithm`;
  3. Ed25519 signature over blake2b-512(`sums`) valid;
  4. global signature over signature‖trusted-comment valid;
  5. trusted comment exactly `syngoid <wantVersion>`, else `signed for "<got>", want "syngoid <wantVersion>"`.
- `ParseSums(sums []byte) (map[string]string, error)`: lines `<64 lowercase hex>  <name>`; rejects malformed lines
  and duplicate names.
- New direct dependency: `golang.org/x/crypto` (blake2b), already allowed by main spec §4.2.

### 4.2 `internal/dist` (store directories, cache, download)

- A **store dir** holds `SHA256SUMS`, `SHA256SUMS.minisig` and `<os>-<arch>/syngoid` (mode 0755). The user cache is
  `${XDG_CACHE_HOME:-~/.cache}/syngoid/bin/<ver>/`; `fetch --dir DIR` writes the same layout into `DIR`, which then
  works as `--bin-store DIR`.
- `Lookup(p)`: verify the store's sums and signature for this version (§4.1), then hash `<os>-<arch>/syngoid` and
  compare with the `syngoid_<ver>_<os>_<arch>` line.
  - Binary absent → miss.
  - Hash mismatch → `WARNING: cached agent for <os>/<arch> does not match SHA256SUMS, removing it`, file removed,
    miss.
  - Sums or signature present but invalid → warning, both removed, miss.
- `Download(ctx, p)`, URLs from the `--download-url` template (`{version}`, `{os}`, `{arch}`, `{file}`):
  1. GET `SHA256SUMS` and `SHA256SUMS.minisig` (each capped at 1 MiB) and `Verify` them.
  2. GET `syngoid_<ver>_<os>_<arch>` into a temp file in the store dir, hashing while streaming (capped at 256 MiB);
     compare with the sums line.
  3. `chmod 0755`, then rename sums, signature and binary into place. Concurrent runs are safe: every write is a
     temp file plus rename, and every file written is verified content.
  - `net/http`, system roots, `http.ProxyFromEnvironment`, 30 s connect and response-header timeouts, no overall
    timeout (the context cancels). Scheme `https` or `http` only (A7). Non-2xx → `GET <url>: <status>`.
  - Logs `INFO: downloading syngoid agent for <os>/<arch> from <url>`.
  - Nothing is installed unless every check passed; temp files are removed on every failure.

### 4.3 Resolution (replaces the M2 note in main spec §5.3)

`bootstrap.Resolver.Resolve(ctx, p)` (context added), first hit wins:

1. self, when `p` is the initiator's platform;
2. `--bin-store` dirs, then `/usr/lib/syngoid/bin` (unverified, as in M2);
3. user cache `Lookup` — release builds only;
4. `Download` into the user cache — release builds only, and not with `--no-download`.

A dev or dirty build skips 3 and 4. Failure is a bootstrap failure (exit 2):
`no syngoid agent binary for <os>/<arch>` followed by the download error when one was attempted, and the remedies
`syngoid fetch --platform <os>/<arch>` and `--bin-store=DIR` (`DIR/<os>-<arch>/syngoid`). A run downloads at most
once per platform.

### 4.4 `syngoid fetch` (refines main spec §3.7)

```
syngoid fetch (--platform OS/ARCH ... | --all) [--dir DIR] [--download-url TEMPLATE] [--quiet] [--debug]
```

- Exactly one of `--platform` (repeatable; each must be a §11.1 target) or `--all` (every §11.1 target); otherwise
  a usage error.
- Target store: the user cache, or `DIR`. Sums and signature are fetched and verified once per invocation.
- A platform already present and valid: `INFO: <os>/<arch> already present`, no download.
- Dev or dirty build: `CRITICAL ERROR: fetch needs a release build`, exit 2.
- Every requested platform is attempted; exit 0 if all succeeded, else 2.
- `fetch` ignores `--no-download`.

### 4.5 Out of scope

Pruning old versions from the initiator cache; verifying `--bin-store` or `/usr/lib/syngoid/bin` binaries (trusted
as before, the handshake checks the version); self-update.

## 5. Testing

- **`internal/trust` golden vectors** made by the real `minisign` CLI with a throwaway key
  (`scripts/gen-trust-testdata.sh`, output committed under `internal/trust/testdata/`): valid; tampered sums;
  unknown key; trusted comment for another version; trusted comment edited after signing (global signature fails);
  legacy `Ed` signature; malformed files. Native fuzzing of `ParsePublicKey`, `ParseSignature`, `ParseSums`.
- **`internal/trust/trusttest`**: a test-only signer (Ed25519 over blake2b-512, minisign format) plus key
  replacement, so other packages build signed releases for any version. The golden vectors keep it honest.
- **`internal/dist`** against `httptest`: full download and install; bad signature or wrong version writes nothing;
  hash mismatch removes the temp file; 404/500; size caps; context cancel mid-body; cache hit makes no request;
  corrupt cached binary warns, is removed and re-downloaded; `ftp:` rejected, `http:` accepted; two concurrent
  downloads into one dir.
- **`bootstrap.Resolver`**: table tests of the order, `--no-download`, dev build, remedy text.
- **`cli` / `app` / `main`**: `fetch` parsing and usage errors; `fetch --all --dir` against `httptest` with a
  release-stamped `buildinfo.Version`; `TestMainFetchNotYet` (`internal/app/app_test.go`) replaced.
- **Pipeline:** `mise run release-check` on every PR; the `verify` job on every tag.

## 6. Delivery

Branch `m3a-distribution` from `main`, two PRs:

1. **Client** — `internal/trust`, `internal/dist`, resolver, `fetch`, README; main spec §3.7, §5.3, §11.4, §11.5.
   Needs the two real public keys (§3.1) before merge.
2. **Pipeline** (after PR 1: `release-check` uses its verifier) — `mise.toml` tools and tasks, `.goreleaser.yaml`, `ci.yml`, `release.yml`; main spec §1.2, §11.1–11.3,
   §12, §13, §14.

Then, by the maintainer: `release` environment, secrets, tag protection. Then, each only after asking: push
`v0.1.0-rc.1` (maintainer approves the environment; `verify` must pass), then push `v0.1.0`.

## 7. Verification items (resolved in PR 2 by test)

1. `minisign -G` and `minisign -S` read the password from stdin when no terminal is attached (needed by
   `release-check` and `release.yml`). If they do not, work on PR 2 stops and the maintainer chooses between a
   passwordless release key (`minisign -G -W`, protected only by the environment approval) and another signer.
2. Two `release-snapshot` runs of the same commit produce an identical `SHA256SUMS`.
3. GoReleaser's `checksum` step lists the `formats: [binary]` artifacts as well as the tar.gz archives.
