# syngoid M1 — Local Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A working `syngoid` binary that replicates local→local with the full sync engine (planner, executor, remediation), delegation-first privilege model with preflight, `--dry-run`/`--confirm`, and syncoid-compatible naming — tested against a fake ZFS model and real ZFS on Linux CI, including interop with Perl syncoid 2.2.0.

**Architecture:** `internal/zfs` wraps the `zfs` CLI as argv calls; `internal/endpoint` exposes it as an `Endpoint` interface (with an in-memory fake); `internal/engine` gathers state, plans per dataset with a pure planner, and executes actions over two `Endpoint`s; `internal/app` wires CLI, logging, signals and exit codes. Remote endpoints (M2) plug in behind the same interface.

**Tech Stack:** Go 1.27, `spf13/pflag`, `klauspost/compress`, `pierrec/lz4/v4`, `golang.org/x/term`, `log/slog`, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-10-18-syngoid-design.md`

## Roadmap (separate plans, written when M1 lands)

- **M1 (this plan):** spec §3 (all parsing; remote locations parse but `sync` rejects them), §4 local parts, §6 (local), §8 (all), §9 (local + codecs), §10.1, §10.3, §12.1, §12.2 (minus RPC), §12.3 (Linux local), §12.4 (local interop), §14.1, `version` and `permissions` subcommands.
- **M2 — Remote:** §5 bootstrap (self + stores), §7 RPC/yamux/agent, `internal/sshx`, `RemoteEndpoint`, `serve`, remote codec placement §9.2, remote preflight, RPC component tests, `ssh localhost` integration, §14.2–14.4.
- **M3 — Release & observability:** §11 (GoReleaser, minisign verify, download, `fetch`, signing CI, packages), §10.2 OTLP export, FreeBSD/OmniOS nightly jobs.

## Global Constraints

- Module path `github.com/piotaixr/syngoid`; `go.mod` has `go 1.27`; `mise.toml` pins `go = "1.27.1"`.
- `CGO_ENABLED=0` everywhere. Unix-only build targets: linux, freebsd, illumos, darwin.
- Third-party dependencies in M1 are limited to `github.com/spf13/pflag`, `github.com/klauspost/compress`, `github.com/pierrec/lz4/v4`, `golang.org/x/term` (ruling R26). Anything else requires a spec change.
- No shell strings for ZFS: every `zfs`/`ps`/`id` invocation is an argv slice. Child env sets `LC_ALL=C` and appends `:/sbin:/usr/sbin:/usr/local/sbin:/bin:/usr/bin` to `PATH`.
- Console log prefixes exactly: `DEBUG: `, `INFO: ` (stdout); `WARNING: `, `CRITICAL ERROR: ` (stderr).
- Compat names exactly: sync snapshot `syncoid_<identifier_><hostname>_YYYY-MM-DD:hh:mm:ss-GMT<sign>HH:MM`; hold tag `syncoid_<identifier_><hostname>`; property `syncoid:sync`.
- Exit codes: 0 ok, 1 soft failure, 2 critical/global failure.
- All tests pass with `go test -race ./...`; `go vet ./...` and `golangci-lint run` are clean.
- Every commit message ends with a blank line then `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Nothing is pushed to `origin` during M1 (user decision: push after first implementation).

## Review Focus

1. **"dataset does not exist" from the *target* side** (e.g. target parent missing) must be a critical error, never the "source vanished → skip, exit 0" path. → Task 19 test `TestExecutorTargetMissingParentIsCritical`.
2. **Dataset names containing spaces** (legal in ZFS) must survive parsing and argv building. → Task 5 test `TestParseTreeNameWithSpace`, Task 6 test `TestSendArgsNameWithSpace`.
3. **Sibling-prefix datasets** (`tank/data` vs `tank/data2`) must not be mapped as parent/child when computing target names. → Task 17 test `TestGatherSiblingPrefixNotChild`.
4. **Identifiers/hostnames with regex metacharacters** (`.`) and half-hour / negative timezones must produce exact names and a prune pattern that matches only its own snapshots. → Task 7 tests `TestSyncSnapNameHalfHourOffsets`, `TestPrunePatternQuotesMeta`.
5. **Ctrl-C mid-transfer** must stop without starting new actions, leave no running child, print the summary, and exit 2. → Task 19 test `TestExecutorCancelMidTransfer`, Task 21 test `TestMainCancelledExit2`.

---

## File Structure

```
cmd/syngoid/main.go                  signal setup, os.Exit(app.Main(...))
internal/buildinfo/buildinfo.go      Version, Commit, Dirty, ProtocolVersion
internal/app/app.go                  Main: dispatch, sync/permissions/version commands
internal/app/summary.go              end-of-run summary
internal/logx/console.go             slog console handler
internal/zfs/errors.go               typed errors + Classify
internal/zfs/exec.go                 Exec interface, OSExec, CommandError
internal/zfs/client.go               Client (sudo prefix, output helper)
internal/zfs/query.go                ListTree/ListSnapshots/ListBookmarks/GetProps/LocalProps/Exists + parsers
internal/zfs/opts.go                 --sendoptions/--recvoptions mini-syntax
internal/zfs/mutate.go               Snapshot/Destroy/Hold/Release/Bookmark/AbortResume
internal/zfs/sendrecv.go             SendSpec/RecvSpec, SendArgs/EstimateArgs, Send/Receive/SendEstimate
internal/naming/naming.go            sync snapshot name, hold tag, prune pattern, bookmark fallback
internal/stream/codec.go             codecs
internal/stream/copy.go              buffered copy
internal/stream/progress.go          renderers + humanize helpers
internal/endpoint/endpoint.go        Endpoint interface, HostInfo, SendOpts
internal/endpoint/local.go           LocalEndpoint
internal/endpoint/fake/fake.go       in-memory ZFS model endpoint
internal/cli/options.go              Options, Parse
internal/cli/location.go             ParseLocation
internal/perms/allow.go              zfs allow parser, Effective
internal/perms/required.go           Required, Missing, GrantCommand
internal/engine/state.go             DatasetState, Status, Result, Side
internal/engine/order.go             ordering, matching, bookmark fallback
internal/engine/actions.go           Action types + Describe
internal/engine/plan.go              PlanDataset, PlanOptions
internal/engine/gather.go            Gather, GatherOne
internal/engine/dag.go               Order
internal/engine/recvopts.go          RecvArgs
internal/engine/estimate.go          EstimateAction
internal/engine/render.go            RenderPlan
internal/engine/exec.go              Executor
internal/engine/preflight.go         Preflight, PreflightReport
test/integration/*_test.go           //go:build integration
scripts/ci/setup-zfs.sh              CI pool + user setup
.github/workflows/ci.yml             test, lint, cross-compile, integration
```

---

### Task 1: Project scaffold, build info, `version` command, CI

**Files:**
- Create: `go.mod`, `mise.toml`, `.gitignore`, `.golangci.yml`, `LICENSE`, `README.md`
- Create: `internal/buildinfo/buildinfo.go`, `internal/app/app.go`, `cmd/syngoid/main.go`
- Create: `.github/workflows/ci.yml`
- Test: `internal/app/app_test.go`

**Interfaces:**
- Produces: `buildinfo.Version string` (ldflags-settable, default `"dev"`), `buildinfo.Commit string`, `buildinfo.Dirty string` (`"true"`/`""`), `const buildinfo.ProtocolVersion = 1`, `func buildinfo.IsDev() bool` (Version == "dev" or Dirty == "true").
- Produces: `type app.Env struct { Stdin io.Reader; Stdout, Stderr io.Writer; StdinIsTTY, StderrIsTTY bool; Hostname func() (string, error); Now func() time.Time }` and `func app.Main(ctx context.Context, args []string, env app.Env) int`. Later tasks add fields to `Env` and cases to `Main`.

- [ ] **Step 1:** In `/Users/remi/projects/syngoid`: `mise use go@1.27.1`, `go mod init github.com/piotaixr/syngoid`, set `go 1.27`. `.gitignore`: `/dist/`, `/syngoid`. `LICENSE`: GPLv3 text from `https://www.gnu.org/licenses/gpl-3.0.txt`. `README.md`: one paragraph stating syngoid is a Go rewrite of syncoid from the sanoid project (https://github.com/jimsalterjrs/sanoid), GPLv3, link to the spec.

- [ ] **Step 2: Write the failing test** `TestMainVersion`: `Main(ctx, []string{"version"}, env)` returns 0 and stdout contains `syngoid dev` and `protocol 1`. `TestMainUnknownFlagExit2`: `Main(ctx, []string{"--nope"}, env)` returns 2 and stderr is non-empty.

- [ ] **Step 3:** Run `go test ./internal/app/` — FAIL (package missing).

- [ ] **Step 4: Implement.** `version` prints `syngoid <Version> (commit <Commit>[, dirty]) protocol <ProtocolVersion>`. Any parse error → print to stderr, return 2. `cmd/syngoid/main.go` builds `Env` from `os` and calls `os.Exit(app.Main(...))` (signals come in Task 21).

- [ ] **Step 5:** `go test ./internal/app/` — PASS.

- [ ] **Step 6:** `.golangci.yml` enabling default linters plus `errorlint`, `gofumpt`. `.github/workflows/ci.yml` with jobs: `test` (ubuntu-latest: `go vet ./...`, `go test -race ./...`), `lint` (golangci-lint action), `cross` (matrix over `linux/{amd64,arm64,arm,riscv64,ppc64le}`, `freebsd/{amd64,arm64}`, `illumos/amd64`, `darwin/{amd64,arm64}`; `CGO_ENABLED=0 GOOS=… GOARCH=… GOARM=7 go build ./cmd/syngoid`). Run locally: `go vet ./... && golangci-lint run` — clean.

- [ ] **Step 7: Commit** `chore: scaffold module, version command and CI`.

---

### Task 2: Console logging

**Files:**
- Create: `internal/logx/console.go`
- Test: `internal/logx/console_test.go`

**Interfaces:**
- Produces: `const logx.LevelCritical = slog.LevelError`; attribute keys `logx.KeyHost = "host"`, `logx.KeyDataset = "dataset"`, `logx.KeyAction = "action"`; `type logx.Config struct { Stdout, Stderr io.Writer; Quiet, Debug bool }`; `func logx.New(cfg Config, extra ...slog.Handler) *slog.Logger` (fan-out to console handler + extras; M3 adds OTLP as an extra); `func logx.Critical(l *slog.Logger, msg string, args ...any)`.

- [ ] **Step 1: Write failing tests:**
  - `TestConsolePrefixes`: Info → stdout `INFO: hello\n`; Warn → stderr `WARNING: w\n`; Critical → stderr `CRITICAL ERROR: c\n`; Debug suppressed unless `Debug: true`, then stdout `DEBUG: d\n`.
  - `TestConsoleQuiet`: with `Quiet`, Info and Warn produce nothing; Critical still printed.
  - `TestConsoleHostPrefix`: `l.Debug("zfs list", logx.KeyHost, "backup@nas")` with Debug → `DEBUG: [backup@nas] zfs list\n`. Other attributes are not printed.
  - `TestConsoleMultiline`: a message with trailing newline is printed with exactly one trailing newline.
- [ ] **Step 2:** `go test ./internal/logx/` — FAIL.
- [ ] **Step 3: Implement** a `slog.Handler` (mutex-guarded writes; `WithAttrs` carries `host` forward).
- [ ] **Step 4:** `go test ./internal/logx/` — PASS.
- [ ] **Step 5: Commit** `feat(logx): syncoid-style console logging`.

---

### Task 3: ZFS error classification

**Files:**
- Create: `internal/zfs/errors.go`
- Test: `internal/zfs/errors_test.go`

**Interfaces:**
- Produces: sentinel errors `zfs.ErrPartialState`, `zfs.ErrResumeSourceGone`, `zfs.ErrNoDataset`, `zfs.ErrBookmarksUnsupported`; `type zfs.DestinationExistsError struct{ Name string }` (implements `error`); `func zfs.Classify(stderr string) error` (nil when unrecognised).

- [ ] **Step 1: Write failing table test** `TestClassify`:

| stderr | expected |
|---|---|
| `cannot receive incremental stream: destination tank/b contains partially-complete state from "zfs receive -s".` | `ErrPartialState` |
| `cannot receive new filesystem stream: dataset is busy` | `ErrPartialState` |
| `cannot resume send: 'tank/a@s1' used in the initial send no longer exists` | `ErrResumeSourceGone` |
| `cannot resume send: incremental source 0x1f2e no longer exists` | `ErrResumeSourceGone` |
| `warning: x\ncannot restore to tank/b@s2: destination already exists\n` | `&DestinationExistsError{Name: "tank/b@s2"}` (via `errors.As`) |
| `cannot open 'tank/x': dataset does not exist` | `ErrNoDataset` |
| `invalid type 'bookmark'` | `ErrBookmarksUnsupported` |
| `operation not applicable to datasets of this type` | `ErrBookmarksUnsupported` |
| `something else` | `nil` |

- [ ] **Step 2:** `go test ./internal/zfs/ -run TestClassify` — FAIL.
- [ ] **Step 3: Implement** with substring checks; `DestinationExistsError` parsed from the line matching `^cannot restore to ([^:]*): destination already exists$`.
- [ ] **Step 4:** PASS.
- [ ] **Step 5: Commit** `feat(zfs): classify zfs stderr into typed errors`.

---

### Task 4: Command execution and ZFS client

**Files:**
- Create: `internal/zfs/exec.go`, `internal/zfs/client.go`
- Test: `internal/zfs/exec_test.go`, `internal/zfs/client_test.go`, `internal/zfs/zfstest/fakeexec.go`

**Interfaces:**
- Consumes: `zfs.Classify` (Task 3).
- Produces:
  - `type zfs.Cmd struct { Argv []string; Stdin io.Reader; Stdout io.Writer }`
  - `type zfs.Exec interface { Run(ctx context.Context, c Cmd) error }` — non-zero exit returns `*CommandError`.
  - `type zfs.CommandError struct { Argv []string; ExitCode int; Stderr string; Kind error }` with `Error() string` = `"<argv joined by space>: exit <code>: <first non-empty stderr line>"` and `Unwrap() error { return Kind }` where `Kind = Classify(Stderr)`.
  - `type zfs.OSExec struct { Log *slog.Logger; Host string }` (Host used only as `logx.KeyHost` in debug logs).
  - `type zfs.Client struct { Exec Exec; Sudo bool }`; methods `(c *Client) Argv(args ...string) []string` (prefix `sudo -n zfs` or `zfs`), `(c *Client) Output(ctx, args ...string) (string, error)`.
  - Test helper `zfstest.FakeExec` with `Responses map[string]Response` keyed by argv joined with spaces, `type Response struct { Stdout, Stderr string; Exit int; Block bool }`, and `Calls [][]string`.

- [ ] **Step 1: Write failing tests:**
  - `TestOSExecCapturesStderrAndExit`: argv `sh -c "echo out; echo 'dataset is busy' >&2; exit 3"` → `*CommandError` with `ExitCode==3`, `Stderr=="dataset is busy\n"`, `errors.Is(err, ErrPartialState)`; stdout writer got `out\n`.
  - `TestOSExecEnv`: `sh -c 'echo "$LC_ALL"; echo "$PATH"'` → first line `C`, second ends with `:/sbin:/usr/sbin:/usr/local/sbin:/bin:/usr/bin`.
  - `TestOSExecCancelKillsGroup`: `sh -c 'sleep 30 & wait'` with a 200 ms context timeout returns an error within 2 s.
  - `TestOSExecDebugLog`: debug logger receives `zfs list -H` with host attr.
  - `TestClientSudoPrefix`: `Client{Sudo:true}.Argv("list","-H")` == `[sudo -n zfs list -H]`; without sudo `[zfs list -H]`.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement.** `OSExec.Run`: `exec.CommandContext`, `SysProcAttr{Setpgid: true}`, `cmd.Cancel` sends SIGTERM to `-pgid`, `cmd.WaitDelay = 10*time.Second`; stderr captured into a buffer capped at 64 KiB; log argv at Debug before running.
- [ ] **Step 4:** PASS.
- [ ] **Step 5: Commit** `feat(zfs): argv command execution and client`.

---

### Task 5: ZFS queries and parsers

**Files:**
- Create: `internal/zfs/types.go`, `internal/zfs/query.go`
- Test: `internal/zfs/query_test.go`, `internal/zfs/testdata/*.txt`

**Interfaces:**
- Consumes: `Client`, `zfstest.FakeExec` (Task 4), `ErrNoDataset`, `ErrBookmarksUnsupported` (Task 3).
- Produces:
  - `type zfs.DatasetInfo struct { Name, Type, Origin string; Used int64; ResumeToken, SyncProp string }` — `-` maps to `""` for `Origin`, `ResumeToken`, `SyncProp`.
  - `type zfs.Snapshot struct { Name, GUID string; CreateTxg, Creation uint64 }` — `Name` is the part after `@`; `Creation` = `creation*1000 + counter`, where `counter` increments for repeated `creation` values within one listing (0..999).
  - `type zfs.Bookmark` with the same fields (`Name` after `#`).
  - `(c *Client) ListTree(ctx, root string, recursive bool) ([]DatasetInfo, error)` → `zfs list -Hp -o name,type,origin,used,receive_resume_token,syncoid:sync -t filesystem,volume [-r] root`.
  - `(c *Client) ListSnapshots(ctx, root string, recursive bool) (map[string][]Snapshot, error)` → `zfs get -Hp -t snapshot -o name,property,value guid,createtxg,creation (-r|-d 1) root`; on a non-`ErrNoDataset` error retry once with `zfs get -Hp (-r|-d 1) -o name,property,value all root`, keeping only entries whose `type` is `snapshot`. Order within a dataset = listing order.
  - `(c *Client) ListBookmarks(ctx, root string, recursive bool) (map[string][]Bookmark, error)` → same with `-t bookmark`; `ErrBookmarksUnsupported` → empty map, nil.
  - `(c *Client) GetProps(ctx, ds string, props ...string) (map[string]string, error)` → `zfs get -Hp -o property,value p1,p2 ds`.
  - `(c *Client) LocalProps(ctx, ds string) (map[string]string, error)` → `zfs get -Hp -s local -o property,value all ds`.
  - `(c *Client) Exists(ctx, ds string) (bool, error)` → `zfs list -H -o name ds`; `ErrNoDataset` → false, nil.

- [ ] **Step 1: Write fixtures** in Linux OpenZFS `-Hp` tab-separated format: `tree.txt` (root fs, child fs, a clone with origin, a volume, `syncoid:sync` values `-`, `false`, `nas,db1`), `snaps.txt` (two datasets; two snapshots with identical `creation`), `bookmarks.txt`. Note in a header comment of each fixture that it was hand-written and must be replaced by captured output in M2 (§14.3).
- [ ] **Step 2: Write failing tests:**
  - `TestParseTree`: fields mapped, `-` → `""`, `Used` parsed.
  - `TestParseTreeNameWithSpace`: dataset `tank/my data` parsed intact.
  - `TestParseSnapshotsTieBreak`: two snapshots with `creation` 1700000000 → `Creation` 1700000000000 and 1700000000001 in listing order.
  - `TestListSnapshotsFallback`: FakeExec fails the `-t snapshot` call with `invalid option`, answers the `all` call; result contains only snapshots.
  - `TestListBookmarksUnsupported`: stderr `invalid type 'bookmark'` → empty, nil.
  - `TestExistsFalseOnNoDataset`, `TestExistsErrorOnOther`.
  - `TestQueryArgv`: each method issues exactly the argv listed above (recursive and non-recursive).
- [ ] **Step 3:** FAIL.
- [ ] **Step 4: Implement** parsers (`strings.Split(line, "\t")`, error on wrong field count naming the line).
- [ ] **Step 5:** PASS.
- [ ] **Step 6: Commit** `feat(zfs): dataset, snapshot and bookmark queries`.

---

### Task 6: Send/receive specs, option mini-syntax, mutating commands

**Files:**
- Create: `internal/zfs/opts.go`, `internal/zfs/sendrecv.go`, `internal/zfs/mutate.go`
- Test: `internal/zfs/opts_test.go`, `internal/zfs/sendrecv_test.go`, `internal/zfs/mutate_test.go`

**Interfaces:**
- Produces:
  - `type zfs.Opt struct { Flag byte; Value string }`; `func zfs.ParseOpts(s string) ([]Opt, error)`; `func zfs.FilterOpts(opts []Opt, allowed string) []string`.
  - Constants: `SendAllowedStream = "LPVRXbcehpsvw"`, `SendAllowedBookmark = "LVcew"`, `SendAllowedResume = "PVev"`, `EstimateAllowedStream = "LVRXbcehpsw"`, `EstimateAllowedResume = "Ve"`, `RecvAllowed = "hoxuv"`.
  - `type zfs.SendSpec struct { Dataset, To, FromSnap, FromBookmark, FromOrigin string; Intermediates bool; ResumeToken string; Options []Opt }`.
  - `type zfs.RecvSpec struct { Dataset string; Args []string }`.
  - `func zfs.SendArgs(s SendSpec) []string`, `func zfs.EstimateArgs(s SendSpec) []string` (both without the leading `zfs`).
  - `(c *Client) Send(ctx, s SendSpec, w io.Writer) error`, `(c *Client) Receive(ctx, s RecvSpec, r io.Reader) error`, `(c *Client) SendEstimate(ctx, s SendSpec) (int64, error)`.
  - `(c *Client) Snapshot(ctx, names []string) error` (one invocation), `Destroy(ctx, ds string, snaps []string) error` (`zfs destroy ds@a,b,c`), `DestroyDataset(ctx, ds string, recursive bool) error`, `Hold(ctx, tag, snapshot string) error`, `Release(ctx, tag, snapshot string) error`, `Bookmark(ctx, snapshot, bookmark string) error` (full names), `AbortResume(ctx, ds string) error` (`zfs receive -A ds`).

- [ ] **Step 1: Write failing tests:**
  - `TestParseOpts`: `"ux recordsize o compression=lz4"` → `FilterOpts(…, RecvAllowed)` == `[-u -x recordsize -o compression=lz4]`; `"Lc e"` → `[-L -c -e]`; `"xo foo"` → error; trailing `"o"` → error `option -o requires a value`.
  - `TestFilterOptsDropsDisallowed`: `"Lw R"` filtered by `SendAllowedBookmark` → `[-L -w]`.
  - `TestSendArgs` table: full `{Dataset:"tank/a", To:"s1", Options:"Lc"}` → `[send -L -c tank/a@s1]`; incremental `{FromSnap:"s1", To:"s2", Intermediates:true}` → `[send -I tank/a@s1 tank/a@s2]`; `Intermediates:false` → `-i`; bookmark `{FromBookmark:"b1", To:"s2", Options:"LR"}` → `[send -L -i tank/a#b1 tank/a@s2]`; clone `{FromOrigin:"tank/o@s0", To:"s1"}` → `[send -i tank/o@s0 tank/a@s1]`; resume `{ResumeToken:"tok", Options:"Pv"}` → `[send -P -v -t tok]`.
  - `TestSendArgsNameWithSpace`: `tank/my data@s1` is a single argv element.
  - `TestEstimateArgs`: incremental `-i` spec → `[send -nvP -i tank/a@s1 tank/a@s2]` (fixes spec §8.9 #4: no forced `-I`); resume → `[send -nvP -t tok]` with only `V`,`e` options kept.
  - `TestParseEstimate`: output `full\ttank/a@s1\t1000\nsize\t1000` → 4096 (minimum); `size\t123456` → 123456; resume output last line `incremental\t...\t555555` → 555555; garbage → 0, nil.
  - `TestMutateArgv`: every mutating method issues the exact argv above; `Destroy` with 3 snaps → `zfs destroy tank/a@x,y,z`.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement.** Flag order: filtered options first, then the stream flag/arguments. `SendEstimate` runs `EstimateArgs` and parses the **last** line: resume form takes the last tab field; otherwise strip the `size` label; non-numeric → 0; 0 < n < 4096 → 4096; command failure → 0, nil.
- [ ] **Step 4:** PASS.
- [ ] **Step 5: Commit** `feat(zfs): send/receive specs, option filtering, mutating commands`.

---

### Task 7: Naming

**Files:**
- Create: `internal/naming/naming.go`
- Test: `internal/naming/naming_test.go`

**Interfaces:**
- Produces: `func naming.SyncSnapName(identifier, host string, t time.Time) string`, `func naming.HoldTag(identifier, host string) string`, `func naming.SyncSnapPattern(identifier, host string) *regexp.Regexp`, `func naming.BookmarkFallback(snap, guid string) string` (`snap + guid[:6]`, whole guid if shorter), `func naming.ValidIdentifier(s string) bool` (`^[a-zA-Z0-9-_:.]+$`).

- [ ] **Step 1: Write failing tests:**
  - `TestSyncSnapName`: host `nas`, no identifier, `2026-10-18 14:03:07` in UTC+2 → `syncoid_nas_2026-10-18:14:03:07-GMT02:00`; UTC−5 → `…-GMT-05:00`; identifier `offsite` → `syncoid_offsite_nas_…`.
  - `TestSyncSnapNameHalfHourOffsets`: UTC+5:30 → `GMT05:30`; UTC−3:30 → `GMT-03:30`; UTC → `GMT00:00`.
  - `TestHoldTag`: `("", "nas")` → `syncoid_nas`; `("offsite","nas")` → `syncoid_offsite_nas`.
  - `TestPrunePatternStrict`: pattern for `nas` matches `syncoid_nas_2026-10-18:14:03:07-GMT02:00`, does **not** match `syncoid_nas2_2026-10-18:14:03:07-GMT02:00`, `syncoid_offsite_nas_…`, or `syncoid_nas_manual`.
  - `TestPrunePatternQuotesMeta`: identifier `a.b` pattern does not match `syncoid_aXb_nas_…`; host `db.lan` does not match `syncoid_dbXlan_…`.
  - `TestValidIdentifier`: `off-site_1:x.y` ok; `bad/id`, `with space`, `` (empty) rejected.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement** `SyncSnapPattern` as `^syncoid_` + `QuoteMeta(identifier_)` + `QuoteMeta(host)` + `_\d{4}-\d{2}-\d{2}:\d{2}:\d{2}:\d{2}-GMT-?\d{2}:\d{2}$`.
- [ ] **Step 4:** PASS.
- [ ] **Step 5: Commit** `feat(naming): syncoid-compatible sync snapshot and hold names`.

---

### Task 8: Stream codecs

**Files:**
- Create: `internal/stream/codec.go`
- Test: `internal/stream/codec_test.go`

**Interfaces:**
- Produces: `type stream.Codec string` with `None="none"`, `ZstdFast="zstd-fast"`, `Zstd="zstd"`, `LZ4="lz4"`, `Gzip="gzip"`; `func stream.ParseCodec(s string) (c Codec, deprecated bool, err error)`; `func stream.NewEncoder(c Codec, w io.Writer) (io.WriteCloser, error)`; `func stream.NewDecoder(c Codec, r io.Reader) (io.ReadCloser, error)`.

- [ ] **Step 1: Write failing tests:**
  - `TestParseCodecLegacy` table: `zstd-fast`,`zstdmt-fast`→`ZstdFast`; `zstd-slow`,`zstdmt-slow`,`xz`→`Zstd`; `gzip`→`Gzip` (not deprecated); `pigz-fast`,`pigz-slow`→`Gzip` (deprecated); `lzo`→`LZ4` (deprecated); `lz4`,`none` as-is; `bogus` → error. The `deprecated` flag is true exactly for legacy names that differ from the canonical name.
  - `TestCodecRoundTrip`: for every codec, 8 MiB of random-ish data round-trips byte-identical.
  - `FuzzCodecRoundTrip` over input bytes for every codec.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement** with `klauspost/compress/zstd` (`ZstdFast` = `SpeedDefault`, `Zstd` = `SpeedBetterCompression`), `pierrec/lz4/v4`, `klauspost/compress/gzip` level 3; `None` is a no-op closer.
- [ ] **Step 4:** PASS (`go test -run . -fuzz FuzzCodecRoundTrip -fuzztime 10s ./internal/stream/` also passes).
- [ ] **Step 5: Commit** `feat(stream): compression codecs`.

---

### Task 9: Buffered copy and progress rendering

**Files:**
- Create: `internal/stream/copy.go`, `internal/stream/progress.go`
- Test: `internal/stream/copy_test.go`, `internal/stream/progress_test.go`

**Interfaces:**
- Produces:
  - `func stream.Copy(ctx context.Context, dst io.Writer, src io.Reader, bufferSize int64) (int64, error)` — reader and writer goroutines decoupled by a bounded queue of 128 KiB chunks totalling at most `bufferSize`; returns on EOF, first error, or ctx cancel.
  - `type stream.ProgressMode string` with `ProgressAuto="auto"`, `ProgressBar="bar"`, `ProgressPlain="plain"`, `ProgressNone="none"`.
  - `type stream.Renderer interface { Start(label string, estimate int64); Update(rawBytes int64); Finish(rawBytes int64, elapsed time.Duration, err error) }`.
  - `func stream.NewRenderer(mode ProgressMode, w io.Writer, isTTY bool, log *slog.Logger) Renderer` — `auto` → bar if `isTTY` else plain.
  - `func stream.HumanBytes(n int64) string`, `func stream.HumanDuration(d time.Duration) string`.

- [ ] **Step 1: Write failing tests:**
  - `TestCopyTransfersAll`: 10 MiB through `Copy` with 1 MiB buffer is identical; returned count 10 MiB.
  - `TestCopyCancel`: src blocks forever; cancel → returns `context.Canceled` within 1 s.
  - `TestCopyWriterError`: dst fails after 1 MiB → error returned, reader stops.
  - `TestHumanBytes`: 512→`512 B`, 1024→`1.0 KiB`, 1288490189→`1.2 GiB`. `TestHumanDuration`: 13s→`0:00:13`, 3725s→`1:02:05`.
  - `TestBarRender`: estimate 2 GiB, update 1 GiB after a fake clock of 10 s → the last written segment starts with `\r` and contains `50%`, `1.0 GiB/2.0 GiB`, `102.4 MiB/s`, `ETA 0:00:10`; `Finish` writes a trailing `\n`. Unknown estimate (0) omits `%` and ETA. Updates throttled to one write per 200 ms.
  - `TestPlainRender`: `Start` writes nothing; `Finish(2 GiB, 20s, nil)` logs INFO `transferred 2.0 GiB in 0:00:20 (102.4 MiB/s)`.
  - `TestNoneRender`: nothing written.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement** (renderer takes an injectable clock via an unexported field for tests).
- [ ] **Step 4:** PASS.
- [ ] **Step 5: Commit** `feat(stream): buffered copy and progress renderers`.

---

### Task 10: Endpoint interface and LocalEndpoint

**Files:**
- Create: `internal/endpoint/endpoint.go`, `internal/endpoint/local.go`
- Test: `internal/endpoint/local_test.go`

**Interfaces:**
- Consumes: `zfs.Client` and its methods (Tasks 4–6), `stream.NewEncoder/NewDecoder` (Task 8).
- Produces:
  - `type endpoint.HostInfo struct { OS, Arch, User string; UID int; Groups []string; ZFSVersion string }`.
  - `type endpoint.SendOpts struct { Codec stream.Codec; Progress func(rawBytes int64) }`.
  - `type endpoint.Endpoint interface` with: `Label() string` (`""` local; `user@host` remote in M2), `Info(ctx) (HostInfo, error)`, `ListTree`, `ListSnapshots`, `ListBookmarks`, `GetProps`, `LocalProps`, `Exists`, `Snapshot`, `Destroy`, `DestroyDataset`, `Hold`, `Release`, `Bookmark`, `AbortResume`, `SendEstimate` — same signatures as the `zfs.Client` methods — plus `ReceiveBusy(ctx, datasets []string) (map[string]bool, error)`, `Send(ctx, s zfs.SendSpec, o SendOpts, w io.Writer) error`, `Receive(ctx, s zfs.RecvSpec, codec stream.Codec, r io.Reader) error`, `Allow(ctx, ds string) (string, error)` (raw `zfs allow ds` output; parsed by `perms`).
  - `func endpoint.NewLocal(c *zfs.Client, exec zfs.Exec) *Local`.

- [ ] **Step 1: Write failing tests** (FakeExec-backed):
  - `TestLocalInfo`: responses for `id -un`→`backup`, `id -u`→`1001`, `id -Gn`→`backup zfsadm`, `zfs version`→`zfs-2.3.4-1\nzfs-kmod-2.3.4-1` → `HostInfo{OS: runtime.GOOS, Arch: runtime.GOARCH, User:"backup", UID:1001, Groups:[backup zfsadm], ZFSVersion:"zfs-2.3.4-1"}`; failing `zfs version` → empty `ZFSVersion`, no error.
  - `TestLocalReceiveBusy`: `ps -Ao args=` output contains `zfs receive -s -F tank/b` and `zfs recv tank/c/d` → `{tank/b:true, tank/c:false, tank/c/d:true}` for query `[tank/b tank/c tank/c/d]`.
  - `TestLocalSendCodecAndProgress`: FakeExec `zfs send …` writes 1 MiB; with `Codec: Zstd` the writer receives zstd data that decodes to the original; progress callback's final value is 1 MiB.
  - `TestLocalReceiveDecodes`: zstd-encoded input reaches `zfs receive` stdin decoded.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement.** Busy regex per line: `zfs *(receive|recv)[^/]*` + `QuoteMeta(ds)` + `$`. `Send` pipes `zfs send` stdout → counting writer → encoder → `w`. Extend `zfstest.FakeExec` so a response can stream its stdout and capture stdin.
- [ ] **Step 4:** PASS.
- [ ] **Step 5: Commit** `feat(endpoint): Endpoint interface and local implementation`.

---

### Task 11: Fake endpoint (in-memory ZFS model)

**Files:**
- Create: `internal/endpoint/fake/fake.go`, `internal/endpoint/fake/stream.go`
- Test: `internal/endpoint/fake/fake_test.go`

**Interfaces:**
- Consumes: `endpoint.Endpoint` (Task 10), zfs types and errors (Tasks 3, 5, 6).
- Produces:
  - `func fake.NewWorld() *World`; `(w *World) Create(name, typ string)`; `(w *World) Snap(ds, name string) zfs.Snapshot` (unique GUID, monotonically increasing CreateTxg and Creation); `(w *World) MarkBookmark(ds, snap, bm string)`; `(w *World) SetProp(ds, key, value string)` (`syncoid:sync` and local props); `(w *World) Dataset(name string) *Dataset` (nil if absent); exported `Dataset` fields `Name, Type, Origin string; Snaps []zfs.Snapshot; Bookmarks []zfs.Bookmark; Holds map[string][]string; ResumeToken string; Used int64; Props map[string]string`.
  - `func fake.New(w *World, info endpoint.HostInfo) *Endpoint` implementing `endpoint.Endpoint`; `(e *Endpoint) Calls() []string` (method + main argument, e.g. `Destroy tank/a@x,y`).
  - `type fake.Faults struct { Receive func(s zfs.RecvSpec) error; Send func(s zfs.SendSpec) error; Busy map[string]bool; Allow map[string]string; BlockSend bool }` set via `e.Faults`.

- [ ] **Step 1: Write failing tests:**
  - `TestFakeFullThenIncremental`: source has `s1,s2,s3`; full send of `s1` into an absent target creates it with `s1` (same GUID); `-I s1→s3` adds `s2,s3`; `-i s1→s3` on a fresh copy adds only `s3`.
  - `TestFakeIncrementalWrongBaseFails`: incremental from a GUID the target does not have → error with stderr classified as a generic failure.
  - `TestFakeForceRollback`: the target has an extra snapshot newer than the base; receive with `-F` drops it; without `-F` → error.
  - `TestFakeResumeToken`: `Faults.Receive` returning `ErrPartialState` once; the endpoint records `ResumeToken="tok-1"` on the target when `-s` was in `Args`; a `Resume` send with that token completes and clears it.
  - `TestFakeBookmarkSend`: `-i ds#bm ds@s3` works when the bookmark GUID matches the target's newest snapshot.
  - `TestFakeReceiveMissingParent`: receiving into `pool/a/b` when `pool/a` is absent fails with `cannot open 'pool/a': dataset does not exist`.
  - `TestFakeBlockSendCancel`: `BlockSend` blocks until ctx is cancelled, then returns `context.Canceled`.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement.** The fake stream is a JSON header line (`dataset`, `from_guid`, `origin`, `snaps[]`) followed by EOF; codecs are applied as in `Local`. Errors are returned as `*zfs.CommandError` with realistic stderr so `Classify` applies.
- [ ] **Step 4:** PASS.
- [ ] **Step 5: Commit** `test(fake): in-memory ZFS endpoint with fault injection`.

---

### Task 12: CLI options and locations

**Files:**
- Create: `internal/cli/options.go`, `internal/cli/location.go`
- Test: `internal/cli/options_test.go`, `internal/cli/location_test.go`

**Interfaces:**
- Consumes: `zfs.ParseOpts`, `zfs.Opt` (Task 6), `stream.ParseCodec`, `stream.Codec`, `stream.ProgressMode` (Tasks 8–9), `naming.ValidIdentifier` (Task 7).
- Produces:
  - `type cli.Location struct { User, Host, Dataset string }` with `IsRemote() bool` and `String() string` (`[user@]host:dataset` or `dataset`).
  - `func cli.ParseLocation(arg string, localPoolExists func(string) bool) (Location, error)`.
  - `type cli.Command string` with `CmdSync`, `CmdPermissions`, `CmdFetch`, `CmdVersion`, `CmdServe`.
  - `type cli.Options struct` with fields: `Source, Target Location`; `Recursive, SkipParent bool`; `Identifier string`; `NoStream, NoSyncSnap, KeepSyncSnap, CreateBookmark, UseHold, PreserveRecordsize, PreserveProperties, NoRollback, DeleteTargetSnapshots, ForceDelete, NoResume, NoCloneHandling, DumpSnaps, Debug, Quiet bool`; `ExcludeDatasets, IncludeSnaps, ExcludeSnaps []*regexp.Regexp`; `SendOpts, RecvOpts []zfs.Opt`; `Compress stream.Codec`; `BufferSize int64`; `Progress stream.ProgressMode`; `SSHOptions []string`; `SSHConfig string`; `SourceSudo, TargetSudo bool` (`--sudo` sets both); `DryRun, Confirm bool`; `NoDownload bool`; `DownloadURL string`; `BinStores []string`; `RemoteBinDir, OTelEndpoint string`.
  - `type cli.Parsed struct { Command Command; Options *Options; Warnings []string }`; `func cli.Parse(args []string, localPoolExists func(string) bool) (Parsed, error)`.

- [ ] **Step 1: Write failing tests:**
  - `TestParseLocation` table (pool `pool:x` exists locally): `tank/a` → local; `root@nas:tank/a` → `{root, nas, tank/a}`; `@nas:tank/a` → `{"", nas, tank/a}`; `nas:tank/a` → remote `{"", nas, tank/a}`; `pool:x/a` → local `pool:x/a`; `nas:` → error (empty dataset).
  - `TestParseDefaults`: `src dst` → `CmdSync`, `Compress=ZstdFast`, `BufferSize=16<<20`, `Progress=auto`.
  - `TestParseSubcommands`: `permissions a b`, `fetch`, `version`, `serve`, explicit `sync a b`.
  - `TestParseDeprecatedAliases`: `--sshport 2222 --sshkey k --sshcipher c --sshoption A=B --sshconfig f --mbuffer-size 32M --compress pigz-fast --no-privilege-elevation --no-command-checks --monitor-version` → `SSHOptions == [Port=2222 IdentityFile=k Ciphers=c A=B]`, `SSHConfig=f`, `BufferSize=32<<20`, `Compress=Gzip`, and 8 warnings (one per alias or deprecated value).
  - `TestParseRemovedFlags`: each of `--source-bwlimit=1m`, `--target-bwlimit=1m`, `--pv-options=x`, `--insecure-direct-connection=a:1`, `--exclude=x` → error whose message contains `bandwidth limiting is not supported in syngoid`, `--progress`, `not supported`, `not supported`, `--exclude-datasets` respectively.
  - `TestParseValidation`: invalid `--identifier=bad/id` → error; `--sendoptions=R` with `-r` → error `zfs send -R and --recursive aren't compatible`; bad regex in `--exclude-snaps` → error; `--sudo` sets both sides; `--confirm --dry-run` → error `--confirm and --dry-run are mutually exclusive`; `--create-bookmark` without `--no-sync-snap` → error `--create-bookmark requires --no-sync-snap`; wrong positional count for sync → error.
  - `TestParseBufferSize`: `16M`, `512K`, `1G`, `1048576` parse; `abc` → error.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement** with `pflag`; hidden flags for aliases and removed flags so they produce the warnings and errors above; dispatch is on the first non-flag argument if it is a known subcommand.
- [ ] **Step 4:** PASS.
- [ ] **Step 5: Commit** `feat(cli): options, aliases, removed flags and locations`.

---

### Task 13: `zfs allow` parsing and effective permissions

**Files:**
- Create: `internal/perms/allow.go`
- Test: `internal/perms/allow_test.go`, `internal/perms/testdata/allow_*.txt`

**Interfaces:**
- Produces: `type perms.Delegations struct` (opaque parsed form); `func perms.ParseAllow(out string) (Delegations, error)`; `func perms.Effective(d Delegations, dataset, user string, groups []string) map[string]bool`.

- [ ] **Step 1: Write fixtures** in OpenZFS `zfs allow` format, e.g.:

```
---- Permissions on tank/backups/vm ----------------------------------
Local permissions:
	user backup hold
---- Permissions on tank/backups -------------------------------------
Permission sets:
	@recv create,mount,receive
Local permissions:
	user backup snapshot
Descendent permissions:
	group zfsadm destroy
Local+Descendent permissions:
	user backup @recv,release
	everyone bookmark
```

Mark them hand-written, to be replaced by captured output (§14.3).
- [ ] **Step 2: Write failing tests:**
  - `TestEffectiveDescendant`: for `tank/backups/vm`, user `backup`, groups `[backup zfsadm]` → contains `hold` (local on self), `create`,`mount`,`receive`,`release` (set via local+descendent on ancestor), `destroy` (group, descendent on ancestor), `bookmark` (everyone); does **not** contain `snapshot` (local-only on the ancestor).
  - `TestEffectiveSelfLocalOnly`: for `tank/backups` itself, `snapshot` is included and `destroy` (descendent-only) is not.
  - `TestEffectiveOtherUser`: user `alice`, no groups → only `bookmark`.
  - `TestParseAllowEmpty`: empty output → no permissions.
  - `TestParseAllowIgnoresCreateTime`: a `Create time permissions:` section is ignored.
  - `FuzzParseAllow`: never panics.
- [ ] **Step 3:** FAIL.
- [ ] **Step 4: Implement.** Sections in order of appearance; a set name resolves from the section that defines it or the nearest ancestor section defining it.
- [ ] **Step 5:** PASS.
- [ ] **Step 6: Commit** `feat(perms): parse zfs allow and resolve effective permissions`.

---

### Task 14: Required permissions and grant commands

**Files:**
- Create: `internal/perms/required.go`
- Test: `internal/perms/required_test.go`

**Interfaces:**
- Produces:
  - `type perms.Requirement struct { AnyOf []string }` with `String()` = alternatives joined by ` or `.
  - `type perms.Side int` with `perms.Source`, `perms.Target`.
  - `type perms.Needs struct { NoSyncSnap, KeepSyncSnap, UseHold, CreateBookmark, NoRollback, DeleteTargetSnapshots, ForceDelete, PreserveRecordsize, RawSend bool; Props []string; UserProps bool }`.
  - `func perms.Required(side Side, n Needs) []Requirement`; `func perms.Missing(have map[string]bool, req []Requirement) []Requirement`; `func perms.GrantCommand(user, dataset string, req []Requirement) string`.

- [ ] **Step 1: Write failing tests** pinning spec §6.2:
  - `TestRequiredSourceDefault` → `send` (AnyOf `[send]`), `snapshot`, `mount`, `destroy`.
  - `TestRequiredSourceRaw` with `RawSend` → first requirement AnyOf `[send send:raw send:encrypted]`.
  - `TestRequiredSourceNoSyncSnap` → only `send`; plus `bookmark` with `CreateBookmark`; plus `hold`,`release` with `UseHold`.
  - `TestRequiredTargetDefault` → `create`, `mount`, `receive`, `rollback`, `destroy`.
  - `TestRequiredTargetNoRollbackKeep` (`NoRollback`, `KeepSyncSnap`) → `create`,`mount`, AnyOf `[receive receive:append]`; no `rollback`, no `destroy`.
  - `TestRequiredTargetProps` (`PreserveRecordsize`, `Props:[compression atime]`, `UserProps`) → includes `recordsize`, `compression`, `atime`, `userprop`.
  - Requirements are de-duplicated and in a stable order (first appearance).
  - `TestMissing`: have `{send:raw, snapshot, mount}` with the `RawSend` default source set → missing `[destroy]`.
  - `TestGrantCommand`: `("backup","tank/data", [send, snapshot, mount, destroy, receive|receive:append])` → `zfs allow -u backup send,snapshot,mount,destroy,receive tank/data` (first alternative of each).
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** PASS.
- [ ] **Step 5: Commit** `feat(perms): required permission sets and grant commands`.

---

### Task 15: Engine state, ordering and matching

**Files:**
- Create: `internal/engine/state.go`, `internal/engine/order.go`
- Test: `internal/engine/order_test.go`

**Interfaces:**
- Consumes: `zfs.Snapshot`, `zfs.Bookmark` (Task 5).
- Produces:
  - `type engine.Side int` (`Source`, `Target`, `String()` = `source`/`target`).
  - `type engine.Status int` (`StatusOK`, `StatusNothing`, `StatusSkipped`, `StatusSoft`, `StatusCritical`, `String()` = `ok`/`nothing to do`/`skipped`/`failed (soft)`/`failed`).
  - `type engine.Result struct { Source, Target string; Status Status; Message string; Err error; Bytes int64; Duration time.Duration }`.
  - `type engine.DatasetState struct { Source, Target string; SourceSnaps, TargetSnaps []zfs.Snapshot; SourceBookmarks []zfs.Bookmark; TargetExists bool; TargetUsed int64; ResumeToken, SyncProp, Origin string; OriginInScope bool; NewSyncSnap string; TargetIsPoolRoot bool; SourceType string }` — snapshot slices sorted oldest→newest.
  - `func engine.SortSnapshots(s []zfs.Snapshot)`; `func engine.Matching(src, tgt []zfs.Snapshot) (string, bool)`; `func engine.BookmarkFallback(tgt []zfs.Snapshot, bms []zfs.Bookmark) (bm zfs.Bookmark, targetSnap string, ok bool)`; `func engine.NextAfterBookmark(src []zfs.Snapshot, bm zfs.Bookmark) (string, bool)`; `func engine.ExitCode(results []Result) int`.

- [ ] **Step 1: Write failing tests:**
  - `TestSortSnapshotsTxgThenCreation`: CreateTxg wins when both are non-zero; otherwise Creation (tie-break values) decides.
  - `TestMatchingNewestNameAndGUID`: the newest name match with equal GUID wins; a same-name, different-GUID snapshot is ignored.
  - `TestBookmarkFallbackUsesTargetOrder`: two target snapshots match bookmark GUIDs; the one newest by the **target's** CreateTxg is chosen (spec §8.9 #3).
  - `TestNextAfterBookmark`: returns the first source snapshot with CreateTxg ≥ the bookmark's; falls back to Creation when either txg is 0.
  - `TestExitCode`: `{ok, nothing, skipped}`→0; `+soft`→1; `+critical`→2; empty→0.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** PASS.
- [ ] **Step 5: Commit** `feat(engine): state types, ordering and snapshot matching`.

---

### Task 16: Planner

**Files:**
- Create: `internal/engine/actions.go`, `internal/engine/plan.go`
- Test: `internal/engine/plan_test.go`

**Interfaces:**
- Consumes: Task 15 types and helpers; `naming.SyncSnapPattern` (Task 7).
- Produces:
  - `type engine.Action interface { Describe() string; Destructive() bool }` with concrete types `SendFull{Snap}`, `SendIncremental{From, To string; Intermediates bool}`, `SendFromBookmark{Bookmark, To string}`, `SendClone{Origin, To string}`, `Resume{Token string}`, `Hold{Side; Snap string}`, `Release{Side; Snap string}`, `CreateBookmark{Snap string}`, `PruneSyncSnaps{Side; Snaps []string}`, `DestroyTargetSnaps{}`, `ForceDestroyTarget{}`. Only `ForceDestroyTarget` is `Destructive()`.
  - `Describe()` strings exactly: `send full @S`; `send incremental @F → @T` (+ ` (with intermediates)` when set); `send incremental #B → @T`; `send clone of O → @T`; `resume interrupted receive`; `hold @S on <side>`; `release @S on <side>`; `create bookmark #S`; `prune N sync snapshot(s) on <side>`; `destroy target snapshots missing on source`; `DESTROY target recursively (--force-delete)`.
  - `type engine.PlanOptions struct { NoStream, NoSyncSnap, KeepSyncSnap, CreateBookmark, UseHold, DeleteTargetSnapshots, ForceDelete, NoResume, FiltersActive bool; Hostname, Identifier string }`; `func engine.PlanOptionsFrom(o *cli.Options, hostname string) PlanOptions`.
  - `type engine.Plan struct { Source, Target string; Actions []Action; Status Status; Message string }` — `Status` is `StatusOK` when there are actions to run; otherwise the terminal status with its `Message`.
  - `func engine.PlanDataset(s DatasetState, o PlanOptions) Plan`.

- [ ] **Step 1: Write failing table tests** (`TestPlan`, one case per row; the source has snapshots `a,b,c` oldest→newest unless stated; `NewSyncSnap = "syncoid_nas_T"` unless `NoSyncSnap`):

| case | state/options | expected actions / status |
|---|---|---|
| sync=false | `SyncProp:"false"` | `StatusSkipped`, message `Skipping dataset (syncoid:sync=false): tank/a...` |
| sync host list excludes | `SyncProp:"db1, db2"`, host `nas` | `StatusSkipped`, message contains `syncoid:sync doesn't include nas` |
| sync host list includes with spaces | `SyncProp:"db1, nas"` | proceeds (entries are trimmed) |
| first sync | target absent, no earlier sync snapshots | exactly `SendFull{a}`, `SendIncremental{a→syncoid_nas_T, true}` (prune actions are emitted only when there is something to prune) |
| first sync no-stream | `NoStream` | `SendFull{syncoid_nas_T}` only |
| first sync no snaps | source has no snaps | `SendFull{syncoid_nas_T}` |
| no-sync-snap no snaps | `NoSyncSnap`, no snaps | `StatusSoft`, message `no snapshots exist on source tank/a, and you asked for --no-sync-snap.` |
| incremental | target has `a,b` (same GUIDs) | `SendIncremental{b→syncoid_nas_T, true}` |
| incremental no-stream | + `NoStream` | `SendIncremental{b→syncoid_nas_T, false}` |
| filtered chain | `FiltersActive`, target has `a` | `SendIncremental{a→b,false}`, `{b→c,false}`, `{c→syncoid_nas_T,false}` |
| nothing to do | `NoSyncSnap`, target has `a,b,c` | `StatusNothing`, message `no snapshots on source newer than c on target. Nothing to do, not syncing.` |
| resume | `ResumeToken:"tok"` | `Resume{tok}` only |
| resume ignored | + `NoResume` | normal incremental plan |
| bookmark fallback | target has `x` (GUID = bookmark `bm`'s), no name match; source snaps after bm: `b,c` | `SendFromBookmark{bm→b}`, `SendIncremental{b→syncoid_nas_T,true}` |
| bookmark no-stream | + `NoStream` | `SendFromBookmark{bm→syncoid_nas_T}` |
| no match | target has unrelated `z`, `TargetUsed: 1<<20` | `StatusCritical`; message contains `Cowardly refusing to destroy your existing target.` and `did you mistakenly run` |
| no match large | `TargetUsed: 1<<30` | `StatusCritical`; message lacks `did you mistakenly run` |
| force delete | + `ForceDelete` | `ForceDestroyTarget{}` only |
| force delete pool root | + `TargetIsPoolRoot` | `StatusCritical` (never force-destroys a pool root) |
| clone | target absent, `Origin:"tank/o@s0"`, `OriginInScope` | `SendClone{tank/o@s0→a}`, `SendIncremental{a→syncoid_nas_T,true}` |
| holds | `UseHold`, target has `a,b` | after the send: `Hold{Source,T}`, `Release{Source,b}`, `Hold{Target,T}`, `Release{Target,b}` |
| holds first sync | `UseHold`, target absent | `Hold{Source,T}`, `Hold{Target,T}` (no releases) |
| bookmark create | `NoSyncSnap`, `CreateBookmark`, target has `a` | `SendIncremental{a→c,true}`, `CreateBookmark{c}` |
| prune | source has `syncoid_nas_OLD` (valid pattern) and `syncoid_nas2_OLD`; target has `syncoid_nas_OLD` | `PruneSyncSnaps{Source,[syncoid_nas_OLD]}`, `PruneSyncSnaps{Target,[syncoid_nas_OLD]}` |
| keep sync snap | + `KeepSyncSnap` | no prune actions |
| delete target snaps | `DeleteTargetSnapshots` | last action `DestroyTargetSnaps{}` |

  Plus `TestActionDescribe` asserting each exact `Describe()` string above.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement `PlanDataset`** following spec §8.1/§8.4 in this order: sync-property check; resume; determine `newest` (`NewSyncSnap` if set, else newest source snapshot); target-absent branch; target-exists branch (matching → bookmark fallback → force/critical); post actions (holds, bookmark, prune, delete-target). The 64 MiB hint threshold is `TargetUsed < 64*1024*1024`. Message texts are those in the table and spec §8.4.
- [ ] **Step 4:** PASS.
- [ ] **Step 5: Commit** `feat(engine): pure per-dataset planner`.

---

### Task 17: Gather and dependency ordering

**Files:**
- Create: `internal/engine/gather.go`, `internal/engine/dag.go`
- Test: `internal/engine/gather_test.go`, `internal/engine/dag_test.go`

**Interfaces:**
- Consumes: `endpoint.Endpoint` (Task 10), `fake` (Task 11), `cli.Options` (Task 12), Task 15 types.
- Produces:
  - `func engine.Gather(ctx, src, dst endpoint.Endpoint, o *cli.Options, syncSnap string) ([]DatasetState, error)` — returned in source listing order.
  - `func engine.GatherOne(ctx, src, dst endpoint.Endpoint, o *cli.Options, sourceDS, targetDS, syncSnap string, originInScope bool) (DatasetState, error)`.
  - `func engine.TargetName(sourceRoot, targetRoot, ds string) (string, bool)` — false when `ds` is neither the root nor under `root + "/"`.
  - `func engine.Order(states []DatasetState) (order []int, deps [][]int)`.

- [ ] **Step 1: Write failing tests** (fake worlds):
  - `TestGatherRecursive`: source `tank/data`, `tank/data/vm`, `tank/data/vm/disk`; target root `backup/data` absent → 3 states with targets `backup/data`, `backup/data/vm`, `backup/data/vm/disk`, `TargetExists=false`; the source endpoint issued exactly one recursive `ListTree`, one recursive `ListSnapshots`, one recursive `ListBookmarks`.
  - `TestGatherSiblingPrefixNotChild`: `TargetName("tank/data","backup/data","tank/data2")` → false; `tank/data/x` → `backup/data/x`.
  - `TestGatherExcludeAndSkipParent`: `--exclude-datasets=vm$` drops `tank/data/vm` only; `--skip-parent` drops the root.
  - `TestGatherSnapshotFilters`: `--include-snaps=^auto` keeps only `auto*` on both sides; `--exclude-snaps=hourly` wins over include.
  - `TestGatherSyncSnapFiltered`: `--exclude-snaps=^syncoid_` → every state has `NewSyncSnap == ""`; with no filters → `NewSyncSnap == syncSnap`; with `NoSyncSnap` → `""`.
  - `TestGatherOriginInScope`: clone `tank/data/c` with origin `tank/data/vm@s0` → `OriginInScope=true`; origin outside the tree → false; `--no-clone-handling` → false and `Origin == ""`.
  - `TestGatherSourceMissing`: absent source root → error containing `dataset does not exist`.
  - `TestGatherPoolRoot`: target `backup` → `TargetIsPoolRoot=true`.
  - `TestOrderParentAndClone`: parent with absent target precedes children (dependency recorded); a clone follows its origin dataset; clone-of-clone follows both; existing-target children have no parent dependency; ties keep listing order.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement.** Target-side lookups are batched the same way, with a recursive list on the target root (an absent root → empty). `TargetUsed` comes from the target tree's `Used`.
- [ ] **Step 4:** PASS.
- [ ] **Step 5: Commit** `feat(engine): batched gather and dependency ordering`.

---

### Task 18: Receive arguments, estimates, plan rendering

**Files:**
- Create: `internal/engine/recvopts.go`, `internal/engine/estimate.go`, `internal/engine/render.go`
- Test: `internal/engine/recvopts_test.go`, `internal/engine/estimate_test.go`, `internal/engine/render_test.go`, `internal/engine/testdata/plan_*.golden`

**Interfaces:**
- Consumes: Tasks 6, 10, 15, 16.
- Produces:
  - `func engine.RecvArgs(o *cli.Options, full bool, sourceType string, localProps map[string]string, recordsize string, unprivLinux bool) []string`.
  - `var engine.PreservePropsDenylist []string` (spec §8.6 list, verbatim).
  - `func engine.SendSpecFor(s DatasetState, a Action, opts []zfs.Opt) zfs.SendSpec` (applies the per-type allow-list).
  - `func engine.EstimateAction(ctx, src endpoint.Endpoint, s DatasetState, a Action, opts []zfs.Opt) (bytes int64, approx bool)`.
  - `func engine.RenderPlan(w io.Writer, plans []Plan, syncSnap string, est func(Plan, Action) (int64, bool))`.

- [ ] **Step 1: Write failing tests:**
  - `TestRecvArgs`: defaults → `[-s -F]`; `NoResume` drops `-s`; `NoRollback` drops `-F`; `unprivLinux` adds `-u`; `--recvoptions "ux recordsize o compression=lz4"` → filtered options first: `[-u -x recordsize -o compression=lz4 -s -F]`, and `-u` is not duplicated when `unprivLinux` is also set.
  - `TestRecvArgsPreserveProperties`: localProps `{compression:lz4, atime:off, guid:1, mountpoint:/x}` → `-o atime=off -o compression=lz4 -o mountpoint=/x` sorted by key, `guid` excluded; with `unprivLinux` `mountpoint` and `canmount` are excluded too.
  - `TestRecvArgsPreserveRecordsize`: full + filesystem → `-o recordsize=131072`; incremental or volume → none; ignored when `PreserveProperties` is set.
  - `TestEstimateAction`: an action ending at an existing snapshot calls `SendEstimate`; one ending at the not-yet-created `NewSyncSnap` uses the `written@<base>` prop (incremental) or `referenced` (full) with `approx=true`.
  - `TestRenderPlanGolden`: three plans (incremental with sizes, skip, critical) render byte-identical to `plan_basic.golden`:

```
create sync snapshot syncoid_nas_2026-10-18:14:03:07-GMT02:00 on 1 dataset(s)
tank/data → backup/data
  send incremental @a → @syncoid_nas_2026-10-18:14:03:07-GMT02:00 (with intermediates)  ~1.2 GiB
  prune 1 sync snapshot(s) on source
tank/data/vm → backup/data/vm
  skip: Skipping dataset (syncoid:sync=false): tank/data/vm...
tank/data/db → backup/data/db
  CRITICAL: Target backup/data/db exists but has no snapshots matching with tank/data/db! …
Total estimated transfer: ~1.2 GiB
```
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** PASS.
- [ ] **Step 5: Commit** `feat(engine): receive args, estimates and plan rendering`.

---

### Task 19: Executor

**Files:**
- Create: `internal/engine/exec.go`
- Test: `internal/engine/exec_test.go`

**Interfaces:**
- Consumes: everything in Tasks 10–18.
- Produces:
  - `type engine.Executor struct { Src, Dst endpoint.Endpoint; Opts *cli.Options; PlanOpts PlanOptions; Log *slog.Logger; Progress stream.Renderer; Confirm func(question string) bool; SendCodec, RecvCodec stream.Codec; UnprivLinuxTarget bool; SyncSnap string }`.
  - `func (e *Executor) Run(ctx context.Context, states []DatasetState, plans []Plan) []Result`.

- [ ] **Step 1: Write failing tests** (fake endpoints, plans from `PlanDataset`):
  - `TestExecutorFirstAndSecondRun`: run 1 creates the target with `a,b,c,syncoid_nas_T1`; run 2 (new snap `d`, sync snap `T2`) sends incrementally and prunes `T1` on both sides; results `ok`.
  - `TestExecutorBatchedSyncSnaps`: three datasets → exactly one `Snapshot` call with three names; when that call fails, per-dataset fallback runs, and only the failing dataset is `critical`.
  - `TestExecutorBusyTarget`: `Faults.Busy[target]` → that dataset `soft`, it is excluded from the snapshot batch, the others proceed.
  - `TestExecutorResumeThenReplan`: target with `ResumeToken` → `Resume` runs, then re-gather and incremental to the sync snapshot; one `ok` result.
  - `TestExecutorPartialStateNoResume`: `NoResume`; first receive fails `ErrPartialState` → `AbortResume` then a successful retry. With resume enabled the same failure → `critical`.
  - `TestExecutorResumeSourceGone`: resume fails `ErrResumeSourceGone` → `AbortResume`, re-plan, normal incremental → `ok`.
  - `TestExecutorDestinationExistsForceDelete`: `ErrDestinationExists{backup/data@x}` with `ForceDelete` → `Destroy backup/data@x` then re-plan → `ok`; without it → `critical`.
  - `TestExecutorForceDestroyTarget`: plan `ForceDestroyTarget` → `DestroyDataset(target, recursive)`, re-plan reusing `SyncSnap` (no second `Snapshot` call) → `ok`.
  - `TestExecutorConfirmDestructive`: `Confirm` returning false for `ForceDestroyTarget` → nothing destroyed, `critical` with message `destructive action declined`.
  - `TestExecutorCloneFallback`: `SendClone` fails → re-plan with `OriginInScope=false` → full send → `ok`.
  - `TestExecutorSourceVanished`: source dataset removed after gather; the send fails `ErrNoDataset` → `skipped` with a WARN log.
  - `TestExecutorTargetMissingParentIsCritical`: receive fails with `cannot open 'backup/a': dataset does not exist` → `critical` (not skipped).
  - `TestExecutorDependencySkip`: parent full send fails `critical` → child `soft` with message `skipped: dependency tank/data failed`; an unrelated dataset is `ok`.
  - `TestExecutorWarningsOnly`: `Hold`/`Release`/`Prune`/`DestroyTargetSnaps` failures log WARN and the result stays `ok`; `CreateBookmark` failing twice (original + guid-suffixed name via `naming.BookmarkFallback`) → `critical`.
  - `TestExecutorDeleteTargetSnapshots`: target-only snapshots are destroyed oldest-first in batches of 50 (120 snaps → 3 `Destroy` calls of 50/50/20).
  - `TestExecutorRemediationBound`: a fault that always triggers `ErrResumeSourceGone` → `critical` after 3 re-plan rounds.
  - `TestExecutorCancelMidTransfer`: `BlockSend`; cancel ctx → current result `critical`, no further actions start, `Run` returns within 2 s.
  - `TestExecutorStaleBase`: the target's base snapshot GUID changes between plan and execution → re-gather + re-plan instead of sending.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement** spec §8.5/§8.7. Transfer: `io.Pipe` A ← `Src.Send`; `stream.Copy(A → pipe B, Opts.BufferSize)`; `Dst.Receive` ← B. All three run under one child context, cancelled on the first error; the receive error takes precedence for classification. Log INFO before each send in syncoid's wording (e.g. `Sending incremental tank/a@b ... syncoid_nas_T to backup/a (~ 1.2 GiB):`). `ErrNoDataset` maps to "source vanished" only when it came from the **send** side.
- [ ] **Step 4:** PASS.
- [ ] **Step 5: Commit** `feat(engine): executor with remediation and dependency handling`.

---

### Task 20: Preflight

**Files:**
- Create: `internal/engine/preflight.go`
- Test: `internal/engine/preflight_test.go`

**Interfaces:**
- Consumes: `perms` (Tasks 13–14), `endpoint` (Task 10), Task 15 types.
- Produces:
  - `type engine.DatasetMissing struct { Dataset string; Missing []perms.Requirement }`.
  - `type engine.SideReport struct { Side Side; Label, User string; Root, Sudo bool; SudoErr string; Missing []DatasetMissing; GrantDataset string; Grant []perms.Requirement }`.
  - `type engine.PreflightReport struct { Sides [2]SideReport }` with `OK() bool` and `Render(w io.Writer, full bool)`.
  - `func engine.NeedsFrom(o *cli.Options, props []string, userProps bool) perms.Needs`.
  - `func engine.Preflight(ctx, src, dst endpoint.Endpoint, o *cli.Options, states []DatasetState) (PreflightReport, error)`.

- [ ] **Step 1: Write failing tests:**
  - `TestPreflightRootSkips`: `UID 0` on both sides → OK, no `Allow` calls.
  - `TestPreflightSudo`: `TargetSudo`; target `GetProps(name)` succeeds → OK; fails with stderr `sudo: a password is required` → not OK, `SudoErr` contains that text.
  - `TestPreflightMissing`: source grants lack `destroy`; target lacks `receive` → not OK; `Render(false)` output equals:

```
CRITICAL ERROR: missing ZFS permissions
  source tank/data: destroy
  target backup/data: receive
Grant them (as root) with:
  [source]  zfs allow -u backup destroy,mount tank/data
  [target]  zfs allow -u backup receive,create,mount backup
Alternatively, run with --sudo (requires a sudoers rule for zfs).
```

  (`GrantDataset` is the source root; for an absent target root, its closest existing ancestor; local labels are `source`/`target`, remote labels `user@host` in M2. Each grant line lists the missing permissions plus their spec companions — `destroy`/`snapshot`/`receive` imply `mount`, and `receive` implies `create`.)
  - `TestPreflightPreserveProps`: `--preserve-properties` with source local props `{compression, atime, guid}` → the target requires `compression`,`atime` (denylist removes `guid`).
  - `TestPreflightRenderFull`: `Render(true)` additionally prints `Full minimal grants:` with one `zfs allow` line per side containing the complete required set.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement.** Check every source dataset in `states`, every existing target dataset, and (for absent targets) the closest existing ancestor once.
- [ ] **Step 4:** PASS.
- [ ] **Step 5: Commit** `feat(engine): delegation preflight with grant commands`.

---

### Task 21: Wire `sync` and `permissions` commands

**Files:**
- Modify: `internal/app/app.go`, `cmd/syngoid/main.go`
- Create: `internal/app/summary.go`
- Test: `internal/app/app_test.go`, `internal/app/summary_test.go`

**Interfaces:**
- Consumes: all previous tasks.
- Produces: `Env` gains `NewLocal func(sudo bool) endpoint.Endpoint` (tests inject fakes); `func app.RenderSummary(w io.Writer, results []engine.Result, elapsed time.Duration)`.

- [ ] **Step 1: Write failing tests** (`Env.NewLocal` returns fake endpoints):
  - `TestMainSyncEndToEnd`: `sync tank/data backup/data -r` → exit 0, target populated, stdout ends with the summary.
  - `TestMainRemoteNotYet`: `sync nas:tank/a tank/b` → exit 2, stderr `CRITICAL ERROR: remote endpoints are not supported yet (milestone 2)`.
  - `TestMainFetchServeNotYet`: `fetch` → exit 2, stderr contains `not supported yet (milestone 3)`; `serve` → exit 2, stderr contains `not supported yet (milestone 2)`.
  - `TestMainDryRun`: `--dry-run` → exit 0, plan printed (format of Task 18), no `Snapshot`/`Send`/`Destroy` calls.
  - `TestMainConfirmNoTTY`: `--confirm` with `StdinIsTTY=false` → exit 2, stderr contains `--confirm requires an interactive terminal`.
  - `TestMainConfirmDecline`: TTY stdin `n\n` → exit 0, message `aborted by user`, nothing executed; `y\n` → executes.
  - `TestMainPreflightFail`: missing grants → exit 2, report on stderr, nothing executed.
  - `TestMainPermissionsCommand`: `permissions tank/data backup/data` → full grants printed; exit 0 when complete, 2 when missing.
  - `TestMainExitCodes`: one `soft` → 1; one `critical` → 2.
  - `TestMainDumpSnaps`: `--dumpsnaps` prints the merged snapshot lists at INFO.
  - `TestMainCancelledExit2`: ctx cancelled during a blocked send → exit 2 and the summary is still printed.
  - `TestRenderSummary`: exact text:

```
Summary:
  tank/data → backup/data: ok, 1.2 GiB in 0:00:22
  tank/data/vm → backup/data/vm: skipped (Skipping dataset (syncoid:sync=false): tank/data/vm...)
  2 dataset(s): 1 ok, 0 nothing to do, 1 skipped, 0 failed; 1.2 GiB transferred in 0:00:25
```
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement** the flow in spec §8.2 for local endpoints: Gather → Preflight → Plan → dry-run/confirm → Executor.Run → summary → exit code. `UnprivLinuxTarget = info.OS=="linux" && info.UID!=0 && !TargetSudo`. The hostname comes from `Env.Hostname`; the sync snapshot name from `naming.SyncSnapName(o.Identifier, host, env.Now())`. `--quiet` suppresses the summary. In `main.go`: `signal.NotifyContext(ctx, SIGINT, SIGTERM)`; after the first signal, a second SIGINT calls `os.Exit(2)`.
- [ ] **Step 4:** PASS. Then `go build ./cmd/syngoid && ./syngoid --dry-run tank/a tank/b` on macOS (no ZFS) → exit 2 with a `zfs` not-found critical error, not a panic.
- [ ] **Step 5: Commit** `feat(app): sync and permissions commands with dry-run and confirm`.

---

### Task 22: Linux integration suite (real ZFS)

**Files:**
- Create: `scripts/ci/setup-zfs.sh`, `test/integration/helpers_test.go`, `test/integration/sync_test.go`, `test/integration/upstream_test.go`, `test/integration/perms_test.go`
- Modify: `.github/workflows/ci.yml` (job `integration`), `internal/perms/required.go` and spec §6.2 (only if Step 5 says so)

**Interfaces:**
- Consumes: the built binary (`go build -o $TMP/syngoid ./cmd/syngoid`).
- Produces: `//go:build integration` tests run with `sudo -E go test -tags integration ./test/integration/...`, env `SYNGOID_IT=1`.

- [ ] **Step 1:** `setup-zfs.sh` (Ubuntu): install `zfsutils-linux`; create 1 GiB sparse files; `zpool create syngoid-src` / `syngoid-dst`; create user `syngoid-it`. Helpers run the binary as root or as `syngoid-it` (`sudo -u syngoid-it`), and reset pools between tests (`zfs destroy -r` children).
- [ ] **Step 2: Write tests** in `sync_test.go`: `TestITInitialAndIncremental`, `TestITRecursiveWithClone`, `TestITExcludeDatasets`, `TestITUseHold` (`zfs holds` shows `syncoid_<host>`), `TestITCreateBookmark`, `TestITPruneOnlyOwn` (`--identifier` separation), `TestITDeleteTargetSnapshots`, `TestITInterruptThenResume` (kill the process during a 300 MiB send, rerun → `receive_resume_token` consumed and synced), `TestITDryRunNoChanges`.
- [ ] **Step 3: Port upstream scenarios** in `upstream_test.go`, one test per upstream directory of sanoid tag `v2.2.0` `tests/syncoid/`: `TestUpstream001BookmarkReplicationIntermediate`, `…002BookmarkReplicationNoIntermediate`, `…003ForceDelete`, `…004BookmarkReplicationEdgeCase`, `…005ResetResumeState`, `…006ResetResumeState2`, `…007PreserveRecordsize`, `…008ForceDeleteSnapshot`, `…009PreserveProperties`, `…010FilterSnaps`, `…011SyncOutOfOrderSnapshots`, `…012ReceiveResumeToken`, `…013BookmarkReplication0Named`, `…014BookmarkCheckGuidOnExisting`. Each reproduces the upstream script's setup and assertions with `syngoid` instead of `syncoid`, translating removed or renamed flags.
- [ ] **Step 4: Write delegation tests** in `perms_test.go`: `TestITPreflightPrintsWorkingGrant` (as `syngoid-it` without grants → exit 2; run the printed `zfs allow` lines as root; rerun → exit 0); `TestITUnprivLinuxReceiveUnmounted` (target dataset `mounted=no`, `mountpoint` not received).
- [ ] **Step 5: Resolve §14.1** with `TestITReceiveForcePermissions`: grant the target `create,mount,receive,destroy` without `rollback`, then run an incremental that needs `-F` rollback. If it succeeds, remove `rollback` from `perms.Required` (Task 14 test updated) and from spec §6.2 and §14 item 1, in the same commit. If it fails, keep `rollback` and record the observed stderr in §14 item 1.
- [ ] **Step 6:** Add CI job `integration` (ubuntu-latest): `scripts/ci/setup-zfs.sh`, then `sudo -E env PATH=$PATH go test -tags integration -v ./test/integration/...`.
- [ ] **Step 7:** Commit `test(integration): real-ZFS suite, upstream scenarios and delegation checks`. Because nothing is pushed, verify CI by running the job with `act` locally (or in a Linux VM). If neither is available, report that integration tests are unverified.

---

### Task 23: Interop with Perl syncoid 2.2.0

**Files:**
- Create: `test/integration/compat_test.go`
- Modify: `scripts/ci/setup-zfs.sh` (install `libcapture-tiny-perl`; download `syncoid` from `https://raw.githubusercontent.com/jimsalterjrs/sanoid/v2.2.0/syncoid` to `/usr/local/bin/syncoid`, mode 0755)

**Interfaces:**
- Consumes: Task 22 helpers.

- [ ] **Step 1: Write tests:**
  - `TestCompatSyncoidThenSyngoid`: `syncoid syngoid-src/a syngoid-dst/a` (as root), add a snapshot, `syngoid syngoid-src/a syngoid-dst/a` → exit 0, incremental (no full resend), syncoid's old sync snapshot pruned on both sides.
  - `TestCompatSyngoidThenSyncoid`: the reverse direction succeeds incrementally.
  - `TestCompatHolds`: `syncoid --use-hold` then `syngoid --use-hold` → the old hold is released and the new one exists with the same tag.
  - `TestCompatBookmarks`: `syncoid --no-sync-snap --create-bookmark` creates `#<snap>`; after destroying the shared snapshot on the source, `syngoid --no-sync-snap` replicates via the bookmark.
  - `TestCompatIdentifiersIsolated`: `syncoid --identifier=x` and `syngoid --identifier=y` on the same dataset never prune each other's sync snapshots.
  - `TestCompatSnapshotNameFormat`: the newest sync snapshot names created by both tools in the same second differ only by identifier and match `naming.SyncSnapPattern`.
- [ ] **Step 2:** Run as in Task 22 — PASS.
- [ ] **Step 3: Commit** `test(compat): interoperability with Perl syncoid 2.2.0`.
