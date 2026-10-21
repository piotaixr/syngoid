# syngoid M2 — Remote Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `syngoid` replicates between any combination of local and SSH-reachable hosts: it pushes itself to each remote, runs there as `syngoid serve`, and drives the unchanged M1 engine over one multiplexed RPC session per host, with compression on network hops only.

**Architecture:** `internal/rpc` defines the wire protocol (JSON-lines control stream + raw data streams over yamux) and `rpc.Remote`, an `endpoint.Endpoint` client. `internal/agent` serves that protocol on stdin/stdout, dispatching onto an `endpoint.Local`. `internal/sshx` builds and spawns system `ssh` (one ControlMaster per host); `internal/bootstrap` probes the remote, resolves and pushes the agent binary, and performs the handshake with one re-push on mismatch. `internal/app` picks local or remote endpoints per side and the codec per hop; the engine does not change except for two M1 carry-overs.

**Tech Stack:** Go 1.27, existing M1 dependencies, `github.com/hashicorp/yamux` v0.1.2, system OpenSSH client, GitHub Actions (Ubuntu + sshd for integration; vmactions FreeBSD/OmniOS VMs for fixture capture).

**Spec:** `docs/superpowers/specs/2026-10-18-syngoid-design.md` (M1 plan for context: `docs/superpowers/plans/2026-10-18-syngoid-m1-local-core.md`)

**Scope (from the M1 roadmap):** §5 bootstrap (self + local stores; the user cache, downloads and minisign are M3), §7 RPC/yamux/agent, `internal/sshx`, `rpc.Remote`, `serve`, §9.2 codec placement, remote preflight, §12.2 RPC component tests, §12.3 `ssh localhost` integration, §14.2–14.4. Plus two parked M1 findings (Task 11).

## Global Constraints

- Module path `github.com/piotaixr/syngoid`; `go.mod` has `go 1.27`; `mise.toml` pins `go = "1.27.1"`; run Go through mise (`mise exec -- go …`) if the shim is not active.
- `CGO_ENABLED=0` everywhere. Unix-only build targets: linux, freebsd, illumos, darwin.
- Direct third-party dependencies: `spf13/pflag`, `klauspost/compress`, `pierrec/lz4/v4`, `golang.org/x/term`, plus `github.com/hashicorp/yamux v0.1.2` (added in Task 3). Nothing else; SSH is the system `ssh` binary, never a Go SSH library.
- No shell strings for ZFS: every `zfs`/`ps`/`id` invocation is an argv slice. The only shell strings are the remote commands sent through `ssh` (probe, push, serve — Task 9), and every value interpolated into them goes through `sshx.ShellQuote`.
- Child env for `zfs`/`ps`/`id` sets `LC_ALL=C` and appends `:/sbin:/usr/sbin:/usr/local/sbin:/bin:/usr/bin` to `PATH` (M1 `zfs.OSExec`, reused by the agent).
- Console log prefixes exactly: `DEBUG: `, `INFO: ` (stdout); `WARNING: `, `CRITICAL ERROR: ` (stderr). Remote agent log lines and remote commands carry the host prefix, e.g. `DEBUG: [backup@nas] zfs receive -s -F tank/backups/vm`.
- Compat names exactly (unchanged from M1): sync snapshot `syncoid_<identifier_><hostname>_YYYY-MM-DD:hh:mm:ss-GMT<sign>HH:MM`; hold tag `syncoid_<identifier_><hostname>`; property `syncoid:sync`. The hostname is always the **initiator's**.
- Exit codes: 0 ok, 1 soft failure, 2 critical/global failure (connection, bootstrap and handshake failures are global → 2).
- Wire constants: `buildinfo.ProtocolVersion = 1`; handshake timeout 30 s; yamux `MaxStreamWindowSize` 16 MiB, keepalive disabled; progress events every 500 ms; ControlMaster options `-o ControlMaster=yes -o ControlPath=<private 0700 tmpdir>/cm-<n> -o ControlPersist=60`.
- After the mux is up the agent never writes to its own stderr (yamux `LogOutput` is `io.Discard`; logs travel as events).
- All tests pass with `go test -race ./...`; `go vet ./...` and `golangci-lint run` (CI pins v2.15.0, also with `--build-tags integration`) are clean.
- Every commit message ends with a blank line then the `Co-Authored-By:` line your harness attribution reminder specifies for your model (M1 ruling R8).
- Work on branch `m2-remote` cut from `main` after `fix/preflight-hold-permission` is merged. Pushing, opening PRs and triggering workflows need the user's go-ahead (Tasks 12 and 14 say where).

## Review Focus

1. **Network drop or `kill -9` of the initiator mid-transfer** must leave no orphan `zfs receive`/`zfs send` on the remote (the target is not "busy" on the next run) and must keep the resume token. A Go program dies of SIGPIPE when writing to a closed stdout unless it handles it, which would kill the agent before it kills its children. → Task 7 `TestServeDisconnectKillsChildren`, Task 12 `TestITRemoteKillInitiatorThenResume`.
2. **A remote receive that fails early** (missing parent, busy, permission) while the source keeps streaming must return the target's error promptly, not hang on yamux window back-pressure. → Task 6 `TestRemoteReceiveEarlyFailureUnblocks`.
3. **Slow links and large replies:** one 16 MiB yamux frame on a slow link must not trip yamux's connection write timeout, and a `ListSnapshots` reply for hundreds of thousands of snapshots must not hit a line-length limit. → Task 3 `TestMuxSlowLinkNoWriteTimeout`, Task 1 `TestCodecLargeMessage`.
4. **Hostile or odd names:** a host starting with `-` (`-oProxyCommand=…`), a `--remote-bin-dir` containing spaces or `'`, dataset names with spaces, and RPC params starting with `-` must never become ssh options, shell syntax or zfs flags. → Task 8 `TestNewMasterRejectsOptionLikeHost`, `TestShellQuoteRoundTrip`; Task 9 `TestPushCommandOddDir`; Task 2 `TestValidate`; Task 12 `TestITRemoteDatasetWithSpace`.
5. **Ctrl-C at the terminal during a remote run:** the terminal's SIGINT must reach syngoid only, not the `ssh` children (otherwise the sessions die before the graceful cancel), so agents kill their `zfs` children, the summary prints and the exit code is 2. → Task 8 `TestClientCommandsOwnProcessGroup`, Task 10 `TestMainRemoteCancelledExit2`, Task 12 `TestITRemoteCtrlC`.

## Decisions refining the spec

Each is recorded in the spec by the task that implements it, in the same commit.

- **R-M2-1 (build hash, §3.7/§5.2/§5.5; Task 1):** `buildinfo.BuildHash()` is the ldflags-stamped `buildinfo.Hash` when set, otherwise the hex SHA-256 of the running executable. So an unstamped dev build only matches an agent that is the same file, which is what self-push produces; cross-platform dev agents from `--bin-store` need a stamped hash.
- **R-M2-2 (envelope, §7.1/§7.3; Task 1):** every control line is one `rpc.Message`; `hello` is the first request (id 1); methods `cancel` (agent-side context cancel of a request id) and `shutdown` exist in addition to the Endpoint methods. Data streams start with the line `{"request_id":N}`.
- **R-M2-3 (sessions; Task 9):** one ssh ControlMaster per `[user@]host`, one agent session per (`[user@]host`, sudo). Source and target on the same host with the same sudo share one session.
- **R-M2-4 (remote dir, §5.2/§5.4; Task 9):** the probe receives the bin dir single-quoted with a literal leading `~/`, expands it against `$HOME` itself and prints the absolute result; push and serve use only that absolute, quoted path.
- **R-M2-5 (pruning, §5.5; Task 7):** sibling pruning runs only when the agent lives in `$HOME/.cache/syngoid/<dir>`, considers only directories named like agent dirs (`^(dev|[0-9]+\.[0-9]+\.[0-9]+[0-9A-Za-z.-]*)(-[0-9a-f]{8})?$`, so the M3 initiator cache `~/.cache/syngoid/bin` is never touched), and keeps the current one plus the newest other by mtime.
- **R-M2-6 (process groups; Task 8):** the master-creating `ssh` runs in the foreground process group (it may prompt for a password); every other `ssh` runs in its own process group so a terminal Ctrl-C reaches only syngoid.
- **R-M2-7 (stores, §5.3; Task 9):** M2 resolves self → `--bin-store` dirs → `/usr/lib/syngoid/bin`. The user cache and download are added with their verification in M3.

---

## File Structure

```
internal/buildinfo/buildinfo.go     + Hash, BuildHash, ExeSHA256                     (Task 1)
internal/rpc/proto.go               Message, Hello, HelloReply, Event, Error, method names, params (Task 1)
internal/rpc/codec.go               JSON-lines Codec, data stream header             (Tasks 1, 3)
internal/rpc/validate.go            argv-injection guard for params                  (Task 2)
internal/rpc/mux.go                 yamux config, Client/Server sessions, chunkWriter (Task 3)
internal/rpc/remote.go              Remote: handshake, call, events, non-stream methods (Task 5)
internal/rpc/remote_stream.go       Remote.Send / Remote.Receive                     (Task 6)
internal/agent/server.go            Serve: handshake, dispatch, cancel, shutdown     (Task 4)
internal/agent/events.go            slog handler emitting log events                 (Task 4)
internal/agent/streams.go           data stream rendezvous, send/receive handlers    (Task 6)
internal/agent/prune.go             sibling version dir pruning                      (Task 7)
internal/agent/agenttest/agenttest.go  in-process agent over net.Pipe for tests      (Task 5)
internal/sshx/sshx.go               Config, Master (argv, Run, Spawn, Close), ShellQuote (Task 8)
internal/bootstrap/platform.go      Platform, ParsePlatform                          (Task 9)
internal/bootstrap/probe.go         probe/push/serve commands, ParseProbe            (Task 9)
internal/bootstrap/resolve.go       AgentDirName, DefaultRemoteBinDir, Resolver      (Task 9)
internal/bootstrap/connect.go       Transport, Connect, CheckHello                   (Task 9)
internal/bootstrap/pool.go          Pool (session/master reuse, Close)               (Task 9)
internal/app/app.go, remote.go      remote endpoints, codec placement, serve          (Tasks 7, 10)
internal/engine/preflight.go        remote labels in Render                          (Task 10)
internal/engine/dag.go, plan.go     M1 carry-overs                                   (Task 11)
internal/endpoint/local.go          ps fallback                                       (Task 14)
test/integration/remote_test.go     ssh localhost suite                              (Task 12)
scripts/ci/setup-zfs.sh             + sshd, keys, sudoers                             (Task 12)
scripts/ci/capture-fixtures.sh      fixture capture on real ZFS                      (Task 14)
.github/workflows/fixtures.yml      workflow_dispatch: Linux, FreeBSD, OmniOS        (Task 14)
```

---

### Task 1: Wire messages, JSON-lines codec, error mapping, build hash

**Files:**
- Create: `internal/rpc/proto.go`, `internal/rpc/codec.go`
- Modify: `internal/buildinfo/buildinfo.go`, `internal/app/app.go` (`runVersion`), spec §3.7 and §7.1 (R-M2-1, R-M2-2)
- Test: `internal/rpc/proto_test.go`, `internal/rpc/codec_test.go`, `internal/buildinfo/buildinfo_test.go`, `internal/app/app_test.go`

**Interfaces:**
- Produces:
  - `var buildinfo.Hash = ""` (ldflags); `func buildinfo.ExeSHA256() (string, error)` (hex SHA-256 of `os.Executable()`, computed once); `func buildinfo.BuildHash() string` (`Hash` if set, else `ExeSHA256()`, `"unknown"` on error).
  - Method name constants `rpc.MethodHello = "hello"`, `MethodInfo = "info"`, `MethodListTree = "list_tree"`, `MethodListSnapshots = "list_snapshots"`, `MethodListBookmarks = "list_bookmarks"`, `MethodGetProps = "get_props"`, `MethodLocalProps = "local_props"`, `MethodExists = "exists"`, `MethodSnapshot = "snapshot"`, `MethodDestroy = "destroy"`, `MethodDestroyDataset = "destroy_dataset"`, `MethodHold = "hold"`, `MethodRelease = "release"`, `MethodBookmark = "bookmark"`, `MethodAbortResume = "abort_resume"`, `MethodSendEstimate = "send_estimate"`, `MethodReceiveBusy = "receive_busy"`, `MethodSend = "send"`, `MethodReceive = "receive"`, `MethodAllow = "allow"`, `MethodCancel = "cancel"`, `MethodShutdown = "shutdown"`.
  - `type rpc.Hello struct { Protocol int; Version, BuildHash string; Debug, Sudo bool }` — JSON keys `protocol, version, build_hash, debug, sudo`.
  - `type rpc.HelloReply struct { Protocol int; Version, BuildHash, ExeSHA256, OS, Arch, User string; UID int; Groups []string; ZFSVersion, Home string }` — JSON keys `protocol, version, build_hash, exe_sha256, os, arch, user, uid, groups, zfs_version, home`. Hello and HelloReply are frozen: fields may be added, never renamed or removed.
  - `type rpc.Message struct { ID uint64 "id,omitempty"; Method string "method,omitempty"; Params, Result json.RawMessage "…,omitempty"; Error *Error "error,omitempty"; Event *Event "event,omitempty" }` — a request has `Method`, an event has `Event`, anything else is a response.
  - `type rpc.Event struct { Kind string "kind" ("log"|"progress"); RequestID uint64 "request_id,omitempty"; Level string "level,omitempty" ("DEBUG"|"INFO"|"WARN"|"ERROR"); Msg string "msg,omitempty"; Bytes int64 "bytes,omitempty" }`.
  - `type rpc.Error struct { Code, Message string; Stderr string; Argv []string; ExitCode int }` (keys `code, message, stderr, argv, exit_code`), codes `CodeCommand = "command"`, `CodeInvalid = "invalid"`, `CodeCanceled = "canceled"`, `CodeUnknownMethod = "unknown_method"`, `CodeInternal = "internal"`.
  - `func rpc.EncodeError(err error) *Error` and `func (e *Error) Err() error`.
  - `type rpc.InvalidError struct { Field, Value string }` with `Error() = "invalid " + Field + ": " + strconv.Quote(Value)`.
  - Params types (JSON keys snake_case of the field names): `DatasetParams{Dataset string; Recursive bool}` (list_*, exists, local_props, allow, abort_resume, destroy_dataset), `GetPropsParams{Dataset string; Props []string}`, `SnapshotParams{Names []string}`, `DestroyParams{Dataset string; Snaps []string}`, `HoldParams{Tag, Snapshot string}` (hold, release), `BookmarkParams{Snapshot, Bookmark string}`, `SendEstimateParams{Spec zfs.SendSpec}`, `ReceiveBusyParams{Datasets []string}`, `SendParams{Spec zfs.SendSpec; Codec stream.Codec}`, `ReceiveParams{Spec zfs.RecvSpec; Codec stream.Codec}`, `CancelParams{ID uint64}`. `zfs.SendSpec`/`zfs.RecvSpec` travel with their Go field names (both ends run the same version).
  - `func rpc.NewCodec(rw io.ReadWriter) *Codec`; `(c *Codec) Write(m Message) error` (mutex-guarded `json.Encoder`, one line per message); `(c *Codec) Read() (Message, error)` (`json.Decoder`, no line-length limit; not safe for concurrent readers).

- [ ] **Step 1: Write failing tests:**
  - `TestBuildHashPrefersStamp`: with `Hash = "abc"` → `"abc"`; with `Hash = ""` → equals `ExeSHA256()` (64 hex chars).
  - `TestMainVersion` (extend M1's): stdout matches `^syngoid dev \(commit unknown, build [0-9a-f]{12}\) protocol 1\n$` (version line becomes `syngoid <Version> (commit <Commit>[, dirty], build <BuildHash()[:12]>) protocol <ProtocolVersion>`).
  - `TestMessageRoundTrip`: a request (`hello` with `Hello{Protocol:1, Version:"dev", BuildHash:"h", Debug:true}`), a response with `Result`, a response with `Error`, a log event and a progress event each survive `Write`→`Read` field-identical; the encoded hello line equals `{"id":1,"method":"hello","params":{"protocol":1,"version":"dev","build_hash":"h","debug":true,"sudo":false}}` + `\n`.
  - `TestErrorMapping`: `EncodeError(&zfs.CommandError{Argv: [zfs receive tank/b], ExitCode: 1, Stderr: "cannot receive incremental stream: destination tank/b contains partially-complete state from \"zfs receive -s\".\n"})` → `Code "command"`; `.Err()` is a `*zfs.CommandError` with the same Argv/ExitCode/Stderr and `errors.Is(err, zfs.ErrPartialState)`. `context.Canceled` and `context.DeadlineExceeded` → `"canceled"` → `.Err()` is `context.Canceled`. `&InvalidError{"dataset", "-x"}` → `"invalid"` → `errors.As` gives `*InvalidError`. `errors.New("boom")` → `"internal"`, `.Err().Error() == "boom"`.
  - `TestCodecLargeMessage`: a `Result` of 32 MiB (a `map[string][]zfs.Snapshot` with 300 000 snapshots) round-trips.
  - `FuzzCodecRead`: arbitrary bytes never panic `Read` (spec §12.1 "RPC decoder").
- [ ] **Step 2:** `go test ./internal/rpc/ ./internal/buildinfo/ ./internal/app/` — FAIL.
- [ ] **Step 3: Implement** the types and functions above; `runVersion` prints the new line.
- [ ] **Step 4:** PASS (plus `go test -run '^$' -fuzz FuzzCodecRead -fuzztime 10s ./internal/rpc/`).
- [ ] **Step 5:** Spec: §3.7 `version` line format and R-M2-1; §7.1 envelope and data-stream header per R-M2-2.
- [ ] **Step 6: Commit** `feat(rpc): wire messages, JSON-lines codec and build hash`.

---

### Task 2: Parameter validation (argv-injection guard, spec §7.3)

**Files:**
- Create: `internal/rpc/validate.go`
- Test: `internal/rpc/validate_test.go`

**Interfaces:**
- Consumes: params types, `InvalidError` (Task 1); `zfs.SendAllowedStream`, `zfs.RecvAllowed`.
- Produces: `func rpc.Validate(method string, params any) error` — returns `*InvalidError` naming the first bad field; the agent calls it before touching the endpoint. Rules:
  - component: `^[A-Za-z0-9_.: -]+$` (ZFS allows spaces); a dataset is `/`-joined non-empty components, ≤ 255 bytes, not starting with `-`.
  - snapshot/bookmark short names: one component, not starting with `-`; full snapshot `ds@short`, full bookmark `ds#short`.
  - hold tag: 1–255 bytes, not starting with `-`, no control characters.
  - resume token: `^[0-9A-Za-z][0-9A-Za-z-]*$`.
  - property name: `^[a-z0-9_.:][a-z0-9_.:-]*$`.
  - `SendSpec`: `Dataset` valid; `To`, `FromSnap`, `FromBookmark` valid short names when set; `FromOrigin` a full snapshot when set; `ResumeToken` valid when set; every `Options[i].Flag` in `zfs.SendAllowedStream` with an empty `Value`.
  - `RecvSpec.Args`: each element is one of `-s -F -u -h -v`, or `-x` followed by a property name, or `-o` followed by `prop=value` with a valid prop and a value free of NUL and newline.

- [ ] **Step 1: Write failing table test** `TestValidate` (method, params → ok or the field named in the error): `exists {tank/my data}` ok; `exists {-oops}` → `dataset`; `exists {tank//a}` → `dataset`; `destroy {tank/a, [s1, "-r"]}` → `snaps`; `hold {"-d", tank/a@s}` → `tag`; `hold {syncoid_nas, tank/a@s}` ok; `bookmark {tank/a@s, tank/a#b}` ok; `bookmark {tank/a@s, tank/a@b}` → `bookmark`; `send {Dataset: tank/a, ResumeToken: "-t"}` → `spec.resume_token`; `send {Options: [{Flag:'R'}]}` ok, `[{Flag:'o'}]` → `spec.options`; `receive {Args: [-s -F -o compression=lz4 -x mountpoint]}` ok; `[-o "-x=1"]` → `spec.args`; `[--force]` → `spec.args`; `[-o "a=b\nc"]` → `spec.args`; `get_props {tank/a, ["syncoid:sync", "-H"]}` → `props`.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** PASS.
- [ ] **Step 5: Commit** `feat(rpc): validate request parameters against argv injection`.

---

### Task 3: yamux sessions and data stream framing

**Files:**
- Create: `internal/rpc/mux.go`
- Modify: `internal/rpc/codec.go` (data header), `go.mod`/`go.sum` (`github.com/hashicorp/yamux v0.1.2`)
- Test: `internal/rpc/mux_test.go`

**Interfaces:**
- Produces:
  - `const rpc.MaxStreamWindow = 16 << 20`, `const rpc.MaxFrameWrite = 256 << 10`, `var rpc.connWriteTimeout = 2 * time.Minute` (unexported so tests can shorten it).
  - `func rpc.MuxClient(conn io.ReadWriteCloser) (*yamux.Session, error)`, `func rpc.MuxServer(conn io.ReadWriteCloser) (*yamux.Session, error)` — config: `EnableKeepAlive=false`, `MaxStreamWindowSize=MaxStreamWindow`, `ConnectionWriteTimeout=connWriteTimeout`, `LogOutput=io.Discard`.
  - `func rpc.ChunkWriter(w io.Writer) io.Writer` — forwards each `Write` in pieces of at most `MaxFrameWrite`, so no single yamux frame takes long on a slow link.
  - `func rpc.WriteDataHeader(w io.Writer, id uint64) error` (writes `{"request_id":N}\n`); `func rpc.ReadDataHeader(r *bufio.Reader) (uint64, error)` (reads one line with `ReadSlice('\n')` on a reader of the default 4096-byte size, so the bytes after the header stay in `r` for the caller; longer lines, id 0 and bad JSON are errors).

- [ ] **Step 1: Write failing tests:**
  - `TestMuxStreamsOverPipe`: over `net.Pipe`, client opens two streams, writes 8 MiB on each concurrently; server reads both byte-identical.
  - `TestMuxSlowLinkNoWriteTimeout`: `connWriteTimeout = 500 * time.Millisecond`; the client side of the pipe is throttled to 2 MiB/s; writing 6 MiB through `ChunkWriter(stream)` succeeds (control: the same write without `ChunkWriter` fails with `yamux.ErrConnectionWriteTimeout`, which proves the test exercises the timeout). If the control does not fail, yamux already bounds its frames: drop `ChunkWriter` and the control, keep the slow-link test, and say so in the task report.
  - `TestDataHeader`: header + payload written to a pipe; `ReadDataHeader` returns the id and the following `Read`s return exactly the payload; a 5000-byte first line → error.
  - `TestMuxConfigSilent`: the session config has `LogOutput == io.Discard` and `EnableKeepAlive == false`.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** PASS.
- [ ] **Step 5: Commit** `feat(rpc): yamux sessions and data stream framing`.

---

### Task 4: Agent server core

**Files:**
- Create: `internal/agent/server.go`, `internal/agent/events.go`
- Test: `internal/agent/server_test.go`

**Interfaces:**
- Consumes: Tasks 1–3; `endpoint.Endpoint`, `fake` (M1).
- Produces:
  - `type agent.Config struct { NewEndpoint func(sudo bool, log *slog.Logger) endpoint.Endpoint; Home, Exe, ExeSHA256 string; Prune func(exe, home string, log *slog.Logger) }` (`Prune` may be nil; Task 7 sets it).
  - `func agent.Serve(ctx context.Context, conn io.ReadWriteCloser, cfg Config) error`.
  - Behaviour: `MuxServer(conn)`; the first accepted stream is the control stream; the first message must be `hello` (anything else → error response, return). The endpoint is created with `Hello.Sudo` and a logger whose handler emits `Event{Kind:"log"}` (level DEBUG when `Hello.Debug`, else INFO). The reply carries `ProtocolVersion`, `buildinfo.Version`, `buildinfo.BuildHash()`, `cfg.ExeSHA256`, `runtime.GOOS/GOARCH`, `ep.Info` (user, uid, groups, zfs version) and `cfg.Home`. When the initiator's protocol and version equal the agent's (and, for dev builds, the build hash), `cfg.Prune` runs once. Each later request runs in its own goroutine with its own context; `Validate` runs first; `cancel` cancels the named request; `shutdown` replies then returns nil. When the session closes or ctx ends, every in-flight request context is cancelled and `Serve` returns after all handlers return. Unknown method → `CodeUnknownMethod`. `send`/`receive` → `CodeUnknownMethod` until Task 6.

- [ ] **Step 1: Write failing tests** (raw harness in the test file: `net.Pipe`, `MuxClient`, open the control stream, `rpc.Codec`; the agent's endpoint is a `fake.Endpoint` on a `fake.World`):
  - `TestServeHello`: reply has `Protocol 1`, `Version "dev"`, `BuildHash == buildinfo.BuildHash()`, `ExeSHA256 == cfg.ExeSHA256`, `OS/Arch == runtime.GOOS/GOARCH`, user/uid/groups/zfs version from the fake `HostInfo`, `Home == cfg.Home`.
  - `TestServeFirstMessageMustBeHello`: first request `exists` → error response, `Serve` returns an error.
  - `TestServeDispatch` table over every non-stream method: the result decodes to what the fake returns for the same call, and mutations show in the World (`snapshot` creates the snapshots, `destroy` removes them, `hold` adds the tag…).
  - `TestServeCommandError`: `exists` on a dataset whose lookup the test endpoint fails with a `*zfs.CommandError` → response `Code "command"` with the stderr.
  - `TestServeRejectsInvalidParams`: `destroy {"-r", [x]}` → `Code "invalid"`; the fake's `Calls()` is empty.
  - `TestServeUnknownMethod` → `Code "unknown_method"`.
  - `TestServeLogEvents`: with `Debug:true`, a test endpoint logging `l.Debug("zfs list")` produces `Event{Kind:"log", Level:"DEBUG", Msg:"zfs list"}`; with `Debug:false` it produces nothing and `l.Info` still does.
  - `TestServeConcurrentRequests`: a request blocked in the endpoint (test wrapper whose `Exists` waits for ctx) does not delay a second `list_tree`.
  - `TestServeCancel`: `cancel {id}` of the blocked request → its response is `Code "canceled"`.
  - `TestServeEOFCancelsInFlight`: closing the client conn → the blocked request's ctx is cancelled and `Serve` returns within 2 s.
  - `TestServeShutdown`: `shutdown` → success response, then `Serve` returns nil.
  - `TestServePrunesOnlyOnMatch`: `Prune` is called once when the hello matches, never when `Version` differs.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** PASS.
- [ ] **Step 5: Commit** `feat(agent): serve the RPC protocol over a multiplexed session`.

---

### Task 5: `rpc.Remote` (handshake and non-stream methods)

**Files:**
- Create: `internal/rpc/remote.go`, `internal/agent/agenttest/agenttest.go`
- Test: `internal/rpc/remote_test.go` (package `rpc_test`)

**Interfaces:**
- Consumes: Tasks 1–4.
- Produces:
  - `func rpc.Handshake(ctx context.Context, conn io.ReadWriteCloser, h Hello, label string, log *slog.Logger) (*Remote, HelloReply, error)` — `MuxClient`, opens the control stream, sends `hello`, waits for the reply under ctx (callers apply the 30 s timeout); on any failure closes conn. The returned `*Remote` is usable even when the caller then rejects the reply (it must `Close` it).
  - `type rpc.Remote` implementing `endpoint.Endpoint`; `Label()` returns `label`; every method is one `call`; `Info` calls `info`.
  - `func (r *Remote) Close() error` — sends `shutdown` (waits ≤ 5 s for the reply), closes the session. Idempotent.
  - `var rpc.ErrConnectionLost = errors.New("connection to agent lost")`; transport failures are returned as `fmt.Errorf("%s: %w", label, ErrConnectionLost)` (with the session's error appended when known).
  - `const rpc.cancelGrace = 15 * time.Second` (var in tests): when ctx ends during a call, `Remote` sends `cancel`, waits up to `cancelGrace` for the response, then returns `ctx.Err()`.
  - Log events are re-logged with `log.Log(ctx, level, msg, logx.KeyHost, label)`; `ERROR` maps to `logx.LevelCritical`.
  - `func agenttest.Start(t testing.TB, ep endpoint.Endpoint, label string, debug bool, log *slog.Logger) *rpc.Remote` — runs `agent.Serve` on one end of a `net.Pipe` with `NewEndpoint` returning `ep`, handshakes on the other, registers cleanup (Close + wait for Serve). Used by Tasks 5, 6 and 10.

- [ ] **Step 1: Write failing tests** (agent backed by `fake` via `agenttest.Start`):
  - `TestRemoteQueriesMatchFake`: `ListTree`, `ListSnapshots`, `ListBookmarks`, `GetProps`, `LocalProps`, `Exists`, `Allow`, `ReceiveBusy`, `SendEstimate`, `Info` return exactly what the fake returns when called directly.
  - `TestRemoteMutations`: `Snapshot`, `Destroy`, `DestroyDataset`, `Hold`, `Release`, `Bookmark`, `AbortResume` change the World as direct calls would.
  - `TestRemoteErrorsClassified`: `Exists` on a dataset name the fake answers with `cannot open 'tank/x': dataset does not exist` → `errors.Is(err, zfs.ErrNoDataset)` and `errors.As(err, **zfs.CommandError)`; a partial-state fault → `errors.Is(err, zfs.ErrPartialState)`.
  - `TestRemoteInvalidParams`: `Exists(ctx, "-x")` → `errors.As(err, **rpc.InvalidError)`.
  - `TestRemoteCancel`: a call blocked in the agent's endpoint; cancelling ctx returns `context.Canceled` within 1 s and the agent's handler saw its ctx cancelled.
  - `TestRemoteConnectionLost`: closing the pipe fails the in-flight call and every later call with `errors.Is(err, rpc.ErrConnectionLost)` and the label in the message.
  - `TestRemoteLogEvents`: debug on; an agent-side `Debug("zfs receive -s -F tank/b")` reaches the initiator's `logx` console as `DEBUG: [backup@nas] zfs receive -s -F tank/b\n`.
  - `TestHandshakeTimeout`: the peer never answers; `Handshake` with a 200 ms ctx returns `context.DeadlineExceeded` and closes conn.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement** (one reader goroutine routes responses to pending calls by id and events to the logger/progress callbacks; writes go through the mutex-guarded codec).
- [ ] **Step 4:** PASS.
- [ ] **Step 5: Commit** `feat(rpc): remote endpoint over the agent protocol`.

---

### Task 6: Data streams (send and receive across the wire)

**Files:**
- Create: `internal/agent/streams.go`, `internal/rpc/remote_stream.go`
- Modify: `internal/agent/server.go` (accept loop, `send`/`receive` dispatch)
- Test: `internal/agent/streams_test.go`, `internal/rpc/remote_stream_test.go`

**Interfaces:**
- Consumes: Tasks 3–5.
- Produces:
  - Agent: every stream accepted after the control stream starts with a data header; a rendezvous pairs it with its `send`/`receive` request whichever arrives first; `var agent.dataStreamTimeout = 30 * time.Second` (an unpaired request fails `CodeInternal` "no data stream", an unpaired stream is closed).
  - Agent `send`: `ep.Send(ctx, spec, endpoint.SendOpts{Codec, Progress}, rpc.ChunkWriter(stream))`; `Progress` emits `Event{Kind:"progress", RequestID, Bytes}` at most every 500 ms, and a final one before the response; then the stream is closed and the response written (the final progress event precedes the response on the control stream).
  - Agent `receive`: `ep.Receive(ctx, spec, codec, bufReader)` where `bufReader` is the `*bufio.Reader` that read the header; then close the stream and respond.
  - `(r *Remote) Send(ctx, s zfs.SendSpec, o endpoint.SendOpts, w io.Writer) error`: send the request, open a stream, write the header, `io.Copy(w, stream)` to EOF, then return the response's error. A failing `w` → send `cancel`, close the stream, wait for the response (≤ `cancelGrace`), return the `w` error. Progress events for the request call `o.Progress`.
  - `(r *Remote) Receive(ctx, s zfs.RecvSpec, codec stream.Codec, rd io.Reader) error`: send the request, open a stream, write the header, copy `rd` → `ChunkWriter(stream)` in a goroutine, half-close the stream on EOF, return the response's error. If reading `rd` fails → `cancel`, close the stream, wait for the response (≤ `cancelGrace`), return the read error unchanged (so the engine's `errors.Is(err, errUpstream)` keeps working). If an error response arrives while the copy is still running → close the stream (unblocks the copy) and return that error; as with `Local`, the caller closes `rd` to unblock a reader stuck in `Read`.
  - Both return only after the agent's response (or `cancelGrace`), so the remote `zfs` process has exited when they return.

- [ ] **Step 1: Write failing tests:**
  - `TestRemoteTransferCodecs` table over `none`, `zstd-fast`, `zstd`, `lz4`, `gzip` × {fake local → Remote target, Remote source → fake local, Remote → Remote with a plain `io.Copy` relay}: full send of `s1` then `-I s1→s3`; the target World ends with `s1,s2,s3` and matching GUIDs.
  - `TestRemoteSendProgress`: the agent-side endpoint (test wrapper) reports progress 1000 times over 1.2 s ending at 5 MiB; the initiator's `o.Progress` is called at most 4 times and last with 5 MiB.
  - `TestRemoteReceiveEarlyFailureUnblocks`: target parent missing on the agent; `Receive` with an endless reader returns the `dataset does not exist` `*zfs.CommandError` within 2 s.
  - `TestRemoteReceiveUpstreamError`: `rd` returns `errX` after 64 KiB → `errors.Is(err, errX)`; the agent's receive was cancelled and the target World is unchanged.
  - `TestRemoteSendCancel`: agent fake `BlockSend`; cancelling ctx returns `context.Canceled` within 2 s and the agent handler has returned.
  - `TestRemoteSendDownstreamError`: `w` fails after the first write → `Send` returns that error; the agent's send ctx was cancelled.
  - `TestAgentDataStreamTimeout`: `dataStreamTimeout = 100 * time.Millisecond`; a raw `send` request without a stream → error response containing `no data stream`.
  - `TestAgentStreamBeforeRequest`: raw harness opens the data stream first, then sends `receive`; the receive succeeds.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** PASS.
- [ ] **Step 5: Commit** `feat(rpc): stream zfs send and receive through the agent`.

---

### Task 7: `syngoid serve` and agent lifecycle

**Files:**
- Create: `internal/agent/prune.go`
- Modify: `internal/agent/server.go` (`ErrSetup`), `internal/app/app.go` (`CmdServe`), `cmd/syngoid/main.go`, spec §5.5 (R-M2-5), §7.4
- Test: `internal/agent/prune_test.go`, `internal/app/app_test.go`, `cmd/syngoid/main_test.go`

**Interfaces:**
- Consumes: Tasks 4–6; `Env.NewLocal`, `Env.LoggerReady` (M1).
- Produces:
  - `func agent.PruneSiblings(exe, home string, log *slog.Logger)` per R-M2-5; errors logged at DEBUG.
  - `app.Main` `serve`: `signal.Ignore(syscall.SIGPIPE)`; builds `agent.Config{NewEndpoint: func(sudo, log) { env.LoggerReady(log); return env.NewLocal(sudo) }, Home: os.Getenv("HOME"), Exe: os.Executable(), ExeSHA256: buildinfo.ExeSHA256(), Prune: agent.PruneSiblings}` and runs `agent.Serve(ctx, stdioConn{env.Stdin, env.Stdout}, cfg)`; returns 0 on nil, else 2, printing `CRITICAL ERROR: serve: <err>` to stderr only when `errors.Is(err, agent.ErrSetup)` (Task 7 adds `var agent.ErrSetup`, which `Serve` wraps around every failure before the control stream is accepted). Nothing is ever written to stdout except by the mux.
  - `main.go`: `signal.NotifyContext` also listens for `SIGHUP`.

- [ ] **Step 1: Write failing tests:**
  - `TestPruneSiblings`: under `home/.cache/syngoid/` dirs `1.0.0` (oldest), `1.1.0`, `1.2.0` (current, holds exe), `dev-0123abcd` (newest mtime other than current), `bin`, `notes` → after pruning: `1.2.0`, `dev-0123abcd`, `bin`, `notes` remain.
  - `TestPruneSiblingsOutsideDefaultBase`: exe under `/opt/x/1.2.0/` → nothing removed.
  - `TestMainServeHandshake`: `Main(ctx, ["serve"], env)` with pipes as stdin/stdout and fake `NewLocal`; `rpc.Handshake` over the pipes succeeds; after `Remote.Close()`, `Main` returns 0 and stderr is empty.
  - `TestServeDisconnectKillsChildren` (`cmd/syngoid`, re-executes the test binary as `main()` when `SYNGOID_RUN_MAIN=1`): `PATH` starts with a temp dir holding a `zfs` shim (`case $1 in send) echo $$ > "$SHIM_PIDFILE"; exec sleep 300;; version) echo zfs-2.3.4-1;; *) exit 0;; esac`); run `serve` with pipes, handshake, start `Remote.Send` of `tank/a@s1` into `io.Discard`, wait for the pid file, then close the agent's stdin and its stdout read end. Within 15 s the shim pid no longer exists (`syscall.Kill(pid, 0)` → `ESRCH`) and the serve process has exited.
  - `TestServeStderrSilent`: the same setup with `debug:true` and a full `list_tree` call → the serve process's stderr is empty.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** PASS.
- [ ] **Step 5:** Spec §5.5 pruning rule (R-M2-5); §7.4: the agent ignores SIGPIPE and treats SIGHUP like stdin EOF.
- [ ] **Step 6: Commit** `feat(agent): serve subcommand, child cleanup on disconnect, version dir pruning`.

---

### Task 8: `internal/sshx` — ssh command building and spawning

**Files:**
- Create: `internal/sshx/sshx.go`
- Test: `internal/sshx/sshx_test.go`

**Interfaces:**
- Produces:
  - `type sshx.Config struct { SSH string /* default "ssh" */; Options []string; ConfigFile string; StdinIsTTY bool; Log *slog.Logger }`.
  - `func sshx.NewControlDir() (string, error)` — `os.MkdirTemp("", "syngoid-ssh-")` (mode 0700).
  - `func sshx.NewMaster(cfg Config, user, host, controlDir string, n int) (*Master, error)` — rejects an empty host, a host or user starting with `-`, containing whitespace, control characters or `@` (user) — error `invalid ssh destination %q`.
  - `(m *Master) Label() string` — `user@host` or `host`.
  - Argv (exact order; ours first because ssh keeps the first value of each option): `ssh [-F ConfigFile] -o ControlPath=<dir>/cm-<n> <control> [-o opt]… [-o BatchMode=yes] -T -- <dest> <remote command>`; `<control>` is `-o ControlMaster=yes -o ControlPersist=60` for the master and `-o ControlMaster=no` for clients. `BatchMode=yes` is added when `!StdinIsTTY` unless an option's key equals `batchmode` case-insensitively. The master's remote command is `exit`.
  - `(m *Master) Start(ctx) error` — runs the master command in the **foreground** process group with the process's stdin, so ssh can prompt; returns `*sshx.Error` on failure.
  - `(m *Master) Run(ctx, cmd string, stdin io.Reader) ([]byte, error)` — client command in its own process group (`Setpgid`), stdout returned, stderr captured (last 64 KiB) into `*sshx.Error{Label string; Argv []string; ExitCode int; Stderr string}` on failure.
  - `(m *Master) Spawn(ctx, cmd string) (*Proc, error)` — client command in its own process group, not tied to ctx after start; `*Proc` is an `io.ReadWriteCloser` (reads stdout, writes stdin), `Stderr() string` (last 64 KiB so far), `Close()` (closes stdin, waits ≤ 5 s, then kills the group).
  - `(m *Master) Close() error` — `ssh -o ControlPath=… -O exit -- <dest>`; errors ignored when the master is already gone.
  - `func sshx.ShellQuote(s string) string` (single quotes, `'` → `'\''`); `func sshx.ShellJoin(args ...string) string`.
  - Every invocation is logged at DEBUG with `logx.KeyHost` = label.

- [ ] **Step 1: Write failing tests:**
  - `TestMasterArgv`: options `[Port=2222]`, config `/c`, TTY → master argv `ssh -F /c -o ControlPath=/d/cm-0 -o ControlMaster=yes -o ControlPersist=60 -o Port=2222 -T -- backup@nas exit`; client `Run` argv for `echo hi` is the same with `-o ControlMaster=no` and `echo hi` last.
  - `TestBatchMode`: no TTY → `-o BatchMode=yes` before `-T`; with user option `batchmode=no` → not added.
  - `TestNewMasterRejectsOptionLikeHost`: hosts `-oProxyCommand=x`, `a b`, `""`, user `-l` → error.
  - `TestShellQuoteRoundTrip`: for `plain`, `with space`, `it's`, `$HOME`, `a"b`, `;rm -rf /`, `~/x` → `sh -c 'printf %s '"$(ShellQuote(s))"` prints `s` exactly.
  - `TestClientCommandsOwnProcessGroup`: `SSH` points at a shim script `#!/bin/sh` / `ps -o pgid= -p $$`; `Run` prints a pgid different from the test process's; `Start` (master) runs in the test's own group (shim writes its pgid to a file).
  - `TestRunErrorCapturesStderr`: shim exiting 255 with `Permission denied (publickey).` on stderr → `*sshx.Error` with that stderr and exit code 255.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** PASS.
- [ ] **Step 5: Commit** `feat(sshx): ControlMaster ssh commands with safe quoting`.

---

### Task 9: Bootstrap — probe, resolve, push, handshake, session pool

**Files:**
- Create: `internal/bootstrap/platform.go`, `probe.go`, `resolve.go`, `connect.go`, `pool.go`
- Modify: spec §5.2–§5.5 (R-M2-3, R-M2-4, R-M2-7)
- Test: `internal/bootstrap/*_test.go` (with a `TestMain` that turns the test binary into an agent when `SYNGOID_TEST_AGENT=1`: `agent.Serve` over stdio backed by a `fake` World)

**Interfaces:**
- Consumes: Tasks 1, 5, 7, 8.
- Produces:
  - `type bootstrap.Platform struct { OS, Arch string }` (`String()` = `os/arch`); `func bootstrap.ParsePlatform(unameS, unameM, isainfo string) (Platform, error)`; `func bootstrap.Self() Platform`. Mapping per spec §5.2; result must be one of the §11.1 targets, else `unsupported remote platform <os>/<arch> (uname -s=<s>, uname -m=<m>); supported: linux/amd64, linux/arm64, linux/arm, linux/riscv64, linux/ppc64le, freebsd/amd64, freebsd/arm64, illumos/amd64, darwin/amd64, darwin/arm64`.
  - `func bootstrap.AgentDirName() string` (`buildinfo.Version`, plus `-` + `BuildHash()[:8]` for dev builds); `func bootstrap.DefaultRemoteBinDir() string` = `~/.cache/syngoid/` + `AgentDirName()`.
  - `func bootstrap.ProbeCommand(dir string) string` = `ShellJoin("sh", "-c", probeScript, "syngoid-probe", dir)` with:
    ```sh
    D=$1; case $D in "~/"*) D=$HOME/${D#"~/"};; esac
    uname -s; uname -m
    if [ "$(uname -s)" = SunOS ]; then isainfo -k; else echo -; fi
    printf '%s\n' "$HOME" "$D"
    if [ -x "$D/syngoid" ]; then echo yes; else echo no; fi
    ```
  - `type bootstrap.ProbeResult struct { Platform; Home, Dir string; Executable bool }`; `func bootstrap.ParseProbe(out string) (ProbeResult, error)` (exactly 6 lines; `Dir` must be absolute).
  - `func bootstrap.PushCommand(dir string) string` = `ShellJoin("sh", "-c", `umask 077; mkdir -p "$1" && cat > "$1/.syngoid.tmp.$$" && chmod 700 "$1/.syngoid.tmp.$$" && mv -f "$1/.syngoid.tmp.$$" "$1/syngoid"`, "syngoid-push", dir)`; `func bootstrap.ServeCommand(dir string) string` = `ShellJoin(dir+"/syngoid", "serve")`.
  - `type bootstrap.Resolver struct { BinStores []string; PackagedStore string /* "" → /usr/lib/syngoid/bin */; Executable func() (string, error) /* nil → os.Executable */ }`; `(r Resolver) Resolve(p Platform) (string, error)` — self when `p == Self()`, else the first regular file `DIR/<os>-<arch>/syngoid` over `BinStores` then `PackagedStore`; failure: `no syngoid agent binary for <os>/<arch>: put one at DIR/<os>-<arch>/syngoid and pass --bin-store=DIR, or install /usr/lib/syngoid/bin/<os>-<arch>/syngoid`.
  - `type bootstrap.Transport interface { Label() string; Run(ctx context.Context, cmd string, stdin io.Reader) ([]byte, error); Spawn(ctx context.Context, cmd string) (Proc, error) }`; `type bootstrap.Proc interface { io.ReadWriteCloser; Stderr() string }` (`*sshx.Master`/`*sshx.Proc` satisfy them).
  - `type bootstrap.Options struct { RemoteBinDir string; Resolver Resolver; Sudo, Debug bool; Log *slog.Logger; HandshakeTimeout time.Duration /* 0 → 30 s */ }`.
  - `func bootstrap.CheckHello(got rpc.HelloReply, pushedSHA string) error` — wraps `bootstrap.ErrMismatch` when protocol or version differ, when this is a dev build and build hashes differ, or when `pushedSHA != ""` and `ExeSHA256` differs.
  - `func bootstrap.Connect(ctx context.Context, t Transport, o Options) (*rpc.Remote, error)`: probe → platform → if not executable: resolve + push (INFO `pushing syngoid agent to <label>:<dir>/syngoid`). Then up to two attempts: spawn serve, handshake (timeout), `CheckHello`; on any handshake failure or mismatch in the first attempt, close, resolve + push, retry. Final failure: `<label>: agent handshake failed: <err>` followed by `\nremote stderr:\n<stderr>` when the proc's stderr is non-empty.
  - `type bootstrap.MasterTransport interface { Transport; Start(ctx context.Context) error; Close() error }`; `type bootstrap.Pool struct { NewTransport func(user, host string, n int) (MasterTransport, error); Opts Options }`; `func bootstrap.NewSSHPool(cfg sshx.Config, o Options) (*Pool, error)` (creates the control dir; `NewTransport` builds `sshx.NewMaster`); `(p *Pool) Endpoint(ctx, user, host string, sudo bool) (*rpc.Remote, error)` (one started master per `user@host`, one `Connect` per (`user@host`, sudo), R-M2-3); `(p *Pool) Close() error` (closes remotes, then masters, then removes the control dir).

- [ ] **Step 1: Write failing tests** (`shTransport` in the test file runs `sh -c cmd` locally with `HOME` set to a temp dir and `SYNGOID_TEST_AGENT=1` in `Spawn`'s env, counting `Run` calls whose command starts with the push script):
  - `TestParsePlatform` table: (`Linux`,`x86_64`) → linux/amd64; (`Linux`,`aarch64`) → linux/arm64; (`Linux`,`armv7l`) → linux/arm; (`Linux`,`riscv64`), (`Linux`,`ppc64le`); (`FreeBSD`,`amd64`), (`FreeBSD`,`arm64`); (`SunOS`,`i86pc`,`amd64`) → illumos/amd64; (`SunOS`,`i86pc`,`i386`) → error; (`Darwin`,`arm64`), (`Darwin`,`x86_64`); (`Linux`,`armv6l`) and (`OpenBSD`,`amd64`) → error listing the supported platforms.
  - `TestProbeScriptExpandsTilde`: running `ProbeCommand("~/.cache/syngoid/x")` via sh → `Dir == $HOME/.cache/syngoid/x`, `Executable == false`; after creating an executable `syngoid` there → `true`.
  - `TestPushCommandAtomic`: pushing 1 MiB to a fresh dir → file content equal, file mode 0700, dir mode 0700, no `.syngoid.tmp.*` left.
  - `TestPushCommandOddDir`: dir `$HOME/it's a dir` works for probe, push and serve commands.
  - `TestResolveOrder`: self for `Self()`; store hit; packaged store hit; precedence store > packaged; the exact failure message.
  - `TestAgentDirName`: release build `1.2.3` → `1.2.3`; dev → `dev-` + 8 hex.
  - `TestCheckHello` table: equal → nil; protocol 2, version `1.0.1`, dev build hash differs, pushed SHA differs → `errors.Is(err, ErrMismatch)`.
  - `TestConnectPushesWhenMissing`: empty HOME → `Connect` succeeds, exactly 1 push, `$HOME/.cache/syngoid/dev-<hash8>/syngoid` has the test binary's SHA-256; `Info` over the session works.
  - `TestConnectReusesExisting`: a second `Connect` → 0 pushes.
  - `TestConnectRepushOnMismatch`: pre-placed executable `syngoid` script that prints garbage and exits → 1 push, success.
  - `TestConnectFailsAfterSecondMismatch`: `Resolver.Executable` returns a script `echo boom >&2; exit 1` → error contains `agent handshake failed` and `boom`; 2 pushes.
  - `TestPoolReuse`: fake `MasterTransport`s counting `Start`; `Endpoint(u,h,false)` twice → same `*rpc.Remote`, 1 master; `Endpoint(u,h,true)` → a second remote, still 1 master; `Close` closes both remotes and the master, and removes the control dir.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** PASS.
- [ ] **Step 5:** Spec §5.2–§5.5 per R-M2-3, R-M2-4, R-M2-7.
- [ ] **Step 6: Commit** `feat(bootstrap): probe, push and handshake remote agents`.

---

### Task 10: Remote sync, permissions and dry-run in the app

**Files:**
- Create: `internal/app/remote.go`
- Modify: `internal/app/app.go`, `cmd/syngoid/main.go`, `internal/engine/preflight.go`, `README.md` (status: milestone 2; remote syntax, delegation on remotes, `--sudo` semantics, what is pushed where)
- Test: `internal/app/remote_test.go`, `internal/engine/preflight_test.go`

**Interfaces:**
- Consumes: Tasks 5, 9; M1 `app.Env`, `engine.Preflight`.
- Produces:
  - `type app.Connector interface { Endpoint(ctx context.Context, loc cli.Location, sudo bool) (endpoint.Endpoint, error); Close() error }`.
  - `Env.NewConnector func(o *cli.Options, log *slog.Logger) (Connector, error)`; `main.go` sets it to a `bootstrap.NewSSHPool` with `sshx.Config{Options: o.SSHOptions, ConfigFile: o.SSHConfig, StdinIsTTY: env.StdinIsTTY, Log: log}` and `bootstrap.Options{RemoteBinDir: o.RemoteBinDir, Resolver: {BinStores: o.BinStores}, Debug: o.Debug, Log: log}`.
  - `runSync`: per side, local → `env.NewLocal(sudo)`, remote → `connector.Endpoint(ctx, loc, sudo)`; the connector is created only when a side is remote and closed (deferred) after the summary. Any connect error → `CRITICAL ERROR: <err>`, exit 2; a remote side with `Env.NewConnector == nil` → `CRITICAL ERROR: remote endpoints are not configured`, exit 2. The M1 `remote endpoints are not supported yet` error is removed.
  - Codec placement (§9.2): `codec := stream.None; if o.Source.IsRemote() || o.Target.IsRemote() { codec = o.Compress }`; `Executor.SendCodec = Executor.RecvCodec = codec`.
  - `PreflightReport.Render`: for a remote side the dataset line reads `  <side> <label>:<dataset>: …` and grant/sudo lines are tagged `[<host>]` (label without `user@`); local sides keep M1's `<side> <dataset>` and `[<side>]`.

- [ ] **Step 1: Write failing tests** (`Env.NewConnector` returns a test connector that serves each `[user@]host` from its own `fake.World` through `agenttest.Start` and records every `Endpoint` call and the codecs seen by `Send`/`Receive`):
  - `TestPreflightRenderRemoteLabels`: source `backup@db1` missing `snapshot, destroy` on `tank/data`, target `backup@nas` missing `receive` on `tank/backups` → `Render(false)` equals (the spec §6.4 layout; companions as in M1):
    ```
    CRITICAL ERROR: missing ZFS permissions
      source backup@db1:tank/data: snapshot, destroy
      target backup@nas:tank/backups: receive
    Grant them (as root) with:
      [db1]  zfs allow -u backup snapshot,destroy,mount tank/data
      [nas]  zfs allow -u backup receive,create,mount tank/backups
    Alternatively, run with --sudo (requires a sudoers rule for zfs).
    ```
  - `TestMainRemoteTargetEndToEnd`: `sync -r tank/data backup@nas:pool/data` → exit 0; `nas` World populated; summary printed.
  - `TestMainRemoteSourceEndToEnd`: `db1:tank/data pool/data` → exit 0.
  - `TestMainRemoteToRemoteSameHost`: `nas:tank/a nas:pool/a` → one `Endpoint` call; `--target-sudo` → two calls (`sudo=false`, `sudo=true`).
  - `TestMainCodecPlacement`: local→remote and remote→remote with `--compress=lz4` → every `Send` and `Receive` saw `lz4`; local→local (no connector created) → `none`.
  - `TestMainRemoteDryRun`: `--dry-run` to a remote target → plan printed, no mutating calls on either World.
  - `TestMainRemotePermissions`: `permissions tank/data backup@nas:pool/data` → output contains `[nas]  zfs allow -u backup`; exit 2 with missing grants, 0 when complete.
  - `TestMainRemoteConnectFailureExit2`: connector error `nas: agent handshake failed: boom` → exit 2, stderr `CRITICAL ERROR: nas: agent handshake failed: boom`.
  - `TestMainRemoteCancelledExit2`: blocked remote send, ctx cancelled → exit 2, summary printed, connector closed.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** PASS. Then `go build ./cmd/syngoid && ./syngoid --dry-run tank/a nobody@nonexistent.invalid:tank/b` → exit 2 with a `CRITICAL ERROR:` naming the ssh failure, no panic.
- [ ] **Step 5: Commit** `feat(app): replicate to and from remote hosts`.

---

### Task 11: M1 carry-overs (force-delete ordering, bookmark-path release)

**Files:**
- Modify: `internal/engine/dag.go`, `internal/engine/exec.go` (caller), `internal/engine/plan.go`
- Test: `internal/engine/dag_test.go`, `internal/engine/plan_test.go`, `internal/engine/exec_test.go`

**Interfaces:**
- Produces: `func engine.Order(states []DatasetState, forceDelete bool) (order []int, deps [][]int)` — with `forceDelete`, every state also depends on its in-scope parent even when the parent's target exists (a parent's `ForceDestroyTarget` is recursive). The executor passes `e.Opts.ForceDelete`.
- Planner: on the bookmark-fallback path with `UseHold`, the source-side `Release` of the matched snapshot is omitted when no source snapshot has that name (it exists only as a bookmark there); the target-side `Release` stays.

- [ ] **Step 1: Write failing tests:**
  - `TestOrderForceDeleteChildrenFollowParent`: states `tank/a` (clone of `tank/z@s`, target exists), `tank/a/c` (target exists), `tank/z` → without force order `[1 2 0]`; with force `[2 0 1]` and `deps[1] == [0]`.
  - `TestExecutorForceDeleteParentKeepsChildren`: the scenario above where the parent plans `ForceDestroyTarget` → after `Run` the child's target exists with its snapshots and all results are `ok`.
  - `TestPlan` new row "holds bookmark fallback": `UseHold`, bookmark fallback state → actions contain `Release{Target, x}` and no `Release{Source, x}`.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** PASS (whole `./internal/engine/` suite).
- [ ] **Step 5: Commit** `fix(engine): order children after force-deleted parents, skip bookmark-only source release`.

---

### Task 12: `ssh localhost` integration suite

**Files:**
- Create: `test/integration/remote_test.go`
- Modify: `scripts/ci/setup-zfs.sh`, `test/integration/helpers_test.go` (remote helpers)

**Interfaces:**
- Consumes: the built binary (Task 10), M1 helpers.
- Setup additions (`setup-zfs.sh`): install and start `openssh-server`; `ssh-keygen -t ed25519 -N '' -f /root/.ssh/id_ed25519` if absent; append the public key to `~syngoid-it/.ssh/authorized_keys` (dir 0700, file 0600, owned by `syngoid-it`) and to `/root/.ssh/authorized_keys`; `ssh-keyscan -H localhost >> /root/.ssh/known_hosts`; `/etc/sudoers.d/syngoid-it` with `syngoid-it ALL=(root) NOPASSWD: <zfs path>` (mode 0440, `visudo -cf` check).
- Helper: `remoteLoc(user, ds string) string` → `user@localhost:ds`; `agentPath(user string) string` → `<home>/.cache/syngoid/dev-<hash8>/syngoid` where hash8 is the first 8 hex chars of the built binary's SHA-256.

- [ ] **Step 1: Write tests** (`//go:build integration`, same gating as M1):
  - `TestITRemoteTargetDelegated`: `syngoid syngoid-src/a syngoid-it@localhost:syngoid-dst/a` without grants → exit 2 and a `[localhost]  zfs allow -u syngoid-it …` line; run every printed grant line as root; rerun → exit 0; `agentPath("syngoid-it")` exists with mode 0700 and the built binary's SHA-256; target `mounted` is `no`.
  - `TestITRemoteSource`: `syngoid-it@localhost:syngoid-src/a syngoid-dst/a` with source grants → exit 0, incremental on the second run.
  - `TestITRemoteToRemote`: both sides `syngoid-it@localhost:` → exit 0.
  - `TestITRemoteSudo`: `--target-sudo`, no target grants → exit 0.
  - `TestITRemoteRepushOnCorruption`: truncate the agent binary to 1 KiB → next run exit 0 and the binary's SHA-256 is restored.
  - `TestITRemoteKillInitiatorThenResume`: 300 MiB dataset; SIGKILL the initiator once the target shows a `receive_resume_token` or 2 s of transfer have passed; within 15 s no process matches `zfs receive.*syngoid-dst/a` (`pgrep -f`), the token is set; rerun → exit 0, snapshot present, token cleared.
  - `TestITRemoteCtrlC`: SIGINT to the initiator mid-send → exit 2, stdout contains `Summary:`, no `zfs receive` for the dataset remains after 15 s.
  - `TestITRemoteDatasetWithSpace`: `syngoid-src/my data` → `syngoid-it@localhost:syngoid-dst/my data` → exit 0.
  - `TestITRemoteCodecs`: `--compress` each of `zstd-fast zstd lz4 gzip none` → exit 0 and target snapshot GUIDs equal the source's.
  - `TestITRemoteDebugHostPrefix`: `--debug` output contains `DEBUG: [syngoid-it@localhost] zfs receive`.
- [ ] **Step 2:** `go vet -tags integration ./test/integration/` — clean. These cannot run on macOS (M1 ruling R6).
- [ ] **Step 3:** Commit `test(integration): remote replication over ssh localhost`.
- [ ] **Step 4:** Ask the user to push `m2-remote` (or open the PR); then read the `integration` job's result with `gh run view --log` and fix failures in follow-up commits. Report the suite as unverified until a CI run passes.

---

### Task 13: Mux throughput (spec §14 item 2)

**Files:**
- Create: `internal/rpc/mux_bench_test.go`, `test/integration/throughput_test.go`
- Modify: spec §14 item 2, `internal/rpc/mux.go` (`MaxStreamWindow`, only if the numbers say so)

- [ ] **Step 1: Write** `BenchmarkMuxThroughput/window={4,16,64}MiB`: client → server over two `os.Pipe` pairs, 256 MiB through one stream with `ChunkWriter`; reports MB/s (`b.SetBytes`). It builds sessions from an unexported config helper taking the window, so `MaxStreamWindow` stays the production value.
- [ ] **Step 2: Write** `TestITRemoteThroughput` (integration): 1 GiB random-data dataset; time `syngoid --compress=none syngoid-src/t syngoid-it@localhost:syngoid-dst/t` against the baseline `zfs send syngoid-src/t@s | ssh syngoid-it@localhost sudo -n zfs receive syngoid-dst/b` (sudoers from Task 12); `t.Log` both rates; fail only if syngoid is below 50 % of the baseline.
- [ ] **Step 3:** Run the benchmark locally (`go test -run '^$' -bench MuxThroughput -benchtime 3x ./internal/rpc/`) and, after CI ran, read the integration log. Record both sets of numbers in spec §14 item 2, noting that CI loopback is not a 10 GbE link; change `MaxStreamWindow` only if 64 MiB beats 16 MiB by more than 20 % on both.
- [ ] **Step 4: Commit** `test(rpc): mux throughput benchmark, record §14.2 results`.

---

### Task 14: Captured fixtures on Linux, FreeBSD, illumos (spec §14 items 3–4)

**Files:**
- Create: `scripts/ci/capture-fixtures.sh`, `.github/workflows/fixtures.yml`, `internal/zfs/testdata/{linux,freebsd,illumos}/*`, `internal/perms/testdata/{linux,freebsd,illumos}/*`, `internal/endpoint/testdata/{linux,freebsd,illumos}/ps.txt`
- Modify: `internal/endpoint/local.go` (`ReceiveBusy` fallback), `internal/zfs/query_test.go`, `internal/zfs/errors_test.go`, `internal/perms/allow_test.go`, `internal/endpoint/local_test.go`, spec §14 items 3–4; remove the hand-written M1 fixtures once captured ones cover them

**Interfaces:**
- `capture-fixtures.sh OUTDIR` (POSIX sh, as root): creates pool `syngoidfx` on a 256 MiB file vdev; builds `fx/a` (snapshots `s1 s2 s3`, two with the same second), `fx/a/c` clone of `fx/a@s1`, a volume, bookmark `fx/a#b1`, a `syncoid:sync=false` dataset, a dataset named `fx/my data`; grants: user, group, `everyone`, an `@set`, local, descendent, local+descendent, create-time. Writes the stdout of exactly the M1 argv (`zfs list -Hp -o name,type,origin,used,receive_resume_token,syncoid:sync -t filesystem,volume -r syngoidfx`, the two `zfs get` listings, `zfs allow` on `fx/a` and `fx/a/c`, `zfs send -nvP` full / `-i` / `-I` / `-t`), the stderr of: receive into partially-complete state, destination already exists, dataset does not exist, resume after the source snapshot is destroyed; and `ps -Ao args=` and `ps -eo args` while a `sleep` named `zfs receive -s fx/x` (via `exec -a` where available, else a copied `sleep` binary) runs. Destroys the pool at the end, also on failure (`trap`).
- `fixtures.yml` (`workflow_dispatch`): jobs `linux` (ubuntu-latest, `zfsutils-linux`), `freebsd` (`vmactions/freebsd-vm@v1`), `omnios` (`vmactions/omnios-vm@v1`), each runs the script and uploads `OUTDIR` as artifact `fixtures-<os>`.
- `Local.ReceiveBusy`: when `ps -Ao args=` fails, run `ps -eo args` and drop its first (header) line.

- [ ] **Step 1:** Write the script and workflow; `sh -n scripts/ci/capture-fixtures.sh` and `shellcheck` (if installed) clean.
- [ ] **Step 2: Write failing test** `TestLocalReceiveBusyPsFallback`: FakeExec fails `ps -Ao args=` with exit 1 and answers `ps -eo args` with `ARGS\nzfs receive -s tank/b\n` → `{tank/b: true}`.
- [ ] **Step 3:** FAIL; implement the fallback; PASS.
- [ ] **Step 4: Commit** `ci: capture zfs, allow and ps fixtures on Linux, FreeBSD and OmniOS` (script, workflow, fallback).
- [ ] **Step 5:** Ask the user for the go-ahead to push and run `gh workflow run fixtures.yml --ref m2-remote`; then `gh run download <id> -D <scratch>` and copy the artifacts into the testdata dirs above.
- [ ] **Step 6: Write tests over the captured files:** `TestParseCapturedTree/<os>`, `TestParseCapturedSnapshots/<os>` (tie-break counter on the same-second pair), `TestParseCapturedBookmarks/<os>`, `TestClassifyCaptured/<os>` (each stderr file maps to its expected sentinel), `TestEffectiveCaptured/<os>` (expected effective set per grant case), `TestParseEstimateCaptured/<os>`, `TestReceiveBusyCaptured/<os>` (`ps` output marks `fx/x` busy). Fix any parser difference the fixtures reveal, each with its own test row.
- [ ] **Step 7:** PASS. Record the observed platform differences (or their absence) in spec §14 items 3–4; delete the hand-written fixtures whose cases are now covered.
- [ ] **Step 8: Commit** `test: parsers against captured Linux, FreeBSD and illumos output`.
