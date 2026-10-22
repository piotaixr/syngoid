# syngoid — Design Spec

- **Date:** 2026-10-18
- **Status:** Approved in brainstorming, pending written-spec review
- **Repository:** https://github.com/piotaixr/syngoid
- **Go module:** `github.com/piotaixr/syngoid`
- **License:** GPLv3 (derivative of syncoid from the sanoid project)

## 1. Overview

syngoid is a Go rewrite of `syncoid` (sanoid project, Perl, v2.2.0), the ZFS replication tool. It replicates
ZFS datasets between local and/or remote hosts using `zfs send`/`zfs receive`.

### 1.1 Goals

1. **Easy distribution:** a single static binary per platform, no runtime dependencies beyond `zfs`, `ssh`, and a
   POSIX `sh` on remotes.
2. **Minimal dependencies on remote hosts:** no `mbuffer`, `pv`, `lzop`/`zstd`/`pigz`/`lz4`/`xz`, `socat`, or Perl.
   The binary pushes itself to remotes and runs there as an agent.
3. **Compatible core:** reads and writes the same snapshot, hold, and bookmark names and honours the same
   `syncoid:sync` property as syncoid, so syngoid can take over (and hand back) existing replications.
4. **Least privilege:** ZFS permission delegation (`zfs allow`) is the default; `sudo` is opt-in.
5. **Faster for many small datasets:** batched ZFS queries and snapshot operations over one persistent connection
   per host.
6. **Inspectable:** a pure planner enables `--dry-run` and interactive `--confirm`.

### 1.2 Non-goals (v1)

- CLI flag compatibility (a cleaned-up CLI with deprecated aliases is provided; see §3).
- Parallel transfers (the executor is DAG-shaped so a worker pool can be added later).
- Bandwidth limiting.
- Insecure direct (unencrypted TCP) connections.
- Restricted-shell targets that can run only `zfs` (the agent binary must be executable on every remote).
- Traces and metrics export (logs only via OTLP).
- Self-update, Homebrew tap, FreeBSD port.

## 2. Decision log

| # | Decision | Rationale |
|---|---|---|
| D1 | Language: Go | Trivial static cross-compilation to linux/freebsd/illumos/darwin with `CGO_ENABLED=0`; pure-Go compression libs; process/pipe orchestration fits goroutines + `io.Copy`. |
| D2 | Compatible core, cleaned-up CLI | Take over existing replications without keeping every legacy flag. |
| D3 | Same binary on both ends (`syngoid serve` over SSH), pushed automatically if missing | Removes all remote tool dependencies. Hosts able to run `zfs` but unable to install software rarely have `mbuffer`/`pv` either. |
| D4 | Pushed binary never runs under sudo; it invokes `sudo -n zfs` itself when `--sudo` is set | The binary lives in a user-writable directory; running it as root would be a privilege escalation path. Keeps existing sudoers rules (`NOPASSWD: /sbin/zfs`) valid. |
| D5 | Delegation (`zfs allow`) first, `--sudo` opt-in, mandatory preflight | Least privilege; preflight prints exact `zfs allow` commands on failure. |
| D6 | Drop `--insecure-direct-connection` | Not needed. |
| D7 | Architecture A: thin agent, one multiplexed SSH session per host | Logic centralized and testable; one SSH auth per host; mux allows control + data + events on one connection. |
| D8 | Pure planner + executor | Enables `--dry-run`, `--confirm`, table-driven tests. |
| D9 | No parallel transfers in v1 | Deferred; batching gives most of the speedup. |
| D10 | No bandwidth limit in v1 | Possibly never needed. Passing the old flags is an error, not silently ignored. |
| D11 | OTLP/HTTP log export from the initiator | Observability without remote network requirements; HTTP avoids the gRPC dependency weight. |
| D12 | minisign-signed `SHA256SUMS`, key embedded, CI secret with approval-gated releases | mise-style verification; simple pure-Go verification. |
| D13 | Fix five known syncoid bugs rather than replicate them | See §8.9. |

## 3. Command-line interface

```
syngoid [sync] [OPTIONS] SOURCE TARGET
syngoid permissions [OPTIONS] SOURCE TARGET
syngoid fetch [--platform OS/ARCH ...] [--all]
syngoid version
syngoid serve            (internal; started by the initiator over SSH)
```

`sync` is the default subcommand when the first argument is not a known subcommand.

### 3.1 SOURCE / TARGET syntax

Unchanged from syncoid:

- `pool/dataset` — local.
- `[user@]host:pool/dataset` — remote via SSH.
- Ambiguity rule (kept): an argument of the form `name:rest` with no `@` and no `/` before the `:` is local if a local
  pool named `name` exists, otherwise remote.

### 3.2 Options kept with the same meaning

`-r/--recursive`, `--skip-parent`, `--identifier=EXTRA` (validated `^[a-zA-Z0-9-_:.]+$`), `--no-stream`,
`--no-sync-snap`, `--keep-sync-snap`, `--create-bookmark` (only with `--no-sync-snap`), `--use-hold`,
`--preserve-recordsize`, `--preserve-properties`, `--no-rollback`, `--delete-target-snapshots`, `--force-delete`,
`--exclude-datasets=REGEX` (repeatable), `--include-snaps=REGEX` (repeatable), `--exclude-snaps=REGEX` (repeatable),
`--no-resume`, `--no-clone-handling`, `--dumpsnaps`, `--debug`, `--quiet`, `--sendoptions=OPTS`, `--recvoptions=OPTS`.

`--sendoptions`/`--recvoptions` keep syncoid's mini-syntax: a space-separated string of single-letter flags where
`o` and `x` consume the next word as their value (e.g. `"ux recordsize o compression=lz4"` →
`-u -x recordsize -o compression=lz4`). Per-send-type allow-lists are kept:

| Send type | Allowed send flags |
|---|---|
| full / incremental | `L P V R X b c e h p s v w` |
| from bookmark | `L V c e w` |
| resume (`-t`) | `P V e v` |
| size estimate | full/incr: `L V R X b c e h p s w`; resume: `V e` |

Allowed receive flags: `h o x u v`. `R` in `--sendoptions` together with `--recursive` is a usage error.

### 3.3 Options re-implemented in-process

| Option | Behaviour |
|---|---|
| `--compress=zstd-fast\|zstd\|lz4\|gzip\|none` | Default `zstd-fast`. Applied only on network hops (§9.2). Legacy values map with a deprecation warning: `zstd-fast`,`zstdmt-fast` → `zstd-fast`; `zstd-slow`,`zstdmt-slow`,`xz` → `zstd`; `gzip`,`pigz-fast`,`pigz-slow` → `gzip`; `lzo` → `lz4`. Level mapping (klauspost/compress): `zstd-fast` = `SpeedDefault`, `zstd` = `SpeedBetterCompression`, `gzip` = level 3. |
| `--buffer-size=SIZE` | In-process ring buffer at the initiator relay. Default `16M`. Legacy alias `--mbuffer-size`. |
| `--progress=auto\|bar\|plain\|none` | Default `auto` (`bar` on a TTY, else `plain`). Replaces `--pv-options` (which becomes an error with a hint). |

### 3.4 SSH options

- `-o/--ssh-option=OPTION` (repeatable) and `--ssh-config=FILE`.
- Deprecated aliases (warning, translated): `--sshoption` → `-o`, `--sshconfig` → `--ssh-config`,
  `--sshkey=F` → `-o IdentityFile=F`, `--sshport=P` → `-o Port=P`, `--sshcipher|-c=C` → `-o Ciphers=C`.

### 3.5 New options

| Option | Meaning |
|---|---|
| `--sudo`, `--source-sudo`, `--target-sudo` | Prefix `zfs` invocations with `sudo -n` on both / one side. |
| `-n/--dry-run` | Gather, preflight, plan; print the plan; exit without changes. |
| `--confirm` | Print the plan and ask y/N before executing; also prompts for destructive runtime remediations. Requires a TTY (error otherwise). |
| `--no-download` | Never download agent binaries; use self or local store only. |
| `--download-url=TEMPLATE` | Mirror URL template with `{version}`, `{os}`, `{arch}`, `{file}` placeholders. Default: `https://github.com/piotaixr/syngoid/releases/download/v{version}/{file}`. |
| `--bin-store=DIR` | Additional local directory searched for agent binaries (`DIR/<os>-<arch>/syngoid`). |
| `--remote-bin-dir=DIR` | Remote directory for the agent binary (default `~/.cache/syngoid/<ver>`). |
| `--otel-endpoint=URL` | Enable OTLP/HTTP log export to URL (also enabled by standard `OTEL_EXPORTER_OTLP_*` env vars). |

### 3.6 Removed options

| Option | Behaviour when passed |
|---|---|
| `--source-bwlimit`, `--target-bwlimit` | Error: "bandwidth limiting is not supported in syngoid". |
| `--pv-options` | Error with hint to use `--progress`. |
| `--insecure-direct-connection` | Error: not supported. |
| `--no-privilege-elevation` | Warning, ignored (delegation is the default). |
| `--no-command-checks`, `--monitor-version` | Warning, ignored. |
| `--exclude` | Error with hint to use `--exclude-datasets`. |

### 3.7 Subcommands

- **`permissions SOURCE TARGET [OPTIONS]`** — connects to both sides (same bootstrap as `sync`), computes the
  required permission set from OPTIONS (§6.2), checks it, and prints both the full minimal grant commands and the
  missing grants for each side. Exit 0 if everything is granted, 2 otherwise.
- **`fetch [--platform OS/ARCH ...] [--all]`** — downloads and verifies agent binaries for this exact version into
  the local cache (§11.5). For pre-seeding before air-gapped use.
- **`version`** — prints `syngoid <version> (commit <commit>[, dirty], build <hash12>) protocol <n>`. The build hash
  is the ldflags-stamped `buildinfo.Hash` when set, otherwise the hex SHA-256 of the running executable; `hash12`
  is its first 12 characters. An unstamped dev build therefore only matches an agent that is the same file (which
  is what self-push produces); cross-platform dev agents from `--bin-store` need a stamped hash.
- **`serve`** — agent mode; speaks the RPC protocol (§7) on stdin/stdout. Not intended for direct use.

## 4. Architecture

```
            initiator (syngoid sync)                          remote host(s)
 ┌──────────────────────────────────────────┐         ┌──────────────────────────┐
 │ cli → engine ─► Endpoint (interface)     │         │ syngoid serve (agent)    │
 │                 ├─ LocalEndpoint ─► zfs  │  ssh    │  rpc server              │
 │                 └─ RemoteEndpoint ─► rpc ┼─yamux──►│   └─ LocalEndpoint ─► zfs│
 │ stream (codecs, buffer, progress)        │         └──────────────────────────┘
 │ bootstrap, sshx, perms, logx             │
 └──────────────────────────────────────────┘
```

### 4.1 Packages

| Package | Responsibility |
|---|---|
| `cmd/syngoid` | `main`: subcommand dispatch. |
| `internal/cli` | Flag parsing (`spf13/pflag`), deprecated-alias translation, validation → typed `Options`. |
| `internal/zfs` | Typed wrapper over the `zfs` CLI: argv building, `-H -p` output parsing, `Dataset`/`Snapshot`/`Bookmark` types, send/recv as `io` streams, optional `sudo -n` prefix, stderr → typed error classification. |
| `internal/endpoint` | `Endpoint` interface and `LocalEndpoint` (backed by `internal/zfs`). |
| `internal/rpc` | Message types, protocol version, JSON-lines codec, yamux session setup, `RemoteEndpoint` (RPC client implementing `Endpoint`). |
| `internal/agent` | `serve`: RPC server dispatching onto a `LocalEndpoint`; child process-group management. |
| `internal/sshx` | Builds and spawns system `ssh` commands (ControlMaster, merged options, BatchMode), exposes stdio as a stream. |
| `internal/bootstrap` | Remote probe, binary resolution (self → stores → download), minisign verification, push, handshake retry. |
| `internal/stream` | Codecs (zstd/lz4/gzip/none), ring buffer, progress counting. |
| `internal/perms` | Required-permission computation, `zfs allow` output parser, effective-permission resolution, grant-command generation. |
| `internal/engine` | Gather, plan (pure), DAG, executor, remediation. |
| `internal/logx` | `log/slog` fan-out: console handler (syncoid-style prefixes) + optional OTLP handler. |

### 4.2 Rules

- **No shell strings** except the bootstrap probe/push (§5). All `zfs` invocations are argv slices executed directly
  (locally) or by the agent (remotely). There is no `escapeshellparam` equivalent.
- `internal/engine` depends only on `Endpoint`; it knows nothing about SSH, RPC, compression, or sudo.
- Direct third-party dependencies are limited to pure-Go modules: `spf13/pflag`, `hashicorp/yamux`,
  `klauspost/compress`, `pierrec/lz4/v4`, `golang.org/x/term` (terminal detection), `go.opentelemetry.io/otel`
  (+ `sdk/log`, `exporters/otlp/otlplog/otlploghttp`, `contrib/bridges/otelslog`). minisign verification is implemented in-repo on `crypto/ed25519` + `golang.org/x/crypto/blake2b`
  (for prehashed `ED` signatures).
- All `zfs` (and `ps`, `id`) child processes run with `LC_ALL=C` so stderr classification is locale-independent, and
  with `PATH` extended by `/sbin:/usr/sbin:/usr/local/sbin:/bin:/usr/bin`.

### 4.3 Endpoint interface (shape)

```go
type Endpoint interface {
    Info(ctx) (HostInfo, error)                       // os, arch, user, uid, groups, zfs version, is Linux
    ListTree(ctx, root string, recursive bool) ([]DatasetInfo, error)      // name, type, origin, syncoid:sync, receive_resume_token, used
    ListSnapshots(ctx, root string, recursive bool) (map[string][]Snapshot, error) // guid, createtxg, creation
    ListBookmarks(ctx, root string, recursive bool) (map[string][]Bookmark, error)
    LocalProps(ctx, dataset string) (map[string]string, error)             // zfs get -s local -H all
    GetProps(ctx, dataset string, props ...string) (map[string]string, error)
    Exists(ctx, dataset string) (bool, error)
    Snapshot(ctx, names []string) error                // single zfs snapshot invocation
    Destroy(ctx, dataset string, snaps []string) error // zfs destroy ds@a,b,c
    DestroyDataset(ctx, dataset string, recursive bool) error
    Hold(ctx, tag, snapshot string) error
    Release(ctx, tag, snapshot string) error
    Bookmark(ctx, snapshot, bookmark string) error
    AbortResume(ctx, dataset string) error             // zfs receive -A
    ReceiveBusy(ctx, dataset string) (bool, error)     // ps -Ao args= scan
    SendEstimate(ctx, SendSpec) (int64, error)         // zfs send -nvP
    Send(ctx, SendSpec, Codec) (Stream, error)         // stream encoded with Codec
    Receive(ctx, RecvSpec, Codec, io.Reader) (RecvResult, error)
    Allow(ctx, dataset string) (perms.Delegations, error) // zfs allow <dataset>, parsed
}
```

Exact signatures are settled during implementation; the method set above is the contract the engine relies on.

## 5. Remote bootstrap

### 5.1 Connection

For each remote host the initiator starts one OpenSSH ControlMaster
(`-o ControlMaster=yes -o ControlPath=<private 0700 tmpdir>/cm -o ControlPersist=60`) — as syncoid does today — so
probe, push, and `serve` reuse a single authenticated connection (one password prompt / hardware-key touch). The
master is closed with `ssh -O exit` at the end of the run. User `-o` options and `--ssh-config` are passed to every
invocation. `-o BatchMode=yes` is added when stdin is not a TTY unless the user passed a `BatchMode` option.

### 5.2 Probe

One `ssh host sh -c '<probe>'` returning, line-oriented: `uname -s`, `uname -m`, `isainfo -k` (illumos only),
`$HOME`, and whether `<remote-bin-dir>/syngoid` is executable.

Platform mapping: `Linux`→`linux`, `FreeBSD`→`freebsd`, `SunOS`→`illumos`, `Darwin`→`darwin`;
`x86_64`/`amd64`/`i86pc`(+`isainfo -k`=`amd64`)→`amd64`, `aarch64`/`arm64`→`arm64`, `armv7l`→`arm` (GOARM=7),
`riscv64`→`riscv64`, `ppc64le`→`ppc64le`. Anything else → error listing supported platforms.

Remote bin dir default: `~/.cache/syngoid/<version>/`; for dev builds `~/.cache/syngoid/<version>-<buildhash[:8]>/`.

### 5.3 Binary resolution (initiator side, first hit wins)

1. **Self** — if remote `os/arch` equals the initiator's, push `os.Executable()`.
2. **Local stores** — `--bin-store` dirs, then `/usr/lib/syngoid/bin/<os>-<arch>/syngoid`, then the cache
   `${XDG_CACHE_HOME:-~/.cache}/syngoid/bin/<ver>/<os>-<arch>/syngoid`. Cached binaries are re-verified against the
   cached signed `SHA256SUMS` before use.
3. **Download** — unless `--no-download` or a dev build: fetch, verify (§11.4), cache, use.

Failure of all three is a critical error naming the platform and the remedies (`syngoid fetch --platform …`,
`--bin-store`).

### 5.4 Push

`ssh host sh -c 'umask 077; mkdir -p "$D" && cat > "$D/.syngoid.tmp.$$" && chmod 700 "$D/.syngoid.tmp.$$" && mv -f "$D/.syngoid.tmp.$$" "$D/syngoid"'`
with the binary on stdin. `$D` is passed as a single-quoted, validated path (no shell metacharacters other than
`~` expansion at the start).

### 5.5 Start and handshake

`ssh host '<D>/syngoid' serve`. The first yamux stream is the control stream; the initiator sends `Hello`, the
agent replies `Hello` (§7.2). Rules:

- `protocol` and `version` must be equal; for dev builds `build_hash` must also be equal.
- The agent reports the SHA-256 of its own executable; if the initiator pushed in this run, it must equal the pushed
  file's hash (detects truncation/corruption).
- On mismatch: close, push, restart, handshake again — once. A second mismatch is a critical error.
- Handshake timeout: 30 s.
- After a successful handshake with a matching initiator the agent prunes sibling version directories, but only when
  it runs from `$HOME/.cache/syngoid/<dir>`. Only real directories named like agent directories
  (`^(dev|[0-9]+\.[0-9]+\.[0-9]+[0-9A-Za-z.-]*)(-[0-9a-f]{8})?$`) are candidates, so other entries such as the
  initiator cache `~/.cache/syngoid/bin` are never touched. The current directory and the candidate with the newest
  modification time are kept (all of them if several tie for newest); strictly older candidates are removed. Paths
  are compared with symlinks resolved.

Before the mux is established the initiator captures the remote's stderr and prints it on any bootstrap/handshake
failure.

## 6. Privilege model and preflight

### 6.1 Model

- Default: no privilege elevation. All `zfs` commands run as the SSH user (remote) or the invoking user (local).
- `--sudo` / `--source-sudo` / `--target-sudo`: `zfs` commands on that side are prefixed with `sudo -n`. The agent
  binary itself is never run through sudo.
- If the effective user is root (uid 0) on a side, no sudo and no delegation checks are needed for that side.
- With sudo on a side, preflight verifies `sudo -n zfs list -H -o name <dataset>` succeeds non-interactively instead
  of checking delegations.
- **Linux, unprivileged target** (not root, no sudo): receive with `-u` (do not mount) and exclude `mountpoint` and
  `canmount` from preserved properties with a warning (these cannot be delegated on Linux).

### 6.2 Required permissions (computed from options)

| Side | Condition | Required |
|---|---|---|
| Source | always | `send` (with `w` in `--sendoptions`, `send:raw` or `send:encrypted` also satisfies) |
| Source | not `--no-sync-snap` | `snapshot`, `mount` |
| Source | not `--no-sync-snap` and not `--keep-sync-snap` | `destroy`, `mount` |
| Source | not `--no-stream` | `hold` (`zfs send -I` places temporary holds, released by the kernel — see §14) |
| Source | `--use-hold` | `hold`, `release` |
| Source | `--create-bookmark` | `bookmark` |
| Target | always | `create`, `mount`, and `receive` (or `receive:append` if `--no-rollback`) |
| Target | not `--no-sync-snap` and not `--keep-sync-snap`; or `--delete-target-snapshots`; or `--force-delete` | `destroy`, `mount` |
| Target | `--use-hold` | `hold`, `release` |
| Target | `--preserve-recordsize` | `recordsize` |
| Target | `--preserve-properties` | each locally-set source property that will be passed (§8.6), plus `userprop` if any user property |
| Target | `--recvoptions` with `o PROP=…` | `PROP` |

### 6.3 Effective permission resolution

The agent runs `zfs allow <dataset>` (no privilege needed), whose output lists permission sets on the dataset and its
ancestors. The resolver computes the effective set for the user from: user grants, grants to any of the user's groups
(groups obtained via `id -Gn`, not `os/user`, so NSS/LDAP groups are included), `everyone` grants, named `@set`
expansion, and local vs descendent scoping per ancestor level.

Datasets checked: every dataset in scope on the source; on the target, every existing target dataset in scope, and
for target datasets that do not exist yet, the closest existing ancestor (descendent permissions).

### 6.4 Preflight behaviour

Runs after gather, before planning output, in `sync`, `--dry-run`, and `permissions`. On failure it reports every
missing grant on every side, then prints ready-to-paste commands, e.g.:

```
CRITICAL ERROR: missing ZFS permissions
  source backup@db1:tank/data: snapshot, destroy
  target backup@nas:tank/backups: receive:append
Grant them (as root) with:
  [db1]  zfs allow -u backup snapshot,destroy,mount tank/data
  [nas]  zfs allow -u backup create,mount,receive:append tank/backups
Alternatively, run with --sudo (requires a sudoers rule for zfs).
```

Exit code 2; nothing is modified.

## 7. RPC protocol

### 7.1 Transport

- The agent's stdin/stdout carry a yamux session; the initiator is the yamux client.
- **Stream 1 (control):** newline-delimited JSON, every line one `Message{id, method, params, result, error,
  event}` (all optional): a request has `method`, an event has `event` (`{kind: "log"|"progress", ...}`, agent
  initiated), anything else is a response carrying `result` or `error{code, message, stderr, argv, exit_code}`
  for the request with the same `id`. `hello` (§7.2) is the first request, with id 1.
- **Data streams:** for `Send`/`Receive`, the initiator opens a new yamux stream whose first line is
  `{"request_id":N}`; the remainder is the raw (codec-encoded) ZFS stream.
- yamux configuration: `MaxStreamWindowSize` 16 MiB initially (tunable constant), yamux keepalive disabled (SSH and
  stream EOF handle liveness).

### 7.2 Handshake

```
initiator → Hello{protocol: 1, version, build_hash, debug: bool, sudo: bool}
agent     → Hello{protocol, version, build_hash, exe_sha256, os, arch, user, uid, groups[], zfs_version, home}
```

### 7.3 Methods

One method per `Endpoint` operation (§4.3), plus `cancel` (cancels the agent-side context of a request id) and
`shutdown`. Parameters are validated by the agent: dataset and snapshot names must match ZFS naming rules and
must not start with `-` (argv-injection guard).

### 7.4 Agent lifecycle

- Each `zfs` child runs in its own process group. When the agent's stdin reaches EOF (initiator exit, `kill -9`,
  network drop → sshd closes the channel), the agent kills all child process groups and exits. This prevents orphan
  `zfs receive` processes that would mark the target busy.
- The agent ignores SIGPIPE, so a dropped connection surfaces as a failed write, and treats SIGHUP like stdin EOF
  (same cleanup, then exit).
- Agent log output is sent as `Event{kind:"log"}`; the agent never writes to its own stderr after the mux is up.

## 8. Sync engine

### 8.1 Compatible core (must match syncoid exactly)

- **Sync snapshot name:** `syncoid_<identifier_><hostname>_<YYYY>-<MM>-<DD>:<hh>:<mm>:<ss>-GMT<sign><HH>:<MM>`
  using local time; `<identifier_>` is `--identifier` plus `_` (empty if unset); `<sign>` is `-` for negative
  offsets and empty otherwise (no `+`). Hostname from `os.Hostname()`.
- **Hold tag:** `syncoid_<identifier_><hostname>`.
- **Bookmark name** (`--create-bookmark`): the snapshot name; on failure retry once with the first 6 characters of
  the snapshot guid appended.
- **`syncoid:sync` property** on each source dataset: `true`, `-`, or empty → sync; `false` → skip (INFO);
  otherwise a comma-separated hostname list → sync only if the local hostname is listed, else skip (INFO).
- **Snapshot ordering:** by `createtxg` when available on both, else by `creation` with a per-listing tie-breaking
  counter.
- **Matching snapshot:** newest source snapshot (by ordering) whose name exists on the target with the same guid.
- **Bookmark fallback:** if no matching snapshot, the newest target snapshot (by the target's own `createtxg`) whose
  guid equals a source bookmark's guid.

### 8.2 Phases

```
1. Connect + bootstrap both sides
2. Gather (batched)
3. Preflight (§6.4)
4. Plan (pure) → DAG of per-dataset action lists + global CreateSyncSnaps batch
5. --dry-run: print plan, exit 0 | --confirm: print plan, prompt
6. Execute: CreateSyncSnaps batch, refresh source snapshot list, then per-dataset actions in topological order
7. Summary, exit code
```

### 8.3 Gather (batched)

Per side, for the whole tree (one call each, `-r` only when `--recursive`):

- `zfs list -Hp -r -t filesystem,volume -o name,type,origin,used,receive_resume_token,syncoid:sync <root>`
- `zfs get -Hp -r -t snapshot -o name,property,value guid,createtxg,creation <root>`
  (fallback on error: `zfs get -Hpd 1 all` per dataset with type filtering, as syncoid does)
- `zfs get -Hp -r -t bookmark -o name,property,value guid,createtxg,creation <root>` on the source (treat
  "invalid type"/"operation not applicable" as "bookmarks unsupported" → empty)
- `ReceiveBusy` check per target dataset is done at execution time, not gather time.

Datasets are filtered by `--exclude-datasets` (regex on full name) and `--skip-parent`; snapshots by
`--include-snaps`/`--exclude-snaps` (exclude wins; if include patterns exist, a snapshot must match one).

### 8.4 Planner

`plan(DatasetState, Options) → []Action` is a pure function. `DatasetState` holds source/target snapshot lists,
source bookmarks, target existence, resume token, `syncoid:sync` value, origin, target `used`, and the planned sync
snapshot name.

Actions:

| Action | Meaning |
|---|---|
| `SkipDataset{reason, status}` | `syncoid:sync` excludes it, no snapshots with `--no-sync-snap`, etc. |
| `SendFull{snap}` | `zfs send ds@snap` (oldest snapshot, or newest with `--no-stream`). |
| `SendIncremental{from, to, intermediates bool}` | `-I` (default) or `-i` (`--no-stream`). With snapshot filters active and intermediates wanted, the planner emits a chain of `-i` sends over the filtered list (syncoid behaviour). |
| `SendFromBookmark{bookmark, to}` | `-i ds#bm ds@to`; without `--no-stream`, sends to the next source snapshot after the bookmark, then a normal incremental. |
| `SendClone{origin, to}` | `-i origin ds@to` for clones whose origin dataset is in scope (unless `--no-clone-handling`). On failure: fall back to ordinary full replication without creating a new sync snapshot. |
| `Resume{token}` | `zfs send -t token`; afterwards the dataset is re-gathered and re-planned. |
| `Hold{side, snap}` / `Release{side, snap}` | `--use-hold`: hold new sync point on both sides, release the previous matching snapshot. Failures are warnings. |
| `CreateBookmark{snap}` | `--create-bookmark` with `--no-sync-snap`. |
| `PruneSyncSnaps{side, snaps}` | Only when this run created a sync snapshot and not `--keep-sync-snap`: destroy own sync snapshots (strict pattern §8.9) except the new one, on both sides, via one `zfs destroy ds@a,b,c` per dataset per side. Failures are warnings. |
| `DestroyTargetSnaps{snaps}` | `--delete-target-snapshots`: after sync, re-gather; destroy target snapshots whose names are not on the source, oldest first, batches of 50. Failures are warnings. |
| `ForceDestroyTarget` | `--force-delete` when the target exists with no matching snapshot or bookmark (never for a pool root dataset): `zfs destroy -r target`, then re-plan without creating another sync snapshot. |

Planning rules mirror syncoid's `syncdataset` (§8.1 for matching; "nothing to do" when the matching snapshot equals
the newest/sync snapshot → status `nothing`; "target exists but no match" without `--force-delete` → critical with
the "cowardly refusing" message, plus the "did you `zfs create` the target?" hint when target `used` < 64 MiB).

**Sync snapshot creation:** the planner assigns one sync snapshot name per run. Datasets that will sync get it,
unless the name is excluded by snapshot filters — in that case the dataset behaves as `--no-sync-snap` (INFO log; the
snapshot is not created). *Behaviour change from syncoid, which created then ignored it.*

**Dry-run estimates:** sends whose endpoint is an existing snapshot use `zfs send -nvP`; sends ending at the
not-yet-created sync snapshot use the `written@<base>` property (incremental) or `referenced` (full), marked `~`.

### 8.5 DAG and execution order

- Within a dataset, actions run in plan order.
- A child dataset whose parent does not exist on the target depends on the parent's full send.
- A clone depends on its origin dataset's send. Ordering is a full topological sort by origin (fixes clone-of-clone
  ordering).
- v1 executes sequentially in topological order. If a dataset fails critically, its dependents are marked
  `skipped (dependency failed)` (soft); unrelated datasets continue.
- `CreateSyncSnaps` runs first: one `zfs snapshot a@s b@s …` per pool (atomic, one txg). If it fails, fall back to
  per-dataset creation so one bad dataset does not block the rest; datasets whose snapshot failed are critical.
  Afterwards the source snapshot list is refreshed (one batched `zfs get`) to obtain guids.

### 8.6 Receive options

Built per receive: `-s` unless `--no-resume`; `-F` unless `--no-rollback`; `-u` on unprivileged Linux targets
(§6.1); filtered `--recvoptions`; and:

- `--preserve-properties`: `-o key=value` for each locally-set source property except: `available`, `compressratio`,
  `createtxg`, `creation`, `clones`, `defer_destroy`, `encryptionroot`, `filesystem_count`, `keystatus`, `guid`,
  `logicalreferenced`, `logicalused`, `mounted`, `objsetid`, `origin`, `receive_resume_token`, `redact_snaps`,
  `referenced`, `refcompressratio`, `snapshot_count`, `type`, `used`, `usedbychildren`, `usedbydataset`,
  `usedbyrefreservation`, `usedbysnapshots`, `userrefs`, `snapshots_changed`, `volblocksize`, `written`, `version`,
  `volsize`, `casesensitivity`, `normalization`, `utf8only` (and `mountpoint`, `canmount` on unprivileged Linux).
- else `--preserve-recordsize`: `-o recordsize=<source recordsize>` for filesystems on initial (full) sends.

### 8.7 Execution and remediation

Before each send: check the target is not busy (`ReceiveBusy`; busy → soft failure for that dataset) and that the
action's preconditions hold (incremental base exists on both sides with equal guid). Stale preconditions → re-gather
and re-plan the dataset.

Typed errors from `internal/zfs` (stderr classification, `LC_ALL=C`):

| Error | stderr pattern | Remediation |
|---|---|---|
| `ErrPartialState` | `contains partially-complete state`, `dataset is busy` (FreeBSD) | If `--no-resume`: `zfs receive -A`, retry once. Else critical. |
| `ErrResumeSourceGone` | `used in the initial send no longer exists`, `incremental source … no longer exists` | `zfs receive -A`, re-gather, re-plan. |
| `ErrDestinationExists{Name}` | `cannot restore to X: destination already exists` | With `--force-delete`: `zfs destroy X`, re-plan without new sync snap. Else critical. |
| `ErrNoDataset` | `dataset does not exist` | Source vanished since gather → skip with WARN (status `skipped`, not a failure). |

Remediation loop: at most 3 re-gather/re-plan rounds per dataset; exceeding it is critical. With `--confirm`, any
destructive remediation (`ForceDestroyTarget`, destroying a conflicting destination) prompts before running.

### 8.8 Results and exit code

Each dataset ends with `Result{Status: ok|nothing|skipped|soft|critical, Err, Bytes, Duration}`.
Exit code: 0 if all `ok`/`nothing`/`skipped`(by property or vanished); 1 if any `soft` and no `critical`; 2 if any
`critical`, or on any global failure (usage error, connection/bootstrap/signature failure, preflight failure,
cancellation).

### 8.9 Bugs fixed relative to syncoid 2.2.0

1. Prune pattern was a prefix match (`^syncoid_<id><host>`), so host `nas` pruned `nas2`'s snapshots. Now:
   `^syncoid_<QUOTED id_><QUOTED host>_\d{4}-\d{2}-\d{2}:\d{2}:\d{2}:\d{2}-GMT-?\d{2}:\d{2}$`.
2. The "did you `zfs create` the target?" hint was built but never printed. Now printed.
3. Bookmark fallback sorted target snapshots using source metadata. Now sorted by target `createtxg`.
4. Size estimates always used `-I`. Now use the send's actual `-i`/`-I`.
5. `die` during snapshot listing / receive-state reset aborted whole recursive runs. Now per-dataset critical errors.

## 9. Data path

### 9.1 Streams

`Endpoint.Send` returns a stream encoded with the requested codec; `Endpoint.Receive` accepts a stream in a declared
codec. The initiator relay connects them through the ring buffer (`--buffer-size`).

### 9.2 Compression placement

Compress next to `zfs send`, decompress next to `zfs receive`, only when a network hop exists:

| Source → Target | Path |
|---|---|
| local → local | `zfs send` → buffer → `zfs receive` (codec `none`) |
| local → remote | `zfs send` → compress (initiator) → buffer → yamux → agent decompress → `zfs receive` |
| remote → local | agent `zfs send` → compress → yamux → buffer → decompress (initiator) → `zfs receive` |
| remote → remote | agent compress → yamux → buffer (pass-through, no recompression) → yamux → agent decompress |

### 9.3 Progress

Size estimate from `SendEstimate` (`zfs send -nvP`, minimum 4096 bytes when known, `0` = unknown). The sending side
counts **uncompressed** bytes and reports them (remote: `Event{kind:"progress"}` every 500 ms) so progress is
comparable to the estimate. `bar`: one updating line (percent, bytes, rate, ETA). `plain`: one line at start and one
at end of each transfer (size, duration, rate). `none`: nothing. `--quiet` implies `none`.

### 9.4 Cancellation

Context cancellation closes the data streams; agents kill `zfs send`/`receive` process groups. Thanks to
`receive -s`, a resume token remains for the next run.

## 10. Errors, logging, observability, signals

### 10.1 Console logging

- Levels and prefixes as syncoid: `DEBUG:`, `INFO:` → stdout; `WARNING:`, `CRITICAL ERROR:` → stderr.
- `--quiet` suppresses INFO and WARN; `--debug` enables DEBUG, including every executed command with host prefix,
  e.g. `DEBUG: [backup@nas] zfs receive -s -F tank/backups/vm`.
- Remote agent logs arrive as events and are printed with the host prefix.
- Errors are wrapped with dataset and action context:
  `tank/vm/disk0: SendIncremental @a→@b: zfs receive: dataset is busy`.
- End-of-run summary (unless `--quiet`): per dataset status, bytes, duration; totals.
- `--dumpsnaps` prints the merged snapshot lists per dataset at INFO.

### 10.2 OpenTelemetry log export

- `internal/logx` fans `slog` records out to the console handler and, when enabled, an `otelslog` bridge handler on
  the OTel logs SDK with the **OTLP/HTTP (protobuf)** exporter. No gRPC exporter.
- Enabled only if `--otel-endpoint` is given or `OTEL_EXPORTER_OTLP_ENDPOINT` / `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT`
  is set. Standard env vars (`OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_RESOURCE_ATTRIBUTES`, `OTEL_SERVICE_NAME`) are
  honoured by the SDK. There is no CLI flag for headers (secrets must not appear in argv).
- Resource: `service.name=syngoid`, `service.version`, `host.name`.
- Record attributes: `syngoid.run_id` (random per run), and where applicable `syngoid.dataset`, `syngoid.action`,
  `syngoid.host`. Severity: DEBUG→DEBUG, INFO→INFO, WARN→WARN, CRITICAL→ERROR.
- Structured events: `run.start` (source, target, options), `dataset.result` (status, bytes, duration, error),
  `run.end` (exit code, totals, duration).
- Export level is independent of `--quiet`: INFO and above always; DEBUG only with `--debug`.
- Batch processor with bounded queue (drop when full). Shutdown flush capped at 5 s. Export failure → one `WARNING:`
  on stderr; never affects the exit code.
- Only the initiator exports.

### 10.3 Signals

- First SIGINT/SIGTERM: cancel the run (§9.4), start no new actions, print the summary, exit 2.
- Second SIGINT: exit immediately.
- Sync snapshots left by interrupted runs are removed by the next successful run's pruning.

## 11. Release, distribution, trust

### 11.1 Builds

GoReleaser in GitHub Actions on `vX.Y.Z` tags. `CGO_ENABLED=0`, `-trimpath`, version/commit/build hash via
`-ldflags`; reproducible. The `go` directive in `go.mod` is the current stable Go release at project start.

| OS | Architectures |
|---|---|
| linux | amd64, arm64, arm (v7), riscv64, ppc64le |
| freebsd | amd64, arm64 |
| illumos | amd64 |
| darwin | amd64, arm64 |

### 11.2 Artifacts

- `syngoid_<ver>_<os>_<arch>.tar.gz` (binary, LICENSE, README)
- `SHA256SUMS`, `SHA256SUMS.minisig` (trusted comment exactly `syngoid <ver>`)
- deb/rpm via nfpm
- `syngoid-remote-bins_<ver>.tar.gz` (all platforms laid out as `<os>-<arch>/syngoid`, for `/usr/lib/syngoid/bin` or
  `--bin-store`)
- GitHub artifact attestations (SLSA provenance) for human verification (`gh attestation verify`); not verified by
  the binary.

### 11.3 Signing

minisign key; secret key stored as a GitHub Actions secret in a protected environment requiring maintainer approval
per release. The binary embeds a list of trusted public keys (current and next) to allow rotation.

### 11.4 Download verification

1. Download `SHA256SUMS`, `SHA256SUMS.minisig`, and the platform archive (HTTPS, system roots, `HTTPS_PROXY`
   honoured; URL from `--download-url` template).
2. Verify the minisign signature against an embedded key.
3. Verify the signed trusted comment equals `syngoid <expected version>` (anti-rollback / anti-substitution).
4. Verify the archive SHA-256 against its `SHA256SUMS` line.
5. Extract in-process; store binary + verified `SHA256SUMS` in the cache.

Only the initiator's own exact version is ever fetched. Dev builds (version `dev` or dirty tree) never download.

### 11.5 Locations

| Location | Path |
|---|---|
| Packaged store | `/usr/lib/syngoid/bin/<os>-<arch>/syngoid` |
| User cache | `${XDG_CACHE_HOME:-~/.cache}/syngoid/bin/<ver>/<os>-<arch>/syngoid` (+ `SHA256SUMS`, `.minisig`) |
| Remote agent | `~/.cache/syngoid/<ver>/syngoid` (dir 0700), override `--remote-bin-dir` |

## 12. Testing

### 12.1 Unit (every PR, `-race`)

- Planner: table-driven over every `syncdataset` path (full, `--no-stream`, `-I`, filtered `-i` chain, resume,
  bookmark fallback incl. target-side sort, clones, `--force-delete`, no-match with/without hint, `syncoid:sync`
  values, sync-snap filtered out).
- `internal/zfs` parsers and error classifier: golden fixtures captured from real Linux, FreeBSD, and illumos output
  (`list`, `get`, `allow`, `send -nvP`, stderr messages).
- `internal/perms`: `zfs allow` fixtures (user/group/everyone, `@sets`, local vs descendent, ancestors, Linux
  exceptions); required-set computation per option combination.
- Naming: sync snapshot and hold names; strict prune pattern incl. `nas`/`nas2` regression.
- CLI: alias translation; removed-flag errors.
- Bootstrap verification: wrong key, tampered sums, mismatched trusted comment, bad archive hash.
- Stream: codec round-trips, ring buffer; native Go fuzzing for codecs, zfs parsers, RPC decoder.

### 12.2 Component (no ZFS, no SSH)

- `FakeEndpoint`: in-memory ZFS model (datasets, snapshots with guid/createtxg, bookmarks, holds, resume tokens);
  fake send streams encode the snapshot range; receive validates the base and applies. Fault injection: partial
  state, busy, source gone, destination exists, mid-stream disconnect.
- Engine end-to-end on `FakeEndpoint`: recursive runs, remediation loops, dependency skips, dry-run golden output,
  `--confirm` destructive prompts, exit-code aggregation.
- RPC end-to-end: initiator ↔ real agent over `net.Pipe` + yamux, agent backed by `FakeEndpoint`: handshake and
  version mismatch, streaming, cancellation, child cleanup on stdin EOF.

### 12.3 Integration with real ZFS

- **Linux, every PR:** GitHub Actions Ubuntu runner, `zfsutils-linux`, file-backed pools. Local→local; remote via
  `ssh localhost` as an unprivileged user with only `zfs allow` grants (real bootstrap push, handshake, preflight
  pass/fail — verifying the printed `zfs allow` command fixes it — and `receive -u`); interruption mid-send then
  resume.
- **FreeBSD and OmniOS, nightly:** VM-based jobs (vmactions) running the same suite.
- **Cross-compile** all targets on every PR.

### 12.4 Compatibility with Perl syncoid (Linux CI)

- syncoid initial replication → syngoid incremental takeover, and the reverse.
- Holds and bookmarks interoperate.
- Concurrent pruning by both tools / multiple hosts never deletes foreign snapshots.
- Scenarios ported from upstream sanoid `tests/syncoid/`.

### 12.5 Process

TDD per unit; `go test -race`, `go vet`, `golangci-lint` on every PR. No coverage percentage target.

## 13. Deferred (explicitly out of v1)

Parallel transfers (`--jobs`), bandwidth limiting, OTel traces and metrics, JSON plan output, insecure direct
connections, plain `zfs send | ssh zfs recv` fallback for restricted-shell hosts, self-update, Homebrew tap,
FreeBSD port, moving data streams to separate SSH sessions.

## 14. Verification items (resolved during implementation by test, not by guess)

1. Exact delegated permission set required by `zfs receive -F` — *resolved by the first Linux CI run (OpenZFS on
   ubuntu-latest):* an unprivileged `zfs receive -s -F -u` that rolls the target back succeeds with only
   `create,mount,receive,destroy`; `rollback` is not needed and was removed from §6.2. The same run showed that
   `zfs send -I` fails with `cannot hold: permission denied` without the `hold` delegation on the source (the default
   incremental mode takes temporary holds), so §6.2 now requires `hold` unless `--no-stream`.
   `TestITReceiveForcePermissions` and `TestITPreflightPrintsWorkingGrant` pin both outcomes.
2. yamux throughput through SSH on a 10 GbE-class link with the 16 MiB window — benchmark; tune window size.
3. `zfs allow` output format differences across Linux/FreeBSD/illumos — captured as fixtures before writing the
   parser.
4. `ps -Ao args=` availability and format on illumos (fallback: `ps -eo args`) — captured as fixtures.
