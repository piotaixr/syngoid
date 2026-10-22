# syngoid

syngoid is a Go rewrite of `syncoid`, the ZFS replication tool from the
[sanoid project](https://github.com/jimsalterjrs/sanoid) by Jim Salter and contributors. It replicates ZFS datasets
with `zfs send` / `zfs receive`. It reads and writes the same sync snapshot, hold and bookmark names as syncoid 2.2.0
and honours the same `syncoid:sync` property, so it can take over an existing syncoid replication and hand it back.

Like sanoid/syncoid, syngoid is licensed under the GNU GPL v3; see [LICENSE](LICENSE). The design is described in
the [spec](docs/superpowers/specs/2026-10-18-syngoid-design.md).

## Status: milestone 2 (remote)

SOURCE and TARGET may now be `[user@]host:pool/dataset`, replicated over SSH between any two hosts (local → remote,
remote → local, remote → remote). The sync engine of milestone 1 is unchanged: planner, executor, resume and
remediation, recursion, clones, bookmarks, holds, pruning, preflight permission checks, `--dry-run` and `--confirm`.

- **Self-pushed agent.** syngoid uses the system `ssh` (one ControlMaster per host, so one password prompt or
  hardware-key touch) and needs no installation on the remote. On first use it pushes its own binary to
  `~/.cache/syngoid/<version>/syngoid` on that host (`<version>-<hash8>/`, with the first 8 hex digits of the build
  hash, for a development build; change the directory with `--remote-bin-dir`), starts
  `syngoid serve` there and talks to it over the ssh connection. Later runs reuse the pushed binary; older version
  directories are pruned. The remote needs no `mbuffer`, `pv` or compressor.
- **Other platforms.** The running binary is pushed as is when the remote has the same OS and architecture. For any
  other platform syngoid looks for `DIR/<os>-<arch>/syngoid` (for example `freebsd-amd64`) in each `--bin-store DIR`,
  then in `/usr/lib/syngoid/bin`, and fails with the missing path otherwise. A development build (version `dev`)
  only accepts an agent with its own build hash, so build the initiator and every `--bin-store` binary with the same
  stamped hash ([Build](#build)); otherwise the handshake fails with `agent mismatch: build hash …`.
- **Delegation on remotes.** Preflight checks the remote user's `zfs allow` delegations on the remote host. Missing
  grants are printed per host so each line can be pasted on the right machine:

  ```
  CRITICAL ERROR: missing ZFS permissions
    source backup@db1:tank/data: snapshot, destroy
    target backup@nas:tank/backups: receive
  Grant them (as root) with:
    [db1]  zfs allow -u backup snapshot,destroy,mount tank/data
    [nas]  zfs allow -u backup receive,create,mount tank/backups
  ```

- **`--sudo` on a remote** (also `--source-sudo` / `--target-sudo`) makes the agent run `sudo -n zfs …` there. The
  agent itself never runs under sudo.
- **Compression.** `--compress` applies to the stream whenever a side is remote and is ignored for local → local.
- A failed connection, push or handshake is a global failure: exit code 2.
- **Dropped links.** The ssh master runs with `-o ServerAliveInterval=15 -o ServerAliveCountMax=4` (unless you pass
  your own `-o ServerAliveInterval=…`), so a silent network drop fails the run within about a minute. Set
  `ClientAliveInterval` (e.g. `15`) in the remote's `sshd_config` as well, so the remote agent and its `zfs receive`
  are stopped as quickly instead of after TCP gives up.

Changes since milestone 1:

- `syngoid version` prints the build hash too: `syngoid <version> (commit <commit>, build <hash12>) protocol <n>`.
- With `--force-delete` a child dataset now waits for its parent, so a parent that fails skips its whole subtree.

Not yet available (milestone 3): `syngoid fetch`, release downloads (`--no-download`, `--download-url`) and OTLP
export (`--otel-endpoint`); the flags are accepted but have no effect yet.

## Build

The Go toolchain is pinned with [mise](https://mise.jdx.dev/) (`mise.toml`: Go 1.27.1):

```sh
mise install
CGO_ENABLED=0 mise exec -- go build -o syngoid ./cmd/syngoid
```

With a matching Go on your `PATH`, `CGO_ENABLED=0 go build -o syngoid ./cmd/syngoid` works too. Supported targets:
Linux, FreeBSD, illumos and macOS. Version information can be stamped with
`-ldflags "-X github.com/piotaixr/syngoid/internal/buildinfo.Version=… -X github.com/piotaixr/syngoid/internal/buildinfo.Commit=…"`.

Without a stamped hash a development build identifies itself by the SHA-256 of its executable, so binaries built
for different platforms never match as initiator and agent. Stamp the same
`-X github.com/piotaixr/syngoid/internal/buildinfo.Hash=<value>` (or a release `Version`) into all of them, for
example a local binary plus a `--bin-store` for a FreeBSD remote:

```sh
h=$(git rev-parse HEAD)
CGO_ENABLED=0 go build -ldflags "-X github.com/piotaixr/syngoid/internal/buildinfo.Hash=$h" -o syngoid ./cmd/syngoid
CGO_ENABLED=0 GOOS=freebsd GOARCH=amd64 go build -ldflags "-X github.com/piotaixr/syngoid/internal/buildinfo.Hash=$h" \
  -o store/freebsd-amd64/syngoid ./cmd/syngoid
./syngoid --bin-store store tank/data backup@freebsd-nas:pool/data
```

Tests and lint:

```sh
mise exec -- go vet ./...
mise exec -- go test -race ./...
mise exec -- go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0 run
```

## Usage

```
syngoid [sync] [flags] SOURCE TARGET
syngoid permissions [flags] SOURCE TARGET
syngoid version
```

`sync` is the default subcommand. SOURCE and TARGET are datasets, local or `[user@]host:dataset`; they must differ and
neither may be inside the other. `syngoid --help` lists every flag.

```sh
# Replicate tank/data to backup/data (created by the first run).
syngoid tank/data backup/data

# Replicate a whole tree, skipping datasets matching a regex.
syngoid -r --exclude-datasets='/scratch$' tank/data backup/data

# Show what would happen, with size estimates, and change nothing.
syngoid --dry-run -r tank/data backup/data

# Print the plan and ask before running it (and before any destructive step).
syngoid --confirm -r tank/data backup/data

# Check the ZFS delegations these options need and print the zfs allow commands.
syngoid permissions -r tank/data backup/data

# Run zfs through sudo -n on the target only.
syngoid --target-sudo tank/data backup/data

# Push to a remote host; pass ssh options with -o and compress the stream.
syngoid -r --compress=zstd-fast -o Port=2222 tank/data backup@nas:pool/data

# Pull from a remote host, or relay between two (the stream goes through this machine).
syngoid -r db1:tank/data backup/data
syngoid -r db1:tank/data nas:pool/data

# Check the delegations on both hosts.
syngoid permissions -r tank/data backup@nas:pool/data
```

Options kept from syncoid with the same meaning include `-r/--recursive`, `--skip-parent`, `--identifier`,
`--no-stream`, `--no-sync-snap`, `--keep-sync-snap`, `--create-bookmark`, `--use-hold`, `--preserve-recordsize`,
`--preserve-properties`, `--no-rollback`, `--delete-target-snapshots`, `--force-delete`, `--exclude-datasets`,
`--include-snaps`, `--exclude-snaps`, `--no-resume`, `--no-clone-handling`, `--dumpsnaps`, `--debug`, `--quiet`,
`--sendoptions` and `--recvoptions` (same mini-syntax and allow-lists).

## Differences from syncoid

- **Delegation first.** syngoid never elevates privileges on its own: `zfs` runs as the invoking user and relies on
  `zfs allow` delegations. `--sudo`, `--source-sudo` and `--target-sudo` opt in to `sudo -n zfs …`; as root no
  checks are needed. On an unprivileged Linux target the receive uses `-u` (not mounted), and `--preserve-properties`
  leaves out `mountpoint` and `canmount` with a warning, since those cannot be delegated on Linux.
- **Preflight.** Before changing anything, syngoid computes the permissions the chosen options need on each side
  and checks them. Missing grants stop the run (exit 2) with ready-to-paste `zfs allow` commands;
  `syngoid permissions` prints that report on demand.
- **`--dry-run` / `--confirm`.** A pure planner shows each dataset's plan with size estimates; `--confirm` asks y/N
  before running it and before each destructive remediation, and needs a terminal.
- **In-process data path.** No `mbuffer`, `pv` or external compressors: `--buffer-size` (default `16M`) sizes an
  in-process buffer and `--progress=auto|bar|plain|none` replaces `--pv-options`.
- **Renamed flags** (still accepted, with a deprecation warning): `--mbuffer-size` → `--buffer-size`,
  `--sshoption` → `-o/--ssh-option`, `--sshconfig` → `--ssh-config`, `--sshkey=F` → `-o IdentityFile=F`,
  `--sshport=P` → `-o Port=P`, `--sshcipher`/`-c` → `-o Ciphers=C`. Legacy `--compress` values map to
  `zstd-fast|zstd|lz4|gzip|none`.
- **Removed flags.** `--source-bwlimit`, `--target-bwlimit` (no bandwidth limiting), `--pv-options` (use
  `--progress`), `--insecure-direct-connection` and `--exclude` (use `--exclude-datasets`) are errors.
  `--no-privilege-elevation`, `--no-command-checks` and `--monitor-version` are ignored with a warning.
- **Fixed syncoid 2.2.0 bugs** (spec §8.9): sync snapshot pruning no longer matches other hosts whose name shares a
  prefix (`nas` vs `nas2`); the "did you `zfs create` the target?" hint is actually printed; the bookmark fallback
  orders target snapshots by their own `createtxg`; size estimates use the send's real `-i`/`-I`; and a failure while
  listing or resetting one dataset no longer aborts a whole recursive run.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | every dataset was replicated, had nothing to do, or was skipped (e.g. `syncoid:sync=false`) |
| 1 | at least one soft failure (e.g. target busy with another `zfs receive`, no snapshot with `--no-sync-snap`) |
| 2 | a critical failure: usage error, failed preflight, a dataset that could not be replicated, or cancellation |

## Integration tests

`test/integration` runs syngoid against real ZFS, including delegation checks as an unprivileged user and
interoperability with Perl syncoid 2.2.0. The tests sit behind the `integration` build tag and run only on Linux
with ZFS, as root, with `SYNGOID_IT=1`. They only touch two dedicated file-backed pools.

Prepare an Ubuntu host. The script installs ZFS, creates the pools `syngoid-src` and `syngoid-dst` from sparse files
under `/var/tmp/syngoid-it`, adds the user `syngoid-it` (with a home and a shell), and installs syncoid 2.2.0 pinned by
checksum. For the remote tests, which replicate over `ssh localhost`, it also creates root's ed25519 key (authorized
for `syngoid-it`, `localhost` in root's `known_hosts`) and adds the sudoers rule `/etc/sudoers.d/syngoid-it` that lets
`syngoid-it` run `zfs` through `sudo -n`. Those tests need an `sshd` on `localhost:22`. The script installs and starts
one (`openssh-server`, enabled at boot and listening on every interface with the distribution's defaults, which allow
password logins for every account) only under GitHub Actions or when you opt in with `SYNGOID_IT_SSHD=1`; otherwise it
requires an `sshd` that already listens and stops with instructions. Opt in only on a throwaway machine, or firewall
port 22:

```sh
scripts/ci/setup-zfs.sh
```

Then run:

```sh
sudo -E env PATH=$PATH SYNGOID_IT=1 go test -tags integration -v -timeout 45m ./test/integration/...
```

Every test first destroys all children of the two test pools, and the suite refuses to run when they do not exist.
`SYNGOID_IT_INVASIVE=1` also enables the tests that briefly change the system clock. Use it only on a throwaway
machine or CI runner.
