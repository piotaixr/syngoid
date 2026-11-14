# syngoid — Parallel Transfers (`--jobs`) Design

- **Date:** 2026-11-11
- **Status:** Approved in brainstorming, pending written-spec review
- **Main spec:** `docs/superpowers/specs/2026-10-18-syngoid-design.md` (this note lifts D9 and refines §1.2, §3.5,
  §8.5, §9.3, §9.4, §10.3, §12 and §13; the implementation edits those sections in the same commits as the code)

## 1. Motivation

A recursive `src:pool/srv → backup:tank/backups/src` run of 21 datasets took 5:57 for 1.3 GiB. Every small dataset
(~37 KiB) took 16–17 s; the large ones took only a few seconds more. Perl syncoid shows the same delay, so the cost
is in ZFS, not in syngoid.

The incrementals were `zfs send -I` streams spanning eight days of sanoid snapshots. `zfs receive` commits every
snapshot of such a stream separately, and each commit waits for transaction-group syncs on the target pool, so the
receive time grows with the number of snapshots rather than with the bytes. *(Working hypothesis, consistent with the
log; to be confirmed by comparing a run carrying one snapshot per dataset.)*

Concurrent receives share txg syncs, so running independent datasets in parallel should cut such a run to roughly
the time of its slowest datasets. The executor is already DAG-shaped for this (main spec §1.2, §8.5).

The same investigation showed two usability gaps addressed here too: the progress bar sits at 100 % with a
misleading rate while ZFS is still receiving, and the first Ctrl-C silently aborts running transfers with no way to
let them finish.

**Done when:** `--jobs 4` on a run like the one above finishes in a fraction of the sequential time with the same
results; `--jobs 1` (the default) produces the same results and log sequence as today; Ctrl-C drains, then aborts,
then exits, with a message at each step.

## 2. Decisions

| # | Decision | Rationale |
|---|---|---|
| J1 | New option `-j/--jobs N`, integer ≥ 1, default 1. | Opt-in; same behaviour as today and as syncoid unless asked for. |
| J2 | Worker pool over the existing dependency graph, not per-subtree jobs. | Balances well: one big dataset occupies one slot while small ones pass through the others. |
| J3 | Among ready datasets, the lowest listing index starts first. | With `--jobs 1` this is exactly today's `Order`; output stays predictable. |
| J4 | First SIGINT drains (finish running datasets, start none); second aborts; third exits. SIGTERM/SIGHUP abort at once. | Lets an interactive user stop cleanly; keeps service-manager stops fast. |
| J5 | Bar mode becomes a multi-row live region, one row per running transfer, with a "waiting for zfs receive" state. | Shows parallel work and explains the receive tail that looked like a hang. |
| J6 | Rates and ETA cover the stream phase only; the receive tail is reported separately. | `36.9 KiB … 2.2 KiB/s` was the stream rate diluted by the receive tail. |
| J7 | No per-host limit, no size-based ordering, no bandwidth limit. | YAGNI; `--jobs` bounds both sides together. |

## 3. Command line

`-j, --jobs N` — "number of datasets replicated at the same time (default 1); each running transfer uses its own
`--buffer-size` buffer". `N < 1` or not an integer is a usage error. Added to main spec §3.5.

## 4. Execution (main spec §8.5)

### 4.1 Phases

`Executor.Run` keeps its pre-phases unchanged and sequential: busy check, batched sync-snapshot creation, refresh and
re-plan. Then it **dispatches**:

- A dataset is *ready* when every dataset in its `deps` (from `Order`) has a final result.
- When a slot is free, the dispatcher takes the ready dataset with the lowest index:
  - plan-time outcome (`logLater`): log it now, no slot used;
  - a dependency ended other than ok or nothing to do: skipped as a soft failure (existing message), no slot used;
  - otherwise it occupies a slot and runs `runOne` (including `regather` when stale) in its own goroutine.
- If nothing is running, nothing is ready and datasets remain (a dependency cycle, which should not happen), the
  lowest-index remaining dataset starts — the same fallback as `Order`.
- `Run` returns when every dataset has a final result and no goroutine is running. Results stay indexed by state,
  so the summary keeps listing order.

### 4.2 Shared state

- `runState` bookkeeping (`results`, `done`, `logLater`, `stale`, `visited`, `failed`) is guarded by one mutex. A
  dataset goroutine writes only its own entries, plus `invalidate`, which takes the mutex.
- `invalidate` stays correct: under `--force-delete` every child depends on its parent, so the descendants of a
  destroyed target have not been dispatched yet when the parent finishes.
- `dsRun` stays single-goroutine per dataset.
- `Executor.warnedUnpriv` gets a mutex.
- `Confirm` calls are serialized by the executor, so one question is on screen at a time; while it waits, the
  progress region is suspended (§6.4).
- `Src`/`Dst` are used concurrently. `rpc.Remote` is documented safe for concurrent use; `endpoint.Local` only runs
  processes. Both are covered by the `-race` tests below.

### 4.3 Failures

Unchanged per dataset: each outcome is decided inside its `dsRun` exactly as now. A critical failure never cancels
other running datasets; its dependents are skipped when they are dispatched. `ExitCode` is unchanged.

## 5. Signals and cancellation (main spec §9.4, §10.3)

### 5.1 Controller

`cmd/syngoid/main.go` replaces `signal.NotifyContext` and `forceExitOnSecondSignal` with a small controller that
owns the run context and a drain channel, and counts signals:

| Signal | Executor dispatching | Otherwise |
|---|---|---|
| 1st SIGINT | drain | abort |
| 2nd SIGINT (after drain) | abort | exit 2 immediately |
| SIGINT after abort | exit 2 immediately | exit 2 immediately |
| SIGTERM / SIGHUP | abort | abort |

*Abort* cancels the run context, i.e. today's first-signal behaviour. The executor *arms* draining when dispatch
starts and disarms it when `Run` returns, so a Ctrl-C during gather, preflight or planning aborts as today.
`app.Env` gains the drain channel and the arm/disarm hooks; tests drive the controller through a signal channel.
`Start` registers for the signals before it returns, as `signal.NotifyContext` did, so no signal arriving during
startup takes its default action; handling then runs in a goroutine until `Stop`.

### 5.2 Drain

- No new dataset is dispatched; datasets never dispatched end critical with message "cancelled", as cancelled ones
  do today. Plan-time outcomes keep their result. A drained run exits 2, like an aborted one, even though the run
  context was never cancelled (`app` checks the drain state as well as `ctx.Err()`).
- Running datasets run their whole remaining plan (send, release, bookmark, prune), so no hold or half-finished
  post-step is left behind.
- Logged once: `WARNING: interrupted: finishing N running dataset(s); press Ctrl-C again to abort them (they will
  resume on the next run)`.

### 5.3 Abort

Today's behaviour: context cancellation, agents kill `zfs send`/`receive`, `receive -s` leaves resume tokens.
New: logged once, when at least one dataset is running, `WARNING: aborting N running transfer(s); press Ctrl-C
again to exit immediately`, and the progress rows switch to "aborting…" (§6.2). Accepted deviation: the warning
counts running transfers (between progress Start and Finish), so a Ctrl-C landing only in a non-transfer step (busy
check, post-steps) logs no "aborting" warning.

## 6. Progress (main spec §9.3)

### 6.1 Interface

```go
type Renderer interface {
	Start(label string, estimate int64) Transfer // label is the source dataset
	Aborting()                                   // every running row shows "aborting…"
	Close()                                      // stops redrawing, clears the region
}

type Transfer interface {
	Update(rawBytes int64)
	Sent() // the whole stream has been handed to the receiver
	Finish(rawBytes int64, err error)
}
```

`Executor.transfer` calls `Start` after the INFO send line, `Update` from the send progress callback, `Sent` when its
copy goroutine has written the whole stream into the receive pipe (`stream.Copy` returned nil), and `Finish` once the
receive has returned. The stream phase is `Start → Sent`; the receive tail is `Sent → Finish`. When `Sent` never
happens (failure), the tail is not reported.

Deviation from the first draft of this interface: `Finish` takes no elapsed time; the renderer times both phases with
its own clock (`Start`, `Sent`, `Finish`).

### 6.2 Bar mode

- A live region at the bottom of stderr, one row per running transfer, in start order. A 200 ms ticker redraws it,
  so spinners and timers move while no bytes arrive. Rows are truncated to the terminal width
  (`golang.org/x/term.GetSize` on stderr, 80 columns when unknown).
- Height cap: when the terminal height is known and the region has more rows than height − 1, it shows the first
  height − 2 rows and a final `… and K more` row (K = hidden rows), so no row scrolls off the screen where it could
  not be erased.
- Row states:

  ```
  pool/srv/app                    65%  356.9 KiB/581.3 KiB  32.8 KiB/s  ETA 0:00:07
  pool/srv/www.example.org     ⠙ waiting for zfs receive  36.9 KiB sent  0:00:14
  pool/srv/files.example       ⠙ aborting…  0:00:02
  ```

  The streaming row is today's line. The waiting row's timer counts from `Sent`; the aborting row's from `Aborting`.
  Spinner frames: `⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏`.
- On `Finish` the row leaves the region and one permanent line is printed above it: today's final line, with the
  stream-phase rate, followed by `  zfs receive +H:MM:SS` when the tail is at least one second.
- With `--jobs 1` the region has at most one row: today's output plus the waiting state.

### 6.3 Plain mode

The end line names the dataset, since lines from parallel jobs interleave, and reports the tail when at least one
second:

```
INFO: pool/srv/app: transferred 556.9 KiB in 0:00:02 (278.5 KiB/s); zfs receive finished 0:00:15 later
```

`none` and `--quiet` are unchanged.

### 6.4 Logging above the region

In bar mode the console log handler's stdout and stderr writers are wrapped by the renderer's terminal: each complete
line erases the region, is written to its stream, and the region is redrawn; partial lines are buffered until their
newline. The terminal is created in `app` before the logger, so every log line, including remote agent events, goes
through it. The `--confirm` prompter writes through the same terminal and suspends redrawing until it has its answer.
While the question is on screen, log lines and final lines from other datasets are held, in order, and are written
to their streams once the answer is in, before the region is redrawn.

## 7. Testing (main spec §12)

All unit tests run with `-race`.

- **Engine (fake endpoint).** The fake gains a send gate so a test can hold sends open and release them in a chosen
  order. Cases: `--jobs 3` runs exactly three datasets at once, never four; a child starts only after its parent on
  a first sync and under `--force-delete`; a failed parent skips its children while unrelated datasets continue;
  results are in listing order whatever the completion order; drain lets running datasets finish ok and ends the rest
  "cancelled"; abort ends running datasets "cancelled"; `--jobs 1` gives today's results and log sequence (the
  existing executor tests stay unchanged).
- **Renderer.** Fake clock and fake terminal, golden region output: rows appearing and leaving, spinner frames,
  waiting and aborting states, a log line printed above the region, truncation at a given width, the final line with
  and without a receive tail. Plain mode: dataset name and tail.
- **Signal controller.** Signals fed through a channel: SIGINT → drain → abort → exit; SIGTERM → abort; SIGINT
  before dispatch → abort.
- **Options.** `--jobs` parsing and validation.
- **Integration (real ZFS, CI).** A recursive tree of about six datasets synced with `--jobs 3` and with `--jobs 1`
  ends with the same snapshots on the target.

## 8. Documentation

- Main spec: D9 becomes "Parallel transfers via `--jobs` (opt-in)"; remove the non-goal in §1.2 and `--jobs` from
  §13; update §3.5, §8.5, §9.3, §9.4, §10.3 and §12 as above.
- README: the option, and a short note on why catch-up runs carrying many snapshots are slow (receive cost per
  snapshot) and how `--jobs` or more frequent runs help.
