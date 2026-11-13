# syngoid — Parallel Transfers (`--jobs`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replicate independent datasets concurrently with `-j/--jobs N`, show one live progress row per running transfer (including a "waiting for zfs receive" state), and make Ctrl-C drain, then abort, then exit.

**Architecture:** `stream` gets a per-transfer `Renderer` and a `Terminal` that owns a live region at the bottom of stderr and prints every console line above it. `engine.Executor.Run` keeps its batched pre-phases and replaces the sequential loop with a dispatcher that starts ready datasets up to `Opts.Jobs` at a time. A new `internal/sigctl` controller turns signals into drain/abort/exit and reaches the executor through `app.Env`.

**Tech Stack:** Go 1.27, `golang.org/x/term` (already a dependency), `log/slog`, existing fake endpoint for engine tests.

**Spec:** `docs/superpowers/specs/2026-11-11-syngoid-parallel-jobs-design.md` (cited "design §N") and the main spec `docs/superpowers/specs/2026-10-18-syngoid-design.md` (cited "spec §N").

## Global Constraints

- Module `github.com/piotaixr/syngoid`; run tools through mise: `mise run test` (`go test -race ./...`), `mise run vet`, `mise run lint`; `mise run check` must pass at the end of every task.
- No new direct dependencies.
- Console prefixes exactly `DEBUG: `, `INFO: ` (stdout), `WARNING: `, `CRITICAL ERROR: ` (stderr). Exit codes 0 ok, 1 soft failure, 2 critical/global/interrupted.
- `--jobs` default 1; with 1 the results and the log sequence equal today's (the existing executor tests stay unchanged and green).
- Exact copy (design §5, §6):
  - drain: `interrupted: finishing N running dataset(s); press Ctrl-C again to abort them (they will resume on the next run)` (WARN)
  - abort: `aborting N running transfer(s); press Ctrl-C again to exit immediately` (WARN)
  - waiting row: `waiting for zfs receive`; aborting row: `aborting…`; spinner frames `⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏`
  - bar final-line tail: `  zfs receive +H:MM:SS`; plain line: `<dataset>: transferred <bytes> in <H:MM:SS> (<rate>/s)` plus `; zfs receive finished <H:MM:SS> later`
  - the receive tail is shown only when it is at least 1 s.
- Region redraw period 200 ms; terminal width from `term.GetSize` on stderr, 80 when unknown.
- Every commit message ends with a blank line and `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Each task that changes behaviour described in the main spec edits that spec section in the same commit (listed per task).
- Deviation from design §6.1, recorded in the design note by Task 1: `Transfer.Finish(rawBytes int64, err error)` takes no elapsed time; the renderer times both phases with its own clock.

## Review Focus

1. **Narrow terminals and multi-byte glyphs.** A long dataset name in an 40-column terminal must not wrap (wrapping breaks the cursor-up redraw), and truncation must count runes, not bytes (`⠙`, `…`). → Task 2 `TestTerminalTruncatesRunes`.
2. **stdout redirected while stderr is a TTY** (`syngoid … > run.log`). The log file must receive plain lines only; escape sequences go to stderr only. → Task 2 `TestTerminalWriterKeepsEscapesOffOtherStream`.
3. **Log lines from several goroutines at once** (agent events, parallel datasets) racing the ticker redraw. Expected: whole lines, no interleaving, clean under `-race`. → Task 2 `TestTerminalConcurrentWriters`.
4. **Ctrl-C when nothing is running** (all remaining datasets are waiting on dependencies or have plan-time outcomes). Expected: no "finishing 0" message; unstarted datasets end "cancelled"; exit 2. → Task 4 `TestExecutorDrainNothingRunning`.
5. **`--jobs` larger than the number of datasets, and invalid values** (`0`, `-1`, `abc`). Expected: the large value just works; invalid ones are usage errors naming `--jobs`. → Task 3 `TestParseJobs`, `TestExecutorJobsMoreThanDatasets`.

---

## File Structure

```
internal/stream/progress.go        Renderer/Transfer interfaces, plain, none, ResolveMode, NewRenderer   (Tasks 1, 2)
internal/stream/terminal.go        Terminal: live region, line writers, Suspend                          (Task 2)
internal/stream/bar.go             bar renderer drawing rows into a Terminal                             (Task 2)
internal/engine/transfer.go        Start/Update/Sent/Finish calls                                        (Task 1)
internal/engine/exec.go            dispatcher, locking, drain/abort                                      (Tasks 3, 4)
internal/engine/interrupts.go      Interrupts interface                                                  (Task 4)
internal/cli/options.go            Jobs option                                                           (Task 3)
internal/sigctl/sigctl.go          signal controller                                                     (Task 5)
internal/app/app.go                Terminal wiring, Env.TermWidth, Env.Interrupts, exit code             (Tasks 2, 5)
cmd/syngoid/main.go                width func, controller                                                (Tasks 2, 5)
test/integration/sync_test.go      TestITJobs                                                            (Task 6)
README.md, main spec               docs                                                                  (Tasks 1–6)
```

---

### Task 1: Per-transfer renderer and the receive tail

**Files:**
- Modify: `internal/stream/progress.go`, `internal/engine/transfer.go`, `internal/engine/exec.go:65-68` (default renderer)
- Test: `internal/stream/progress_test.go`, `internal/engine/exec_test.go`
- Docs: spec §9.3 (plain line, receive tail); design §6.1 (Finish signature note)

**Interfaces:**
- Produces:
  ```go
  type Renderer interface {
  	Start(label string, estimate int64) Transfer
  	Aborting()
  	Close()
  }
  type Transfer interface {
  	Update(rawBytes int64)
  	Sent()
  	Finish(rawBytes int64, err error)
  }
  ```
  `NewRenderer(mode ProgressMode, w io.Writer, isTTY bool, log *slog.Logger) Renderer` keeps its signature in this task. `barRenderer`, `plainRenderer` keep an unexported `now func() time.Time` field for tests.

- [ ] **Step 1: Write the failing stream tests**

  - `TestPlainRenderTail`: fake clock; `Start("tank/a", 0)`, advance 2 s, `Update(570266)`, `Sent()`, advance 15 s, `Finish(570266, nil)` → exactly one INFO record `tank/a: transferred 556.9 KiB in 0:00:02 (278.5 KiB/s); zfs receive finished 0:00:15 later`.
  - `TestPlainRenderShortTail`: tail 500 ms → message ends at `(…/s)` with no `; zfs receive` part.
  - `TestPlainRenderNoSent`: `Finish` without `Sent` → stream phase is Start→Finish, no tail.
  - `TestPlainRenderErrorLogsNothing`: kept, adapted to the new API.
  - `TestBarRenderTail`: bar, estimate 36864, 37786 bytes in 1 s, `Sent`, +15 s, `Finish(…, nil)` → output ends with `tank/a  100%  36.9 KiB/36.0 KiB  36.9 KiB/s  ETA 0:00:00  zfs receive +0:00:15\x1b[K\n` (rate over the stream phase only).
  - Existing `TestBarRender*` adapted: `Start` returns the handle; `Update`/`Finish` go through it.

- [ ] **Step 2: Run them and see them fail**

  Run: `mise exec -- go test ./internal/stream/ -run 'Render' -v` — Expected: compile errors (`Start` returns nothing, no `Sent`).

- [ ] **Step 3: Implement the interfaces in `internal/stream/progress.go`**

  Each `Start` returns a transfer value that records `start`, `sent` (zero until `Sent`) and the label. Stream duration = `sent − start` when `Sent` was called, else `finish − start`; tail = `finish − sent` only when `Sent` was called. `noneRenderer` returns a no-op transfer. Bar keeps one active line as today (`Sent` is a no-op apart from recording the time; `Aborting`/`Close` no-ops); its final line uses the stream-phase rate and appends the tail before `\x1b[K`.

- [ ] **Step 4: Write the failing engine test**

  `TestExecutorTransferPhases` in `exec_test.go`: a `recordingRenderer` (test type implementing `Renderer`, appending `"start tank/a"`, `"sent tank/a"`, `"finish tank/a <nil>"` to a mutex-guarded slice). One successful incremental → events in that order. With `env.src.Faults.Send` returning an error → `start`, `finish … injected` and no `sent`.

- [ ] **Step 5: Wire `Executor.transfer`**

  `h := e.Progress.Start(st.Source, est)` replaces `Start`; the send progress callback calls `h.Update(n)`; the copy goroutine calls `h.Sent()` after `stream.Copy` returned nil and before closing `bW`; `h.Finish(n, err)` replaces `Finish`. `start`/`time.Since` stay only if still used for `Result.Duration` (they are not: drop them from `transfer`).

- [ ] **Step 6: Run everything**

  Run: `mise run check` — Expected: PASS.

- [ ] **Step 7: Update docs and commit**

  Spec §9.3: plain line format with dataset name and receive tail; bar rate covers the stream phase. Design §6.1: `Finish(rawBytes, err)` note.

  ```bash
  git add internal/stream internal/engine docs/superpowers/specs
  git commit -m "feat(stream): per-transfer renderer with receive-tail reporting"
  ```

---

### Task 2: Terminal live region and multi-row bar

**Files:**
- Create: `internal/stream/terminal.go`, `internal/stream/bar.go` (bar moves out of `progress.go`)
- Modify: `internal/stream/progress.go`, `internal/app/app.go:113-206`, `cmd/syngoid/main.go`, `internal/engine/exec.go:65-68` and `internal/engine/exec_test.go:108` (new `NewRenderer` signature)
- Test: `internal/stream/terminal_test.go`, `internal/stream/bar_test.go` (bar tests move here), `internal/app/app_test.go`
- Docs: spec §9.3 (bar = live region)

**Interfaces:**
- Consumes: Task 1 `Renderer`, `Transfer`.
- Produces:
  ```go
  func NewTerminal(w io.Writer, width func() int, tick <-chan time.Time) *Terminal
  func (t *Terminal) Writer(dst io.Writer) io.Writer      // complete lines go above the region; partial lines buffered
  func (t *Terminal) SetRegion(render func(width int) []string) // nil = no region
  func (t *Terminal) Print(line string)                   // one permanent line (no trailing \n given) above the region, on w
  func (t *Terminal) Redraw()
  func (t *Terminal) Suspend() (resume func())            // erase region, stop drawing until resume
  func (t *Terminal) Close()                              // stop the tick goroutine, erase the region
  func ResolveMode(m ProgressMode, isTTY bool) ProgressMode // auto → bar if isTTY else plain
  func NewRenderer(mode ProgressMode, term *Terminal, log *slog.Logger, now func() time.Time) Renderer
  ```
  `NewRenderer`: `bar` needs a non-nil `term`; `auto` is treated as `plain`. `app.Env` gains `TermWidth func() int` (nil → 80).

- [ ] **Step 1: Write the failing Terminal tests** (fake writers, `tick` nil, explicit `Redraw`)

  Drawing contract the tests pin: with `n` region rows currently drawn, *erase* writes `"\r\x1b[" + n + "A\x1b[J"` (nothing when `n == 0`); *draw* writes each row truncated to `width−1` runes followed by `"\n"`. `Writer(dst)` on a complete line: erase on `w`, write the line to `dst`, draw on `w`.
  - `TestTerminalRegionRedraw`: region of 2 rows, `Redraw` twice → second redraw starts with `\r\x1b[2A\x1b[J`.
  - `TestTerminalWriterPrintsAbove`: region of 1 row, `Writer(w).Write("INFO: x\n")` → bytes on `w` are erase, `INFO: x\n`, row.
  - `TestTerminalWriterBuffersPartialLines`: `Write("INFO: a")` writes nothing; `Write("b\nINFO: c\n")` writes both lines once each.
  - `TestTerminalWriterKeepsEscapesOffOtherStream` (Review Focus 2): `out := &bytes.Buffer{}`, `Writer(out).Write("INFO: x\n")` → `out` is exactly `INFO: x\n`; escapes only on `w`.
  - `TestTerminalTruncatesRunes` (Review Focus 1): width 12, row `"pool/srv/blog ⠙ waiting…"` → drawn row has exactly 11 runes, valid UTF-8.
  - `TestTerminalSuspend`: `resume := Suspend()` erases; `Redraw` while suspended writes nothing; `resume()` draws.
  - `TestTerminalConcurrentWriters` (Review Focus 3): 8 goroutines × 100 lines through two `Writer`s plus concurrent `Redraw` under `-race`; every line appears whole in its destination.
  - `TestTerminalClose`: `Close` erases the region and stops a tick goroutine (send on a test `tick` channel after `Close` does not draw).

- [ ] **Step 2: Run and see them fail**

  Run: `mise exec -- go test ./internal/stream/ -run Terminal -v` — Expected: `undefined: NewTerminal`.

- [ ] **Step 3: Implement `internal/stream/terminal.go`**

  One mutex guards every write to `w` and to wrapped destinations, the drawn row count, the suspended flag and the per-destination partial-line buffers. When `tick` is non-nil a goroutine calls `Redraw` on every tick until `Close`.

- [ ] **Step 4: Write the failing bar tests in `bar_test.go`** (fake clock, `width` 80)

  - `TestBarRows`: two transfers started (`tank/a` estimate 581.3 KiB at 356.9 KiB after 10.9 s; `tank/blog` sent 36.9 KiB 14 s ago) → region rows, labels padded to the widest current label:
    `tank/a     61%  356.9 KiB/581.3 KiB  32.7 KiB/s  ETA 0:00:07` and `tank/blog  ⠋ waiting for zfs receive  36.9 KiB sent  0:00:14` (compute exact digits in the test from the inputs; the formats are the design §6.2 ones).
  - `TestBarSpinnerFrames`: waiting row frame = `frames[int(now.Sub(sent)/(200*time.Millisecond))%10]`; check frames at +0, +200 ms, +2 s.
  - `TestBarUnknownEstimate`: `label  bytes  rate/s`.
  - `TestBarAborting`: `Aborting()` → every row `label  ⠋ aborting…  0:00:00`, timer counting from the `Aborting` call.
  - `TestBarFinishPrintsFinalLine`: `Finish` removes the row and `Print`s the Task 1 final line (no `\r`, no `\x1b[K`; with the tail when ≥ 1 s); on error the same line without tail.
  - `TestBarJobsOne`: one transfer → region has at most one row.

- [ ] **Step 5: Implement `internal/stream/bar.go`**

  The bar renderer keeps running transfers in start order under its own mutex, installs `SetRegion` with a render func that formats the rows, calls `Redraw` on `Start`/`Sent`/`Finish`/`Aborting` (no per-`Update` draw; the ticker paces it), and `Print`s the final line on `Finish`. `Close` calls `SetRegion(nil)`. Remove `barThrottle` and its test.

- [ ] **Step 6: Wire app and main**

  In `runSync`: resolve the mode first (`ResolveMode(o.Progress, env.StderrIsTTY)`, `none` under `--quiet`); in bar mode create `time.NewTicker(200*time.Millisecond)` and `term := stream.NewTerminal(env.Stderr, width, ticker.C)`, defer `term.Close()` and `ticker.Stop()`, and build the logger on `term.Writer(env.Stdout)` / `term.Writer(env.Stderr)`. The executor's `Progress` is `NewRenderer(mode, term, log, env.Now)`. The runtime `confirm` closure calls `resume := term.Suspend()` around `p.ask` when `term != nil` (the prompter keeps writing to the raw `env.Stdout`). `cmd/syngoid/main.go` sets `TermWidth` from `term.GetSize(int(os.Stderr.Fd()))`, 80 on error or ≤ 0.

  App test `TestMainSyncBarModeLogsAboveRegion`: `StderrIsTTY: true`, `--progress bar`, one dataset → stdout contains the INFO send line with no escape sequence; stderr contains the final bar line.

- [ ] **Step 7: Run everything**

  Run: `mise run check` — Expected: PASS.

- [ ] **Step 8: Docs and commit**

  Spec §9.3: `bar` is a live region of one row per running transfer with the waiting and aborting states; console lines print above it.

  ```bash
  git add internal/stream internal/app cmd/syngoid docs/superpowers/specs
  git commit -m "feat(stream): live progress region with one row per transfer"
  ```

---

### Task 3: `--jobs` and the dispatcher

**Files:**
- Modify: `internal/cli/options.go`, `internal/engine/exec.go`
- Test: `internal/cli/options_test.go`, `internal/engine/exec_test.go`
- Docs: spec D9, §1.2, §3.5, §8.5, §13

**Interfaces:**
- Produces: `cli.Options.Jobs int`; flag `fs.IntVarP(&r.jobs, "jobs", "j", 1, "number of datasets replicated at the same time; each running transfer uses its own --buffer-size buffer")`. The executor reads `e.Opts.Jobs` (values < 1 behave as 1, so existing tests that leave it 0 run sequentially).

- [ ] **Step 1: Failing option test**

  `TestParseJobs`: default 1; `-j 4` and `--jobs=4` → 4; `--jobs 0`, `--jobs -1` → error containing `--jobs must be at least 1`; `--jobs abc` → parse error mentioning `jobs`.

- [ ] **Step 2: Implement the option** in `newFlagSet`/`build`, run `mise exec -- go test ./internal/cli/ -run TestParseJobs` → PASS.

- [ ] **Step 3: Failing executor tests**

  Gate sends with `env.src.Faults.Send` (called before the stream is written; blocking in it holds a dataset in its send): the hook records the dataset into a mutex-guarded running set and max, signals a `started` channel, waits on a per-dataset `release` channel, then removes itself from the running set and returns nil.
  - `TestExecutorJobsLimit`: `tank/a…e` (recursive, `SkipParent`, target `backup` exists, each with `s1`), `Jobs = 3` → three `started` before any release, max running exactly 3, all five `StatusOK`.
  - `TestExecutorJobsMoreThanDatasets` (Review Focus 5): two datasets, `Jobs = 10` → both run concurrently, both ok.
  - `TestExecutorJobsParentFirst`: first sync of `tank/data`, `tank/data/a`, `Jobs = 2` → in the child's Send hook, `env.tw.Dataset("backup/data")` is non-nil.
  - `TestExecutorJobsForceDeleteSerializesChild`: targets exist and match, `ForceDelete = true`, `Jobs = 2` → the child's hook never observes the parent in the running set.
  - `TestExecutorJobsResultOrder`: three datasets released in reverse order → results in listing order, each `Bytes > 0`.
  - `TestExecutorDependencySkip`: run its table for `Jobs` 1 and 3 (subtests) with unchanged expectations.

- [ ] **Step 4: Run and see them fail**

  Run: `mise exec -- go test ./internal/engine/ -run 'Jobs|DependencySkip' -race -v` — Expected: `TestExecutorJobsLimit` times out waiting for the second `started` (sequential executor). Give the test helpers a 5 s timeout that calls `t.Fatal`, never a bare channel wait.

- [ ] **Step 5: Implement the dispatcher in `exec.go`**

  - `runState` gains `mu sync.Mutex`, `finished []bool` (final result and no goroutine), `failed []bool` (moved from `Run`). After the pre-phases every access to `results`, `done`, `logLater`, `stale`, `visited`, `finished`, `failed` holds `r.mu`; `states[i]`/`plans[i]` are only touched by dataset `i`'s goroutine once dispatched. `record`, `finish`, `planned`, `invalidate` take the lock (split into locked/unlocked variants as needed); `logOutcome` may run under it.
  - Dependency checks move from `runOne` to the dispatcher; `runOne` keeps the cancellation check, `regather` and `runDataset`.
  - `Executor` gains `mu sync.Mutex` for `warnedUnpriv` and `confirmMu sync.Mutex`; `forceDestroy` calls `e.confirm(q)`, which holds `confirmMu` around `e.Confirm`.
  - Dispatch loop (replaces the `for _, i := range order` loop; `Order` still supplies `deps`):

    ```
    jobs := max(1, e.Opts.Jobs); running := 0; doneCh := make(chan int)
    for {
        for running < jobs && ctx.Err() == nil {
            i := lowest index with !visited and every dep finished
            if i < 0 && running == 0 { i = lowest !visited }        // cycle fallback
            if i < 0 { break }
            visited[i] = true
            switch {
            case logLater[i]: log it; finished[i] = true
            case done[i]:     finished[i] = true
            case a dep failed: finish soft "skipped: dependency <src> failed"; failed[i], finished[i] = true
            default: running++; go func() { r.runOne(ctx, i); doneCh <- i }()
            }
        }
        if running == 0 { break }
        select { case i := <-doneCh: running--; finished[i] = true; failed[i] = status ∉ {ok, nothing}
                 case <-ctx.Done(): /* stop starting; keep collecting */ }
    }
    for each !visited i in index order: logLater → log; !done → finish critical msgCancelled
    ```
    After `ctx.Done` fires once, set the select's ctx case to a nil channel so the loop only collects.

- [ ] **Step 6: Run everything**

  Run: `mise run check` — Expected: PASS, including every pre-existing executor test unchanged.

- [ ] **Step 7: Docs and commit**

  Spec: D9 → "Parallel transfers via `--jobs` (opt-in, default 1)"; drop the §1.2 non-goal and `--jobs` from §13; §3.5 row for `-j/--jobs N`; §8.5 "executes up to `--jobs` datasets at once in dependency order, lowest listing index first".

  ```bash
  git add internal/cli internal/engine docs/superpowers/specs
  git commit -m "feat(engine): run up to --jobs datasets concurrently"
  ```

---

### Task 4: Drain and abort in the executor

**Files:**
- Create: `internal/engine/interrupts.go`
- Modify: `internal/engine/exec.go`
- Test: `internal/engine/exec_test.go`

**Interfaces:**
- Consumes: Task 3 dispatcher; Task 1 `Renderer.Aborting`.
- Produces:
  ```go
  // Interrupts lets the user drain a run (design §5).
  type Interrupts interface {
  	Arm() (disarm func())     // draining is possible from now until disarm
  	Drain() <-chan struct{}   // closed when the user asked to drain
  }
  // Executor gains:
  Interrupts Interrupts        // nil: never drains
  func (e *Executor) Drained() bool // the dispatcher stopped because of a drain
  ```

- [ ] **Step 1: Failing tests** (a `fakeInterrupts` with a `drain` channel it closes on demand and counters for `Arm`/disarm)

  - `TestExecutorDrain`: four datasets, `Jobs = 2`, gated sends; after two `started`, close `drain`, then release both → statuses ok, ok, critical, critical; messages of the last two `cancelled`; one WARN exactly `interrupted: finishing 2 running dataset(s); press Ctrl-C again to abort them (they will resume on the next run)`; `Drained()` true; no `Send` call for the last two.
  - `TestExecutorDrainFinishesWholePlan`: `UseHold` on, one dataset gated, drain while gated → after release, its `Release` call is in `env.src.Calls()` (post-steps still run).
  - `TestExecutorDrainNothingRunning` (Review Focus 4): `drain` already closed when `Arm` is called → no dataset starts, all `cancelled`, no "finishing" WARN, `Drained()` true.
  - `TestExecutorAbortMessage`: `BlockSend`, two datasets, `Jobs = 2`, cancel ctx after both started → WARN `aborting 2 running transfer(s); press Ctrl-C again to exit immediately`; the recording renderer saw one `Aborting()`; both `cancelled`.
  - `TestExecutorArmsOnlyWhileDispatching`: `Arm` called once, disarm called once, both before `Run` returns; `Arm` happens after the sync snapshots exist (check `env.src.Calls()` from inside `Arm`).

- [ ] **Step 2: Run and see them fail**

  Run: `mise exec -- go test ./internal/engine/ -run 'Drain|Abort|Arms' -race -v` — Expected: compile error, `Interrupts` undefined.

- [ ] **Step 3: Implement**

  - In `Run`, around the dispatch loop: `if e.Interrupts != nil { disarm := e.Interrupts.Arm(); defer disarm(); drain = e.Interrupts.Drain() }`.
  - Before starting each dataset, check `drain` without blocking; add `case <-drain:` to the select. On drain: set `e.drained`, stop starting, log the drain WARN with the current `running` count when it is > 0, set `drain = nil`.
  - `stop := context.AfterFunc(ctx, func(){ … })` registered at dispatch start (stopped at the end): under `r.mu`, when `running > 0`, log the abort WARN and call `e.Progress.Aborting()`. Keep `running` in `runState` so both read it under the lock.
  - Unstarted datasets after a drain end exactly as after a cancel (Task 3's tail loop).

- [ ] **Step 4: Run everything**

  Run: `mise run check` — Expected: PASS.

- [ ] **Step 5: Commit**

  ```bash
  git add internal/engine
  git commit -m "feat(engine): drain and abort messages for interrupted runs"
  ```

---

### Task 5: Signal controller and app wiring

**Files:**
- Create: `internal/sigctl/sigctl.go`, `internal/sigctl/sigctl_test.go`
- Modify: `cmd/syngoid/main.go`, `internal/app/app.go`
- Test: `internal/app/app_test.go`
- Docs: spec §9.4, §10.3

**Interfaces:**
- Consumes: Task 4 `engine.Interrupts`, `Executor.Drained`.
- Produces:
  ```go
  package sigctl
  func New(parent context.Context, exit func(code int)) (*Controller, context.Context)
  func (c *Controller) Handle(sig os.Signal)   // state machine, design §5.1
  func (c *Controller) Listen()                // signal.Notify(SIGINT, SIGTERM, SIGHUP); Handle each until Stop
  func (c *Controller) Stop()                  // signal.Stop, end Listen
  func (c *Controller) Arm() (disarm func())   // implements engine.Interrupts
  func (c *Controller) Drain() <-chan struct{}
  ```
  `app.Env` gains `Interrupts engine.Interrupts` (nil: no draining).

- [ ] **Step 1: Failing controller tests** (call `Handle` directly; `exit` records codes)

  `TestControllerSequence` table, each row a signal sequence with expected `Drain` closed?, ctx cancelled?, exit codes:
  - armed, INT → drain closed, ctx live.
  - armed, INT, INT → ctx cancelled.
  - armed, INT, INT, INT → exit(2) once.
  - unarmed, INT → ctx cancelled, drain open; then INT → exit(2).
  - armed, TERM → ctx cancelled, drain open; HUP the same.
  - armed, INT, TERM → ctx cancelled.
  - armed then disarmed, INT → ctx cancelled.
  - cancelled, TERM → nothing more (no exit).
  `TestControllerConcurrentHandle`: concurrent `Handle`/`Arm` under `-race`.

- [ ] **Step 2: Run and see them fail**

  Run: `mise exec -- go test ./internal/sigctl/ -race -v` — Expected: package does not exist.

- [ ] **Step 3: Implement `internal/sigctl`**, then replace `signal.NotifyContext` and `forceExitOnSecondSignal` in `main.go` with `ctl, ctx := sigctl.New(context.Background(), os.Exit)`, `go ctl.Listen()`, `defer ctl.Stop()`, and `Interrupts: ctl` in `Env`. In `runSync`: `ex.Interrupts = env.Interrupts`; after `Run`, exit 2 when `ctx.Err() != nil || ex.Drained()`.

- [ ] **Step 4: App test**

  `TestMainSyncDrainedExit2`: `Env.Interrupts` is a fake whose `Drain()` is already closed → exit code 2; summary shows the dataset `cancelled`.

- [ ] **Step 5: Run everything**

  Run: `mise run check` — Expected: PASS.

- [ ] **Step 6: Docs and commit**

  Spec §10.3: the design §5.1 table and messages; §9.4: drain lets running datasets finish their plan.

  ```bash
  git add internal/sigctl internal/app cmd/syngoid docs/superpowers/specs
  git commit -m "feat: Ctrl-C drains, then aborts, then exits"
  ```

---

### Task 6: Integration test and README

**Files:**
- Modify: `test/integration/sync_test.go`, `README.md`
- Docs: spec §12

- [ ] **Step 1: Write `TestITJobs`**

  Under `srcPool/jobs`: datasets `a`, `a/x`, `b`, `c`, `c/y`, `d`, each with a 1 MiB file and snapshots `s1`, `s2`. Run `syngoidOK(t, "--recursive", "--jobs", "3", src, dstPool+"/j3")` and `syngoidOK(t, "--recursive", "--jobs", "1", src, dstPool+"/j1")`; for every dataset, the short snapshot names under `j3` and `j1` are equal and contain `s1`, `s2` and one sync snapshot. Add a second `--jobs 3` run after a new snapshot `s3` everywhere → every target has `s3`.

- [ ] **Step 2: Run it**

  Run on a host with ZFS (CI integration job): `mise exec -- go test -tags integration ./test/integration/ -run TestITJobs -v` — Expected: PASS. Locally without ZFS, check it compiles: `mise exec -- go vet -tags integration ./test/integration/`.

- [ ] **Step 3: README and spec §12**

  README: `-j/--jobs N` in the options list, and a short "Slow catch-up runs" note: receives commit each snapshot of an `-I` stream separately, so a run carrying many snapshots per dataset is slow whatever the size; run more often, or use `--jobs`. Spec §12.3: the parallel integration case.

- [ ] **Step 4: Commit**

  ```bash
  git add test/integration README.md docs/superpowers/specs
  git commit -m "test: parallel sync integration case; document --jobs"
  ```
