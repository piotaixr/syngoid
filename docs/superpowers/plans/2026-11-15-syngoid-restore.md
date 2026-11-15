# syngoid — `restore` Command Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `syngoid restore SOURCE TARGET`, which sends one snapshot per dataset (newest, `--at TIME` or `--snapshot NAME`) from a backup to an empty target, and `syngoid restore --list SOURCE`, which shows the restore points.

**Architecture:** `restore` runs through the existing `runSync` pipeline (gather → preflight → plan → execute). The CLI turns the command into `cli.Options` with `Restore` set and the sync-only behaviours forced off, so receive flags and preflight needs come out right without changes there. A new pure planner `engine.PlanRestore` replaces `PlanDataset` for restore, and the executor takes the planner as a field so its re-plans use it too. Reading which snapshot a resume token belongs to needs one new endpoint method, carried over the agent protocol.

**Tech Stack:** Go 1.27, `spf13/pflag`, `log/slog`, `text/tabwriter`, the existing fake endpoint for unit tests, real ZFS for integration tests.

**Spec:** `docs/superpowers/specs/2026-11-15-syngoid-restore-design.md` (cited "design §N") and the main spec `docs/superpowers/specs/2026-10-18-syngoid-design.md` (cited "spec §N").

## Global Constraints

- Module `github.com/piotaixr/syngoid`; run tools through mise: `mise run test` (`go test -race ./...`), `mise run vet`, `mise run lint`; `mise run check` must pass at the end of every task.
- No new direct dependencies.
- `sync` and `permissions` behave exactly as today: their existing tests stay unchanged and green, except where a task says a signature changed.
- Anti-goals (design §2): restore never destroys, rolls back or overwrites anything on the target and never creates, destroys, holds or releases anything on the source. The only exemption is `zfs receive -A` on this restore's own partial receive.
- Console prefixes exactly `DEBUG: `, `INFO: ` (stdout), `WARNING: `, `CRITICAL ERROR: ` (stderr). Exit codes 0 ok, 1 soft failure, 2 critical/global/interrupted.
- Exact copy:
  - conflict block (design §6.4), on stderr:
    ```
    CRITICAL ERROR: restore needs an empty target; these datasets already exist:
      <target>
      <target>
    Destroy them first if they are to be replaced, or restore to another location.
    ```
    A target is shown as `Location.String()` would show it (with `user@host:` when remote).
  - plan messages: `already restored at @<snap>`; `target <target> already exists`;
    `target <target> is a pool root and always exists; restore its children with --skip-parent`;
    `no snapshot of <source> at or before <TIME>`; `no snapshot <NAME> on <source>`; `no snapshots on <source>`.
    `<TIME>` is the `--at` instant formatted with `time.RFC3339`.
  - `--at` usage error: `--at: invalid time "<value>" (want 2026-11-10, 2026-11-10T03:00, 2026-11-10T03:00:00 or RFC 3339 with an offset)`.
- Every commit message ends with a blank line and `Co-Authored-By: Claude Fable 5.2 <noreply@anthropic.com>`.
- Each task that changes behaviour described in the main spec edits that spec section in the same commit (listed per task).

## Review Focus

1. **`--at` without an offset on a machine that is not in UTC.** `--at 2026-11-10T03:00` must mean 03:00 local time, and a date alone 00:00:00 local. → Task 1 `TestParseAt` with a fixed `+02:00` zone.
2. **A target snapshot with the right name but another guid** (the source dataset was destroyed and re-created under the same name). It must be a conflict, never "already restored". → Task 4 `TestPlanRestore` case `same name other guid`.
3. **Restoring straight into a pool root** (`syngoid restore -r nas:backup/data newhost:tank`). The whole command must fail with the `--skip-parent` hint and send nothing. → Task 6 `TestRestorePoolRootTarget`.
4. **`--snapshot NAME` present on the children but not on the top dataset.** The top dataset is a soft failure, the children are skipped with it, nothing is sent, exit 1. → Task 6 `TestRestoreParentWithoutSnapshot`.
5. **A user pastes a full snapshot path** (`--snapshot backup/data@daily`). It must be a usage error that says to give the part after `@`, not a silent "found nowhere". → Task 1 `TestParseRestoreRejects`.

---

## File Structure

```
internal/cli/restore.go            RestoreOptions, ParseAt, restore flag rules                (Task 1)
internal/cli/options.go            CmdRestore, new flags, Options.Restore, Parse branch        (Task 1)
internal/engine/selection.go       Selection, Pick, Missing                                    (Task 2)
internal/zfs/sendrecv.go           ParseResumeGUID, Client.ResumeGUID                          (Task 3)
internal/endpoint/endpoint.go      Endpoint.ResumeGUID                                         (Task 3)
internal/endpoint/local.go, internal/endpoint/fake/, internal/rpc/, internal/agent/handlers.go
                                   ResumeGUID implementations and wire method                  (Task 3)
internal/buildinfo/buildinfo.go    ProtocolVersion 2                                           (Task 3)
internal/engine/state.go, gather.go  DatasetState.ResumeGUID                                   (Task 3)
internal/engine/restore.go         PlanRestore                                                 (Task 4)
internal/engine/plan.go            Plan.Conflict, Plan.Notes                                   (Task 4)
internal/engine/render.go          render Notes                                                (Task 4)
internal/engine/exec.go, remediate.go  Executor.Planner                                        (Task 5)
internal/app/restore.go            restore checks, dry-run notes, listing command              (Tasks 6, 7)
internal/app/app.go, remote.go     command routing, help, Env.Location, openSource             (Tasks 6, 7)
internal/engine/restorepoints.go   GatherSource, RestorePoints, RenderRestorePoints            (Task 7)
test/integration/restore_test.go   real-ZFS tests                                              (Task 8)
README.md, main spec               docs                                                        (Tasks 1–8)
```

---

### Task 1: Command line

**Files:**
- Create: `internal/cli/restore.go`, `internal/cli/restore_test.go`
- Modify: `internal/cli/options.go` (`Command` constants, `subcommands`, `Options`, `raw`, `newFlagSet`, `Parse`, `build`)
- Modify: main spec §3.5 and §3.7 (new options, subcommands): add the `restore` synopsis and flag table from design §5.

**Interfaces:**
- Produces:
  ```go
  const CmdRestore Command = "restore"

  // RestoreOptions is set on Options for the restore command only.
  type RestoreOptions struct {
      At       *time.Time // nil unless --at
      Snapshot string     // "" unless --snapshot
      List     bool       // --list: Options.Target is the zero Location
  }

  // in Options:
  Restore *RestoreOptions

  // ParseAt parses an --at value; a value without an offset is in loc.
  func ParseAt(s string, loc *time.Location) (time.Time, error)
  ```
- For `restore`, `Parse` returns `Options` with `NoSyncSnap`, `NoStream`, `NoRollback` and `NoCloneHandling` all `true` and `NoResume` `false`. Later tasks rely on these forced values: they make the existing `RecvArgs` emit `-s` without `-F` and the existing `NeedsFrom` produce exactly the needs of design §8.

- [ ] **Step 1: Write the failing tests** in `internal/cli/restore_test.go`

```go
func TestParseAt(t *testing.T) {
    zone := time.FixedZone("test", 2*3600)
    for _, c := range []struct{ in, want string }{ // want in RFC 3339
        {"2026-11-10", "2026-11-10T00:00:00+02:00"},
        {"2026-11-10T03:00", "2026-11-10T03:00:00+02:00"},
        {"2026-11-10 03:00", "2026-11-10T03:00:00+02:00"},
        {"2026-11-10T03:00:05", "2026-11-10T03:00:05+02:00"},
        {"2026-11-10T03:00:05Z", "2026-11-10T03:00:05Z"},
        {"2026-11-10T03:00:05-05:00", "2026-11-10T03:00:05-05:00"},
    } { /* ParseAt(c.in, zone) equals the instant of c.want */ }
    for _, bad := range []string{"", "yesterday", "10/11/2026", "2026-11-10T25:00", "2026-13-01"} {
        /* error text is exactly the --at usage error of Global Constraints, with bad quoted */
    }
}

func TestParseRestore(t *testing.T) {
    // "restore", "-r", "--snapshot", "daily", "backup/data", "tank/data"
    //   → Command CmdRestore; Restore.Snapshot "daily"; Restore.At nil; Recursive;
    //     NoSyncSnap, NoStream, NoRollback, NoCloneHandling true; NoResume false.
    // "restore", "--at", "2026-11-10T03:00:00Z", "backup/data", "tank/data"
    //   → Restore.At equals 2026-11-10T03:00:00Z.
    // "restore", "backup/data", "tank/data" → Restore non-nil, At nil, Snapshot "".
    // "restore", "--list", "-r", "backup/data" → Restore.List; Source.Dataset "backup/data"; Target zero.
    // "tank/data", "backup/data" (sync) → Restore nil.
}

func TestParseRestoreRejects(t *testing.T) { // each: error contains the text
    // restore --at 2026-11-10 --snapshot x S T         → "--at and --snapshot are mutually exclusive"
    // restore --snapshot backup/data@daily S T          → `--snapshot: invalid name "backup/data@daily" (want the part after @)`
    // restore --snapshot "" S T, "a/b", "a#b"           → "--snapshot: invalid name"
    // restore --<flag> S T for every flag in the rejected list below → "--<flag> is not available with restore"
    // restore S                                          → "restore requires exactly SOURCE and TARGET (got 1 arguments)"
    // restore --list S T                                 → "restore --list takes only SOURCE (got 2 arguments)"
    // restore --list --dry-run S, --jobs 2, --preserve-properties, --at, --snapshot, --target-sudo
    //                                                    → "--<flag> is not available with restore --list"
    // --at 2026-11-10 S T, --snapshot x S T, --list S T (sync), permissions --at … S T
    //                                                    → "--<flag> requires the restore command"
    // restore tank/data tank/data/sub                    → the existing overlap error
}
```

- [ ] **Step 2: Run them to see them fail**

Run: `go test ./internal/cli/ -run 'TestParseAt|TestParseRestore' -v`
Expected: FAIL (undefined `ParseAt`, `CmdRestore`).

- [ ] **Step 3: Implement**

- `ParseAt`: try the layouts `2006-01-02`, `2006-01-02T15:04`, `2006-01-02T15:04:05` with `time.ParseInLocation(…, loc)` after replacing one space with `T`, then `time.RFC3339`.
- New flags in `newFlagSet`: `--at` ("restore: newest snapshot created at or before TIME"), `--snapshot` ("restore: the snapshot named NAME"), `--list` ("restore: list the restore points of SOURCE and exit").
- `Parse` uses `ParseAt(value, time.Local)`.
- Rejected on `restore` (checked with `fs.Changed`): `identifier`, `no-stream`, `no-sync-snap`, `keep-sync-snap`, `create-bookmark`, `use-hold`, `no-rollback`, `delete-target-snapshots`, `force-delete`, `include-snaps`, `exclude-snaps`, `no-clone-handling`, `no-resume`, `dumpsnaps`, and the hidden `sshport`, `sshkey`, `sshcipher`, `sshoption`, `sshconfig`, `mbuffer-size`, `no-privilege-elevation`, `no-command-checks`, `monitor-version`.
- Accepted with `restore --list`: `list`, `recursive`, `skip-parent`, `exclude-datasets`, `sudo`, `source-sudo`, `ssh-option`, `ssh-config`, `bin-store`, `remote-bin-dir`, `no-download`, `download-url`, `debug`, `quiet`. Any other changed flag is rejected.
- With `--list`, `checkOverlap` and TARGET parsing are skipped.

- [ ] **Step 4: Run the package tests**

Run: `go test -race ./internal/cli/`
Expected: PASS, including every existing test.

- [ ] **Step 5: Update main spec §3.5 and §3.7, run `mise run check`, commit**

```bash
git add internal/cli docs/superpowers/specs/2026-10-18-syngoid-design.md
git commit -m "feat(cli): restore command line"
```

---

### Task 2: Snapshot selection

**Files:**
- Create: `internal/engine/selection.go`, `internal/engine/selection_test.go`

**Interfaces:**
- Consumes: `cli.RestoreOptions` (Task 1); `zfs.Snapshot{Name, GUID string; CreateTxg, Creation uint64}` (`Creation` in seconds).
- Produces:
  ```go
  // Selection says which snapshot of each dataset a restore takes: the one
  // named Name, else the newest created at or before At, else the newest.
  type Selection struct {
      At   *time.Time
      Name string
  }
  func SelectionFrom(r *cli.RestoreOptions) Selection
  // Pick returns the selected snapshot of snaps (sorted oldest to newest).
  func (sel Selection) Pick(snaps []zfs.Snapshot) (zfs.Snapshot, bool)
  // Missing is the soft-failure message for a dataset where Pick found nothing.
  func (sel Selection) Missing(source string, snaps []zfs.Snapshot) string
  ```

- [ ] **Step 1: Write the failing test**

```go
func TestSelectionPick(t *testing.T) {
    snaps := []zfs.Snapshot{ // oldest to newest
        {Name: "a", GUID: "1", CreateTxg: 10, Creation: 1000},
        {Name: "b", GUID: "2", CreateTxg: 20, Creation: 2000},
        {Name: "c", GUID: "3", CreateTxg: 30, Creation: 3000},
    }
    at := func(sec int64) *time.Time { t := time.Unix(sec, 0); return &t }
    for _, c := range []struct {
        name string; sel Selection; want string; ok bool
    }{
        {"newest", Selection{}, "c", true},
        {"at between", Selection{At: at(2500)}, "b", true},
        {"at exactly (inclusive)", Selection{At: at(2000)}, "b", true},
        {"at after all", Selection{At: at(9000)}, "c", true},
        {"at before all", Selection{At: at(999)}, "", false},
        {"name present", Selection{Name: "a"}, "a", true},
        {"name absent", Selection{Name: "zz"}, "", false},
    } { /* Pick(snaps): name and ok */ }
    // Pick(nil) is false for every selection.
    // Snapshots without CreateTxg (0) and Creation 1000, 2000, 3000 give the same results.
}

func TestSelectionMissing(t *testing.T) {
    // Selection{Name:"daily"}.Missing("backup/data", snaps) == "no snapshot daily on backup/data"
    // Selection{At: 2026-11-10T03:00:00Z}.Missing("backup/data", snaps)
    //     == "no snapshot of backup/data at or before 2026-11-10T03:00:00Z"
    // any selection with snaps == nil → "no snapshots on backup/data"
}
```

- [ ] **Step 2: Run to see it fail**

Run: `go test ./internal/engine/ -run TestSelection -v` — Expected: FAIL (undefined `Selection`).

- [ ] **Step 3: Implement** `selection.go`. For `At`, walk from the newest and return the first snapshot with `Creation <= At.Unix()`.

- [ ] **Step 4: Run** `go test -race ./internal/engine/ -run TestSelection` — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/engine/selection.go internal/engine/selection_test.go
git commit -m "feat(engine): restore snapshot selection"
```

---

### Task 3: Guid of a resume token

A partially received target dataset has a resume token and no snapshots. Design §6.3 accepts it only when the token is for the chosen snapshot, so the gathered state must carry the guid the token points to.

**Files:**
- Modify: `internal/zfs/sendrecv.go`, `internal/zfs/sendrecv_test.go`
- Modify: `internal/endpoint/endpoint.go`, `internal/endpoint/local.go`, `internal/endpoint/fake/fake.go` (+ their tests)
- Modify: `internal/rpc/proto.go`, `internal/rpc/validate.go`, `internal/rpc/remote.go`, `internal/agent/handlers.go` (+ their tests)
- Modify: `internal/buildinfo/buildinfo.go` (`ProtocolVersion = 2`) and any test or golden that prints `protocol 1`
- Modify: `internal/engine/state.go`, `internal/engine/gather.go`, `internal/engine/gather_test.go`
- Modify: main spec §7.3 (methods): add the `resume_guid` method and note protocol version 2.

**Interfaces:**
- Produces:
  ```go
  // zfs
  // ParseResumeGUID returns, in decimal, the toguid of `zfs send -nvP -t`
  // output, or "" when the output has none.
  func ParseResumeGUID(out string) string
  // ResumeGUID runs `zfs send -nvP -t token`. A failing command is not an
  // error: the guid is "" unless the output it did print carries one.
  func (c *Client) ResumeGUID(ctx context.Context, token string) (string, error)

  // endpoint.Endpoint
  ResumeGUID(ctx context.Context, token string) (string, error)

  // rpc
  const MethodResumeGUID = "resume_guid"
  type ResumeGUIDParams struct{ Token string `json:"token"` }

  // engine.DatasetState
  // ResumeGUID is the guid of the snapshot ResumeToken resumes, in decimal
  // like zfs.Snapshot.GUID; "" when unknown or not gathered.
  ResumeGUID string
  ```
- `engine.Gather` and `engine.GatherOne` fill `ResumeGUID` by calling **the source endpoint's** `ResumeGUID` for every state with a `ResumeToken`, only when `o.Restore != nil`. For `sync` no extra call is made.

- [ ] **Step 1: Write the failing tests**

```go
// internal/zfs/sendrecv_test.go
func TestParseResumeGUID(t *testing.T) {
    for os, want := range map[string]string{
        "linux":   "5387764070742741370", // 0x4ac52ee0723bd17a
        "freebsd": "3850504801561111959", // 0x356fbdb756fc6d97
        "illumos": "9737808090337195794", // 0x8723a4dd73793712
    } { /* ParseResumeGUID(testdata/<os>/estimate_resume.txt) == want */ }
    // ParseResumeGUID("size\t4096\n") == ""; ParseResumeGUID("\ttoguid = zz\n") == ""
}
// TestClientResumeGUID: a fake Exec that prints the linux file and exits non-zero → that guid, nil error;
//   argv ends with: send -nvP -t <token>.

// internal/endpoint/fake/fake_test.go
// TestFakeResumeGUID: interrupt a full receive of tank/a@one into backup/a (existing fault helper),
//   then src.ResumeGUID(token of backup/a) == GUID of tank/a@one; ResumeGUID("nope") == "", nil.

// internal/rpc/remote_test.go, validate_test.go, internal/agent/server_test.go
// resume_guid round-trips a token to the endpoint and returns its string;
//   an empty token is rejected by validation like the other required-string params.

// internal/engine/gather_test.go
// TestGatherResumeGUID: target dataset with a resume token;
//   with o.Restore set → state.ResumeGUID == the source snapshot's GUID;
//   with o.Restore nil → "" and the source endpoint saw no ResumeGUID call.
```

- [ ] **Step 2: Run to see them fail**

Run: `go test ./internal/zfs/ ./internal/endpoint/... ./internal/rpc/ ./internal/agent/ ./internal/engine/ -run 'ResumeGUID' -v`
Expected: FAIL (undefined).

- [ ] **Step 3: Implement.** `Client.Output` discards stdout on failure, so `ResumeGUID` runs `c.Exec.Run` with its own buffer. The fake reads the snapshot from its resume registry (`internal/endpoint/fake/stream.go`). Wire the method exactly like `send_estimate` in `proto.go`, `validate.go`, `remote.go` and `handlers.go`.

- [ ] **Step 4: Run** `mise run check` — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal docs/superpowers/specs/2026-10-18-syngoid-design.md
git commit -m "feat: read the snapshot guid of a resume token"
```

---

### Task 4: `PlanRestore`

**Files:**
- Create: `internal/engine/restore.go`, `internal/engine/restore_test.go`
- Modify: `internal/engine/plan.go` (`Plan` fields), `internal/engine/render.go`, `internal/engine/render_test.go`
- Modify: main spec §8.4 (planner): add a "Restore planner" part with the table of design §6.2 and the rule of §6.3.

**Interfaces:**
- Consumes: `Selection` (Task 2), `DatasetState.ResumeGUID` (Task 3).
- Produces:
  ```go
  // in Plan:
  // Conflict marks a restore plan that failed because the target dataset
  // already exists (design §6.3).
  Conflict bool
  // Notes are extra lines RenderPlan prints after the actions of an OK plan.
  Notes []string

  // PlanRestore plans the restore of one dataset pair (design §6.2).
  func PlanRestore(s DatasetState, sel Selection) Plan
  ```
- `RenderPlan` prints each note of a `StatusOK` plan as `"  " + note` on its own line, after the plan's actions.

- [ ] **Step 1: Write the failing tests**

```go
func TestPlanRestore(t *testing.T) {
    one := zfs.Snapshot{Name: "one", GUID: "11", CreateTxg: 1, Creation: 100}
    two := zfs.Snapshot{Name: "two", GUID: "22", CreateTxg: 2, Creation: 200}
    src := []zfs.Snapshot{one, two}
    base := DatasetState{Source: "backup/d", Target: "tank/d", SourceSnaps: src}
    with := func(f func(*DatasetState)) DatasetState { s := base; f(&s); return s }
    for _, c := range []struct {
        name   string
        s      DatasetState
        sel    Selection
        status Status
        action string // Describe() of the single action, "" for none
        msg    string
        conflict bool
    }{
        {"no target", base, Selection{}, StatusOK, "send full @two", "", false},
        {"no target, by name", base, Selection{Name: "one"}, StatusOK, "send full @one", "", false},
        {"no snapshots", with(func(s *DatasetState) { s.SourceSnaps = nil }), Selection{},
            StatusSoft, "", "no snapshots on backup/d", false},
        {"name absent", base, Selection{Name: "zz"}, StatusSoft, "", "no snapshot zz on backup/d", false},
        {"finished", with(func(s *DatasetState) { s.TargetExists = true; s.TargetSnaps = []zfs.Snapshot{two} }),
            Selection{}, StatusNothing, "", "already restored at @two", false},
        {"interrupted", with(func(s *DatasetState) { s.TargetExists = true; s.ResumeToken = "tok"; s.ResumeGUID = "22" }),
            Selection{}, StatusOK, "resume interrupted receive", "", false},
        {"token for another snapshot", with(func(s *DatasetState) { s.TargetExists = true; s.ResumeToken = "tok"; s.ResumeGUID = "11" }),
            Selection{}, StatusCritical, "", "target tank/d already exists", true},
        {"token with unknown guid", with(func(s *DatasetState) { s.TargetExists = true; s.ResumeToken = "tok" }),
            Selection{}, StatusCritical, "", "target tank/d already exists", true},
        {"token and snapshots", with(func(s *DatasetState) { s.TargetExists = true; s.ResumeToken = "tok"; s.ResumeGUID = "22"; s.TargetSnaps = []zfs.Snapshot{one} }),
            Selection{}, StatusCritical, "", "target tank/d already exists", true},
        {"other snapshot", with(func(s *DatasetState) { s.TargetExists = true; s.TargetSnaps = []zfs.Snapshot{one} }),
            Selection{}, StatusCritical, "", "target tank/d already exists", true},
        {"same name other guid", with(func(s *DatasetState) { s.TargetExists = true; s.TargetSnaps = []zfs.Snapshot{{Name: "two", GUID: "99"}} }),
            Selection{}, StatusCritical, "", "target tank/d already exists", true},
        {"extra snapshot", with(func(s *DatasetState) { s.TargetExists = true; s.TargetSnaps = []zfs.Snapshot{two, {Name: "local", GUID: "5"}} }),
            Selection{}, StatusCritical, "", "target tank/d already exists", true},
        {"empty existing target", with(func(s *DatasetState) { s.TargetExists = true }),
            Selection{}, StatusCritical, "", "target tank/d already exists", true},
        {"pool root", with(func(s *DatasetState) { s.Target = "tank"; s.TargetExists = true; s.TargetIsPoolRoot = true }),
            Selection{}, StatusCritical, "", "target tank is a pool root and always exists; restore its children with --skip-parent", true},
        {"sync property is ignored", with(func(s *DatasetState) { s.SyncProp = "false" }),
            Selection{}, StatusOK, "send full @two", "", false},
        {"clone is a full send", with(func(s *DatasetState) { s.Origin = "backup/o@x"; s.OriginInScope = true }),
            Selection{}, StatusOK, "send full @two", "", false},
    } { /* status, len(Actions) and Describe(), Message, Conflict; Source/Target copied into the Plan */ }
}

// TestPlanRestoreOnlySafeActions: for every case above, each action's dynamic type is SendFull or Resume.

// render_test.go — TestRenderPlanNotes: an OK plan with Actions [SendFull{Snap:"two"}] and
//   Notes ["set compression=zstd", "set quota=10G"] renders
//   "backup/d → tank/d\n  send full @two\n  set compression=zstd\n  set quota=10G\n"; the existing golden is unchanged.
```

- [ ] **Step 2: Run to see them fail**

Run: `go test ./internal/engine/ -run 'TestPlanRestore|TestRenderPlanNotes' -v` — Expected: FAIL (undefined `PlanRestore`).

- [ ] **Step 3: Implement** `PlanRestore` and the `RenderPlan` change. The "no qualifying snapshot" check comes before the target checks, so a dataset without a pick is a soft failure whatever the target holds.

- [ ] **Step 4: Run** `go test -race ./internal/engine/` — Expected: PASS.

- [ ] **Step 5: Update main spec §8.4, commit**

```bash
git add internal/engine docs/superpowers/specs/2026-10-18-syngoid-design.md
git commit -m "feat(engine): restore planner"
```

---

### Task 5: Planner as an executor parameter

**Files:**
- Modify: `internal/engine/exec.go` (`Executor`, `regather`, `refresh`), `internal/engine/remediate.go` (`replan`)
- Test: `internal/engine/exec_test.go`
- Modify: main spec §8.7 (execution and remediation): one sentence saying the executor re-plans with the planner it was given.

**Interfaces:**
- Consumes: `PlanRestore`, `Selection` (Tasks 2, 4).
- Produces:
  ```go
  // in Executor:
  // Planner plans one dataset wherever the executor plans again (refresh,
  // regather, replan). nil means PlanDataset with PlanOpts.
  Planner func(DatasetState) Plan
  ```

- [ ] **Step 1: Write the failing tests** in `exec_test.go`, with the helpers the existing executor tests use

```go
// TestExecutorRestoreRun: world with tank/d@one, tank/d@two, tank/d/c@one and an empty backup;
//   Opts as cli.Parse returns them for `restore -r tank/d backup/d`;
//   Planner = func(s DatasetState) Plan { return PlanRestore(s, Selection{}) }; plans from the same function.
//   After Run: both results StatusOK; backup/d has exactly [two], backup/d/c exactly [one];
//   the source endpoint's Calls() contains no Snapshot, Destroy, Hold, Release or Bookmark;
//   the target endpoint's Calls() contains no Destroy or DestroyDataset;
//   every Receive call has -s and no -F.

// TestExecutorRestoreResumeReplansWithPlanner: interrupt the first Run with the fake's receive fault,
//   run again with freshly gathered states and plans → result StatusOK, backup/d has exactly [two],
//   and the Planner was called at least once during the second Run (count calls in the closure).

// TestExecutorRestoreNeverDestroysOnConflict: make the fake's Receive fail with zfs.DestinationExistsError
//   for backup/d → result StatusCritical; target Calls() contains no Destroy or DestroyDataset.
```

- [ ] **Step 2: Run to see them fail**

Run: `go test ./internal/engine/ -run TestExecutorRestore -v` — Expected: FAIL (unknown field `Planner`).

- [ ] **Step 3: Implement.** Add `func (e *Executor) plan(s DatasetState) Plan` and use it at the three `PlanDataset(…, e.PlanOpts)` call sites (`exec.go` in `regather` and `refresh`, `remediate.go` in `replan`).

- [ ] **Step 4: Run** `go test -race ./internal/engine/` — Expected: PASS; every existing executor test unchanged.

- [ ] **Step 5: Update main spec §8.7, commit**

```bash
git add internal/engine docs/superpowers/specs/2026-10-18-syngoid-design.md
git commit -m "refactor(engine): executor takes its planner"
```

---

### Task 6: `restore` in the app

**Files:**
- Create: `internal/app/restore.go`, `internal/app/restore_test.go`, `internal/app/testdata/restore_dryrun.golden`
- Modify: `internal/app/app.go` (`Main` routing and usage text, `runSync`)
- Modify: main spec §6.2 (required permissions): add the restore needs of design §8; main spec §12: the tests below.

**Interfaces:**
- Consumes: `cli.CmdRestore`, `Options.Restore` (Task 1); `engine.SelectionFrom`, `Selection.Pick` (Task 2); `engine.PlanRestore`, `Plan.Conflict`, `Plan.Notes` (Task 4); `Executor.Planner` (Task 5); `engine.PreservedPropKeys(localProps map[string]string, unprivLinux bool) []string` and `Endpoint.LocalProps` (existing).
- Produces: `syngoid restore SOURCE TARGET` end to end. `Main` sends `CmdRestore` without `Restore.List` to `runSync`. (`--list` is routed in Task 7; until then it returns `criticalf(env, "restore --list is not implemented")`.)

Behaviour inside `runSync` when `o.Restore != nil`, in this order after `engine.Gather`:
1. **Nothing qualifies** (design §5.4): if `Pick` finds no snapshot for any state, exit 2 with one of
   `no dataset under <SOURCE> has a snapshot named <NAME>`, `no dataset under <SOURCE> has a snapshot at or before <TIME>`, `no dataset under <SOURCE> has any snapshot`, each followed by `; list the restore points with: syngoid restore --list <SOURCE>` (`<SOURCE>` as typed, with `-r ` before it when `-r` was given).
2. **Conflicts** (design §6.4): plan every state with `PlanRestore`; if any plan has `Conflict`, print the conflict block of Global Constraints and exit 2. The pool-root case prints its own plan message as an extra line after the block.
3. Preflight, unchanged.
4. Plans are the `PlanRestore` plans; the executor gets `Planner` set to the same function.
5. With `--preserve-properties`, before `--dry-run` or `--confirm` rendering, each `StatusOK` plan gets `Notes` `set <key>=<value>` for the keys of `PreservedPropKeys`, in that order (this needs the target's `HostInfo` earlier than today).
6. `--confirm` without a terminal is the same error as for `sync`.

- [ ] **Step 1: Write the failing tests** in `restore_test.go`, built on `newSyncEnv` (world: `tank/data@one`, `tank/data/vm@one`, empty `backup`)

```go
// TestRestoreNewest: add tank/data@two. run("restore","-r","tank/data","backup/data") == 0;
//   backup/data has exactly [two], backup/data/vm exactly [one]; guids equal the source's;
//   ep.Calls() has no Snapshot, Destroy, DestroyDataset, Hold, Release, Bookmark.
// TestRestoreSnapshot: add tank/data@two; --snapshot one → backup/data has exactly [one].
// TestRestoreAt: fake creations are 1000, 2000, … seconds in creation order; add tank/data@two (3000);
//   --at 1970-01-01T00:40:00Z (2400 s) → backup/data has exactly [one].
// TestRestoreRerunDoesNothing: run twice → second exit 0, stdout contains "already restored at @one",
//   no Receive in the second run's calls.
// TestRestoreConflictSendsNothing: world.Create("backup/data/vm","") first → exit 2;
//   stderr contains the conflict block with exactly "  backup/data/vm"; no Receive call; backup/data absent.
// TestRestorePoolRootTarget: run("restore","-r","tank/data","backup") → exit 2; stderr contains
//   "target backup is a pool root and always exists; restore its children with --skip-parent"; no Receive call.
// TestRestoreSkipParentIntoPoolRoot: run("restore","-r","--skip-parent","tank/data","backup") == 0; backup/vm has [one].
// TestRestoreNothingQualifies: --snapshot zz → exit 2; stderr contains
//   "no dataset under tank/data has a snapshot named zz; list the restore points with: syngoid restore --list -r tank/data".
// TestRestoreParentWithoutSnapshot: world.Snap("tank/data/vm","only"); --snapshot only → exit 1;
//   no Receive call; summary shows tank/data failed (soft) and tank/data/vm skipped by its dependency.
// TestRestoreMissingTargetParent: run("restore","tank/data","backup/a/b") → exit 2, backup/a not created,
//   stderr names backup/a/b (the existing missing-parent failure of sync).
// TestRestoreDryRun: world.SetProp("tank/data","compression","zstd");
//   run("restore","-r","--dry-run","--preserve-properties","tank/data","backup/data") == 0;
//   stdout equals testdata/restore_dryrun.golden (two "send full @one" plans, "  set compression=zstd"
//   under tank/data only, the total line); no mutating call.
// TestRestorePreflight: HostInfo of an unprivileged user (as TestMainPreflightFail sets it up).
//   Allow output granting `send` on tank/data and `receive,create,mount` on backup → exit 0.
//   Without `receive` → exit 2 and stderr has a `zfs allow` line for backup that lists receive,
//   and no line asks for snapshot, destroy, hold, rollback or bookmark on either side.
// TestRestoreConfirmNoTTY: --confirm without StdinIsTTY → exit 2, same message as sync.
// TestMainHelp (existing, extended): usage lists "syngoid restore [flags] SOURCE TARGET" and
//   "syngoid restore --list [flags] SOURCE".
```

- [ ] **Step 2: Run to see them fail**

Run: `go test ./internal/app/ -run 'TestRestore|TestMainHelp' -v` — Expected: FAIL.

- [ ] **Step 3: Implement.** Keep the restore-only logic (steps 1, 2 and 5 above) in `internal/app/restore.go` as functions `runSync` calls; `runSync` itself only gains the branches.

- [ ] **Step 4: Run** `mise run check` — Expected: PASS.

- [ ] **Step 5: Update the main spec, commit**

```bash
git add internal/app docs/superpowers/specs/2026-10-18-syngoid-design.md
git commit -m "feat: restore command"
```

---

### Task 7: `restore --list` and user documentation

**Files:**
- Create: `internal/engine/restorepoints.go`, `internal/engine/restorepoints_test.go`, `internal/app/testdata/restore_list.golden`
- Modify: `internal/app/restore.go`, `internal/app/restore_test.go`, `internal/app/app.go` (`Env.Location`, routing), `internal/app/remote.go` (`openSource`)
- Modify: `README.md` (Usage synopsis; new "Disaster recovery" section with the content of design §11), main spec §3.7 (listing output).

**Interfaces:**
- Consumes: `Options.Restore.List` (Task 1).
- Produces:
  ```go
  // engine
  // GatherSource lists the datasets in scope under o.Source.Dataset
  // (honouring Recursive, SkipParent, ExcludeDatasets), in listing order,
  // and their snapshots sorted oldest to newest.
  func GatherSource(ctx context.Context, src endpoint.Endpoint, o *cli.Options) (datasets []string, snaps map[string][]zfs.Snapshot, err error)

  // RestorePoint is one snapshot name shared by Have datasets.
  type RestorePoint struct {
      Name    string
      Created uint64   // latest creation among the snapshots of that name, seconds
      Have    int
      Missing []string // datasets in scope without it, in listing order
  }
  // RestorePoints groups snaps by name: newest Created first, then by Name.
  func RestorePoints(datasets []string, snaps map[string][]zfs.Snapshot) []RestorePoint
  // RenderRestorePoints writes the table of design §10; total is len(datasets).
  func RenderRestorePoints(w io.Writer, pts []RestorePoint, total int, loc *time.Location)

  // app.Env
  // Location is the time zone of displayed times; nil means time.Local.
  Location *time.Location

  // app (remote.go): opens only the source side of o.
  func openSource(ctx context.Context, env Env, o *cli.Options, log *slog.Logger) (endpoint.Endpoint, func(), error)
  ```
- Table format: `text/tabwriter` with min width 0, tab width 0, padding 3, pad char space, no flags. Header `CREATED`, `SNAPSHOT`, `DATASETS`. `CREATED` is `2006-01-02 15:04` in `loc`. `DATASETS` is `<have>/<total>`. A fourth, headerless column holds `missing: a, b, c` when 1–3 datasets are missing and `missing: N datasets` when more. Every line, the header included, writes all four cells, each terminated by a tab (the fourth may be empty), so the columns align over the whole table; trailing spaces are then trimmed from every line.
- No snapshots in scope: stderr `WARNING: no snapshots under <SOURCE>`, exit 1. A listing failure is exit 2.

- [ ] **Step 1: Write the failing tests**

```go
// restorepoints_test.go
func TestRestorePoints(t *testing.T) {
    datasets := []string{"b/d", "b/d/a", "b/d/b", "b/d/c", "b/d/e"}
    // snaps: "daily1" on all five with creations 100..104; "daily2" on all but b/d/e, creations 200..203;
    //        "solo" on b/d/a only, creation 150.
    // RestorePoints → [
    //   {Name:"daily2", Created:203, Have:4, Missing:["b/d/e"]},
    //   {Name:"solo",   Created:150, Have:1, Missing:["b/d","b/d/b","b/d/c","b/d/e"]},
    //   {Name:"daily1", Created:104, Have:5, Missing:nil},
    // ]
    // Two names with the same Created are ordered by Name.
    // RestorePoints(datasets, nil) is empty.
}

func TestRenderRestorePoints(t *testing.T) {
    // The three points above, total 5, loc time.UTC → exactly:
    // "CREATED            SNAPSHOT   DATASETS\n" +
    // "1970-01-01 00:03   daily2     4/5        missing: b/d/e\n" +
    // "1970-01-01 00:02   solo       1/5        missing: 4 datasets\n" +
    // "1970-01-01 00:01   daily1     5/5\n"
}

// TestGatherSource: with the fake; -r lists the tree in listing order; SkipParent drops the root;
//   ExcludeDatasets drops matches; a missing source root returns an error matching zfs.ErrNoDataset.

// app/restore_test.go (Env.Location = time.UTC)
// TestRestoreList: add tank/data@two. run("restore","--list","-r","tank/data") == 0;
//   stdout equals testdata/restore_list.golden (rows "two 1/2 missing: tank/data/vm" then "one 2/2");
//   no mutating call; NewLocal called once.
// TestRestoreListNoSnapshots: a world whose tank/empty has no snapshot → exit 1,
//   stderr contains "WARNING: no snapshots under tank/empty", stdout empty.
// TestRestoreListMissingSource: run("restore","--list","tank/nope") → exit 2.
```

- [ ] **Step 2: Run to see them fail**

Run: `go test ./internal/engine/ ./internal/app/ -run 'RestorePoints|GatherSource|TestRestoreList' -v` — Expected: FAIL.

- [ ] **Step 3: Implement.** `GatherSource` shares the scope rules with `Gather` (extract them rather than copying). `openSource` shares the per-location opening with `openSides`. `Main` routes `Restore.List` to the listing function, replacing Task 6's placeholder.

- [ ] **Step 4: Write the README section.** Three commands (list, restore newest, restore at a point), the empty-target rule, and every note of design §11, including the R16 re-run rule and the warning about replicating back after restoring an older point.

- [ ] **Step 5: Run** `mise run check` — Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal README.md docs/superpowers/specs/2026-10-18-syngoid-design.md
git commit -m "feat: restore --list and disaster recovery documentation"
```

---

### Task 8: Integration tests on real ZFS

These run only on Linux with ZFS, as root, with `SYNGOID_IT=1` (README "Integration tests"). They also settle the three open points of design §7. If one of those turns out differently from what the design assumes, stop and report it instead of changing the design's rules.

**Files:**
- Create: `test/integration/restore_test.go`
- Modify: design note §7: replace the "To verify" list with what was observed.

**Interfaces:**
- Consumes: the helpers of `test/integration/helpers_test.go` (`begin`, `zfs`, `syngoid`, `syngoidOK`, `syngoidAs`, `snaps`, `snapsR`, `prop`, `exists`, `interruptSync`, `requireResumeToken`, `resumeToken`, `writeRandomFile`, `fileSHA256`, `mountpoint`, `holdTags`, `bookmarksR`, `equal`). The "backup" is a tree under `srcPool`, the "new host" a location under `dstPool`.

- [ ] **Step 1: Write the tests** (build tag `integration`)

```go
// TestITRestoreNewest: backup tree b, b/c with snapshots s1, s2 (b) and s1 (b/c), a file in each.
//   restore -r → target has exactly [s2] and [s1]; guids and file hashes equal the source's;
//   afterwards snapsR(source) is unchanged, bookmarksR(source) is empty, no snapshot has a hold.
// TestITRestoreSnapshotAndAt: snapshots s1, s2 a second apart (waitNextSecond);
//   --snapshot s1 → exactly [s1]; into another target, --at <creation of s1 as RFC 3339> → exactly [s1].
// TestITRestoreRerunAndConflict: restore; re-run → exit 0, stdout has "already restored at @";
//   zfs snapshot <target>@local; re-run → exit 2, stderr has "restore needs an empty target",
//   and the target's snapshot list is unchanged.
// TestITRestoreExistingTargetSendsNothing: zfs create the child target first → exit 2;
//   the parent target does not exist afterwards.
// TestITRestoreInterruptThenResume: 300 MiB file; interruptSync(t, "restore", src, dst);
//   requireResumeToken(dst); re-run → exit 0, token "-", file hash equal, exactly one snapshot.
// TestITRestoreClone: b/o@s1 and clone b/cl of it, snapshot b/cl@s1;
//   restore -r → target cl exists with origin "-" and the clone's data.
// TestITRestoreUnprivileged: syngoidAs(itUser, …) with only `send` delegated on the source tree and
//   `receive,create,mount` on the target parent → exit 0. (design §7 item 2)
// TestITRestoreAbortOwnPartial: as itUser, interrupt a restore, then destroy the source snapshot it was
//   receiving and take a new one; re-run. Record what happens: the design expects a conflict (the token is
//   for another snapshot). Then, as itUser, run `zfs receive -A <target>` and record whether it succeeds
//   with the delegations above and whether the dataset is gone afterwards. (design §7 items 1 and 3)
// TestITRestoreThenReplicateBack: restore newest; write a file on the restored dataset;
//   syngoid <restored> <backup> → exit 0 and stdout shows an incremental send, not a full one.
```

- [ ] **Step 2: Run them**

Run: `scripts/ci/setup-zfs.sh` once on a throwaway Ubuntu host, then
`sudo -E env PATH=$PATH SYNGOID_IT=1 go test -tags integration -v -timeout 45m -run TestITRestore ./test/integration/...`
Expected: PASS. `TestITRestoreAbortOwnPartial` logs its observations with `t.Log`.

- [ ] **Step 3: Record the observations** of design §7 in the design note. If `zfs receive -A` needs a permission beyond `receive,create,mount`, add it to the restore needs (main spec §6, README) and to `TestITRestoreUnprivileged` in the same commit.

- [ ] **Step 4: Run** `mise run check` (lint covers the `integration` tag) — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add test/integration docs
git commit -m "test: restore against real ZFS"
```
