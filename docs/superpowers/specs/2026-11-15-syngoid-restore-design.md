# syngoid — `restore` Command Design

- **Date:** 2026-11-15
- **Status:** Approved
- **Main spec:** `docs/superpowers/specs/2026-10-18-syngoid-design.md` (this note adds a subcommand; the
  implementation edits the main spec's command-line, planner, preflight and testing sections in the same commits as
  the code)

## 1. Motivation

syngoid replicates snapshots between hosts so that a host can be rebuilt after a disaster. Rebuilding is the part
that has no dedicated command today. It is possible with `syngoid -r --no-stream --no-sync-snap BACKUP NEWHOST`, but:

- three flags must be remembered during an outage;
- only the newest snapshot can be restored, which is the wrong one when the newest backup already contains the
  corruption or the deletion;
- `sync` is allowed to roll back, send incrementals into and (with `--force-delete`) destroy existing target
  datasets, which is more than a restore should ever do.

Plain `zfs` does not cover it either: `zfs send -R` always carries the whole snapshot history and needs one snapshot
name shared by every descendant.

**Done when:** one command restores a dataset tree from a backup to an empty location on a chosen host, one snapshot
per dataset, at the newest point or at a point the user picks by time or by name; a second command lists the points
available; the backup is left unmodified; and normal replication from the restored host back to the backup continues
incrementally when the newest point was restored.

## 2. Goals, non-goals, anti-goals

**Goals**

- `syngoid restore SOURCE TARGET`: one full send per dataset, recursive with `-r`.
- Choose the point: newest (default), `--at TIME`, or `--snapshot NAME`.
- `syngoid restore --list SOURCE`: show the restore points of a backup.
- Reuse the engine: resume, `--jobs`, progress, preflight, `--dry-run`, `--confirm`, remote endpoints.
- A restore that was interrupted can be re-run and continues.

**Non-goals (first version)**

- Restoring onto existing datasets (in-place repair by rollback or incremental). May be added later without changing
  the command's shape.
- Restoring clones as clones.
- Snapshot filters (`--include-snaps`, `--exclude-snaps`) on `restore`.
- Cleaning up `syncoid_<oldhost>_…` snapshots that arrive with the restore.
- A `permissions` variant for restore; `restore --dry-run` already stops on a failed preflight with the report.

**Anti-goals**

- `restore` must never destroy, roll back or overwrite anything on the target, and never create, destroy, hold or
  release anything on the source. The one exemption is the partial receive state of this restore's own interrupted
  transfer, which may be aborted (`zfs receive -A`) when it can no longer be resumed (§7).
- A partial restore must never look like a complete one (exit code and summary must show it).

## 3. Approaches

| | Approach | Trade-off |
|---|---|---|
| **A (chosen)** | A restore mode in the existing pipeline with its own small planner. Gather, preflight, executor, progress and rendering are reused; the executor takes the planner as a parameter. | One refactor of the executor. Restore's rules live in one pure function that cannot emit a destructive or incremental action. |
| B | Translate `restore` into `sync --no-stream --no-sync-snap` with a trimmed snapshot list and keep `PlanDataset`. | Least code, but `PlanDataset` and the executor's re-plans know how to roll back, send incrementals and use bookmarks; "non-empty target fails" would be a guard outside them, with a risk of `sync` behaviour leaking into a restore. |
| C | A standalone send/receive loop. | Duplicates resume, parallel jobs, progress and preflight, or ships without them. |

A was picked by the user: the command run during an outage should be unable to plan anything but full sends.

## 4. Decisions

| # | Decision | Decided by |
|---|---|---|
| R1 | New subcommand `restore`; `sync` stays the default. | user |
| R2 | Point selection: newest by default, `--at TIME`, `--snapshot NAME`; the two flags are mutually exclusive. | user |
| R3 | Listing mode `restore --list`. | user |
| R4 | Snapshot filters are not available on `restore`. | user |
| R5 | Only an empty target is supported; an existing target dataset fails the whole command before anything is sent. No `--force-delete`. | user |
| R6 | Data only by default; `--preserve-properties` opts in, as for `sync`. | user |
| R7 | Approach A. | user |
| R8 | An existing target dataset is accepted only when it is this restore's own earlier work (§6.3). | proposal, not objected to |
| R9 | A dataset with no qualifying snapshot is a soft failure (exit 1), and its descendants are skipped. | proposal, not objected to |
| R10 | Clones are restored as full, independent datasets. | proposal, not objected to |
| R11 | The `syncoid:sync` property is ignored by `restore`. | proposal, not objected to |
| R12 | No holds on the source; the receive never uses `-F`. | proposal / author's call |
| R13 | A date without a time in `--at` means 00:00:00 of that day, local time. | author's call |
| R14 | `--no-resume` is not available on `restore`: receives are always resumable. | author's call |
| R15 | Restore may abort its own partial receive state, and nothing else on the target. | author's call |
| R16 | The rule for existing target datasets stays strict (§6.3): a re-run works only while the selection still resolves to the same snapshots and the restored datasets are untouched. | user |

## 5. Command line

```
syngoid restore [flags] SOURCE TARGET
syngoid restore --list [flags] SOURCE
```

SOURCE and TARGET follow the rules of `sync` (local or `[user@]host:dataset`, different, neither inside the other).

### 5.1 Flags

| Flag | Meaning |
|---|---|
| `--at TIME` | For each dataset, restore the newest snapshot created at or before TIME. |
| `--snapshot NAME` | For each dataset, restore the snapshot named NAME (the part after `@`). |
| `--list` | List the restore points of SOURCE and exit; takes no TARGET. |

Kept from `sync` with the same meaning: `-r/--recursive`, `--skip-parent`, `--exclude-datasets`,
`--preserve-properties`, `--preserve-recordsize`, `-j/--jobs`, `--compress`, `--buffer-size`, `--progress`,
`--dry-run`, `--confirm`, `--sudo`, `--source-sudo`, `--target-sudo`, `-o/--ssh-option`, `--ssh-config`,
`--sendoptions`, `--recvoptions`, `--debug`, `--quiet`, `--otel-endpoint`, and the agent distribution flags
(`--bin-store`, `--remote-bin-dir`, `--no-download`, `--download-url`).

Every other `sync` flag is a usage error on `restore`, with a message naming the flag: `--identifier`, `--no-stream`,
`--no-sync-snap`, `--keep-sync-snap`, `--create-bookmark`, `--use-hold`, `--no-rollback`,
`--delete-target-snapshots`, `--force-delete`, `--include-snaps`, `--exclude-snaps`, `--no-clone-handling`,
`--no-resume` (R14), `--dumpsnaps`, and the deprecated or ignored aliases `sync` still accepts.
`--at`, `--snapshot` and `--list` are usage errors on `sync` and `permissions`.

`--sendoptions` containing `R` is a usage error on `restore` (it sends one snapshot per dataset, never a replication
stream): `--sendoptions R is not available with restore: restore sends one snapshot per dataset`.

With `--list`, only `-r`, `--skip-parent`, `--exclude-datasets`, the source sudo flags, the ssh flags, the agent
distribution flags, `--debug` and `--quiet` are accepted.

### 5.2 `--at TIME`

Accepted forms: `2026-11-10`, `2026-11-10T03:00`, `2026-11-10T03:00:00`, and RFC 3339 with an offset or `Z`. A space
may replace the `T`. Without an offset the time is local to the machine running syngoid. A date alone is 00:00:00 of
that day (R13), so `--at 2026-11-10` means "as it was before the 10th began". Anything else is a usage error that
lists the accepted forms.

Comparison uses the snapshot's `creation` property (seconds), inclusive: `creation <= TIME`. Among the qualifying
snapshots the newest by the engine's existing ordering (`SortSnapshots`) is chosen.

### 5.3 `--snapshot NAME`

NAME is a bare snapshot name. A NAME that is empty or contains `@`, `/` or `#` is a usage error.

### 5.4 Nothing qualifies anywhere

If the selection matches no dataset at all (no snapshot at or before TIME, or NAME found nowhere, or no snapshots at
all), the command fails before preflight with exit 2 and a message that suggests `syngoid restore --list`.

## 6. Planning

### 6.1 Selection

A pure function picks the snapshot of one dataset from its source snapshots (sorted oldest to newest) and the
selection (newest, at TIME, or NAME). It returns the snapshot or "none". The same function serves the planner, the
executor's re-plans and `--list`. Selection is applied at planning time, not by trimming the gathered state, so a
re-gather inside the executor selects again with the same rule.

### 6.2 `PlanRestore`

A pure function, the counterpart of `PlanDataset`, from one `DatasetState` and the selection to a `Plan`. It can only
produce these outcomes:

| Situation | Outcome |
|---|---|
| No snapshot qualifies | soft failure: `no snapshot of <source> at or before <TIME>` / `no snapshot <NAME> on <source>` / `no snapshots on <source>` |
| Target does not exist | `SendFull{chosen}` |
| Target exists with a resume token, and resuming is acceptable (§6.3) | `Resume{token}` |
| Target exists and is this restore's finished work (§6.3) | nothing to do: `already restored at @<chosen>` |
| Target exists otherwise | critical: `target <target> already exists` |

It never emits `SendIncremental`, `SendFromBookmark`, `SendClone`, `ForceDestroyTarget`, `DestroyTargetSnaps`,
`PruneSyncSnaps`, `Hold`, `Release` or `CreateBookmark`. A unit test asserts this over the whole case table.

A clone (source dataset with an origin) is planned like any other dataset: a full send (R10). The size estimate
therefore shows its real, larger size. `syncoid:sync` is not consulted (R11).

### 6.3 Existing target datasets

An existing target dataset is acceptable in exactly two cases:

1. **Finished:** it has exactly one snapshot, with the chosen snapshot's name and guid, and no resume token.
2. **Interrupted:** it has a resume token and no snapshots, and the token is for the chosen snapshot (its `toguid`
   equals the chosen snapshot's guid).

How the token's guid is read is left to the builder. `zfs send -nvP -t <token>` prints `toguid` on Linux, FreeBSD
and illumos (see `internal/zfs/testdata/*/estimate_resume.txt`).

The rule is deliberately strict (R16). The user's reasoning: a backup being restored from is not expected to change.
Restoring the newest point means the original host is down, so no new snapshot arrives on the backup; going back in
time on a live host means an explicit `--at` or `--snapshot`, which new snapshots on the backup do not affect.

Anything else is a conflict, including: a different snapshot, extra snapshots, a token for another snapshot, or a
dataset with neither snapshots nor token. With `--skip-parent` the top target dataset is not restored and so is not
checked. A target that is a pool root always exists and is therefore always a conflict; the message for that case
points to `--skip-parent`.

### 6.4 Whole-command check

After gathering and before preflight output, dry-run rendering or any transfer, every dataset in scope is planned.
If any plan is the "already exists" conflict, the command prints all conflicting target datasets and exits 2. Nothing
is sent, including to datasets that had no conflict.

```
CRITICAL ERROR: restore needs an empty target; these datasets already exist:
  root@newhost:tank/data/db
  root@newhost:tank/data/www
Destroy them first if they are to be replaced, or restore to another location.
```

If a conflict only appears later (a dataset created between the check and its receive), that dataset fails as
critical and the run continues for datasets that do not depend on it, as in `sync`.

### 6.5 Missing snapshots in the tree

A dataset with no qualifying snapshot is a soft failure. Its descendants cannot be received without it, so the
existing dependency rule skips them as soft failures too. The run exits 1 and the summary lists them (R9). No empty
placeholder dataset is created.

## 7. Execution

- The executor gains a planner parameter (a function from `DatasetState` to `Plan`); `sync` passes `PlanDataset` with
  its options, `restore` passes `PlanRestore` with the selection. Every place that re-plans (`regather`, `refresh`,
  `replan` in remediation) uses it. With `sync` the behaviour and log sequence are unchanged.
- Restore runs without a sync snapshot: no snapshot creation phase, no pruning.
- Order and parallelism are the existing ones: a child waits for its parent, since every parent needs a first send.
- The receive is the existing resumable receive without `-F` (R12). On an unprivileged Linux target it keeps `-u` and
  the property exclusions of `sync`.
- Remediation: restore keeps two paths. When the snapshot to send is gone from the source, it re-plans. When a
  resume fails because its source snapshot is gone, it aborts that partial receive state (R15) and re-plans. The
  `--no-resume` retry path is unreachable (R14), and so is `destroyConflict`, because `--force-delete` cannot be
  set; tests pin both.
- Ctrl-C, exit codes, summary and progress are those of `sync`.

**To verify on real ZFS during implementation** (integration tests, in the style of the main spec's verified list):

1. What `zfs receive -A` leaves behind for an interrupted full receive into a new dataset (nothing, or an empty
   dataset). If an empty dataset can remain, §6.3 gains a third acceptable case for it or the abort is followed by
   removing what this run created; the builder picks whichever keeps the anti-goals.
2. An unprivileged full receive without `-F` into a non-existent dataset works with `receive,create,mount`.
3. `zfs receive -A` on this restore's partially received new dataset works for an unprivileged user with only the
   delegations of §8. If it needs more, that permission is added to the target needs of §8 and to the README.

## 8. Preflight

Restore uses the existing preflight with needs derived for a restore:

| Side | Needs |
|---|---|
| Source | `send` (or `send:raw` / `send:encrypted` with `w` in `--sendoptions`) |
| Target | `receive` (rendered as the existing preflight does for a receive without `-F`), `create`, `mount`; plus `recordsize` with `--preserve-recordsize`; plus each property carried by `--preserve-properties` (and `userprop`) |

No `snapshot`, `destroy`, `hold`, `rollback` or `bookmark` on either side. The report and the ready-to-paste
`zfs allow` lines are unchanged.

## 9. Dry run and confirm

`--dry-run` and `--confirm` print the existing plan rendering: one `send full @<snapshot>` line per dataset with its
size estimate, and the soft failures of §6.5. With `--preserve-properties`, each dataset's plan also lists the
properties that will be set on the target (name and value), so a user who does not know how the backup was made can
see what would be carried before opting in. This listing is restore output only; `sync --dry-run` is unchanged.

## 10. Listing

`syngoid restore --list [-r] SOURCE` opens only the source, gathers datasets and snapshots (honouring `-r`,
`--skip-parent` and `--exclude-datasets`) and prints one row per distinct snapshot name, newest first:

```
$ syngoid restore --list -r nas:backup/data
CREATED            SNAPSHOT                              DATASETS
2026-11-15 00:00   autosnap_2026-11-15_00:00:02_daily    12/12
2026-11-14 00:00   autosnap_2026-11-14_00:00:01_daily    12/12
2026-11-13 00:00   autosnap_2026-11-13_00:00:02_daily    11/12   missing: backup/data/new-vm
```

- CREATED is the latest `creation` among the snapshots of that name, in local time, to the minute.
- DATASETS is how many datasets in scope have a snapshot of that name, over the number in scope.
- `missing:` names the datasets without it when at most three are missing; otherwise `missing: N datasets`.
- Rows are ordered by CREATED, newest first, then by name.
- A SOURCE with no snapshots prints a message on stderr and exits 1. Other failures exit 2.

The output is for people; no machine-readable format in this version.

## 11. Documentation

- README: a "Disaster recovery" section with the three commands (list, restore newest, restore at a point), the
  empty-target rule, and these notes:
  - Restored datasets inherit their properties from the target pool unless `--preserve-properties` is given, and the
    backup's own settings (for example `readonly`) are what that flag carries.
  - A re-run continues an interrupted restore only while the backup and the restored datasets are unchanged. Finish
    the restore before putting the new host to use (replication back to the backup, sanoid, or any other snapshot
    on the restored datasets makes a re-run fail), and pause pruning on the backup while a restore with `--at` is
    in progress.
  - After restoring the newest point, `syngoid NEWHOST BACKUP` continues incrementally.
  - After restoring an older point, the backup holds newer snapshots; a normal `sync` from the restored host would
    roll the backup back and destroy them. Keep them by replicating to a new backup location, or accept the loss
    knowingly.
  - Sync snapshots named after the lost host (`syncoid_<oldhost>_…`) arrive with the restore and are not pruned by
    the new host; destroy them by hand when no longer needed.
- Main spec: add `restore` to the command line section, `PlanRestore` to the planner section, the restore needs to
  the preflight table, and the tests below to the testing section.
- `syngoid --help` lists the subcommand and its flags.

## 12. Testing

- **Selection:** table-driven over newest / `--at` (before, exactly at, after all, before all) / `--snapshot`
  (present, absent), including datasets without `createtxg`.
- **`--at` parsing:** every accepted form, local-time interpretation with a fixed zone, rejected forms.
- **`PlanRestore`:** table-driven over §6.2 and §6.3, plus the assertion that no forbidden action type is ever
  produced.
- **CLI:** accepted and rejected flags per §5.1, `--at` with `--snapshot`, `--list` with a TARGET, new flags on
  `sync`.
- **App, with the fake endpoint:** whole-command conflict check sends nothing; nothing-qualifies error; soft failure
  and skipped descendants give exit 1; `--dry-run` output (golden file); `--list` output (golden file).
- **Executor:** existing tests pass unchanged with the planner parameter; a restore run re-plans with `PlanRestore`.
- **Integration (real ZFS):**
  - restore newest, recursive; data and snapshot guids match the source; exactly one snapshot per target dataset;
  - `--at` and `--snapshot` pick the expected snapshot per dataset;
  - re-running a finished restore does nothing and exits 0;
  - re-running after a new snapshot appeared on the source (default selection), or after a snapshot was added to a
    restored dataset, fails the command with nothing sent;
  - an interrupted restore resumes on re-run;
  - an existing target dataset fails the command with nothing sent and the target unchanged;
  - a clone is restored as an independent dataset;
  - source side is unchanged after a restore (same snapshots, no holds, no bookmarks);
  - unprivileged restore with only the delegations of §8;
  - after restoring the newest point, `syngoid` from the restored dataset back to the backup sends an incremental.

## 13. Left to the builder

- Names and placement of the new types and functions, and how `restore` shares flag parsing with `sync`.
- How the restore options reach the transfer and preflight code (a restore-specific options value, or the existing
  options with fixed values), as long as §6 and §8 hold.
- How the resume token's guid is read (§6.3).
- Exact wording of messages not quoted in this document.
