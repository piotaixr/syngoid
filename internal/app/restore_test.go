package app

import (
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/piotaixr/syngoid/internal/endpoint"
)

// snapNamesOf returns the snapshot names of ds, or nil when ds is absent.
func (se *syncEnv) snapNamesOf(ds string) []string {
	d := se.world.Dataset(ds)
	if d == nil {
		return nil
	}
	var names []string
	for _, s := range d.Snaps {
		names = append(names, s.Name)
	}
	return names
}

// callsWithPrefix returns the recorded endpoint calls starting with one of
// the prefixes.
func (se *syncEnv) callsWithPrefix(prefixes ...string) []string {
	var out []string
	for _, c := range se.ep.Calls() {
		for _, p := range prefixes {
			if strings.HasPrefix(c, p+" ") {
				out = append(out, c)
				break
			}
		}
	}
	return out
}

func (se *syncEnv) wantSnaps(t *testing.T, ds string, want ...string) {
	t.Helper()
	if got := se.snapNamesOf(ds); !slices.Equal(got, want) {
		t.Errorf("snapshots of %s = %v, want %v", ds, got, want)
	}
}

func (se *syncEnv) wantNoReceive(t *testing.T) {
	t.Helper()
	if got := se.callsWithPrefix("Receive"); len(got) != 0 {
		t.Errorf("receive calls = %q, want none", got)
	}
}

func TestRestoreNewest(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	se.world.Snap("tank/data", "two")
	if got := se.run("restore", "-r", "tank/data", "backup/data"); got != 0 {
		t.Fatalf("exit code = %d, want 0; stderr %q", got, se.stderr)
	}
	se.wantSnaps(t, "backup/data", "two")
	se.wantSnaps(t, "backup/data/vm", "one")
	for _, pair := range [][2]string{{"tank/data", "backup/data"}, {"tank/data/vm", "backup/data/vm"}} {
		src, dst := se.world.Dataset(pair[0]), se.world.Dataset(pair[1])
		if dst == nil || len(dst.Snaps) != 1 {
			continue
		}
		if srcGUID, dstGUID := src.Snaps[len(src.Snaps)-1].GUID, dst.Snaps[0].GUID; srcGUID != dstGUID {
			t.Errorf("%s guid = %s, want %s", pair[1], dstGUID, srcGUID)
		}
	}
	if got := se.callsWithPrefix("Snapshot", "Destroy", "DestroyDataset", "Hold", "Release", "Bookmark"); len(got) != 0 {
		t.Errorf("forbidden calls = %q", got)
	}
}

func TestRestoreSnapshot(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	se.world.Snap("tank/data", "two")
	if got := se.run("restore", "--snapshot", "one", "tank/data", "backup/data"); got != 0 {
		t.Fatalf("exit code = %d, want 0; stderr %q", got, se.stderr)
	}
	se.wantSnaps(t, "backup/data", "one")
}

func TestRestoreAt(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	// The fake clock gives tank/data@one 1700000001 s, tank/data/vm@one
	// 1700000002 s and tank/data@two 1700000003 s.
	se.world.Snap("tank/data", "two")
	if got := se.run("restore", "-r", "--at", "2023-11-14T22:13:22Z", "tank/data", "backup/data"); got != 0 {
		t.Fatalf("exit code = %d, want 0; stderr %q", got, se.stderr)
	}
	se.wantSnaps(t, "backup/data", "one")
	se.wantSnaps(t, "backup/data/vm", "one")
}

func TestRestoreRerunDoesNothing(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	if got := se.run("restore", "tank/data", "backup/data"); got != 0 {
		t.Fatalf("first run: exit code = %d, want 0; stderr %q", got, se.stderr)
	}
	first := len(se.ep.Calls())
	se.stdout.Reset()
	if got := se.run("restore", "tank/data", "backup/data"); got != 0 {
		t.Fatalf("second run: exit code = %d, want 0; stderr %q", got, se.stderr)
	}
	if want := "already restored at @one"; !strings.Contains(se.stdout.String(), want) {
		t.Errorf("stdout does not contain %q:\n%s", want, se.stdout)
	}
	for _, c := range se.ep.Calls()[first:] {
		if strings.HasPrefix(c, "Receive ") {
			t.Errorf("second run received: %q", c)
		}
	}
}

// TestRestoreConflictSendsNothing: backup/data is already restored,
// backup/data/vm exists, and tank/data/www still needs a send; the conflict
// on vm stops the whole command, so www is not received either.
func TestRestoreConflictSendsNothing(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	if got := se.run("restore", "tank/data", "backup/data"); got != 0 {
		t.Fatalf("setup: exit code = %d, want 0; stderr %q", got, se.stderr)
	}
	se.world.Create("backup/data/vm", "")
	se.world.Create("tank/data/www", "")
	se.world.Snap("tank/data/www", "one")
	first := len(se.ep.Calls())
	if got := se.run("restore", "-r", "tank/data", "backup/data"); got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
	want := "CRITICAL ERROR: restore needs an empty target; these datasets already exist:\n" +
		"  backup/data/vm\n" +
		"Destroy them first if they are to be replaced, or restore to another location.\n"
	if !strings.Contains(se.stderr.String(), want) {
		t.Errorf("stderr does not contain %q:\n%s", want, se.stderr)
	}
	if calls := se.ep.Calls()[first:]; len(calls) != 0 {
		t.Errorf("calls = %q, want none", calls)
	}
	if se.world.Dataset("backup/data/www") != nil {
		t.Error("backup/data/www was created")
	}
}

func TestRestorePoolRootTarget(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	if got := se.run("restore", "-r", "tank/data", "backup"); got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
	want := "target backup is a pool root and always exists; restore its children with --skip-parent"
	if !strings.Contains(se.stderr.String(), want) {
		t.Errorf("stderr does not contain %q:\n%s", want, se.stderr)
	}
	se.wantNoReceive(t)
}

func TestRestoreSkipParentIntoPoolRoot(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	if got := se.run("restore", "-r", "--skip-parent", "tank/data", "backup"); got != 0 {
		t.Fatalf("exit code = %d, want 0; stderr %q", got, se.stderr)
	}
	se.wantSnaps(t, "backup/vm", "one")
}

func TestRestoreNothingQualifies(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{
			name: "name",
			args: []string{"--snapshot", "zz", "-r"},
			want: "CRITICAL ERROR: no dataset under tank/data has a snapshot named zz; list the restore points with: syngoid restore --list -r tank/data\n",
		},
		{
			name: "scope flags",
			args: []string{"--snapshot", "zz", "-r", "--skip-parent", "--exclude-datasets=/scratch$", "--exclude-datasets=^x"},
			want: "CRITICAL ERROR: no dataset under tank/data has a snapshot named zz; list the restore points with: syngoid restore --list -r --skip-parent --exclude-datasets='/scratch$' --exclude-datasets='^x' tank/data\n",
		},
		{
			name: "at",
			args: []string{"--at", "1970-01-01T00:00:10Z"},
			want: "CRITICAL ERROR: no dataset under tank/data has a snapshot at or before 1970-01-01T00:00:10Z; list the restore points with: syngoid restore --list tank/data\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			se := newSyncEnv(t, endpoint.HostInfo{})
			args := append(append([]string{"restore"}, tc.args...), "tank/data", "backup/data")
			if got := se.run(args...); got != 2 {
				t.Fatalf("exit code = %d, want 2", got)
			}
			if !strings.Contains(se.stderr.String(), tc.want) {
				t.Errorf("stderr does not contain %q:\n%s", tc.want, se.stderr)
			}
			if calls := se.ep.Calls(); len(calls) != 0 {
				t.Errorf("calls = %q, want none", calls)
			}
		})
	}
	t.Run("no snapshots", func(t *testing.T) {
		se := newSyncEnv(t, endpoint.HostInfo{})
		se.world.Create("tank/empty", "")
		if got := se.run("restore", "tank/empty", "backup/empty"); got != 2 {
			t.Fatalf("exit code = %d, want 2", got)
		}
		want := "CRITICAL ERROR: no dataset under tank/empty has any snapshot; list the restore points with: syngoid restore --list tank/empty\n"
		if !strings.Contains(se.stderr.String(), want) {
			t.Errorf("stderr does not contain %q:\n%s", want, se.stderr)
		}
	})
}

func TestRestoreEmptyScope(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	if got := se.run("restore", "-r", "--exclude-datasets=^tank", "tank/data", "backup/data"); got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
	want := "CRITICAL ERROR: no dataset in scope under tank/data\n"
	if !strings.Contains(se.stderr.String(), want) || strings.Contains(se.stderr.String(), "--list") {
		t.Errorf("stderr = %q, want %q and no --list hint", se.stderr, want)
	}
}

func TestRestoreParentWithoutSnapshot(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	se.world.Snap("tank/data/vm", "only")
	if got := se.run("restore", "-r", "--snapshot", "only", "tank/data", "backup/data"); got != 1 {
		t.Fatalf("exit code = %d, want 1; stderr %q", got, se.stderr)
	}
	se.wantNoReceive(t)
	out := se.stdout.String()
	for _, want := range []string{
		"  tank/data → backup/data: failed (soft) (no snapshot only on tank/data)\n",
		"  tank/data/vm → backup/data/vm: failed (soft) (skipped: dependency tank/data failed)\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout does not contain %q:\n%s", want, out)
		}
	}
}

func TestRestoreMissingTargetParent(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	if got := se.run("restore", "tank/data", "backup/a/b"); got != 2 {
		t.Fatalf("exit code = %d, want 2; stdout %q", got, se.stdout)
	}
	if se.world.Dataset("backup/a") != nil {
		t.Error("backup/a was created")
	}
	if !strings.Contains(se.stderr.String(), "backup/a/b") {
		t.Errorf("stderr does not name backup/a/b:\n%s", se.stderr)
	}
}

func TestRestoreDryRun(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	se.world.SetProp("tank/data", "compression", "zstd")
	if got := se.run("restore", "-r", "--dry-run", "--preserve-properties", "tank/data", "backup/data"); got != 0 {
		t.Fatalf("exit code = %d, want 0; stderr %q", got, se.stderr)
	}
	want, err := os.ReadFile("testdata/restore_dryrun.golden")
	if err != nil {
		t.Fatal(err)
	}
	if got := se.stdout.String(); got != string(want) {
		t.Errorf("stdout differs from golden:\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
	if calls := se.ep.Calls(); len(calls) != 0 {
		t.Errorf("dry run made mutating calls: %q", calls)
	}
}

func TestRestorePreflight(t *testing.T) {
	info := endpoint.HostInfo{User: "backup", UID: 1000, OS: "linux"}

	se := newSyncEnv(t, info)
	se.ep.Faults.Allow = map[string]string{
		"tank/data": allowOutput("tank/data", "backup", "send"),
		"backup":    allowOutput("backup", "backup", "receive,create,mount"),
	}
	if got := se.run("restore", "tank/data", "backup/data"); got != 0 {
		t.Fatalf("granted: exit code = %d, want 0; stderr %q", got, se.stderr)
	}

	se = newSyncEnv(t, info)
	se.ep.Faults.Allow = map[string]string{
		"tank/data": allowOutput("tank/data", "backup", "send"),
		"backup":    allowOutput("backup", "backup", "create,mount"),
	}
	if got := se.run("restore", "tank/data", "backup/data"); got != 2 {
		t.Fatalf("missing receive: exit code = %d, want 2", got)
	}
	var allowLines []string
	for _, line := range strings.Split(se.stderr.String(), "\n") {
		if strings.Contains(line, "zfs allow") {
			allowLines = append(allowLines, line)
		}
	}
	if !slices.ContainsFunc(allowLines, func(l string) bool {
		return strings.HasSuffix(l, " backup") && strings.Contains(l, "receive")
	}) {
		t.Errorf("no zfs allow line for backup lists receive:\n%s", se.stderr)
	}
	for _, line := range allowLines {
		for _, p := range []string{"snapshot", "destroy", "hold", "rollback", "bookmark"} {
			if strings.Contains(line, p) {
				t.Errorf("zfs allow line asks for %s: %q", p, line)
			}
		}
	}
}

func TestRestoreConfirmNoTTY(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	if got := se.run("restore", "--confirm", "tank/data", "backup/data"); got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
	if want := "CRITICAL ERROR: --confirm requires an interactive terminal\n"; se.stderr.String() != want {
		t.Errorf("stderr = %q, want %q", se.stderr, want)
	}
}

func TestRestoreList(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	se.Location = time.UTC
	se.world.Snap("tank/data", "two")
	if got := se.run("restore", "--list", "-r", "tank/data"); got != 0 {
		t.Fatalf("exit code = %d, want 0; stderr %q", got, se.stderr)
	}
	want, err := os.ReadFile("testdata/restore_list.golden")
	if err != nil {
		t.Fatal(err)
	}
	if got := se.stdout.String(); got != string(want) {
		t.Errorf("stdout differs from golden:\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
	if calls := se.ep.Calls(); len(calls) != 0 {
		t.Errorf("listing made mutating calls: %q", calls)
	}
	if len(se.newLocalCalls) != 1 {
		t.Errorf("NewLocal called %d times, want 1", len(se.newLocalCalls))
	}
}

func TestRestoreListNoSnapshots(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	se.world.Create("tank/empty", "")
	if got := se.run("restore", "--list", "tank/empty"); got != 1 {
		t.Fatalf("exit code = %d, want 1", got)
	}
	if !strings.Contains(se.stderr.String(), "WARNING: no snapshots under tank/empty") {
		t.Errorf("stderr = %q", se.stderr)
	}
	if se.stdout.Len() != 0 {
		t.Errorf("stdout = %q, want empty", se.stdout)
	}
}

func TestRestoreListMissingSource(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	if got := se.run("restore", "--list", "tank/nope"); got != 2 {
		t.Fatalf("exit code = %d, want 2; stdout %q", got, se.stdout)
	}
}
