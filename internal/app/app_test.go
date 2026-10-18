package app

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/endpoint/fake"
	"github.com/piotaixr/syngoid/internal/zfs"
)

func testEnv() (Env, *bytes.Buffer, *bytes.Buffer) {
	var stdout, stderr bytes.Buffer
	return Env{
		Stdin:    strings.NewReader(""),
		Stdout:   &stdout,
		Stderr:   &stderr,
		Hostname: func() (string, error) { return "testhost", nil },
		Now:      time.Now,
	}, &stdout, &stderr
}

func TestMainVersion(t *testing.T) {
	env, stdout, _ := testEnv()
	if got := Main(context.Background(), []string{"version"}, env); got != 0 {
		t.Fatalf("exit code = %d, want 0", got)
	}
	out := stdout.String()
	for _, want := range []string{"syngoid dev", "protocol 1"} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout %q does not contain %q", out, want)
		}
	}
}

func TestMainUnknownFlagExit2(t *testing.T) {
	env, _, stderr := testEnv()
	if got := Main(context.Background(), []string{"--nope"}, env); got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
	if stderr.Len() == 0 {
		t.Error("stderr is empty, want an error message")
	}
}

func TestMainUnknownCommandExit2(t *testing.T) {
	env, _, stderr := testEnv()
	if got := Main(context.Background(), []string{"bogus"}, env); got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
	if stderr.Len() == 0 {
		t.Error("stderr is empty, want an error message")
	}
}

// syncEnv is an Env whose local endpoint is one fake host holding both the
// source (tank/...) and the target (backup/...) datasets.
type syncEnv struct {
	Env
	stdout, stderr *bytes.Buffer
	world          *fake.World
	ep             *fake.Endpoint
	newLocalCalls  []bool
}

func newSyncEnv(t *testing.T, info endpoint.HostInfo) *syncEnv {
	t.Helper()
	env, stdout, stderr := testEnv()
	w := fake.NewWorld()
	w.Create("tank", "")
	w.Create("tank/data", "")
	w.Snap("tank/data", "one")
	w.Create("tank/data/vm", "")
	w.Snap("tank/data/vm", "one")
	w.Create("backup", "")
	se := &syncEnv{Env: env, stdout: stdout, stderr: stderr, world: w, ep: fake.New(w, info)}
	se.Now = func() time.Time { return time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC) }
	se.NewLocal = func(sudo bool) endpoint.Endpoint {
		se.newLocalCalls = append(se.newLocalCalls, sudo)
		return se.ep
	}
	return se
}

func (se *syncEnv) run(args ...string) int {
	return Main(context.Background(), args, se.Env)
}

func allowOutput(ds, user, perms string) string {
	return "---- Permissions on " + ds + " ----------------------------------------\n" +
		"Local+Descendent permissions:\n\tuser " + user + " " + perms + "\n"
}

func TestMainSyncEndToEnd(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	if got := se.run("sync", "tank/data", "backup/data", "-r"); got != 0 {
		t.Fatalf("exit code = %d, want 0; stderr %q", got, se.stderr)
	}
	for _, ds := range []string{"backup/data", "backup/data/vm"} {
		if d := se.world.Dataset(ds); d == nil || len(d.Snaps) < 2 {
			t.Errorf("target %s not populated: %+v", ds, d)
		}
	}
	out := se.stdout.String()
	i := strings.LastIndex(out, "Summary:\n")
	if i < 0 {
		t.Fatalf("no summary in stdout:\n%s", out)
	}
	summary := out[i:]
	for _, want := range []string{
		"  tank/data → backup/data: ok, ",
		"  tank/data/vm → backup/data/vm: ok, ",
		"  2 dataset(s): 2 ok, 0 nothing to do, 0 skipped, 0 failed; ",
	} {
		if !strings.Contains(summary, want) {
			t.Errorf("summary %q does not contain %q", summary, want)
		}
	}
	if !strings.HasSuffix(summary, "\n") || strings.Count(summary, "\n") != 4 {
		t.Errorf("summary is not the end of stdout: %q", summary)
	}
	if want := []bool{false, false}; !slices.Equal(se.newLocalCalls, want) {
		t.Errorf("NewLocal sudo args = %v, want %v", se.newLocalCalls, want)
	}
}

func TestMainSyncQuietNoSummary(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	if got := se.run("--quiet", "tank/data", "backup/data"); got != 0 {
		t.Fatalf("exit code = %d, want 0; stderr %q", got, se.stderr)
	}
	if se.stdout.Len() != 0 {
		t.Errorf("stdout = %q, want empty", se.stdout)
	}
}

func TestMainSudoFlagsReachNewLocal(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	se.run("--source-sudo", "tank/data", "backup/data")
	if want := []bool{true, false}; !slices.Equal(se.newLocalCalls, want) {
		t.Errorf("NewLocal sudo args = %v, want %v", se.newLocalCalls, want)
	}
}

func TestMainRemoteNotYet(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	if got := se.run("sync", "nas:tank/a", "tank/b"); got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
	want := "CRITICAL ERROR: remote endpoints are not supported yet (milestone 2)\n"
	if se.stderr.String() != want {
		t.Errorf("stderr = %q, want %q", se.stderr, want)
	}
}

func TestMainFetchServeNotYet(t *testing.T) {
	for cmd, milestone := range map[string]string{"fetch": "milestone 3", "serve": "milestone 2"} {
		se := newSyncEnv(t, endpoint.HostInfo{})
		if got := se.run(cmd); got != 2 {
			t.Errorf("%s: exit code = %d, want 2", cmd, got)
		}
		if want := "not supported yet (" + milestone + ")"; !strings.Contains(se.stderr.String(), want) {
			t.Errorf("%s: stderr %q does not contain %q", cmd, se.stderr, want)
		}
	}
}

func TestMainHelp(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	if got := se.run("--help"); got != 0 {
		t.Fatalf("exit code = %d, want 0", got)
	}
	for _, want := range []string{"Usage:", "--dry-run", "--recursive"} {
		if !strings.Contains(se.stdout.String(), want) {
			t.Errorf("help does not contain %q:\n%s", want, se.stdout)
		}
	}
}

func TestMainDryRun(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	if got := se.run("--dry-run", "-r", "tank/data", "backup/data"); got != 0 {
		t.Fatalf("exit code = %d, want 0; stderr %q", got, se.stderr)
	}
	out := se.stdout.String()
	for _, want := range []string{"tank/data → backup/data\n", "tank/data/vm → backup/data/vm\n", "Total estimated transfer: "} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout does not contain %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "Summary:") {
		t.Errorf("dry run printed a summary:\n%s", out)
	}
	if calls := se.ep.Calls(); len(calls) != 0 {
		t.Errorf("dry run made mutating calls: %q", calls)
	}
	if se.world.Dataset("backup/data") != nil {
		t.Error("dry run created the target")
	}
}

func TestMainConfirmNoTTY(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	if got := se.run("--confirm", "tank/data", "backup/data"); got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
	if want := "--confirm requires an interactive terminal"; !strings.Contains(se.stderr.String(), want) {
		t.Errorf("stderr %q does not contain %q", se.stderr, want)
	}
	if len(se.newLocalCalls) != 0 {
		t.Error("endpoints were created before the usage error")
	}
}

func TestMainConfirmDecline(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	se.StdinIsTTY = true
	se.Stdin = strings.NewReader("n\n")
	if got := se.run("--confirm", "tank/data", "backup/data"); got != 0 {
		t.Fatalf("exit code = %d, want 0; stderr %q", got, se.stderr)
	}
	out := se.stdout.String()
	for _, want := range []string{"tank/data → backup/data\n", "Proceed? [y/N] ", "INFO: aborted by user\n"} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout does not contain %q:\n%s", want, out)
		}
	}
	if calls := se.ep.Calls(); len(calls) != 0 {
		t.Errorf("declined run made calls: %q", calls)
	}
}

func TestMainConfirmAccept(t *testing.T) {
	for _, answer := range []string{"y\n", "YES\n"} {
		se := newSyncEnv(t, endpoint.HostInfo{})
		se.StdinIsTTY = true
		se.Stdin = strings.NewReader(answer)
		if got := se.run("--confirm", "tank/data", "backup/data"); got != 0 {
			t.Fatalf("%q: exit code = %d, want 0; stderr %q", answer, got, se.stderr)
		}
		if d := se.world.Dataset("backup/data"); d == nil || len(d.Snaps) == 0 {
			t.Errorf("%q: target not populated", answer)
		}
		if !strings.Contains(se.stdout.String(), "Summary:") {
			t.Errorf("%q: no summary:\n%s", answer, se.stdout)
		}
	}
}

func TestMainPreflightFail(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{User: "backup", UID: 1000, OS: "linux"})
	if got := se.run("tank/data", "backup/data"); got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
	if want := "CRITICAL ERROR: missing ZFS permissions"; !strings.Contains(se.stderr.String(), want) {
		t.Errorf("stderr %q does not contain %q", se.stderr, want)
	}
	if calls := se.ep.Calls(); len(calls) != 0 {
		t.Errorf("failed preflight made calls: %q", calls)
	}
	if strings.Contains(se.stdout.String(), "Summary:") {
		t.Errorf("failed preflight printed a summary:\n%s", se.stdout)
	}
}

func TestMainPermissionsCommand(t *testing.T) {
	info := endpoint.HostInfo{User: "backup", UID: 1000, OS: "freebsd"}

	se := newSyncEnv(t, info)
	se.ep.Faults.Allow = map[string]string{
		"tank/data": allowOutput("tank/data", "backup", "send,snapshot,mount,destroy"),
		"backup":    allowOutput("backup", "backup", "create,mount,receive,rollback,destroy"),
	}
	if got := se.run("permissions", "tank/data", "backup/data"); got != 0 {
		t.Fatalf("complete: exit code = %d, want 0; stderr %q", got, se.stderr)
	}
	for _, want := range []string{"Full minimal grants:", "[source]  zfs allow -u backup", "[target]  zfs allow -u backup"} {
		if !strings.Contains(se.stdout.String(), want) {
			t.Errorf("stdout does not contain %q:\n%s", want, se.stdout)
		}
	}
	if calls := se.ep.Calls(); len(calls) != 0 {
		t.Errorf("permissions made calls: %q", calls)
	}

	se = newSyncEnv(t, info)
	if got := se.run("permissions", "tank/data", "backup/data"); got != 2 {
		t.Fatalf("missing: exit code = %d, want 2", got)
	}
	for _, want := range []string{"CRITICAL ERROR: missing ZFS permissions", "Full minimal grants:"} {
		if !strings.Contains(se.stdout.String(), want) {
			t.Errorf("stdout does not contain %q:\n%s", want, se.stdout)
		}
	}
}

func TestMainExitCodes(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	w := se.world
	w.Create("tank/empty", "")
	if got := se.run("--no-sync-snap", "tank/empty", "backup/empty"); got != 1 {
		t.Errorf("soft failure: exit code = %d, want 1; stdout %q", got, se.stdout)
	}

	se = newSyncEnv(t, endpoint.HostInfo{})
	se.ep.Faults.Send = func(zfs.SendSpec) error { return errors.New("boom") }
	if got := se.run("tank/data", "backup/data"); got != 2 {
		t.Errorf("critical failure: exit code = %d, want 2; stdout %q", got, se.stdout)
	}
	if !strings.Contains(se.stdout.String(), "Summary:") {
		t.Errorf("no summary after failure:\n%s", se.stdout)
	}
}

func TestMainDumpSnaps(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	se.world.Snap("tank/data", "two")
	if got := se.run("--dry-run", "--dumpsnaps", "tank/data", "backup/data"); got != 0 {
		t.Fatalf("exit code = %d, want 0; stderr %q", got, se.stderr)
	}
	out := se.stdout.String()
	for _, want := range []string{
		"INFO: snapshots of tank/data: one, two\n",
		"INFO: snapshots of backup/data: (none)\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout does not contain %q:\n%s", want, out)
		}
	}
}

func TestMainCancelledExit2(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	se.ep.Faults.BlockSend = true
	se.ep.Faults.Send = func(zfs.SendSpec) error { cancel(); return nil }
	if got := Main(ctx, []string{"tank/data", "backup/data"}, se.Env); got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
	out := se.stdout.String()
	if !strings.Contains(out, "Summary:") || !strings.Contains(out, "failed (cancelled)") {
		t.Errorf("stdout lacks the cancelled summary:\n%s", out)
	}
}

func TestMainHostnameError(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	se.Hostname = func() (string, error) { return "", errors.New("no hostname") }
	if got := se.run("tank/data", "backup/data"); got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
	if !strings.Contains(se.stderr.String(), "CRITICAL ERROR: ") {
		t.Errorf("stderr = %q", se.stderr)
	}
}
