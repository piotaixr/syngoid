package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/piotaixr/syngoid/internal/buildinfo"
	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/endpoint/fake"
	"github.com/piotaixr/syngoid/internal/rpc"
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
	re := regexp.MustCompile(`^syngoid dev \(commit unknown, build [0-9a-f]{12}\) protocol 1\n$`)
	if out := stdout.String(); !re.MatchString(out) {
		t.Errorf("stdout %q does not match %v", out, re)
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

func TestMainFetchNotYet(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	if got := se.run("fetch"); got != 2 {
		t.Errorf("exit code = %d, want 2", got)
	}
	if want := "not supported yet (milestone 3)"; !strings.Contains(se.stderr.String(), want) {
		t.Errorf("stderr %q does not contain %q", se.stderr, want)
	}
}

// duplex joins the initiator's ends of the two pipes into one connection.
type duplex struct {
	io.Reader
	io.Writer
	closers []io.Closer
}

func (d duplex) Close() error {
	for _, c := range d.closers {
		_ = c.Close()
	}
	return nil
}

func TestMainServeHandshake(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	se := newSyncEnv(t, endpoint.HostInfo{User: "backup"})
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	se.Stdin, se.Stdout = inR, outW

	code := make(chan int, 1)
	go func() { code <- Main(context.Background(), []string{"serve"}, se.Env) }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	hello := rpc.Hello{Protocol: buildinfo.ProtocolVersion, Version: buildinfo.Version, BuildHash: buildinfo.BuildHash(), Sudo: true}
	conn := duplex{Reader: outR, Writer: inW, closers: []io.Closer{inW, outR}}
	r, reply, err := rpc.Handshake(ctx, conn, hello, "nas", nil)
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	if reply.User != "backup" {
		t.Errorf("reply.User = %q, want backup", reply.User)
	}
	if err := r.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	select {
	case got := <-code:
		if got != 0 {
			t.Errorf("exit code = %d, want 0", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Main did not return after the remote closed")
	}
	// Stdout is the mux (the handshake above parsed it); stderr is silent.
	if se.stderr.Len() != 0 {
		t.Errorf("stderr = %q, want empty", se.stderr)
	}
	if want := []bool{true}; !slices.Equal(se.newLocalCalls, want) {
		t.Errorf("NewLocal sudo args = %v, want %v", se.newLocalCalls, want)
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

func TestMainConfirmDeclineCancelledExit2(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	se.StdinIsTTY = true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	se.Stdin = readerFunc(func([]byte) (int, error) { cancel(); return 0, io.EOF })
	if got := Main(ctx, []string{"--confirm", "tank/data", "backup/data"}, se.Env); got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

func TestMainPermissionsIgnoresConfirmTTY(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	if got := se.run("permissions", "--confirm", "tank/data", "backup/data"); got != 0 {
		t.Fatalf("exit code = %d, want 0; stderr %q", got, se.stderr)
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
		"tank/data": allowOutput("tank/data", "backup", "send,snapshot,mount,destroy,hold"),
		"backup":    allowOutput("backup", "backup", "create,mount,receive,destroy"),
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

func TestMainPlanOutcomesLogged(t *testing.T) {
	noMatch := func(t *testing.T) *syncEnv {
		se := newSyncEnv(t, endpoint.HostInfo{})
		se.world.Create("backup/data", "")
		se.world.Snap("backup/data", "unrelated")
		return se
	}
	for _, quiet := range []bool{false, true} {
		t.Run(fmt.Sprintf("critical no match quiet=%v", quiet), func(t *testing.T) {
			se := noMatch(t)
			args := []string{"tank/data", "backup/data"}
			if quiet {
				args = append([]string{"--quiet"}, args...)
			}
			if got := se.run(args...); got != 2 {
				t.Fatalf("exit code = %d, want 2", got)
			}
			errOut := se.stderr.String()
			for _, want := range []string{
				"CRITICAL ERROR: Target backup/data exists but has no snapshots matching with tank/data!\n",
				"Cowardly refusing to destroy your existing target.\n",
				"NOTE: Target backup/data dataset is < 64MB used - did you mistakenly run\n",
			} {
				if !strings.Contains(errOut, want) {
					t.Errorf("stderr does not contain %q:\n%s", want, errOut)
				}
			}
			if n := strings.Count(errOut, "CRITICAL ERROR: "); n != 1 {
				t.Errorf("%d CRITICAL ERROR lines, want 1:\n%s", n, errOut)
			}
		})
	}

	t.Run("skip logged as INFO", func(t *testing.T) {
		se := newSyncEnv(t, endpoint.HostInfo{})
		se.world.SetProp("tank/data", "syncoid:sync", "false")
		if got := se.run("tank/data", "backup/data"); got != 0 {
			t.Fatalf("exit code = %d, want 0; stderr %q", got, se.stderr)
		}
		if want := "INFO: Skipping dataset (syncoid:sync=false): tank/data...\n"; !strings.Contains(se.stdout.String(), want) {
			t.Errorf("stdout does not contain %q:\n%s", want, se.stdout)
		}
	})
	t.Run("skip silent under quiet", func(t *testing.T) {
		se := newSyncEnv(t, endpoint.HostInfo{})
		se.world.SetProp("tank/data", "syncoid:sync", "false")
		if got := se.run("--quiet", "tank/data", "backup/data"); got != 0 {
			t.Fatalf("exit code = %d, want 0; stderr %q", got, se.stderr)
		}
		if se.stdout.Len() != 0 || se.stderr.Len() != 0 {
			t.Errorf("stdout %q, stderr %q; want both empty", se.stdout, se.stderr)
		}
	})
	t.Run("soft no snapshots logged as WARNING", func(t *testing.T) {
		se := newSyncEnv(t, endpoint.HostInfo{})
		se.world.Create("tank/empty", "")
		if got := se.run("--no-sync-snap", "tank/empty", "backup/empty"); got != 1 {
			t.Fatalf("exit code = %d, want 1", got)
		}
		if want := "WARNING: no snapshots exist on source tank/empty, and you asked for --no-sync-snap.\n"; !strings.Contains(se.stderr.String(), want) {
			t.Errorf("stderr does not contain %q:\n%s", want, se.stderr)
		}
	})
}

func TestStdioConnCloseUnblocksRead(t *testing.T) {
	// A reader that never returns, like a blocking os.Stdin with no input.
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	c := newStdioConn(readerFunc(func([]byte) (int, error) { <-block; return 0, io.EOF }), io.Discard)
	errc := make(chan error, 1)
	go func() { _, err := c.Read(make([]byte, 8)); errc <- err }()
	time.Sleep(50 * time.Millisecond)
	_ = c.Close()
	select {
	case err := <-errc:
		if err == nil {
			t.Error("Read returned nil error after Close")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Read still blocked after Close")
	}
}

func TestStdioConnReadsInOrder(t *testing.T) {
	c := newStdioConn(strings.NewReader("hello world"), io.Discard)
	got, err := io.ReadAll(io.LimitReader(c, 5))
	if err != nil || string(got) != "hello" {
		t.Fatalf("first read = %q, %v", got, err)
	}
	rest, err := io.ReadAll(c)
	if err != nil || string(rest) != " world" {
		t.Errorf("rest = %q, %v", rest, err)
	}
}
