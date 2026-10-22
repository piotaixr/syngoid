package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/piotaixr/syngoid/internal/buildinfo"
	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/rpc"
	"github.com/piotaixr/syngoid/internal/stream"
	"github.com/piotaixr/syngoid/internal/zfs"
	"github.com/piotaixr/syngoid/internal/zfs/zfstest"
)

// TestMain lets the tests run this package's main() in a child process:
// with SYNGOID_RUN_MAIN=1 the test binary behaves as `syngoid
// $SYNGOID_MAIN_ARGS`.
func TestMain(m *testing.M) {
	if os.Getenv("SYNGOID_RUN_MAIN") == "1" {
		os.Args = append([]string{"syngoid"}, strings.Fields(os.Getenv("SYNGOID_MAIN_ARGS"))...)
		main()
		return
	}
	os.Exit(m.Run())
}

func TestIsTTYFalseForNonTerminals(t *testing.T) {
	null, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = null.Close() }()
	if isTTY(null) {
		t.Error("isTTY(/dev/null) = true, want false")
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	defer func() { _ = w.Close() }()
	if isTTY(r) || isTTY(w) {
		t.Error("isTTY(pipe) = true, want false")
	}
}

func TestPoolExists(t *testing.T) {
	fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{
		"zfs list -H -o name -d 0": {Stdout: "tank\nbackup\n"},
	}}
	var stderr bytes.Buffer
	p := &pools{exec: fe, stderr: &stderr}
	if !p.exists("tank") || p.exists("nas") {
		t.Error("exists(tank) must be true and exists(nas) false")
	}
	if stderr.Len() != 0 {
		t.Errorf("stderr = %q, want empty", stderr.String())
	}
}

func TestPoolExistsWarnsOnceWhenZFSFails(t *testing.T) {
	fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{
		"zfs list -H -o name -d 0": {Stderr: "The ZFS modules are not loaded.\n", Exit: 1},
	}}
	var stderr bytes.Buffer
	p := &pools{exec: fe, stderr: &stderr}
	if p.exists("tank") || p.exists("nas") {
		t.Error("exists must be false when zfs fails")
	}
	if got, want := stderr.String(), "WARNING: Unable to enumerate pools (is zfs available?)\n"; got != want {
		t.Errorf("stderr = %q, want %q", got, want)
	}
}

// notFoundExec fails every command the way zfs.OSExec does for a binary
// missing from PATH.
type notFoundExec struct{}

func (notFoundExec) Run(_ context.Context, c zfs.Cmd) error {
	return fmt.Errorf("%s: %w: %q in PATH", strings.Join(c.Argv, " "), exec.ErrNotFound, c.Argv[0])
}

func TestPoolExistsSilentWithoutZFS(t *testing.T) {
	// A laptop without ZFS relaying between two hosts: no pools, no warning.
	var stderr bytes.Buffer
	p := &pools{exec: notFoundExec{}, stderr: &stderr}
	if p.exists("db1") || p.exists("nas") {
		t.Error("exists must be false without zfs")
	}
	if stderr.Len() != 0 {
		t.Errorf("stderr = %q, want empty", stderr.String())
	}
}

// serveProc is a `syngoid serve` child process with the initiator's end of
// its stdin and stdout.
type serveProc struct {
	remote  *rpc.Remote
	conn    io.Closer // closes the agent's stdin and the read end of its stdout
	stderr  *lockedBuffer
	exited  chan error
	pidfile string
	cmd     *exec.Cmd
	// dropped is set before dropStdout so the initiator's session does not
	// see the agent's stdout end.
	dropped atomic.Bool
	outR    *os.File
}

// dropStdout closes the read end of the agent's stdout, as a vanished peer
// does, while the initiator's session and the agent's stdin stay up.
func (p *serveProc) dropStdout() {
	p.dropped.Store(true)
	_ = p.outR.Close()
}

// lockedBuffer is a bytes.Buffer that may be read while the child writes.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

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

// startServe runs `syngoid serve` with a fake zfs on PATH (the shell script
// body) and handshakes with it. The child is killed at cleanup at the latest.
func startServe(t *testing.T, shim string, debug bool) *serveProc {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "zfs"), []byte("#!/bin/sh\n"+shim), 0o755); err != nil {
		t.Fatal(err)
	}
	p := &serveProc{stderr: new(lockedBuffer), exited: make(chan error, 1), pidfile: filepath.Join(dir, "pid")}

	// A regression must fail the test, not hang it.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, os.Args[0])
	cmd.Env = append(os.Environ(),
		"SYNGOID_RUN_MAIN=1", "SYNGOID_MAIN_ARGS=serve",
		"PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"SHIM_PIDFILE="+p.pidfile,
		"HOME="+t.TempDir())
	cmd.Stderr = p.stderr
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	// The agent's stdout is a pipe of our own, relayed to the initiator's
	// session, so that closing its read end (dropStdout) is visible to the
	// agent only.
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	p.outR = outR
	cmd.Stdout = outW
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	_ = outW.Close()
	p.cmd = cmd
	go func() { p.exited <- cmd.Wait() }()
	out, relay := io.Pipe()
	go func() {
		_, err := io.Copy(relay, outR)
		if !p.dropped.Load() {
			relay.CloseWithError(err)
		}
	}()

	hello := rpc.Hello{Protocol: buildinfo.ProtocolVersion, Version: buildinfo.Version, BuildHash: buildinfo.BuildHash(), Debug: debug}
	hctx, hcancel := context.WithTimeout(ctx, 15*time.Second)
	defer hcancel()
	conn := duplex{Reader: out, Writer: in, closers: []io.Closer{in, outR, out}}
	p.conn = conn
	p.remote, _, err = rpc.Handshake(hctx, conn, hello, "agent", nil)
	if err != nil {
		t.Fatalf("handshake: %v (stderr %q)", err, p.stderr)
	}
	t.Cleanup(func() {
		_ = p.conn.Close()
		cancel()
		t.Logf("serve exit: %v", <-p.exited)
	})
	return p
}

// waitExit waits up to d for the process to exit and returns its Wait error.
func (p *serveProc) waitExit(t *testing.T, d time.Duration) error {
	t.Helper()
	select {
	case err := <-p.exited:
		p.exited <- err // for the cleanup
		return err
	case <-time.After(d):
		t.Fatalf("serve process still running after %v", d)
		return nil
	}
}

// waitPID waits for the shim to record its pid. It fails the test if sent
// delivers a result first, which means the send ended before the shim ran.
func (p *serveProc) waitPID(t *testing.T, sent <-chan error) int {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if b, err := os.ReadFile(p.pidfile); err == nil {
			if pid, _ := strconv.Atoi(strings.TrimSpace(string(b))); pid != 0 {
				t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
				return pid
			}
		}
		select {
		case err := <-sent:
			t.Fatalf("Send returned early: %v (stderr %q)", err, p.stderr)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("shim never started (stderr %q)", p.stderr)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// waitDead fails the test unless process pid is gone within 15 s.
func waitDead(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("zfs child %d survived", pid)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

const blockingSendShim = `case $1 in
send) echo $$ > "$SHIM_PIDFILE"; exec sleep 300;;
version) echo zfs-2.3.4-1;;
*) exit 0;;
esac
`

func startBlockingSend(p *serveProc) <-chan error {
	sent := make(chan error, 1)
	go func() {
		sent <- p.remote.Send(context.Background(), zfs.SendSpec{Dataset: "tank/a", To: "s1"}, endpoint.SendOpts{Codec: stream.None}, io.Discard)
	}()
	return sent
}

func TestServeDisconnectKillsChildren(t *testing.T) {
	p := startServe(t, blockingSendShim, false)
	sent := startBlockingSend(p)
	pid := p.waitPID(t, sent)

	// The initiator vanishes: the agent's stdin hits EOF and its stdout is
	// a broken pipe.
	_ = p.conn.Close()

	waitDead(t, pid)
	if err := p.waitExit(t, 15*time.Second); err != nil {
		t.Errorf("serve exited with %v (stderr %q)", err, p.stderr)
	}
	select {
	case err := <-sent:
		if err == nil {
			t.Error("Send succeeded although the connection was dropped")
		}
	case <-time.After(15 * time.Second):
		t.Error("Send did not return")
	}
}

// TestServeSurvivesBrokenStdout guards the SIGPIPE handling: the agent's
// stdout is fd 1, so Go would kill the process with SIGPIPE on the first
// write after the peer's read end closes, unless the signal is ignored. The
// agent must instead fail the write and clean up its children.
func TestServeSurvivesBrokenStdout(t *testing.T) {
	p := startServe(t, blockingSendShim, false)
	sent := startBlockingSend(p)
	pid := p.waitPID(t, sent)

	// Stdin stays open. A request makes the agent write its answer into
	// the broken pipe.
	p.dropStdout()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	go func() { _, _ = p.remote.ListTree(ctx, "tank/a", false) }()

	waitDead(t, pid)
	_ = p.waitExit(t, 15*time.Second)
	if ws, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		t.Errorf("serve was killed by %v, want it to survive SIGPIPE (stderr %q)", ws.Signal(), p.stderr)
	}
}

func TestServeStderrSilent(t *testing.T) {
	p := startServe(t, `case $1 in
list) printf 'tank/a\tfilesystem\t-\t0\t-\t-\n';;
version) echo zfs-2.3.4-1;;
*) exit 0;;
esac
`, true)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tree, err := p.remote.ListTree(ctx, "tank/a", true)
	if err != nil {
		t.Fatalf("ListTree: %v", err)
	}
	if len(tree) != 1 || tree[0].Name != "tank/a" {
		t.Errorf("tree = %+v, want [tank/a]", tree)
	}
	if err := p.remote.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if err := p.waitExit(t, 15*time.Second); err != nil {
		t.Errorf("serve exited with %v", err)
	}
	if p.stderr.String() != "" {
		t.Errorf("stderr = %q, want empty", p.stderr)
	}
}
