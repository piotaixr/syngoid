package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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
		"zfs list -H -o name -d 0": {Stderr: "zfs: command not found\n", Exit: 127},
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

// serveProc is a `syngoid serve` child process with the initiator's end of
// its stdin and stdout.
type serveProc struct {
	remote  *rpc.Remote
	conn    io.Closer // closes the agent's stdin and the read end of its stdout
	stderr  *lockedBuffer
	exited  chan error
	pidfile string
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
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.exited <- cmd.Wait() }()

	hello := rpc.Hello{Protocol: buildinfo.ProtocolVersion, Version: buildinfo.Version, BuildHash: buildinfo.BuildHash(), Debug: debug}
	hctx, hcancel := context.WithTimeout(ctx, 15*time.Second)
	defer hcancel()
	conn := duplex{Reader: out, Writer: in, closers: []io.Closer{in, out}}
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

func TestServeDisconnectKillsChildren(t *testing.T) {
	p := startServe(t, `case $1 in
send) echo $$ > "$SHIM_PIDFILE"; exec sleep 300;;
version) echo zfs-2.3.4-1;;
*) exit 0;;
esac
`, false)

	sent := make(chan error, 1)
	go func() {
		sent <- p.remote.Send(context.Background(), zfs.SendSpec{Dataset: "tank/a", To: "s1"}, endpoint.SendOpts{Codec: stream.None}, io.Discard)
	}()

	var pid int
	deadline := time.Now().Add(15 * time.Second)
	for pid == 0 {
		if b, err := os.ReadFile(p.pidfile); err == nil {
			pid, _ = strconv.Atoi(strings.TrimSpace(string(b)))
		}
		if pid == 0 {
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
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })

	// The initiator vanishes: the agent's stdin hits EOF and its stdout is
	// a broken pipe.
	_ = p.conn.Close()

	deadline = time.Now().Add(15 * time.Second)
	for {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("zfs child %d survived the disconnect", pid)
		}
		time.Sleep(50 * time.Millisecond)
	}
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
