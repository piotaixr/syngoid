//go:build unix

package bootstrap

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/piotaixr/syngoid/internal/agent"
	"github.com/piotaixr/syngoid/internal/buildinfo"
	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/endpoint/fake"
	"github.com/piotaixr/syngoid/internal/sshx"
)

// testAgentEnv turns the test binary into an agent serving a fake World.
const testAgentEnv = "SYNGOID_TEST_AGENT"

// testHostInfo is what the test agent's endpoint reports.
var testHostInfo = endpoint.HostInfo{OS: "testos", Arch: "testarch", User: "tester", UID: 4242}

func TestMain(m *testing.M) {
	if os.Getenv(testAgentEnv) == "1" {
		os.Exit(runTestAgent())
	}
	os.Exit(m.Run())
}

// runTestAgent serves the protocol on stdin and stdout, the way
// `syngoid serve` does, backed by a fake World.
func runTestAgent() int {
	conn, err := newStdio()
	if err != nil {
		fmt.Fprintln(os.Stderr, "test agent:", err)
		return 2
	}
	sum, _ := buildinfo.ExeSHA256()
	cfg := agent.Config{
		NewEndpoint: func(bool, *slog.Logger) endpoint.Endpoint { return fake.New(fake.NewWorld(), testHostInfo) },
		Home:        os.Getenv("HOME"),
		ExeSHA256:   sum,
	}
	if err := agent.Serve(context.Background(), conn, cfg); err != nil {
		fmt.Fprintln(os.Stderr, "test agent:", err)
		return 2
	}
	return 0
}

// stdio is the test agent's connection. Both descriptors are switched to
// non-blocking mode so the runtime poller serves them and Close unblocks a
// pending Read.
type stdio struct{ in, out *os.File }

func newStdio() (*stdio, error) {
	for _, fd := range []int{0, 1} {
		if err := syscall.SetNonblock(fd, true); err != nil {
			return nil, err
		}
	}
	return &stdio{in: os.NewFile(0, "stdin"), out: os.NewFile(1, "stdout")}, nil
}

func (s *stdio) Read(p []byte) (int, error)  { return s.in.Read(p) }
func (s *stdio) Write(p []byte) (int, error) { return s.out.Write(p) }
func (s *stdio) Close() error                { return errors.Join(s.in.Close(), s.out.Close()) }

// shTransport is a Transport running each command with `sh -c` on this
// host, as a remote login shell would, with HOME set to its own directory.
// Spawned commands run with SYNGOID_TEST_AGENT=1, so a pushed copy of the
// test binary serves as the agent.
type shTransport struct {
	label  string
	home   string
	pushes atomic.Int32
	// closeWait bounds how long a Proc's Close waits before killing it.
	closeWait time.Duration
}

func newShTransport(t *testing.T) *shTransport {
	t.Helper()
	return &shTransport{label: "sh-test", home: t.TempDir(), closeWait: 5 * time.Second}
}

func (s *shTransport) Label() string { return s.label }

func (s *shTransport) env(extra ...string) []string {
	return append(os.Environ(), append([]string{"HOME=" + s.home}, extra...)...)
}

func (s *shTransport) Run(ctx context.Context, cmd string, stdin io.Reader) ([]byte, error) {
	if strings.HasPrefix(cmd, sshx.ShellJoin("sh", "-c", pushScript)) {
		s.pushes.Add(1)
	}
	c := exec.CommandContext(ctx, "sh", "-c", cmd)
	c.Env = s.env()
	c.Stdin = stdin
	var stderr bytes.Buffer
	c.Stderr = &stderr
	out, err := c.Output()
	if err != nil {
		return nil, fmt.Errorf("sh: %w: %s", err, stderr.String())
	}
	return out, nil
}

func (s *shTransport) Spawn(_ context.Context, cmd string) (Proc, error) {
	c := exec.Command("sh", "-c", cmd)
	c.Env = s.env(testAgentEnv + "=1")
	in, err := c.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := c.StdoutPipe()
	if err != nil {
		return nil, err
	}
	p := &shProc{cmd: c, in: in, out: out, wait: s.closeWait, done: make(chan struct{})}
	c.Stderr = &p.stderr
	if err := c.Start(); err != nil {
		return nil, err
	}
	go func() {
		_ = c.Wait()
		close(p.done)
	}()
	return p, nil
}

// shProc is a process spawned by shTransport.
type shProc struct {
	cmd    *exec.Cmd
	in     io.WriteCloser
	out    io.Reader
	stderr lockedBuffer
	wait   time.Duration
	done   chan struct{}
	once   sync.Once
}

func (p *shProc) Read(b []byte) (int, error)  { return p.out.Read(b) }
func (p *shProc) Write(b []byte) (int, error) { return p.in.Write(b) }
func (p *shProc) Stderr() string              { return p.stderr.String() }

// Close closes stdin and waits for the process, killing it after p.wait.
func (p *shProc) Close() error {
	p.once.Do(func() {
		_ = p.in.Close()
		select {
		case <-p.done:
		case <-time.After(p.wait):
			_ = p.cmd.Process.Kill()
			<-p.done
		}
	})
	return nil
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// writeScript writes an executable /bin/sh script.
func writeScript(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
}
