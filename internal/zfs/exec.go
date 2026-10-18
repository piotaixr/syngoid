//go:build unix

package zfs

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
	"syscall"
	"time"

	"github.com/piotaixr/syngoid/internal/logx"
)

const (
	stderrCap = 64 * 1024
	waitDelay = 10 * time.Second
	pathExtra = ":/sbin:/usr/sbin:/usr/local/sbin:/bin:/usr/bin"
)

// Cmd is a single command invocation expressed as an argv slice.
type Cmd struct {
	Argv   []string
	Stdin  io.Reader
	Stdout io.Writer
}

// Exec runs commands. A non-zero exit is reported as *CommandError.
type Exec interface {
	Run(ctx context.Context, c Cmd) error
}

// CommandError describes a command that exited non-zero.
type CommandError struct {
	Argv     []string
	ExitCode int
	Stderr   string
	// Kind is Classify(Stderr); it may be nil.
	Kind error
}

func (e *CommandError) Error() string {
	line := ""
	for _, l := range strings.Split(e.Stderr, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			line = l
			break
		}
	}
	return fmt.Sprintf("%s: exit %d: %s", strings.Join(e.Argv, " "), e.ExitCode, line)
}

func (e *CommandError) Unwrap() error { return e.Kind }

// OSExec runs commands as local processes. Host is only used as the
// logx.KeyHost attribute of debug log lines.
type OSExec struct {
	Log  *slog.Logger
	Host string
}

// cappedBuffer keeps at most max bytes and silently drops the rest.
type cappedBuffer struct {
	buf bytes.Buffer
	max int
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	if room := b.max - b.buf.Len(); room > 0 {
		b.buf.Write(p[:min(room, len(p))])
	}
	return len(p), nil
}

// Run executes c.Argv in its own process group. On context cancellation the
// whole group receives SIGTERM, and SIGKILL after a 10 s grace period.
func (o OSExec) Run(ctx context.Context, c Cmd) error {
	if len(c.Argv) == 0 {
		return errors.New("zfs: empty argv")
	}
	if o.Log != nil {
		var attrs []any
		if o.Host != "" {
			attrs = append(attrs, logx.KeyHost, o.Host)
		}
		o.Log.Debug(strings.Join(c.Argv, " "), attrs...)
	}

	cmd := exec.CommandContext(ctx, c.Argv[0], c.Argv[1:]...)
	cmd.Env = append(os.Environ(), "LC_ALL=C")
	cmd.Env = withPath(cmd.Env)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		// Setpgid makes the child its own group leader, so pgid == pid.
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	}
	cmd.WaitDelay = waitDelay
	cmd.Stdin = c.Stdin
	if c.Stdout != nil {
		cmd.Stdout = c.Stdout
	} else {
		cmd.Stdout = io.Discard
	}
	stderr := &cappedBuffer{max: stderrCap}
	cmd.Stderr = stderr

	err := cmd.Run()
	if err == nil {
		return nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("%s: %w", strings.Join(c.Argv, " "), ctxErr)
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		s := stderr.buf.String()
		return &CommandError{Argv: c.Argv, ExitCode: ee.ExitCode(), Stderr: s, Kind: Classify(s)}
	}
	return fmt.Errorf("%s: %w", strings.Join(c.Argv, " "), err)
}

// withPath returns env with the extra system directories appended to PATH.
func withPath(env []string) []string {
	for i, kv := range env {
		if v, ok := strings.CutPrefix(kv, "PATH="); ok {
			env[i] = "PATH=" + v + pathExtra
			return env
		}
	}
	return append(env, "PATH="+strings.TrimPrefix(pathExtra, ":"))
}
