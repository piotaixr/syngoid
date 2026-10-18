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
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/piotaixr/syngoid/internal/logx"
)

const (
	stderrCap    = 64 * 1024
	defaultGrace = 10 * time.Second
)

// defaultExtraDirs are appended to the child's PATH (and used to resolve a
// bare argv[0]) so zfs is found under a minimal parent PATH such as cron's.
var defaultExtraDirs = []string{"/sbin", "/usr/sbin", "/usr/local/sbin", "/bin", "/usr/bin"}

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

	// Test hooks; zero values select the production defaults.
	grace     time.Duration // SIGTERM to SIGKILL escalation delay (default 10 s)
	extraDirs []string      // directories appended to PATH (default defaultExtraDirs)
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

// Run executes c.Argv in its own process group. A bare argv[0] is resolved
// against the parent PATH extended with the system sbin/bin directories. On
// context cancellation the whole group receives SIGTERM and, if the command
// has not finished after the grace period (10 s), SIGKILL; once Run returns
// after a cancellation any remaining group member is killed too, so no
// orphans are left behind.
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

	dirs := o.extraDirs
	if dirs == nil {
		dirs = defaultExtraDirs
	}
	grace := o.grace
	if grace == 0 {
		grace = defaultGrace
	}

	env := withPath(append(os.Environ(), "LC_ALL=C"), dirs)
	path := c.Argv[0]
	if !strings.Contains(path, "/") {
		resolved, err := lookPath(path, envPath(env))
		if err != nil {
			return fmt.Errorf("%s: %w", strings.Join(c.Argv, " "), err)
		}
		path = resolved
	}

	cmd := exec.CommandContext(ctx, path, c.Argv[1:]...)
	cmd.Args[0] = c.Argv[0]
	cmd.Env = env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var escalate *time.Timer
	cmd.Cancel = func() error {
		// Setpgid makes the child its own group leader, so pgid == pid.
		pgid := cmd.Process.Pid
		err := killGroup(pgid, syscall.SIGTERM)
		if err == nil {
			escalate = time.AfterFunc(grace, func() { _ = killGroup(pgid, syscall.SIGKILL) })
		}
		return err
	}
	// Backstop only: the group SIGKILL above normally closes the pipes first.
	cmd.WaitDelay = grace + 2*time.Second
	cmd.Stdin = c.Stdin
	if c.Stdout != nil {
		cmd.Stdout = c.Stdout
	} else {
		cmd.Stdout = io.Discard
	}
	stderr := &cappedBuffer{max: stderrCap}
	cmd.Stderr = stderr

	err := cmd.Run()
	if ctxErr := ctx.Err(); ctxErr != nil && cmd.Process != nil {
		// Cancelled: reap any group member that outlived the leader.
		if escalate != nil {
			escalate.Stop()
		}
		_ = killGroup(cmd.Process.Pid, syscall.SIGKILL)
		if err != nil {
			return fmt.Errorf("%s: %w", strings.Join(c.Argv, " "), ctxErr)
		}
	}
	if err == nil {
		return nil
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		s := stderr.buf.String()
		return &CommandError{Argv: c.Argv, ExitCode: ee.ExitCode(), Stderr: s, Kind: Classify(s)}
	}
	return fmt.Errorf("%s: %w", strings.Join(c.Argv, " "), err)
}

// killGroup signals the process group pgid. A vanished group is reported as
// os.ErrProcessDone, matching os.Process.Signal.
func killGroup(pgid int, sig syscall.Signal) error {
	err := syscall.Kill(-pgid, sig)
	if errors.Is(err, syscall.ESRCH) {
		return os.ErrProcessDone
	}
	return err
}

// envPath returns the value of PATH in env.
func envPath(env []string) string {
	for _, kv := range env {
		if v, ok := strings.CutPrefix(kv, "PATH="); ok {
			return v
		}
	}
	return ""
}

// lookPath finds an executable named name in the colon-separated path.
func lookPath(name, path string) (string, error) {
	for _, dir := range filepath.SplitList(path) {
		if dir == "" {
			dir = "."
		}
		p := filepath.Join(dir, name)
		if fi, err := os.Stat(p); err == nil && fi.Mode().IsRegular() && fi.Mode().Perm()&0o111 != 0 {
			return p, nil
		}
	}
	return "", fmt.Errorf("%w: %q in PATH", exec.ErrNotFound, name)
}

// withPath returns env with dirs appended to PATH (added if absent).
func withPath(env []string, dirs []string) []string {
	extra := strings.Join(dirs, ":")
	for i, kv := range env {
		if v, ok := strings.CutPrefix(kv, "PATH="); ok {
			env[i] = "PATH=" + v + ":" + extra
			return env
		}
	}
	return append(env, "PATH="+extra)
}
