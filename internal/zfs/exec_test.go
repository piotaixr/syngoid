//go:build unix

package zfs

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/piotaixr/syngoid/internal/logx"
)

func TestOSExecCapturesStderrAndExit(t *testing.T) {
	var out bytes.Buffer
	err := OSExec{}.Run(context.Background(), Cmd{
		Argv:   []string{"sh", "-c", "echo out; echo 'dataset is busy' >&2; exit 3"},
		Stdout: &out,
	})
	var ce *CommandError
	if !errors.As(err, &ce) {
		t.Fatalf("want *CommandError, got %T: %v", err, err)
	}
	if ce.ExitCode != 3 {
		t.Errorf("ExitCode = %d, want 3", ce.ExitCode)
	}
	if ce.Stderr != "dataset is busy\n" {
		t.Errorf("Stderr = %q", ce.Stderr)
	}
	if !errors.Is(err, ErrPartialState) {
		t.Errorf("errors.Is(err, ErrPartialState) = false")
	}
	if out.String() != "out\n" {
		t.Errorf("stdout = %q", out.String())
	}
	if want := "sh -c echo out; echo 'dataset is busy' >&2; exit 3: exit 3: dataset is busy"; ce.Error() != want {
		t.Errorf("Error() = %q, want %q", ce.Error(), want)
	}
}

func TestOSExecDiscardsNilStdout(t *testing.T) {
	if err := (OSExec{}).Run(context.Background(), Cmd{Argv: []string{"sh", "-c", "echo hi"}}); err != nil {
		t.Fatal(err)
	}
}

func TestOSExecStdin(t *testing.T) {
	var out bytes.Buffer
	err := OSExec{}.Run(context.Background(), Cmd{
		Argv:   []string{"cat"},
		Stdin:  strings.NewReader("piped"),
		Stdout: &out,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.String() != "piped" {
		t.Errorf("stdout = %q", out.String())
	}
}

func TestOSExecStartFailureIsNotCommandError(t *testing.T) {
	err := OSExec{}.Run(context.Background(), Cmd{Argv: []string{"syngoid-no-such-binary"}})
	if err == nil {
		t.Fatal("want error")
	}
	var ce *CommandError
	if errors.As(err, &ce) {
		t.Errorf("start failure must not be *CommandError: %v", err)
	}
	if !strings.Contains(err.Error(), "syngoid-no-such-binary") {
		t.Errorf("error should name argv: %v", err)
	}
}

func TestOSExecStderrCapped(t *testing.T) {
	err := OSExec{}.Run(context.Background(), Cmd{
		Argv: []string{"sh", "-c", "head -c 200000 /dev/zero | tr '\\0' x >&2; exit 1"},
	})
	var ce *CommandError
	if !errors.As(err, &ce) {
		t.Fatalf("want *CommandError, got %v", err)
	}
	if len(ce.Stderr) != 64*1024 {
		t.Errorf("len(Stderr) = %d, want %d", len(ce.Stderr), 64*1024)
	}
}

func TestOSExecEnv(t *testing.T) {
	var out bytes.Buffer
	err := OSExec{}.Run(context.Background(), Cmd{
		Argv:   []string{"sh", "-c", `echo "$LC_ALL"; echo "$PATH"`},
		Stdout: &out,
	})
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("output = %q", out.String())
	}
	if lines[0] != "C" {
		t.Errorf("LC_ALL = %q", lines[0])
	}
	if !strings.HasSuffix(lines[1], ":/sbin:/usr/sbin:/usr/local/sbin:/bin:/usr/bin") {
		t.Errorf("PATH = %q", lines[1])
	}
}

func TestOSExecCancelKillsGroup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := OSExec{}.Run(ctx, Cmd{Argv: []string{"sh", "-c", "sleep 30 & wait"}})
	if err == nil {
		t.Fatal("want error")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("took %v, want < 2s", d)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want DeadlineExceeded", err)
	}
}

func TestOSExecDebugLog(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	if err := (OSExec{Log: log, Host: "backup@nas"}).Run(context.Background(), Cmd{Argv: []string{"echo", "x"}}); err != nil {
		t.Fatal(err)
	}
	got := buf.String()
	if !strings.Contains(got, `msg="echo x"`) || !strings.Contains(got, logx.KeyHost+"=backup@nas") {
		t.Errorf("log = %q", got)
	}

	buf.Reset()
	if err := (OSExec{Log: log}).Run(context.Background(), Cmd{Argv: []string{"echo", "x"}}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), logx.KeyHost) {
		t.Errorf("host attr should be omitted: %q", buf.String())
	}
}

func TestOSExecResolvesBareNameThroughExtraDirs(t *testing.T) {
	dir := t.TempDir()
	tool := filepath.Join(dir, "syngoid-fake-tool")
	if err := os.WriteFile(tool, []byte("#!/bin/sh\necho found\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", "/nonexistent-syngoid-dir")

	var out bytes.Buffer
	err := OSExec{extraDirs: []string{dir}}.Run(context.Background(), Cmd{Argv: []string{"syngoid-fake-tool"}, Stdout: &out})
	if err != nil {
		t.Fatal(err)
	}
	if out.String() != "found\n" {
		t.Errorf("stdout = %q", out.String())
	}

	// Without the extra dir the same name must not resolve.
	err = OSExec{extraDirs: []string{"/nonexistent-syngoid-extra"}}.Run(context.Background(), Cmd{Argv: []string{"syngoid-fake-tool"}})
	if !errors.Is(err, exec.ErrNotFound) {
		t.Errorf("err = %v, want exec.ErrNotFound", err)
	}
}

func TestOSExecCancelEscalatesToGroupKill(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "grandchild.pid")
	// Both the shell and its sleep child ignore SIGTERM (ignored signals are
	// inherited), so only the SIGKILL escalation can stop them.
	script := `trap '' TERM; sleep 60 & echo $! > "$1"; wait`
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := OSExec{grace: 200 * time.Millisecond}.Run(ctx, Cmd{Argv: []string{"sh", "-c", script, "sh", pidFile}})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Errorf("Run took %v", d)
	}

	raw, rerr := os.ReadFile(pidFile)
	if rerr != nil {
		t.Fatal(rerr)
	}
	pid, perr := strconv.Atoi(strings.TrimSpace(string(raw)))
	if perr != nil {
		t.Fatal(perr)
	}
	deadline := time.Now().Add(2 * time.Second)
	for syscall.Kill(pid, 0) == nil {
		if time.Now().After(deadline) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
			t.Fatalf("grandchild %d survived cancellation", pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
