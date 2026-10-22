//go:build unix

package sshx

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// writeShim writes an executable /bin/sh script standing in for ssh.
func writeShim(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "ssh")
	if err := os.WriteFile(p, []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

func newTestMaster(t *testing.T, cfg Config, user, host string) *Master {
	t.Helper()
	m, err := NewMaster(cfg, user, host, t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	m.closeWait = 2 * time.Second
	m.killGrace = 200 * time.Millisecond
	return m
}

func TestMasterArgv(t *testing.T) {
	m, err := NewMaster(Config{Options: []string{"Port=2222"}, ConfigFile: "/c", StdinIsTTY: true}, "backup", "nas", "/d", 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := m.Label(); got != "backup@nas" {
		t.Errorf("Label = %q", got)
	}
	wantMaster := []string{
		"ssh", "-F", "/c", "-o", "ControlPath=/d/cm-0", "-n", "-o", "ControlMaster=yes", "-o", "ControlPersist=60",
		"-o", "Port=2222", "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=4", "-T", "--", "backup@nas", "exit",
	}
	if got := m.masterArgv(); !slices.Equal(got, wantMaster) {
		t.Errorf("master argv =\n %q\nwant\n %q", got, wantMaster)
	}
	wantClient := []string{
		"ssh", "-F", "/c", "-o", "ControlPath=/d/cm-0", "-o", "ControlMaster=no", "-o", "BatchMode=yes",
		"-o", "Port=2222", "-T", "--", "backup@nas", "echo hi",
	}
	if got := m.clientArgv("echo hi"); !slices.Equal(got, wantClient) {
		t.Errorf("client argv =\n %q\nwant\n %q", got, wantClient)
	}
	wantExit := []string{"ssh", "-F", "/c", "-o", "ControlPath=/d/cm-0", "-O", "exit", "--", "backup@nas"}
	if got := m.exitArgv(); !slices.Equal(got, wantExit) {
		t.Errorf("exit argv = %q", got)
	}

	// No user: the destination is the bare host.
	m2, _ := NewMaster(Config{StdinIsTTY: true}, "", "nas", "/d", 3)
	if m2.Label() != "nas" || m2.clientArgv("x")[len(m2.clientArgv("x"))-2] != "nas" {
		t.Errorf("bare host label/argv wrong: %q", m2.clientArgv("x"))
	}
	if !slices.Contains(m2.clientArgv("x"), "ControlPath=/d/cm-3") {
		t.Errorf("control path index not used: %q", m2.clientArgv("x"))
	}
}

func TestRunUsesBuiltArgv(t *testing.T) {
	dir := t.TempDir()
	shim := writeShim(t, `for a; do printf '%s\n' "$a"; done`)
	m, err := NewMaster(Config{SSH: shim, Options: []string{"Port=2222"}, ConfigFile: "/c", StdinIsTTY: true}, "backup", "nas", dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	out, err := m.Run(context.Background(), "echo hi", nil)
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Join(m.clientArgv("echo hi")[1:], "\n") + "\n"
	if string(out) != want {
		t.Errorf("shim saw\n%q\nwant\n%q", out, want)
	}
}

func TestBatchMode(t *testing.T) {
	idx := func(a []string, s string) int { return slices.Index(a, s) }
	count := func(a []string, s string) int {
		n := 0
		for _, x := range a {
			if x == s {
				n++
			}
		}
		return n
	}

	// No TTY: the master gets BatchMode=yes after the user options, before -T.
	m, _ := NewMaster(Config{Options: []string{"Port=1"}}, "u", "h", "/d", 0)
	argv := m.masterArgv()
	b := idx(argv, "BatchMode=yes")
	if b < 0 || argv[b-1] != "-o" || b < idx(argv, "Port=1") || b > idx(argv, "-T") {
		t.Errorf("master: BatchMode=yes misplaced: %q", argv)
	}

	// Clients always get it, exactly once, ahead of the user options.
	for _, tty := range []bool{false, true} {
		for _, opts := range [][]string{nil, {"batchmode=no", "Port=1"}, {"BatchMode no"}} {
			m, _ := NewMaster(Config{StdinIsTTY: tty, Options: opts}, "u", "h", "/d", 0)
			argv := m.clientArgv("x")
			if count(argv, "BatchMode=yes") != 1 {
				t.Errorf("tty=%v opts=%q: client BatchMode=yes count != 1: %q", tty, opts, argv)
			}
			if len(opts) > 0 && idx(argv, "BatchMode=yes") > idx(argv, opts[0]) {
				t.Errorf("tty=%v opts=%q: ours must precede the user's: %q", tty, opts, argv)
			}
		}
	}

	// A user BatchMode option keeps the master from adding its own.
	for _, opt := range []string{"batchmode=no", "BatchMode no", "BATCHMODE=yes"} {
		m, _ := NewMaster(Config{Options: []string{opt}}, "u", "h", "/d", 0)
		if slices.Contains(m.masterArgv(), "BatchMode=yes") {
			t.Errorf("option %q: master BatchMode=yes still added: %q", opt, m.masterArgv())
		}
	}

	mt, _ := NewMaster(Config{StdinIsTTY: true}, "u", "h", "/d", 0)
	if slices.Contains(mt.masterArgv(), "BatchMode=yes") {
		t.Errorf("TTY: master BatchMode=yes added")
	}
	// A different option that merely contains the word is not an override.
	mo, _ := NewMaster(Config{Options: []string{"SendEnv=batchmode"}}, "u", "h", "/d", 0)
	if !slices.Contains(mo.masterArgv(), "BatchMode=yes") {
		t.Errorf("unrelated option suppressed BatchMode")
	}
}

func TestServerAlive(t *testing.T) {
	idx := func(a []string, s string) int { return slices.Index(a, s) }

	// Default: the master detects a dead link within ~60 s. The values follow
	// the user options, so a user ServerAliveCountMax still wins.
	m, _ := NewMaster(Config{Options: []string{"Port=1", "ServerAliveCountMax=9"}, StdinIsTTY: true}, "u", "h", "/d", 0)
	argv := m.masterArgv()
	i, c := idx(argv, "ServerAliveInterval=15"), idx(argv, "ServerAliveCountMax=4")
	if i < 0 || c < 0 || argv[i-1] != "-o" || argv[c-1] != "-o" {
		t.Fatalf("master lacks ServerAlive options: %q", argv)
	}
	if i < idx(argv, "ServerAliveCountMax=9") || i > idx(argv, "-T") || c > idx(argv, "-T") {
		t.Errorf("ServerAlive options must follow the user's: %q", argv)
	}
	// Clients share the master's connection and get none of ours.
	if ca := m.clientArgv("x"); slices.Contains(ca, "ServerAliveInterval=15") || slices.Contains(ca, "ServerAliveCountMax=4") {
		t.Errorf("client argv has our ServerAlive options: %q", ca)
	}

	// A user ServerAliveInterval option (any case, either syntax) replaces ours.
	for _, opt := range []string{"ServerAliveInterval=30", "serveraliveinterval 0", "SERVERALIVEINTERVAL=5"} {
		m, _ := NewMaster(Config{Options: []string{opt}}, "u", "h", "/d", 0)
		argv := m.masterArgv()
		if slices.Contains(argv, "ServerAliveInterval=15") || slices.Contains(argv, "ServerAliveCountMax=4") {
			t.Errorf("option %q: our ServerAlive options still added: %q", opt, argv)
		}
		if !slices.Contains(argv, opt) || !slices.Contains(argv, "BatchMode=yes") {
			t.Errorf("option %q: user option or BatchMode missing: %q", opt, argv)
		}
	}
}

func TestNewMasterRejectsOptionLikeHost(t *testing.T) {
	cases := []struct{ user, host string }{
		{"", "-oProxyCommand=x"},
		{"", "a b"},
		{"", ""},
		{"", "a\tb"},
		{"", "a\x00b"},
		{"", "a\nb"},
		{"-l", "h"},
		{"u ser", "h"},
		{"a@b", "h"},
		{"u\x01", "h"},
	}
	for _, c := range cases {
		_, err := NewMaster(Config{}, c.user, c.host, "/d", 0)
		if err == nil {
			t.Errorf("user %q host %q: no error", c.user, c.host)
			continue
		}
		if !strings.Contains(err.Error(), "invalid ssh destination") {
			t.Errorf("user %q host %q: error %q", c.user, c.host, err)
		}
	}
}

func TestShellQuoteRoundTrip(t *testing.T) {
	for _, s := range []string{"plain", "with space", "it's", "$HOME", `a"b`, ";rm -rf /", "~/x", "", "a\nb", "`id`", `back\slash`} {
		out, err := exec.Command("sh", "-c", "printf %s "+ShellQuote(s)).Output()
		if err != nil {
			t.Fatalf("%q: %v", s, err)
		}
		if string(out) != s {
			t.Errorf("round trip %q -> %q", s, out)
		}
	}
	if got := ShellQuote("it's"); got != `'it'\''s'` {
		t.Errorf("ShellQuote(it's) = %s", got)
	}
}

func TestShellJoin(t *testing.T) {
	args := []string{"zfs", "list", "-H", "a b", "it's", ""}
	out, err := exec.Command("sh", "-c", `for a in `+ShellJoin(args...)+`; do printf '<%s>' "$a"; done`).Output()
	if err != nil {
		t.Fatal(err)
	}
	if want := "<zfs><list><-H><a b><it's><>"; string(out) != want {
		t.Errorf("got %q want %q", out, want)
	}
	if ShellJoin() != "" {
		t.Errorf("empty join not empty")
	}
}

func TestClientCommandsOwnProcessGroup(t *testing.T) {
	dir := t.TempDir()
	pgidFile := filepath.Join(dir, "pgid")
	shim := writeShim(t, `ps -o pgid= -p $$ | tr -d ' ' > `+pgidFile+`; ps -o pgid= -p $$`)
	m := newTestMaster(t, Config{SSH: shim}, "u", "h")

	out, err := m.Run(context.Background(), "x", nil)
	if err != nil {
		t.Fatal(err)
	}
	pg, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		t.Fatalf("pgid output %q: %v", out, err)
	}
	if pg == syscall.Getpgrp() {
		t.Errorf("Run shares the test's process group %d", pg)
	}

	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(pgidFile)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(b)); got != strconv.Itoa(syscall.Getpgrp()) {
		t.Errorf("master pgid = %s, want the test's group %d", got, syscall.Getpgrp())
	}

	p, err := m.Spawn(context.Background(), "x")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	// The master step left the test's own pgid in the file; wait for the
	// spawned shim to overwrite it.
	var got string
	for i := 0; i < 300; i++ {
		b, _ = os.ReadFile(pgidFile)
		if got = strings.TrimSpace(string(b)); got != "" && got != strconv.Itoa(syscall.Getpgrp()) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got == "" || got == strconv.Itoa(syscall.Getpgrp()) {
		t.Errorf("spawned pgid = %q, want own group", got)
	}
}

func TestRunErrorCapturesStderr(t *testing.T) {
	shim := writeShim(t, `echo 'Permission denied (publickey).' >&2; exit 255`)
	m := newTestMaster(t, Config{SSH: shim}, "backup", "nas")
	_, err := m.Run(context.Background(), "x", nil)
	var se *Error
	if !errors.As(err, &se) {
		t.Fatalf("err = %v, want *Error", err)
	}
	if se.ExitCode != 255 || !strings.Contains(se.Stderr, "Permission denied (publickey).") || se.Label != "backup@nas" {
		t.Errorf("Error = %+v", se)
	}
	if len(se.Argv) == 0 || se.Argv[len(se.Argv)-1] != "x" {
		t.Errorf("Argv = %q", se.Argv)
	}
	if !strings.Contains(err.Error(), "Permission denied") || !strings.Contains(err.Error(), "255") {
		t.Errorf("message %q", err)
	}

	serr := m.Start(context.Background())
	if !errors.As(serr, &se) || se.ExitCode != 255 || !strings.Contains(se.Stderr, "Permission denied") {
		t.Errorf("Start err = %v", serr)
	}
}

func TestRunStdinStdout(t *testing.T) {
	m := newTestMaster(t, Config{SSH: writeShim(t, `cat`)}, "", "h")
	out, err := m.Run(context.Background(), "x", strings.NewReader("hello"))
	if err != nil || string(out) != "hello" {
		t.Errorf("out %q err %v", out, err)
	}
}

func TestRunCancelKillsGroup(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "pid")
	// A trap-ignoring child that outlives SIGTERM, to exercise the SIGKILL escalation.
	shim := writeShim(t, `trap '' TERM; sleep 30 & echo $! > `+pidFile+`; wait`)
	m := newTestMaster(t, Config{SSH: shim}, "", "h")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := m.Run(ctx, "x", nil); done <- err }()
	waitFile(t, pidFile)
	cancelled := time.Now()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
		if d := time.Since(cancelled); d > 2*time.Second {
			t.Errorf("Run returned %v after cancel", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	b, _ := os.ReadFile(pidFile)
	pid, _ := strconv.Atoi(strings.TrimSpace(string(b)))
	time.Sleep(100 * time.Millisecond)
	if err := exec.Command("kill", "-0", strconv.Itoa(pid)).Run(); err == nil {
		t.Errorf("group member %d survived cancel", pid)
	}
}

func waitFile(t *testing.T, p string) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if b, err := os.ReadFile(p); err == nil && len(b) > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s never appeared", p)
}

func TestSpawnRoundTrip(t *testing.T) {
	m := newTestMaster(t, Config{SSH: writeShim(t, `cat; echo bye >&2`)}, "", "h")
	p, err := m.Spawn(context.Background(), "x")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Write([]byte("ping\n")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(p, buf); err != nil || string(buf) != "ping\n" {
		t.Fatalf("read %q err %v", buf, err)
	}
	if err := p.Close(); err != nil {
		t.Errorf("Close = %v", err)
	}
	if err := p.Close(); err != nil {
		t.Errorf("second Close = %v", err)
	}
	if !strings.Contains(p.Stderr(), "bye") {
		t.Errorf("Stderr = %q", p.Stderr())
	}
	if _, err := p.Read(buf); err == nil {
		t.Errorf("Read after Close succeeded")
	}
}

func TestSpawnCloseKillsStubbornProcess(t *testing.T) {
	m := newTestMaster(t, Config{SSH: writeShim(t, `trap '' TERM; sleep 30 & wait`)}, "", "h")
	m.closeWait = 300 * time.Millisecond
	p, err := m.Spawn(context.Background(), "x")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := p.Close(); err != nil {
		t.Errorf("Close after our own kill = %v, want nil", err)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Errorf("Close took %v", d)
	}
}

func TestSpawnCloseReportsFailure(t *testing.T) {
	m := newTestMaster(t, Config{SSH: writeShim(t, `echo boom >&2; exit 3`)}, "", "h")
	p, err := m.Spawn(context.Background(), "x")
	if err != nil {
		t.Fatal(err)
	}
	var se *Error
	if err := p.Close(); !errors.As(err, &se) || se.ExitCode != 3 || !strings.Contains(se.Stderr, "boom") {
		t.Errorf("Close = %v", err)
	}
}

func TestSpawnStartFailure(t *testing.T) {
	m := newTestMaster(t, Config{SSH: "/nonexistent/ssh"}, "", "h")
	if _, err := m.Spawn(context.Background(), "x"); err == nil {
		t.Error("Spawn with missing ssh succeeded")
	}
	if err := m.Start(context.Background()); err == nil {
		t.Error("Start with missing ssh succeeded")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m2 := newTestMaster(t, Config{SSH: writeShim(t, `cat`)}, "", "h")
	if _, err := m2.Spawn(ctx, "x"); !errors.Is(err, context.Canceled) {
		t.Errorf("Spawn with cancelled ctx = %v", err)
	}
}

func TestSpawnNotTiedToCtxAfterStart(t *testing.T) {
	m := newTestMaster(t, Config{SSH: writeShim(t, `cat`)}, "", "h")
	ctx, cancel := context.WithCancel(context.Background())
	p, err := m.Spawn(ctx, "x")
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	time.Sleep(100 * time.Millisecond)
	if _, err := p.Write([]byte("a")); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 1)
	if _, err := io.ReadFull(p, b); err != nil || b[0] != 'a' {
		t.Errorf("proc died with ctx: %q %v", b, err)
	}
	_ = p.Close()
}

func TestProcStderrKeepsTail(t *testing.T) {
	// 100 KiB of 'a' then a marker: the tail must keep the marker and cap at 64 KiB.
	m := newTestMaster(t, Config{SSH: writeShim(t, `head -c 102400 /dev/zero | tr '\0' a >&2; echo END >&2`)}, "", "h")
	p, err := m.Spawn(context.Background(), "x")
	if err != nil {
		t.Fatal(err)
	}
	_ = p.Close()
	s := p.Stderr()
	if len(s) != 64*1024 || !strings.HasSuffix(s, "END\n") {
		t.Errorf("len %d, suffix %q", len(s), s[max(0, len(s)-8):])
	}

	var tb tailBuffer
	tb.max = 4
	_, _ = tb.Write([]byte("ab"))
	_, _ = tb.Write([]byte("cdefg"))
	if tb.String() != "defg" {
		t.Errorf("tail = %q", tb.String())
	}
}

func TestMasterClose(t *testing.T) {
	dir := t.TempDir()
	argvFile := filepath.Join(dir, "argv")
	shim := writeShim(t, `for a; do printf '%s\n' "$a"; done > `+argvFile)
	m := newTestMaster(t, Config{SSH: shim, StdinIsTTY: true}, "backup", "nas")
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(argvFile)
	if want := strings.Join(m.exitArgv()[1:], "\n") + "\n"; string(b) != want {
		t.Errorf("Close argv %q want %q", b, want)
	}

	for _, msg := range []string{
		"Control socket connect(/d/cm-0): No such file or directory",
		"Control socket connect(/d/cm-0): Connection refused",
	} {
		m := newTestMaster(t, Config{SSH: writeShim(t, `echo '`+msg+`' >&2; exit 255`)}, "", "h")
		if err := m.Close(); err != nil {
			t.Errorf("%q: Close = %v, want nil", msg, err)
		}
	}

	m = newTestMaster(t, Config{SSH: writeShim(t, `echo 'something else broke' >&2; exit 255`)}, "", "h")
	var se *Error
	if err := m.Close(); !errors.As(err, &se) || !strings.Contains(se.Stderr, "something else") {
		t.Errorf("Close = %v, want *Error", err)
	}
}

func TestMasterCloseTimeout(t *testing.T) {
	m := newTestMaster(t, Config{SSH: writeShim(t, `sleep 30`)}, "", "h")
	m.closeWait = 200 * time.Millisecond
	start := time.Now()
	if err := m.Close(); err == nil {
		t.Error("hung -O exit reported success")
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("Close took %v", d)
	}
}

func TestNewControlDir(t *testing.T) {
	d, err := NewControlDir()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(d) }()
	fi, err := os.Stat(d)
	if err != nil || !fi.IsDir() || fi.Mode().Perm() != 0o700 {
		t.Errorf("dir %v mode %v err %v", d, fi.Mode(), err)
	}
	if !strings.HasPrefix(filepath.Base(d), "syngoid-ssh-") {
		t.Errorf("dir %q", d)
	}
}

func TestDebugLogging(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	m := newTestMaster(t, Config{SSH: writeShim(t, `true`), Log: log}, "backup", "nas")
	if _, err := m.Run(context.Background(), "echo hi", nil); err != nil {
		t.Fatal(err)
	}
	s := buf.String()
	if !strings.Contains(s, "host=backup@nas") || !strings.Contains(s, "echo hi") {
		t.Errorf("log = %q", s)
	}
}

func TestSSHEnvNotOverridden(t *testing.T) {
	t.Setenv("LC_ALL", "sentinel")
	m := newTestMaster(t, Config{SSH: writeShim(t, `printf %s "$LC_ALL"`)}, "", "h")
	out, err := m.Run(context.Background(), "x", nil)
	if err != nil || string(out) != "sentinel" {
		t.Errorf("LC_ALL = %q err %v", out, err)
	}
}

func TestStartReturnsDespiteInheritedStderr(t *testing.T) {
	// Like a ProxyCommand child: outlives the foreground ssh holding its stderr.
	shim := writeShim(t, `(sleep 5 >&2 &); exit 0`)
	m := newTestMaster(t, Config{SSH: shim}, "", "h")
	start := time.Now()
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("Start took %v", d)
	}
	if left, _ := filepath.Glob(filepath.Join(filepath.Dir(m.ctlp), "*.err-*")); len(left) != 0 {
		t.Errorf("stderr file left behind: %v", left)
	}
}

func TestStartErrorCarriesStderr(t *testing.T) {
	shim := writeShim(t, `echo 'ssh: connect to host h port 22: Connection refused' >&2; (sleep 5 >&2 &); exit 255`)
	m := newTestMaster(t, Config{SSH: shim}, "", "h")
	start := time.Now()
	var se *Error
	err := m.Start(context.Background())
	if !errors.As(err, &se) || se.ExitCode != 255 || !strings.Contains(se.Stderr, "Connection refused") {
		t.Fatalf("Start err = %v", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("Start took %v", d)
	}
}

func TestStartLeavesStdinAlone(t *testing.T) {
	// `while read a b; do syngoid ...; done < jobs`: the master must not
	// consume the rest of the caller's stdin.
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	const jobs = "pool/a host:pool/a\npool/b host:pool/b\n"
	if _, err := w.WriteString(jobs); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	saved := os.Stdin
	os.Stdin = r
	t.Cleanup(func() { os.Stdin = saved })

	dir := t.TempDir()
	got := filepath.Join(dir, "stdin")
	argvFile := filepath.Join(dir, "argv")
	shim := writeShim(t, `for a; do printf '%s\n' "$a"; done > `+argvFile+`; cat > `+got)
	m, err := NewMaster(Config{SSH: shim}, "", "h", dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(got); err != nil || len(b) != 0 {
		t.Errorf("master read %q from stdin (err %v), want nothing", b, err)
	}
	if rest, err := io.ReadAll(r); err != nil || string(rest) != jobs {
		t.Errorf("stdin left = %q (err %v), want %q", rest, err, jobs)
	}
	if b, _ := os.ReadFile(argvFile); !slices.Contains(strings.Split(string(b), "\n"), "-n") {
		t.Errorf("master argv lacks -n:\n%s", b)
	}
}

func TestStartControlDirMissing(t *testing.T) {
	m := newTestMaster(t, Config{SSH: writeShim(t, `true`)}, "", "h")
	m.ctlp = "/nonexistent-dir/cm-0"
	var se *Error
	if err := m.Start(context.Background()); !errors.As(err, &se) {
		t.Errorf("Start = %v, want *Error", err)
	}
}

func TestRunSucceedsWhenPipeHeldAfterExit(t *testing.T) {
	// ssh exits 0 but a background process keeps stdout open: success, bounded wait.
	m := newTestMaster(t, Config{SSH: writeShim(t, `echo ok; (sleep 5 &) ; exit 0`)}, "", "h")
	m.killGrace = 300 * time.Millisecond
	start := time.Now()
	out, err := m.Run(context.Background(), "x", nil)
	if err != nil || !strings.HasPrefix(string(out), "ok") {
		t.Errorf("out %q err %v", out, err)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Errorf("Run took %v", d)
	}
}

func TestErrorMessage(t *testing.T) {
	if got := (&Error{Label: "h", ExitCode: 1}).Error(); got != "ssh h: exit 1" {
		t.Errorf("empty stderr message %q", got)
	}
	if got := (&Error{Label: "h", ExitCode: 1, Stderr: "w\nfatal\n"}).Error(); got != "ssh h: exit 1: fatal" {
		t.Errorf("message %q", got)
	}
}
