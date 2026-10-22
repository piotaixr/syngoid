//go:build unix

// Package sshx builds and spawns system ssh commands, one OpenSSH
// ControlMaster per host, and quotes values for the remote shell.
//
// Process groups: the master-creating ssh runs in the caller's foreground
// process group because it may prompt for a password on the terminal. Every
// other ssh runs in its own group, so a terminal Ctrl-C reaches only the
// caller, which then shuts the sessions down itself.
package sshx

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
	"sync"
	"syscall"
	"time"
	"unicode"

	"github.com/piotaixr/syngoid/internal/logx"
)

const (
	stderrCap    = 64 * 1024
	defaultWait  = 5 * time.Second // Proc.Close wait and Master.Close timeout
	defaultGrace = 5 * time.Second // SIGTERM to SIGKILL escalation on cancel
	pipeDelay    = 2 * time.Second // backstop for stray holders of a pipe
)

// Config is the ssh invocation configuration shared by every Master.
type Config struct {
	SSH        string   // ssh binary; default "ssh"
	Options    []string // extra "-o" values, e.g. "Port=2222"
	ConfigFile string   // passed as -F when non-empty
	StdinIsTTY bool     // when false, BatchMode=yes is added
	Log        *slog.Logger
}

// Error describes a failed ssh invocation.
type Error struct {
	Label    string
	Argv     []string
	ExitCode int // -1 if ssh did not run to an exit status
	Stderr   string

	cause error
}

func (e *Error) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("ssh %s: %v", e.Label, e.cause)
	}
	line := ""
	for _, l := range strings.Split(e.Stderr, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			line = l
		}
	}
	if line == "" {
		return fmt.Sprintf("ssh %s: exit %d", e.Label, e.ExitCode)
	}
	return fmt.Sprintf("ssh %s: exit %d: %s", e.Label, e.ExitCode, line)
}

func (e *Error) Unwrap() error { return e.cause }

// NewControlDir creates the private (0700) directory holding control sockets.
func NewControlDir() (string, error) {
	return os.MkdirTemp("", "syngoid-ssh-")
}

// Master is one ssh ControlMaster destination. Start brings the master up;
// Run and Spawn then multiplex over it.
type Master struct {
	cfg   Config
	user  string
	host  string
	dest  string
	label string
	ctlp  string // control socket path

	// Test hooks; zero values select the production defaults.
	closeWait time.Duration // Proc.Close wait and Master.Close timeout (default 5 s)
	killGrace time.Duration // SIGTERM to SIGKILL delay on cancel (default 5 s)
}

// NewMaster validates the destination and returns a Master whose control
// socket is <controlDir>/cm-<n>.
func NewMaster(cfg Config, user, host, controlDir string, n int) (*Master, error) {
	if !validName(host, false) || !validName(user, true) {
		return nil, fmt.Errorf("invalid ssh destination %q", joinDest(user, host))
	}
	if cfg.SSH == "" {
		cfg.SSH = "ssh"
	}
	m := &Master{
		cfg:  cfg,
		user: user,
		host: host,
		dest: joinDest(user, host),
		ctlp: fmt.Sprintf("%s/cm-%d", controlDir, n),
	}
	m.label = m.dest
	return m, nil
}

func joinDest(user, host string) string {
	if user == "" {
		return host
	}
	return user + "@" + host
}

// validName rejects values ssh could take for an option or split oddly.
func validName(s string, isUser bool) bool {
	if s == "" {
		return isUser // the user is optional, the host is not
	}
	if strings.HasPrefix(s, "-") {
		return false
	}
	for _, r := range s {
		if unicode.IsSpace(r) || unicode.IsControl(r) || (isUser && r == '@') {
			return false
		}
	}
	return true
}

// Label is "user@host" or "host".
func (m *Master) Label() string { return m.label }

// argv builds the ssh command line. ssh keeps the first value of each
// option: control holds our own options, placed before the user's so they
// win; defaults follow the user's, so a user option wins over them.
func (m *Master) argv(control, defaults []string, remote string) []string {
	a := []string{m.cfg.SSH}
	if m.cfg.ConfigFile != "" {
		a = append(a, "-F", m.cfg.ConfigFile)
	}
	a = append(a, "-o", "ControlPath="+m.ctlp)
	a = append(a, control...)
	for _, o := range m.cfg.Options {
		a = append(a, "-o", o)
	}
	a = append(a, defaults...)
	return append(a, "-T", "--", m.dest, remote)
}

// userSets reports whether a user option sets key (lower case).
func (m *Master) userSets(key string) bool {
	for _, o := range m.cfg.Options {
		if optionKey(o) == key {
			return true
		}
	}
	return false
}

// optionKey returns the lower-cased key of an ssh "Key=Value" / "Key Value" option.
func optionKey(o string) string {
	o = strings.TrimSpace(o)
	if i := strings.IndexAny(o, "= \t"); i >= 0 {
		o = o[:i]
	}
	return strings.ToLower(o)
}

func (m *Master) masterArgv() []string {
	var defaults []string
	if !m.cfg.StdinIsTTY && !m.userSets("batchmode") {
		defaults = append(defaults, "-o", "BatchMode=yes")
	}
	// yamux keepalive is off: ServerAlive on the master, shared by every mux
	// client, detects a silent network drop within ~60 s.
	if !m.userSets("serveraliveinterval") {
		defaults = append(defaults, "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=4")
	}
	// -n: the master never reads stdin, which belongs to the caller (e.g. a
	// `while read` loop); ssh prompts on /dev/tty, not stdin.
	return m.argv([]string{"-n", "-o", "ControlMaster=yes", "-o", "ControlPersist=60"}, defaults, "exit")
}

func (m *Master) clientArgv(cmd string) []string {
	// A mux client never authenticates; BatchMode keeps it from blocking on
	// a password prompt if the master is gone.
	return m.argv([]string{"-o", "ControlMaster=no", "-o", "BatchMode=yes"}, nil, cmd)
}

func (m *Master) exitArgv() []string {
	a := []string{m.cfg.SSH}
	if m.cfg.ConfigFile != "" {
		a = append(a, "-F", m.cfg.ConfigFile)
	}
	return append(a, "-o", "ControlPath="+m.ctlp, "-O", "exit", "--", m.dest)
}

func (m *Master) debug(argv []string) {
	if m.cfg.Log != nil {
		m.cfg.Log.Debug(strings.Join(argv, " "), logx.KeyHost, m.label)
	}
}

func (m *Master) newError(argv []string, exit int, stderr string, cause error) *Error {
	return &Error{Label: m.label, Argv: argv, ExitCode: exit, Stderr: stderr, cause: cause}
}

// exitError converts the error of a finished command into an *Error.
func (m *Master) exitError(argv []string, err error, stderr string) *Error {
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return m.newError(argv, ee.ExitCode(), stderr, nil)
	}
	return m.newError(argv, -1, stderr, err)
}

func (m *Master) wait() time.Duration {
	if m.closeWait > 0 {
		return m.closeWait
	}
	return defaultWait
}

func (m *Master) grace() time.Duration {
	if m.killGrace > 0 {
		return m.killGrace
	}
	return defaultGrace
}

// Start brings the master up in the foreground process group so ssh can
// prompt for a password on the terminal (/dev/tty); its stdin is /dev/null.
// The master then daemonizes (ControlPersist). A failure is an *Error.
//
// The master's stderr goes to a file in the control directory, not a pipe: a
// ProxyCommand child inherits it for the master's whole life, and a pipe read
// end closed after Start would kill such a proxy with SIGPIPE.
func (m *Master) Start(ctx context.Context) error {
	argv := m.masterArgv()
	m.debug(argv)
	errFile, err := os.CreateTemp(filepath.Dir(m.ctlp), filepath.Base(m.ctlp)+".err-")
	if err != nil {
		return m.newError(argv, -1, "", err)
	}
	defer func() {
		_ = errFile.Close()
		_ = os.Remove(errFile.Name())
	}()

	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Stdin = nil  // /dev/null, like -n
	cmd.Stdout = nil // /dev/null
	cmd.Stderr = errFile
	// Same group as the caller: only the ssh process itself is signalled.
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = pipeDelay // SIGKILL if SIGTERM is ignored after a cancel
	err = cmd.Run()
	if errors.Is(err, exec.ErrWaitDelay) {
		err = nil // exited 0
	}
	if err == nil {
		return nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("ssh %s: %w", m.label, ctxErr)
	}
	return m.exitError(argv, err, readTail(errFile, stderrCap))
}

// readTail returns the last limit bytes of f.
func readTail(f *os.File, limit int64) string {
	fi, err := f.Stat()
	if err != nil {
		return ""
	}
	off := max(fi.Size()-limit, 0)
	b := make([]byte, fi.Size()-off)
	n, _ := f.ReadAt(b, off)
	return string(b[:n])
}

// Run executes cmd on the remote host over the master, in its own process
// group, and returns its stdout. A failure is an *Error carrying the last
// 64 KiB of stderr. Cancelling ctx sends SIGTERM to the whole group; if ssh
// has not finished after the grace period it is killed and the group is
// SIGKILLed.
func (m *Master) Run(ctx context.Context, cmd string, stdin io.Reader) ([]byte, error) {
	argv := m.clientArgv(cmd)
	m.debug(argv)
	c := exec.CommandContext(ctx, argv[0], argv[1:]...)
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	c.Stdin = stdin
	var out bytes.Buffer
	c.Stdout = &out
	stderr := &tailBuffer{max: stderrCap}
	c.Stderr = stderr
	// Setpgid: the child leads its own group, so pgid == pid.
	c.Cancel = func() error { return killGroup(c.Process.Pid, syscall.SIGTERM) }
	// After a cancel this bounds both a leader ignoring SIGTERM and pipes
	// still held by the mux master, so Run returns within the grace period.
	c.WaitDelay = m.grace()

	if err := c.Start(); err != nil {
		return nil, m.newError(argv, -1, "", err)
	}
	pgid := c.Process.Pid
	err := c.Wait()
	if errors.Is(err, exec.ErrWaitDelay) {
		err = nil // exited 0; something kept a pipe open
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		_ = killGroup(pgid, syscall.SIGKILL) // reap members that outlived the leader
		return nil, fmt.Errorf("ssh %s: %w", m.label, ctxErr)
	}
	if err != nil {
		return nil, m.exitError(argv, err, stderr.String())
	}
	return out.Bytes(), nil
}

// Spawn starts cmd on the remote host over the master, in its own process
// group. ctx only bounds the start: the process lives until Proc.Close.
func (m *Master) Spawn(ctx context.Context, cmd string) (*Proc, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("ssh %s: %w", m.label, err)
	}
	argv := m.clientArgv(cmd)
	m.debug(argv)

	inR, inW, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		_ = inR.Close()
		_ = inW.Close()
		return nil, err
	}
	c := exec.Command(argv[0], argv[1:]...)
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	c.Stdin = inR
	c.Stdout = outW
	stderr := &tailBuffer{max: stderrCap}
	c.Stderr = stderr
	c.WaitDelay = pipeDelay
	err = c.Start()
	// The child has its own copies now.
	_ = inR.Close()
	_ = outW.Close()
	if err != nil {
		_ = inW.Close()
		_ = outR.Close()
		return nil, m.newError(argv, -1, "", err)
	}

	p := &Proc{
		cmd:    c,
		stdin:  inW,
		stdout: outR,
		stderr: stderr,
		done:   make(chan struct{}),
		wait:   m.wait(),
		fail:   func(err error) error { return m.exitError(argv, err, stderr.String()) },
	}
	go func() {
		p.waitErr = c.Wait()
		close(p.done)
	}()
	return p, nil
}

// Close stops the master (ssh -O exit) with a 5 s timeout. A master that is
// already gone is not an error.
func (m *Master) Close() error {
	argv := m.exitArgv()
	m.debug(argv)
	ctx, cancel := context.WithTimeout(context.Background(), m.wait())
	defer cancel()
	c := exec.Command(argv[0], argv[1:]...)
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	c.Stdout = io.Discard
	stderr := &tailBuffer{max: stderrCap}
	c.Stderr = stderr
	c.WaitDelay = pipeDelay
	if err := c.Start(); err != nil {
		return m.newError(argv, -1, "", err)
	}
	pgid := c.Process.Pid
	stop := context.AfterFunc(ctx, func() { _ = killGroup(pgid, syscall.SIGKILL) })
	err := c.Wait()
	stop()
	if errors.Is(err, exec.ErrWaitDelay) {
		err = nil
	}
	if ctx.Err() != nil {
		return fmt.Errorf("ssh %s: -O exit: %w", m.label, ctx.Err())
	}
	if err == nil {
		return nil
	}
	if s := strings.ToLower(stderr.String()); strings.Contains(s, "no such file") || strings.Contains(s, "control socket connect") {
		return nil
	}
	return m.exitError(argv, err, stderr.String())
}

// Proc is a running remote command: Read returns its stdout, Write feeds its
// stdin.
type Proc struct {
	cmd    *exec.Cmd
	stdin  *os.File
	stdout *os.File
	stderr *tailBuffer
	done   chan struct{} // closed once cmd.Wait returned
	wait   time.Duration
	fail   func(error) error

	waitErr error // valid after done is closed

	closeOnce sync.Once
	closeErr  error
}

func (p *Proc) Read(b []byte) (int, error)  { return p.stdout.Read(b) }
func (p *Proc) Write(b []byte) (int, error) { return p.stdin.Write(b) }

// Stderr returns the last 64 KiB the process wrote to stderr so far.
func (p *Proc) Stderr() string { return p.stderr.String() }

// Close closes stdin, waits up to 5 s for the process to exit, then kills its
// process group. It is idempotent. It returns nil when the process exited 0
// or was killed by Close itself, and an *Error otherwise.
func (p *Proc) Close() error {
	p.closeOnce.Do(func() {
		_ = p.stdin.Close()
		killed := false
		t := time.NewTimer(p.wait)
		select {
		case <-p.done:
			t.Stop()
		case <-t.C:
			killed = true
			_ = killGroup(p.cmd.Process.Pid, syscall.SIGKILL)
			<-p.done
		}
		_ = p.stdout.Close()
		switch {
		case p.waitErr == nil, errors.Is(p.waitErr, exec.ErrWaitDelay):
		case killed:
		default:
			p.closeErr = p.fail(p.waitErr)
		}
	})
	return p.closeErr
}

// killGroup signals the process group pgid.
func killGroup(pgid int, sig syscall.Signal) error {
	err := syscall.Kill(-pgid, sig)
	if errors.Is(err, syscall.ESRCH) {
		return os.ErrProcessDone
	}
	return err
}

// tailBuffer keeps the last max bytes written to it; safe for concurrent use.
type tailBuffer struct {
	mu  sync.Mutex
	buf []byte
	max int
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := len(p)
	if n >= b.max {
		b.buf = append(b.buf[:0], p[n-b.max:]...)
		return n, nil
	}
	if over := len(b.buf) + n - b.max; over > 0 {
		b.buf = append(b.buf[:0], b.buf[over:]...)
	}
	b.buf = append(b.buf, p...)
	return n, nil
}

func (b *tailBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

// ShellQuote quotes s for a POSIX shell in single quotes. Each embedded
// single quote is written as the four characters below:
//
//	'\''
func ShellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// ShellJoin quotes each arg and joins them with spaces.
func ShellJoin(args ...string) string {
	q := make([]string, len(args))
	for i, a := range args {
		q[i] = ShellQuote(a)
	}
	return strings.Join(q, " ")
}
