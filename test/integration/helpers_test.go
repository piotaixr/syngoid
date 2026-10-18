//go:build integration

// Package integration drives the built syngoid binary against real,
// file-backed ZFS pools. It only runs with the integration build tag, as
// root, with SYNGOID_IT=1 and the pools created by scripts/ci/setup-zfs.sh:
//
//	sudo -E env PATH=$PATH SYNGOID_IT=1 go test -tags integration -v ./test/integration/...
//
// Every test starts by destroying all children of the two test pools, so a
// stray run can never touch any other pool.
package integration

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	srcPool = "syngoid-src"
	dstPool = "syngoid-dst"
	// itUser is the unprivileged user of the delegation tests.
	itUser = "syngoid-it"
	// invasiveEnv enables tests that briefly change the system clock.
	invasiveEnv = "SYNGOID_IT_INVASIVE"
)

var (
	// enabled is true when SYNGOID_IT=1 and TestMain's checks passed.
	enabled bool
	// binPath is the syngoid binary built by TestMain, readable and
	// executable by itUser.
	binPath string
	// zfsPath is the absolute path of the zfs command.
	zfsPath string
	// lastEnd is when the previous syngoid run ended.
	lastEnd time.Time
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	if os.Getenv("SYNGOID_IT") != "1" {
		fmt.Println("integration tests skipped: set SYNGOID_IT=1 (see scripts/ci/setup-zfs.sh)")
		return m.Run()
	}
	if os.Geteuid() != 0 {
		fmt.Fprintln(os.Stderr, "integration tests must run as root (sudo -E env PATH=$PATH go test ...)")
		return 2
	}
	var err error
	if zfsPath, err = findZFS(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	for _, pool := range []string{srcPool, dstPool} {
		if err := zfsCommand("", "list", "-H", "-o", "name", pool).Run(); err != nil {
			fmt.Fprintf(os.Stderr, "refusing to run: test pool %s does not exist (run scripts/ci/setup-zfs.sh)\n", pool)
			return 2
		}
	}
	if _, err := user.Lookup(itUser); err != nil {
		fmt.Fprintf(os.Stderr, "refusing to run: user %s does not exist (run scripts/ci/setup-zfs.sh): %v\n", itUser, err)
		return 2
	}

	// The directory must be traversable by itUser; MkdirTemp creates 0700
	// and TMPDIR may point below a private home, so use /tmp explicitly.
	dir, err := os.MkdirTemp("/tmp", "syngoid-it-bin-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	defer func() { _ = os.RemoveAll(dir) }()
	if err := os.Chmod(dir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	binPath = filepath.Join(dir, "syngoid")
	build := exec.Command("go", "build", "-o", binPath, "github.com/piotaixr/syngoid/cmd/syngoid")
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	build.Stdout, build.Stderr = os.Stderr, os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "building syngoid: %v\n", err)
		return 2
	}
	enabled = true
	return m.Run()
}

// findZFS returns the absolute path of zfs.
func findZFS() (string, error) {
	if p, err := exec.LookPath("zfs"); err == nil {
		return filepath.Abs(p)
	}
	for _, p := range []string{"/usr/sbin/zfs", "/sbin/zfs", "/usr/local/sbin/zfs"} {
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}
	return "", errors.New("zfs not found in PATH or the sbin directories")
}

// begin skips the test unless the suite is enabled, and resets both pools
// before and after it.
func begin(t *testing.T) {
	t.Helper()
	if !enabled {
		t.Skip("set SYNGOID_IT=1 and run as root with the test pools (scripts/ci/setup-zfs.sh)")
	}
	resetPools(t)
	t.Cleanup(func() { resetPools(t) })
}

// requireInvasive skips tests that change the system clock unless
// SYNGOID_IT_INVASIVE=1.
func requireInvasive(t *testing.T) {
	t.Helper()
	if os.Getenv(invasiveEnv) != "1" {
		t.Skipf("changes the system clock; set %s=1 to run", invasiveEnv)
	}
}

// resetPools releases every hold in the test pools and destroys all their
// children. The pools themselves are never touched.
func resetPools(t *testing.T) {
	t.Helper()
	for _, pool := range []string{srcPool, dstPool} {
		out := zfs(t, "list", "-H", "-p", "-t", "snapshot", "-o", "name,userrefs", "-r", pool)
		for _, line := range lines(out) {
			name, refs, _ := strings.Cut(line, "\t")
			if refs == "0" {
				continue
			}
			for _, h := range lines(zfs(t, "holds", "-H", name)) {
				f := strings.Split(h, "\t")
				if len(f) >= 2 {
					zfs(t, "release", f[1], name)
				}
			}
		}
		for _, child := range lines(zfs(t, "list", "-H", "-o", "name", "-d", "1", "-t", "filesystem,volume", pool)) {
			if child == pool || !strings.HasPrefix(child, pool+"/") {
				continue
			}
			destroyWithRetry(t, child)
		}
	}
}

// destroyWithRetry destroys ds and everything depending on it, retrying
// while udev still holds a freshly created zvol open.
func destroyWithRetry(t *testing.T, ds string) {
	t.Helper()
	var err error
	for range 10 {
		if _, err = zfsTry("destroy", "-R", "-f", ds); err == nil {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("cannot destroy %s: %v", ds, err)
}

// result is the outcome of one syngoid run.
type result struct {
	code           int
	stdout, stderr string
}

// syngoid runs the binary as root and returns its outcome.
func syngoid(t *testing.T, args ...string) result {
	t.Helper()
	return runSyngoid(t, "", args...)
}

// syngoidOK runs the binary as root and fails the test unless it exits 0.
func syngoidOK(t *testing.T, args ...string) result {
	t.Helper()
	r := syngoid(t, args...)
	if r.code != 0 {
		t.Fatalf("syngoid %s: exit %d, want 0", strings.Join(args, " "), r.code)
	}
	return r
}

// syngoidAs runs the binary as user through sudo and returns its outcome.
func syngoidAs(t *testing.T, user string, args ...string) result {
	t.Helper()
	return runSyngoid(t, user, args...)
}

// runSyngoid runs syngoid, as root when user is empty.
func runSyngoid(t *testing.T, user string, args ...string) result {
	t.Helper()
	cmd := syngoidCmd(user, args...)
	r, err := runCmd(cmd)
	lastEnd = time.Now()
	if err != nil {
		t.Fatalf("running syngoid: %v", err)
	}
	logResult(t, user, args, r)
	return r
}

// runCmd runs cmd to completion and returns its outcome. The error is
// non-nil only when the process could not be run to an exit status; a
// non-zero exit is reported in result.code. It touches no shared state, so
// it can run in a goroutine.
func runCmd(cmd *exec.Cmd) (result, error) {
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	r := result{stdout: stdout.String(), stderr: stderr.String()}
	var ee *exec.ExitError
	switch {
	case err == nil:
		return r, nil
	case errors.As(err, &ee):
		r.code = ee.ExitCode()
		return r, nil
	}
	return r, err
}

// waitNextSecond waits for a wall clock second later than the one the
// previous run ended in, so the next run's sync snapshot name (second
// resolution) differs from the previous one's.
func waitNextSecond() {
	for time.Now().Unix() <= lastEnd.Unix() {
		time.Sleep(time.Until(time.Unix(lastEnd.Unix()+1, 0)))
	}
}

// syngoidCmd prepares a syngoid command, first waiting for a later wall
// clock second than the previous run ended in (see waitNextSecond).
func syngoidCmd(user string, args ...string) *exec.Cmd {
	waitNextSecond()
	var cmd *exec.Cmd
	if user == "" {
		cmd = exec.Command(binPath, args...)
	} else {
		cmd = exec.Command("sudo", append([]string{"-n", "-u", user, "--", binPath}, args...)...)
	}
	cmd.Dir = "/"
	return cmd
}

func logResult(t *testing.T, user string, args []string, r result) {
	t.Helper()
	logTool(t, user, "syngoid", args, r)
}

// logTool logs the outcome of one run of tool by user ("" for root).
func logTool(t *testing.T, user, tool string, args []string, r result) {
	t.Helper()
	who := "root"
	if user != "" {
		who = user
	}
	t.Logf("[%s] %s %s → exit %d\n--- stdout:\n%s--- stderr:\n%s", who, tool, strings.Join(args, " "), r.code, r.stdout, r.stderr)
}

// zfs runs zfs as root and returns its trimmed stdout, failing the test on
// error.
func zfs(t *testing.T, args ...string) string {
	t.Helper()
	out, err := zfsTry(args...)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// zfsCommand returns a zfs command with LC_ALL=C (so its messages are
// predictable), run as root or, when user is non-empty, as user via sudo.
func zfsCommand(user string, args ...string) *exec.Cmd {
	var cmd *exec.Cmd
	if user == "" {
		cmd = exec.Command(zfsPath, args...)
		cmd.Env = append(os.Environ(), "LC_ALL=C")
	} else {
		// sudo resets the environment, so set the locale inside it.
		cmd = exec.Command("sudo", append([]string{"-n", "-u", user, "--", "env", "LC_ALL=C", zfsPath}, args...)...)
	}
	cmd.Dir = "/"
	return cmd
}

// zfsTry runs zfs as root; the error includes the stderr.
func zfsTry(args ...string) (string, error) {
	cmd := zfsCommand("", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("zfs %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(stdout.String()), nil
}

// lines splits trimmed command output into lines; empty output has none.
func lines(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

// prop returns the value of property name of ds, as zfs get prints it.
func prop(t *testing.T, ds, name string) string {
	t.Helper()
	return zfs(t, "get", "-H", "-o", "value", name, ds)
}

// propSource returns the source column of property name of ds.
func propSource(t *testing.T, ds, name string) string {
	t.Helper()
	return zfs(t, "get", "-H", "-o", "source", name, ds)
}

// exists reports whether the dataset, snapshot or bookmark exists. Any
// error other than "does not exist" fails the test, so a negative check
// cannot pass by accident.
func exists(t *testing.T, name string) bool {
	t.Helper()
	_, err := zfsTry("list", "-H", "-o", "name", "-t", "all", name)
	switch {
	case err == nil:
		return true
	case strings.Contains(err.Error(), "does not exist"):
		return false
	}
	t.Fatal(err)
	return false
}

// snaps returns the snapshot names of ds (not of its children) in zfs
// list order, which is creation order.
func snaps(t *testing.T, ds string) []string {
	t.Helper()
	return lines(zfs(t, "list", "-H", "-t", "snapshot", "-o", "name", "-d", "1", ds))
}

// snapsR returns the snapshot names of ds and its descendants in zfs list
// order: datasets by name, snapshots by creation.
func snapsR(t *testing.T, ds string) []string {
	t.Helper()
	return lines(zfs(t, "list", "-H", "-t", "snapshot", "-o", "name", "-r", ds))
}

// bookmarksR returns the bookmark names of ds and its descendants.
func bookmarksR(t *testing.T, ds string) []string {
	t.Helper()
	return lines(zfs(t, "list", "-H", "-t", "bookmark", "-o", "name", "-r", ds))
}

// full prefixes each short snapshot name with ds@.
func full(ds string, names ...string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = ds + "@" + n
	}
	return out
}

// isSyncSnap reports whether a snapshot name is a syncoid/syngoid sync
// snapshot.
func isSyncSnap(name string) bool {
	return strings.Contains(name, "@syncoid_")
}

// withoutSync drops sync snapshots from names.
func withoutSync(names []string) []string {
	return slices.DeleteFunc(slices.Clone(names), isSyncSnap)
}

var syncSuffix = regexp.MustCompile(`@syncoid_.*$`)

// normalizeSync replaces every sync snapshot suffix with "@syncoid_", as
// upstream's `sed 's/@syncoid_.*$/@syncoid_/'` does.
func normalizeSync(names []string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = syncSuffix.ReplaceAllString(n, "@syncoid_")
	}
	return out
}

// syncSnaps returns the sync snapshots of ds matching pattern.
func syncSnaps(t *testing.T, ds string, pattern *regexp.Regexp) []string {
	t.Helper()
	var out []string
	for _, n := range snaps(t, ds) {
		_, short, _ := strings.Cut(n, "@")
		if pattern.MatchString(short) {
			out = append(out, n)
		}
	}
	return out
}

// equal fails the test when got differs from want.
func equal(t *testing.T, what string, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("%s:\n got  %q\n want %q", what, got, want)
	}
}

// hostname returns the local hostname that syngoid puts in sync snapshot
// names and hold tags.
func hostname(t *testing.T) string {
	t.Helper()
	h, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// mountpoint mounts ds if needed and returns its mountpoint.
func mountpoint(t *testing.T, ds string) string {
	t.Helper()
	if prop(t, ds, "mounted") != "yes" {
		zfs(t, "mount", ds)
	}
	return prop(t, ds, "mountpoint")
}

// writeRandomFile writes size pseudo-random (incompressible) bytes to path.
func writeRandomFile(t *testing.T, path string, size int64) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	src := rand.NewChaCha8([32]byte{byte(size), byte(size >> 8), byte(size >> 16)})
	if _, err := io.CopyN(f, src, size); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// fileSHA256 returns the hex SHA-256 of the file at path.
func fileSHA256(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// resumeToken returns the receive_resume_token of ds ("-" when none).
func resumeToken(t *testing.T, ds string) string {
	t.Helper()
	return prop(t, ds, "receive_resume_token")
}

// requireResumeToken fails the test unless ds holds a resume token.
func requireResumeToken(t *testing.T, ds string) {
	t.Helper()
	if tok := resumeToken(t, ds); tok == "-" || tok == "" {
		t.Fatalf("%s has no receive_resume_token after the interrupted transfer", ds)
	}
}

// pipeZFS runs `zfs sendArgs... | zfs recvArgs...`, the receive as user
// through sudo when user is non-empty, copying at most limit bytes of the
// stream when limit > 0 (like `head --bytes`). It returns the receive's
// stderr and error; the send's failure is ignored when the stream was cut.
func pipeZFS(t *testing.T, sendArgs []string, user string, recvArgs []string, limit int64) (string, error) {
	t.Helper()
	send := zfsCommand("", sendArgs...)
	recv := zfsCommand(user, recvArgs...)
	var sendErr, recvErr bytes.Buffer
	send.Stderr, recv.Stderr = &sendErr, &recvErr

	sendOut, err := send.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	recvIn, err := recv.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := send.Start(); err != nil {
		t.Fatal(err)
	}
	if err := recv.Start(); err != nil {
		_ = send.Process.Kill()
		_ = send.Wait()
		t.Fatal(err)
	}
	var src io.Reader = sendOut
	if limit > 0 {
		src = io.LimitReader(sendOut, limit)
	}
	_, _ = io.Copy(recvIn, src)
	_ = recvIn.Close()
	rerr := recv.Wait()
	// Closing the read end makes a cut-off send fail with EPIPE.
	_ = sendOut.Close()
	serr := send.Wait()
	if serr != nil && limit <= 0 {
		t.Logf("zfs %s: %v: %s", strings.Join(sendArgs, " "), serr, sendErr.String())
	}
	if rerr != nil {
		return recvErr.String(), fmt.Errorf("zfs %s: %w", strings.Join(recvArgs, " "), rerr)
	}
	return recvErr.String(), nil
}

// interruptSync lets the stream run until the sender has written
// interruptBytes or for interruptWait, whichever comes first, before
// freezing it. A local transfer can exceed 1 GB/s, so both are small; any
// stream past its BEGIN record leaves a resume token.
const (
	interruptBytes = 8 << 20
	interruptWait  = 200 * time.Millisecond
)

// interruptSync runs syngoid as root and interrupts it in the middle of its
// first zfs send, the way a user pressing ^C would: once the send has
// streamed a little (see interruptBytes), the zfs send process is stopped
// so the transfer cannot complete, then syngoid receives SIGTERM and must
// cancel the receive (leaving a resume token) and exit 2 (spec §9.4, §10.3).
// It replaces upstream's `--source-bwlimit=2m` plus a timed kill, since
// syngoid has no bandwidth limit (spec §3.6).
func interruptSync(t *testing.T, args ...string) result {
	t.Helper()
	cmd := syngoidCmd("", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	pid := cmd.Process.Pid
	var sendPID, recvPID int
	// killAll kills syngoid and its zfs children, which run in their own
	// process groups and would outlive it, keeping the target busy.
	killAll := func() {
		for _, p := range []int{pid, sendPID, recvPID} {
			if p != 0 {
				_ = syscall.Kill(p, syscall.SIGKILL)
			}
		}
	}
	finish := func() result {
		var err error
		select {
		case err = <-done:
		case <-time.After(2 * time.Minute):
			killAll()
			t.Fatalf("syngoid did not exit within 2 minutes (killed: %v)", <-done)
		}
		lastEnd = time.Now()
		r := result{stdout: stdout.String(), stderr: stderr.String()}
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			r.code = ee.ExitCode()
		}
		logResult(t, "", args, r)
		return r
	}
	abort := func(format string, a ...any) {
		t.Helper()
		killAll()
		finish()
		t.Fatalf(format, a...)
	}

	deadline := time.Now().Add(2 * time.Minute)
	for sendPID == 0 || recvPID == 0 {
		select {
		case err := <-done:
			done <- err
			abort("syngoid exited before its transfer could be interrupted")
		default:
		}
		if time.Now().After(deadline) {
			abort("no zfs send and zfs receive children appeared within 2 minutes")
		}
		for p, argv := range childProcs(pid) {
			if len(argv) < 2 || filepath.Base(argv[0]) != "zfs" {
				continue
			}
			switch {
			case argv[1] == "send" && !slices.Contains(argv, "-nvP"):
				sendPID = p
			case argv[1] == "receive":
				recvPID = p
			}
		}
		time.Sleep(5 * time.Millisecond)
	}

	until := time.Now().Add(interruptWait)
	for running(sendPID) && procWritten(sendPID) < interruptBytes && time.Now().Before(until) {
		time.Sleep(5 * time.Millisecond)
	}
	if !running(sendPID) {
		abort("zfs send finished before it could be frozen; the test data is too small")
	}
	if err := syscall.Kill(sendPID, syscall.SIGSTOP); err != nil {
		abort("stopping zfs send: %v", err)
	}
	sent := procWritten(sendPID)
	t.Logf("froze zfs send (pid %d) after %d bytes", sendPID, sent)
	// Wait until zfs receive has read what was sent, so it is past the BEGIN
	// record and a resume token will be saved. Without I/O accounting
	// (sent == 0) this waits the full timeout.
	drainBy := time.Now().Add(5 * time.Second)
	for running(recvPID) && (sent == 0 || procRead(recvPID) < sent) && time.Now().Before(drainBy) {
		time.Sleep(10 * time.Millisecond)
	}
	if !running(recvPID) {
		_ = syscall.Kill(sendPID, syscall.SIGCONT)
		abort("zfs receive ended before syngoid could be interrupted")
	}
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		_ = syscall.Kill(sendPID, syscall.SIGCONT)
		abort("signalling syngoid: %v", err)
	}
	// Let syngoid cancel the receive before the sender may run again.
	cancelBy := time.Now().Add(15 * time.Second)
	for running(recvPID) && time.Now().Before(cancelBy) {
		time.Sleep(10 * time.Millisecond)
	}
	_ = syscall.Kill(sendPID, syscall.SIGCONT)
	r := finish()
	if r.code != 2 {
		t.Fatalf("interrupted syngoid: exit %d, want 2", r.code)
	}
	return r
}

// childProcs returns the argv of every live child process of parent.
func childProcs(parent int) map[int][]string {
	out := map[int][]string{}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return out
	}
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		state, ppid, ok := procStat(pid)
		if !ok || ppid != parent || state == "Z" {
			continue
		}
		cmdline, err := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
		if err != nil || len(cmdline) == 0 {
			continue
		}
		out[pid] = strings.Split(strings.TrimRight(string(cmdline), "\x00"), "\x00")
	}
	return out
}

// procStat returns the state and parent pid of pid from /proc/<pid>/stat.
func procStat(pid int) (state string, ppid int, ok bool) {
	b, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return "", 0, false
	}
	// The command name may contain spaces and parentheses; the fields after
	// the last ')' are: state ppid ...
	i := bytes.LastIndexByte(b, ')')
	if i < 0 {
		return "", 0, false
	}
	f := strings.Fields(string(b[i+1:]))
	if len(f) < 2 {
		return "", 0, false
	}
	ppid, err = strconv.Atoi(f[1])
	return f[0], ppid, err == nil
}

// running reports whether pid exists and is not a zombie.
func running(pid int) bool {
	state, _, ok := procStat(pid)
	return ok && state != "Z"
}

// procWritten returns the bytes pid has written so far (wchar of
// /proc/<pid>/io), or 0 when unknown.
func procWritten(pid int) int64 {
	return procIO(pid, "wchar")
}

// procRead returns the bytes pid has read so far (rchar of /proc/<pid>/io),
// or 0 when unknown.
func procRead(pid int) int64 {
	return procIO(pid, "rchar")
}

// procIO returns counter field of /proc/<pid>/io, or 0 when unknown.
func procIO(pid int, field string) int64 {
	b, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "io"))
	if err != nil {
		return 0
	}
	for _, l := range strings.Split(string(b), "\n") {
		if v, ok := strings.CutPrefix(l, field+": "); ok {
			n, _ := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
			return n
		}
	}
	return 0
}

// setClock sets the system wall clock (GNU date).
func setClock(t *testing.T, tm time.Time) {
	t.Helper()
	at := fmt.Sprintf("@%d.%09d", tm.Unix(), tm.Nanosecond())
	out, err := exec.Command("date", "--utc", "--set", at).CombinedOutput()
	if err != nil {
		t.Fatalf("setting the clock: %v: %s", err, out)
	}
}
