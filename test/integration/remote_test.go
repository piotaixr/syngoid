//go:build integration

package integration

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The remote tests replicate through ssh to this machine: the initiator runs
// as root and reaches itUser@localhost with root's key (scripts/ci/setup-zfs.sh
// sets up sshd, the keys, known_hosts and a sudoers rule for zfs). The remote
// side is therefore a real agent, pushed to itUser's home, running as the
// unprivileged itUser with delegations only.
//
// No test delegates on a pool's root dataset: a grant there would outlive the
// test's cleanup and let later tests pass their "without grants" step. Targets
// created by itUser sit below a parent dataset the test creates, and the
// printed grant lands on that parent.

// createSource creates ds with size bytes of incompressible data.
func createSource(t *testing.T, ds string, size int64) {
	t.Helper()
	zfs(t, "create", ds)
	writeRandomFile(t, filepath.Join(mountpoint(t, ds), "data"), size)
}

// requireSameGUID fails the test unless snapshot name (short, after the '@')
// has the same guid on src and dst.
func requireSameGUID(t *testing.T, src, dst, name string) {
	t.Helper()
	if g, w := prop(t, dst+"@"+name, "guid"), prop(t, src+"@"+name, "guid"); g != w {
		t.Fatalf("guid of %s@%s = %s, want %s", dst, name, g, w)
	}
}

// requireNewestSyncGUID fails the test unless the only sync snapshot of src
// is on dst with the same guid.
func requireNewestSyncGUID(t *testing.T, src, dst string) {
	t.Helper()
	requireSameGUID(t, src, dst, short(oneSyncSnap(t, src, "", hostname(t))))
}

func TestITRemoteTargetDelegated(t *testing.T) {
	beginRemote(t)
	forgetAgents(t, itUser)
	src, parent := srcPool+"/a", dstPool+"/rd"
	dst := parent + "/a"
	createSource(t, src, 4<<20)
	zfs(t, "snapshot", src+"@s1")
	zfs(t, "create", parent)
	args := []string{src, remoteLoc(itUser, dst)}

	// The first run pushes the agent, then stops at the preflight.
	first := grantFromRemotePreflight(t, args...)
	agent := agentPath(t, itUser)
	wantPush := "INFO: [" + itUser + "@" + remoteHost + "] pushing syngoid agent to " + itUser + "@" + remoteHost + ":" + agent
	if !strings.Contains(first.stdout, wantPush) {
		t.Errorf("the first run did not log %q", wantPush)
	}

	second := syngoidOK(t, args...)
	if strings.Contains(second.stdout, "pushing syngoid agent") {
		t.Error("the second run pushed the agent again")
	}
	st, err := os.Stat(agent)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o700 {
		t.Errorf("mode of %s = %v, want 0700", agent, st.Mode().Perm())
	}
	if dirSt, err := os.Stat(filepath.Dir(agent)); err != nil {
		t.Error(err)
	} else if dirSt.Mode().Perm() != 0o700 {
		t.Errorf("mode of %s = %v, want 0700", filepath.Dir(agent), dirSt.Mode().Perm())
		logModes(t, agent)
	}
	if g, w := fileSHA256(t, agent), fileSHA256(t, binPath); g != w {
		t.Errorf("SHA-256 of the pushed agent = %s, want the built binary's %s", g, w)
	}

	if got := prop(t, dst, "mounted"); got != "no" {
		t.Errorf("mounted of %s = %q, want no (an unprivileged Linux target receives with -u)", dst, got)
	}
	requireSameGUID(t, src, dst, "s1")
	requireNewestSyncGUID(t, src, dst)
}

// logModes logs ls -ld, and getfacl when available, of the agent dir and
// its parents up to the home directory, to explain a wrong mode.
func logModes(t *testing.T, agent string) {
	t.Helper()
	dir := filepath.Dir(agent)                            // <home>/.cache/syngoid/<dir>
	home := filepath.Dir(filepath.Dir(filepath.Dir(dir))) // <home>
	paths := []string{home, filepath.Join(home, ".cache"), filepath.Join(home, ".cache", "syngoid"), dir}
	out, err := exec.Command("ls", append([]string{"-ld"}, paths...)...).CombinedOutput()
	t.Logf("ls -ld (%v):\n%s", err, out)
	if _, err := exec.LookPath("getfacl"); err == nil {
		out, err := exec.Command("getfacl", append([]string{"-p"}, paths...)...).CombinedOutput()
		t.Logf("getfacl (%v):\n%s", err, out)
	}
}

func TestITRemoteSource(t *testing.T) {
	beginRemote(t)
	host := hostname(t)
	src, dst := srcPool+"/a", dstPool+"/a"
	createSource(t, src, 4<<20)
	zfs(t, "snapshot", src+"@s1")
	args := []string{remoteLoc(itUser, src), dst}

	grantFromRemotePreflight(t, args...)
	syngoidOK(t, args...)
	sync1 := short(oneSyncSnap(t, src, "", host))
	equal(t, "target snapshots after the initial sync", snaps(t, dst), full(dst, "s1", sync1))

	writeRandomFile(t, filepath.Join(mountpoint(t, src), "two"), 4<<20)
	zfs(t, "snapshot", src+"@s2")
	syngoidOK(t, args...)
	sync2 := short(oneSyncSnap(t, src, "", host))
	if sync2 == sync1 {
		t.Fatalf("second run reused sync snapshot %s", sync1)
	}
	// The incremental run based on the previous sync snapshot, then pruned
	// it on both sides.
	equal(t, "source snapshots", snaps(t, src), full(src, "s1", "s2", sync2))
	equal(t, "target snapshots", snaps(t, dst), full(dst, "s1", "s2", sync2))
	requireSameGUID(t, src, dst, sync2)
	for _, f := range []string{"data", "two"} {
		if g, w := fileSHA256(t, filepath.Join(mountpoint(t, dst), f)), fileSHA256(t, filepath.Join(mountpoint(t, src), f)); g != w {
			t.Fatalf("%s differs on the target", f)
		}
	}
}

func TestITRemoteToRemote(t *testing.T) {
	beginRemote(t)
	host := hostname(t)
	src, parent := srcPool+"/a", dstPool+"/rr"
	dst := parent + "/a"
	createSource(t, src, 4<<20)
	zfs(t, "snapshot", src+"@s1")
	zfs(t, "create", parent)
	args := []string{remoteLoc(itUser, src), remoteLoc(itUser, dst)}

	// One failed run reports the missing grants of both sides.
	grantFromRemotePreflight(t, args...)
	syngoidOK(t, args...)
	sync1 := short(oneSyncSnap(t, src, "", host))
	equal(t, "target snapshots after the initial sync", snaps(t, dst), full(dst, "s1", sync1))

	zfs(t, "snapshot", src+"@s2")
	syngoidOK(t, args...)
	sync2 := short(oneSyncSnap(t, src, "", host))
	equal(t, "source snapshots", snaps(t, src), full(src, "s1", "s2", sync2))
	equal(t, "target snapshots", snaps(t, dst), full(dst, "s1", "s2", sync2))
	requireSameGUID(t, src, dst, sync2)
}

func TestITRemoteSudo(t *testing.T) {
	beginRemote(t)
	src, dst := srcPool+"/a", dstPool+"/a"
	createSource(t, src, 4<<20)
	zfs(t, "snapshot", src+"@s1")
	target := remoteLoc(itUser, dst)

	// No grants anywhere: the delegation preflight refuses without --target-sudo.
	// (A failed ssh would exit 2 as well, hence the message.)
	if r := syngoid(t, src, target); r.code != 2 || !strings.Contains(r.stderr, "missing ZFS permissions") {
		t.Fatalf("syngoid without grants or --target-sudo: exit %d, want 2 with missing ZFS permissions", r.code)
	}
	// With it the agent runs `sudo -n zfs`, allowed by the sudoers rule.
	syngoidOK(t, "--target-sudo", src, target)
	requireSameGUID(t, src, dst, "s1")
	requireNewestSyncGUID(t, src, dst)
}

func TestITRemoteRepushOnCorruption(t *testing.T) {
	beginRemote(t)
	src, parent := srcPool+"/a", dstPool+"/rp"
	dst := parent + "/a"
	createSource(t, src, 4<<20)
	zfs(t, "snapshot", src+"@s1")
	zfs(t, "create", parent)
	args := []string{src, remoteLoc(itUser, dst)}

	grantFromRemotePreflight(t, args...)
	syngoidOK(t, args...)
	agent := agentPath(t, itUser)
	want := fileSHA256(t, binPath)
	if got := fileSHA256(t, agent); got != want {
		t.Fatalf("SHA-256 of the pushed agent = %s, want %s", got, want)
	}

	// A cut-short binary is still executable, so only the failed handshake
	// can tell: the next run must push again.
	if err := os.Truncate(agent, 1024); err != nil {
		t.Fatal(err)
	}
	zfs(t, "snapshot", src+"@s2")
	r := syngoidOK(t, args...)
	if got := fileSHA256(t, agent); got != want {
		t.Errorf("SHA-256 of the agent after the run = %s, want the restored %s", got, want)
	}
	if !strings.Contains(r.stdout, "pushing syngoid agent") {
		t.Error("the run did not log pushing the agent")
	}
	requireSameGUID(t, src, dst, "s2")
}

func TestITRemoteKillInitiatorThenResume(t *testing.T) {
	beginRemote(t)
	src, parent := srcPool+"/a", dstPool+"/rk"
	dst := parent + "/a"
	createSource(t, src, 300<<20)
	zfs(t, "create", parent)
	args := []string{"--compress=none", src, remoteLoc(itUser, dst)}
	grantFromRemotePreflight(t, args...)

	interruptRemote(t, syscall.SIGKILL, receiveInto(dst), args...)

	// The agent notices the lost connection and ends its receive, which
	// leaves a resume token.
	if !waitNoProcs(receiveInto(dst), 15*time.Second) {
		t.Fatalf("a zfs receive into %s survived the initiator for 15 s: %v", dst, findProcs(receiveInto(dst)))
	}
	requireResumeToken(t, dst)

	syngoidOK(t, args...)
	if tok := resumeToken(t, dst); tok != "-" {
		t.Fatalf("receive_resume_token of %s = %q after the rerun, want -", dst, tok)
	}
	requireNewestSyncGUID(t, src, dst)
	if g, w := fileSHA256(t, filepath.Join(mountpoint(t, dst), "data")), fileSHA256(t, filepath.Join(mountpoint(t, src), "data")); g != w {
		t.Fatal("the resumed file differs from the source")
	}
}

func TestITRemoteCtrlC(t *testing.T) {
	beginRemote(t)
	src, parent := srcPool+"/a", dstPool+"/rc"
	dst := parent + "/a"
	createSource(t, src, 300<<20)
	zfs(t, "create", parent)
	args := []string{"--compress=none", src, remoteLoc(itUser, dst)}
	grantFromRemotePreflight(t, args...)

	r := interruptRemote(t, syscall.SIGINT, receiveInto(dst), args...)
	if r.code != 2 {
		t.Errorf("syngoid interrupted by SIGINT: exit %d, want 2", r.code)
	}
	if !strings.Contains(r.stdout, "Summary:") {
		t.Error("the interrupted run printed no Summary:")
	}
	if !waitNoProcs(receiveInto(dst), 15*time.Second) {
		t.Fatalf("a zfs receive into %s remains 15 s after the interrupt: %v", dst, findProcs(receiveInto(dst)))
	}
}

func TestITRemoteDatasetWithSpace(t *testing.T) {
	beginRemote(t)
	src, parent := srcPool+"/my data", dstPool+"/sp"
	dst := parent + "/my data"
	createSource(t, src, 1<<20)
	zfs(t, "snapshot", src+"@s1")
	zfs(t, "create", parent)
	args := []string{src, remoteLoc(itUser, dst)}

	grantFromRemotePreflight(t, args...)
	syngoidOK(t, args...)
	requireSameGUID(t, src, dst, "s1")
	requireNewestSyncGUID(t, src, dst)
}

func TestITRemoteCodecs(t *testing.T) {
	beginRemote(t)
	src, parent := srcPool+"/a", dstPool+"/rcodec"
	createSource(t, src, 4<<20)
	zfs(t, "snapshot", src+"@s1")
	zfs(t, "create", parent)
	// The grant lands on parent and covers every target below it.
	grantFromRemotePreflight(t, src, remoteLoc(itUser, parent+"/grants"))

	for _, codec := range []string{"zstd-fast", "zstd", "lz4", "gzip", "none"} {
		t.Run(codec, func(t *testing.T) {
			dst := parent + "/" + codec
			syngoidOK(t, "--compress="+codec, src, remoteLoc(itUser, dst))
			requireSameGUID(t, src, dst, "s1")
			requireNewestSyncGUID(t, src, dst)
			if g, w := fileSHA256(t, filepath.Join(mountpoint(t, dst), "data")), fileSHA256(t, filepath.Join(mountpoint(t, src), "data")); g != w {
				t.Fatal("the data differs on the target")
			}
		})
	}
}

func TestITRemoteDebugHostPrefix(t *testing.T) {
	beginRemote(t)
	src, parent := srcPool+"/a", dstPool+"/rdbg"
	dst := parent + "/a"
	createSource(t, src, 1<<20)
	zfs(t, "snapshot", src+"@s1")
	zfs(t, "create", parent)
	args := []string{"--debug", src, remoteLoc(itUser, dst)}

	grantFromRemotePreflight(t, args...)
	r := syngoidOK(t, args...)
	if want := "DEBUG: [" + itUser + "@" + remoteHost + "] zfs receive"; !strings.Contains(r.stdout, want) {
		t.Fatalf("--debug output lacks %q", want)
	}
}
