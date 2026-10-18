//go:build integration

package integration

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/piotaixr/syngoid/internal/perms"
)

// grantLine matches the ready-to-paste commands of a preflight failure,
// e.g. "  [target]  zfs allow -u syngoid-it create,mount syngoid-dst/x".
var grantLine = regexp.MustCompile(`(?m)^\s+\[[^\]]*\]\s+(zfs allow .*\S)\s*$`)

// grantFromPreflight runs syngoid as itUser with args, which must fail the
// preflight (exit 2), then runs the printed `zfs allow` commands as root.
// Commands for anything but a child of the test pools fail the test.
func grantFromPreflight(t *testing.T, args ...string) {
	t.Helper()
	r := syngoidAs(t, itUser, args...)
	if r.code != 2 || !strings.Contains(r.stderr, "missing ZFS permissions") {
		t.Fatalf("syngoid as %s without grants: exit %d, want 2 with missing ZFS permissions", itUser, r.code)
	}
	ms := grantLine.FindAllStringSubmatch(r.stderr, -1)
	if len(ms) == 0 {
		t.Fatal("the preflight failure printed no zfs allow command")
	}
	for _, m := range ms {
		f := strings.Fields(m[1])
		inPools := len(f) == 6 && (strings.HasPrefix(f[5], srcPool+"/") || strings.HasPrefix(f[5], dstPool+"/"))
		if !inPools || f[2] != "-u" || f[3] != itUser {
			t.Fatalf("unexpected grant command %q", m[1])
		}
		t.Logf("running printed grant: %s", m[1])
		zfs(t, f[1:]...)
	}
}

// requireOK fails the test unless r exited 0.
func requireOK(t *testing.T, r result) {
	t.Helper()
	if r.code != 0 {
		t.Fatalf("syngoid as %s: exit %d, want 0", itUser, r.code)
	}
}

func TestITPreflightPrintsWorkingGrant(t *testing.T) {
	begin(t)
	host := hostname(t)
	src, parent := srcPool+"/pf", dstPool+"/pf"
	dst := parent + "/dst"
	zfs(t, "create", src)
	zfs(t, "create", parent)

	grantFromPreflight(t, src, dst)
	requireOK(t, syngoidAs(t, itUser, src, dst))
	oneSyncSnap(t, dst, "", host)

	// The incremental run also prunes the previous sync snapshot on both
	// sides, so the grants must cover the steady state too.
	requireOK(t, syngoidAs(t, itUser, src, dst))
	oneSyncSnap(t, src, "", host)
	oneSyncSnap(t, dst, "", host)
}

func TestITUnprivLinuxReceiveUnmounted(t *testing.T) {
	begin(t)
	src, parent := srcPool+"/um", dstPool+"/um"
	dst := parent + "/dst"
	// Not t.TempDir: its cleanup (RemoveAll) would run while src is still
	// mounted there. Destroy src first, then remove the then-empty dir.
	custom, err := os.MkdirTemp("", "syngoid-it-um-")
	if err != nil {
		t.Fatal(err)
	}
	zfs(t, "create", "-o", "mountpoint="+custom, src)
	t.Cleanup(func() {
		destroyWithRetry(t, src)
		if err := os.Remove(custom); err != nil {
			t.Error(err)
		}
	})
	zfs(t, "create", parent)

	// --preserve-properties would pass the locally set mountpoint; an
	// unprivileged Linux target cannot mount, so syngoid receives with -u
	// and leaves mountpoint and canmount out (spec §6.1, §8.6).
	grantFromPreflight(t, "--preserve-properties", src, dst)
	requireOK(t, syngoidAs(t, itUser, "--preserve-properties", src, dst))

	if got := prop(t, dst, "mounted"); got != "no" {
		t.Errorf("mounted of %s = %q, want no", dst, got)
	}
	if got := prop(t, dst, "mountpoint"); got == custom {
		t.Errorf("mountpoint of %s = %q: the source mountpoint was received", dst, got)
	}
	if got := propSource(t, dst, "mountpoint"); got == "local" || strings.HasPrefix(got, "received") {
		t.Errorf("mountpoint source of %s = %q, want inherited or default", dst, got)
	}
}

// TestITReceiveForcePermissions pins spec §14 item 1: an unprivileged
// `zfs receive -F` that must roll the target back needs no `rollback`
// delegation on top of create,mount,receive,destroy (first observed in CI on
// OpenZFS), so perms.Required(Target) must not demand it. The test drives zfs
// directly to isolate the receive.
func TestITReceiveForcePermissions(t *testing.T) {
	begin(t)
	src, parent := srcPool+"/rf", dstPool+"/rf"
	dst := parent + "/dst"
	zfs(t, "create", src)
	zfs(t, "snapshot", src+"@s1")
	zfs(t, "create", parent)
	syngoidOK(t, "--no-sync-snap", src, dst)
	writeRandomFile(t, filepath.Join(mountpoint(t, src), "data"), 1<<20)
	zfs(t, "snapshot", src+"@s2")

	// modifyTarget changes the target after its newest snapshot, so only a
	// receive with -F (rolling the target back) can apply the increment.
	// Then it unmounts it: the unprivileged receive must not need to mount
	// anything.
	modifyTarget := func() {
		writeRandomFile(t, filepath.Join(mountpoint(t, dst), "local-change"), 1<<20)
		zfs(t, "unmount", dst)
	}
	modifyTarget()
	zfs(t, "allow", "-u", itUser, "create,mount,receive,destroy", parent)

	send := []string{"send", "-i", src + "@s1", src + "@s2"}
	stderr, err := pipeZFS(t, send, itUser, []string{"receive", "-s", "-u", dst}, 0)
	if err == nil || !strings.Contains(stderr, "modified") {
		t.Fatalf("setup: the receive without -F should be refused because the target was modified; got %v: %s", err, stderr)
	}

	// The receive syngoid runs on an unprivileged Linux target (spec §8.6).
	recvF := []string{"receive", "-s", "-F", "-u", dst}
	stderr, err = pipeZFS(t, send, itUser, recvF, 0)
	if err != nil {
		t.Fatalf("`zfs %s` failed for %s with create,mount,receive,destroy delegated (%v): %s\n"+
			"Another permission is needed: update perms.Required and spec §6.2 / §14 item 1.",
			strings.Join(recvF, " "), itUser, err, stderr)
	}
	if g, w := prop(t, dst+"@s2", "guid"), prop(t, src+"@s2", "guid"); g != w {
		t.Fatalf("guid of %s@s2 = %s, want %s", dst, g, w)
	}

	// The preflight must not demand more than zfs requires.
	var names []string
	for _, r := range perms.Required(perms.Target, perms.Needs{}) {
		names = append(names, r.AnyOf...)
	}
	if slices.Contains(names, "rollback") {
		t.Fatalf("perms.Required(Target) = %q demands rollback, which zfs receive -F does not need", names)
	}
}
