//go:build integration

package integration

// Interoperability with Perl syncoid 2.2.0 (spec §8.1, §12.4). Both tools run
// as root against the same local pools; each test hands a replication from one
// to the other and checks that sync snapshot names, holds, bookmarks and
// pruning stay compatible. scripts/ci/setup-zfs.sh installs syncoid; the
// tests skip when it is absent.

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/piotaixr/syngoid/internal/naming"
)

// syncoidPath is where scripts/ci/setup-zfs.sh installs syncoid 2.2.0.
const syncoidPath = "/usr/local/bin/syncoid"

// stampLayout is the timestamp layout of sync snapshot names.
const stampLayout = "2006-01-02:15:04:05"

// beginCompat is begin for tests that also need syncoid.
func beginCompat(t *testing.T) {
	t.Helper()
	if enabled {
		if _, err := os.Stat(syncoidPath); err != nil {
			t.Skipf("syncoid 2.2.0 is not installed at %s (run scripts/ci/setup-zfs.sh): %v", syncoidPath, err)
		}
	}
	begin(t)
}

// syncoidCmd prepares a syncoid command, first waiting for a later wall
// clock second than the previous run ended in, so its sync snapshot name
// differs from the previous run's. --compress=none keeps the default lzo
// (which needs lzop) out of the picture.
func syncoidCmd(args ...string) *exec.Cmd {
	waitNextSecond()
	cmd := exec.Command(syncoidPath, append([]string{"--compress=none"}, args...)...)
	cmd.Dir = "/"
	// syncoid finds zfs through PATH, which may lack the sbin directories.
	cmd.Env = append(os.Environ(), "LC_ALL=C",
		"PATH="+os.Getenv("PATH")+":/sbin:/usr/sbin:/usr/local/sbin")
	return cmd
}

// syncoidOK runs syncoid as root and fails the test unless it exits 0.
func syncoidOK(t *testing.T, args ...string) result {
	t.Helper()
	cmd := syncoidCmd(args...)
	r, err := runCmd(cmd)
	lastEnd = time.Now()
	if err != nil {
		t.Fatalf("running syncoid: %v", err)
	}
	logTool(t, "", "syncoid", cmd.Args[1:], r)
	if r.code != 0 {
		t.Fatalf("syncoid %s: exit %d, want 0", strings.Join(args, " "), r.code)
	}
	return r
}

// requireIncremental fails the test when tool's output says it sent the
// oldest snapshot in full to a new target instead of an increment.
func requireIncremental(t *testing.T, tool string, r result) {
	t.Helper()
	if !strings.Contains(r.stdout, "Sending incremental") {
		t.Fatalf("%s did not announce an incremental send", tool)
	}
	if strings.Contains(r.stdout, "Sending oldest full snapshot") {
		t.Fatalf("%s resent the oldest snapshot in full", tool)
	}
}

// requireSameDataset fails the test when ds was recreated since createtxg
// was read from it (a full resend would destroy and receive it anew).
func requireSameDataset(t *testing.T, ds, createtxg string) {
	t.Helper()
	if got := prop(t, ds, "createtxg"); got != createtxg {
		t.Fatalf("createtxg of %s = %s, want %s: the target was recreated", ds, got, createtxg)
	}
}

// requireGUIDsEqual fails the test unless snap exists on both datasets with
// the same guid.
func requireGUIDsEqual(t *testing.T, src, dst, snap string) {
	t.Helper()
	if g, w := prop(t, dst+"@"+snap, "guid"), prop(t, src+"@"+snap, "guid"); g != w {
		t.Fatalf("guid of %s@%s = %s, want %s", dst, snap, g, w)
	}
}

// requireHoldTag fails the test unless the only hold on each snapshot is tag.
func requireHoldTag(t *testing.T, tag string, snaps ...string) {
	t.Helper()
	for _, s := range snaps {
		equal(t, "holds of "+s, holdTags(t, s), []string{tag})
	}
}

// TestCompatSyncoidThenSyngoid takes over a replication started by syncoid.
func TestCompatSyncoidThenSyngoid(t *testing.T) {
	beginCompat(t)
	host := hostname(t)
	src, dst := srcPool+"/c1", dstPool+"/c1"
	zfs(t, "create", src)
	writeRandomFile(t, filepath.Join(mountpoint(t, src), "one"), 1<<20)
	zfs(t, "snapshot", src+"@a1")

	syncoidOK(t, src, dst)
	old := short(oneSyncSnap(t, src, "", host))
	equal(t, "target snapshots after syncoid", snaps(t, dst), full(dst, "a1", old))
	txg := prop(t, dst, "createtxg")

	zfs(t, "snapshot", src+"@a2")
	r := syngoidOK(t, src, dst)
	requireIncremental(t, "syngoid", r)
	requireSameDataset(t, dst, txg)

	// syncoid's sync snapshot is pruned on both sides, syngoid's replaces it.
	newest := short(oneSyncSnap(t, src, "", host))
	if newest == old {
		t.Fatalf("syngoid reused syncoid's sync snapshot %s", old)
	}
	equal(t, "source snapshots", snaps(t, src), full(src, "a1", "a2", newest))
	equal(t, "target snapshots", snaps(t, dst), full(dst, "a1", "a2", newest))
	requireGUIDsEqual(t, src, dst, newest)
}

// TestCompatSyngoidThenSyncoid hands a replication started by syngoid to
// syncoid.
func TestCompatSyngoidThenSyncoid(t *testing.T) {
	beginCompat(t)
	host := hostname(t)
	src, dst := srcPool+"/c2", dstPool+"/c2"
	zfs(t, "create", src)
	writeRandomFile(t, filepath.Join(mountpoint(t, src), "one"), 1<<20)
	zfs(t, "snapshot", src+"@a1")

	syngoidOK(t, src, dst)
	old := short(oneSyncSnap(t, src, "", host))
	equal(t, "target snapshots after syngoid", snaps(t, dst), full(dst, "a1", old))
	txg := prop(t, dst, "createtxg")

	zfs(t, "snapshot", src+"@a2")
	r := syncoidOK(t, src, dst)
	requireIncremental(t, "syncoid", r)
	requireSameDataset(t, dst, txg)

	newest := short(oneSyncSnap(t, src, "", host))
	if newest == old {
		t.Fatalf("syncoid reused syngoid's sync snapshot %s", old)
	}
	equal(t, "source snapshots", snaps(t, src), full(src, "a1", "a2", newest))
	equal(t, "target snapshots", snaps(t, dst), full(dst, "a1", "a2", newest))
	requireGUIDsEqual(t, src, dst, newest)
}

// TestCompatHolds checks that both tools place and release the same hold
// tag, in both directions: a snapshot held by one tool is released by the
// other, which is proven by the pruning that follows (zfs refuses to destroy
// a held snapshot).
func TestCompatHolds(t *testing.T) {
	beginCompat(t)
	host := hostname(t)
	tag := naming.HoldTag("", host)
	src, dst := srcPool+"/c3", dstPool+"/c3"
	zfs(t, "create", src)
	zfs(t, "snapshot", src+"@a1")

	syncoidOK(t, "--use-hold", src, dst)
	first := short(oneSyncSnap(t, src, "", host))
	requireHoldTag(t, tag, src+"@"+first, dst+"@"+first)

	syngoidOK(t, "--use-hold", src, dst)
	second := short(oneSyncSnap(t, src, "", host))
	requireHoldTag(t, tag, src+"@"+second, dst+"@"+second)
	for _, s := range []string{src + "@" + first, dst + "@" + first} {
		if exists(t, s) {
			t.Fatalf("%s still exists: syngoid did not release syncoid's hold before pruning", s)
		}
	}

	syncoidOK(t, "--use-hold", src, dst)
	third := short(oneSyncSnap(t, src, "", host))
	requireHoldTag(t, tag, src+"@"+third, dst+"@"+third)
	for _, s := range []string{src + "@" + second, dst + "@" + second} {
		if exists(t, s) {
			t.Fatalf("%s still exists: syncoid did not release syngoid's hold before pruning", s)
		}
	}
}

// TestCompatBookmarks replicates through a bookmark created by syncoid.
func TestCompatBookmarks(t *testing.T) {
	beginCompat(t)
	src, dst := srcPool+"/c4", dstPool+"/c4"
	zfs(t, "create", src)
	zfs(t, "snapshot", src+"@s1")

	syncoidOK(t, "--no-sync-snap", "--create-bookmark", src, dst)
	equal(t, "source bookmarks", bookmarksR(t, src), []string{src + "#s1"})
	equal(t, "target snapshots", snaps(t, dst), full(dst, "s1"))
	txg := prop(t, dst, "createtxg")

	// With the shared snapshot gone, only the bookmark links source and
	// target.
	zfs(t, "destroy", src+"@s1")
	zfs(t, "snapshot", src+"@s2")
	r := syngoidOK(t, "--no-sync-snap", src, dst)
	requireIncremental(t, "syngoid", r)
	requireSameDataset(t, dst, txg)
	equal(t, "target snapshots", snaps(t, dst), full(dst, "s1", "s2"))
	requireGUIDsEqual(t, src, dst, "s2")
	equal(t, "source bookmarks", bookmarksR(t, src), []string{src + "#s1"})
}

// TestCompatIdentifiersIsolated checks that a syncoid identifier and a
// syngoid identifier on the same source dataset never prune each other's sync
// snapshots.
//
// The identifiers "x" and "y" are deliberately not prefixes of one another:
// syncoid prunes by prefix match (syncoid_x_ also matches syncoid_x_foo_...),
// while syngoid's pattern is strict (spec §8.9 item 1). Identifiers like "a"
// and "ab" would let syncoid's looser matching cross over and fail the test
// for a reason that is not syngoid's.
func TestCompatIdentifiersIsolated(t *testing.T) {
	beginCompat(t)
	host := hostname(t)
	src, dstX, dstY := srcPool+"/c5", dstPool+"/c5x", dstPool+"/c5y"
	zfs(t, "create", src)
	zfs(t, "snapshot", src+"@base")

	// Each run also carries the other identifier's sync snapshots to its
	// target, as -I sends every intermediate snapshot.
	syncoidOK(t, "--identifier=x", src, dstX)
	x1 := short(oneSyncSnap(t, src, "x", host))
	syngoidOK(t, "--identifier=y", src, dstY)
	y1 := short(oneSyncSnap(t, src, "y", host))
	equal(t, "source snapshots", snaps(t, src), full(src, "base", x1, y1))

	// syncoid's x run prunes x1 but not syngoid's y1.
	syncoidOK(t, "--identifier=x", src, dstX)
	x2 := short(oneSyncSnap(t, src, "x", host))
	equal(t, "source snapshots after syncoid", snaps(t, src), full(src, "base", y1, x2))
	equal(t, "x target snapshots after syncoid", snaps(t, dstX), full(dstX, "base", y1, x2))

	// syngoid's y run prunes y1 but neither syncoid's x2 nor the x1 left on
	// its own target.
	syngoidOK(t, "--identifier=y", src, dstY)
	y2 := short(oneSyncSnap(t, src, "y", host))
	equal(t, "source snapshots after syngoid", snaps(t, src), full(src, "base", x2, y2))
	equal(t, "y target snapshots after syngoid", snaps(t, dstY), full(dstY, "base", x1, x2, y2))
}

// TestCompatSnapshotNameFormat runs both tools at the same time, so their
// sync snapshots are created in (about) the same second, and checks that the
// names differ only by identifier and match naming.SyncSnapPattern.
func TestCompatSnapshotNameFormat(t *testing.T) {
	beginCompat(t)
	host := hostname(t)
	src := srcPool + "/c6"
	zfs(t, "create", src)
	zfs(t, "snapshot", src+"@base")

	// Build both commands (each waits for a fresh second), then start them
	// together at the next second boundary. The stamps may still straddle a
	// boundary, so they are compared with a small tolerance below.
	scCmd := syncoidCmd("--identifier=sc", src, dstPool+"/c6sc")
	sgCmd := syngoidCmd("", "--identifier=sg", src, dstPool+"/c6sg")
	time.Sleep(time.Until(time.Now().Truncate(time.Second).Add(time.Second)))
	type outcome struct {
		r   result
		err error
	}
	var wg sync.WaitGroup
	var scOut, sgOut outcome
	for cmd, out := range map[*exec.Cmd]*outcome{scCmd: &scOut, sgCmd: &sgOut} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out.r, out.err = runCmd(cmd)
		}()
	}
	wg.Wait()
	lastEnd = time.Now()
	for _, c := range []struct {
		tool string
		cmd  *exec.Cmd
		out  outcome
	}{{"syncoid", scCmd, scOut}, {"syngoid", sgCmd, sgOut}} {
		if c.out.err != nil {
			t.Fatalf("running %s: %v", c.tool, c.out.err)
		}
		logTool(t, "", c.tool, c.cmd.Args[1:], c.out.r)
		if c.out.r.code != 0 {
			t.Fatalf("%s exit %d, want 0", c.tool, c.out.r.code)
		}
	}

	// oneSyncSnap checks each name against naming.SyncSnapPattern.
	scSnap := short(oneSyncSnap(t, src, "sc", host))
	sgSnap := short(oneSyncSnap(t, src, "sg", host))
	scStamp := strings.TrimPrefix(scSnap, "syncoid_sc_"+host+"_")
	sgStamp := strings.TrimPrefix(sgSnap, "syncoid_sg_"+host+"_")
	// "2006-01-02:15:04:05" followed by "-GMT" and the offset.
	if len(scStamp) <= len(stampLayout) || len(sgStamp) <= len(stampLayout) {
		t.Fatalf("unexpected sync snapshot names %q and %q", scSnap, sgSnap)
	}
	if scZone, sgZone := scStamp[len(stampLayout):], sgStamp[len(stampLayout):]; scZone != sgZone || !strings.HasPrefix(scZone, "-GMT") {
		t.Fatalf("GMT offsets differ: syncoid %q, syngoid %q", scZone, sgZone)
	}
	scTime, err := time.Parse(stampLayout, scStamp[:len(stampLayout)])
	if err != nil {
		t.Fatalf("syncoid stamp in %q: %v", scSnap, err)
	}
	sgTime, err := time.Parse(stampLayout, sgStamp[:len(stampLayout)])
	if err != nil {
		t.Fatalf("syngoid stamp in %q: %v", sgSnap, err)
	}
	if d := scTime.Sub(sgTime).Abs(); d > 2*time.Second {
		t.Fatalf("stamps differ by %s: %s vs %s", d, scSnap, sgSnap)
	}
}
