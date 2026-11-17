//go:build integration

package integration

import (
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// noHolds fails the test if any snapshot of ds or its descendants has a hold:
// a restore never holds or releases anything.
func noHolds(t *testing.T, ds string) {
	t.Helper()
	for _, s := range snapsR(t, ds) {
		if tags := holdTags(t, s); len(tags) != 0 {
			t.Fatalf("%s has holds %q: a restore must not hold anything", s, tags)
		}
	}
}

// sameGUID fails the test unless snapshot name of the target dataset has the
// guid of the same snapshot of the source dataset.
func sameGUID(t *testing.T, src, dst, name string) {
	t.Helper()
	if g, w := prop(t, dst+"@"+name, "guid"), prop(t, src+"@"+name, "guid"); g != w {
		t.Fatalf("guid of %s@%s = %s, want %s", dst, name, g, w)
	}
}

// sameFile fails the test unless file rel has the same content under both
// datasets' mountpoints.
func sameFile(t *testing.T, src, dst, rel string) {
	t.Helper()
	g := fileSHA256(t, filepath.Join(mountpoint(t, dst), rel))
	w := fileSHA256(t, filepath.Join(mountpoint(t, src), rel))
	if g != w {
		t.Fatalf("%s differs between %s and %s", rel, src, dst)
	}
}

// writeAndSnap writes a random file into ds, then takes snapshot name.
func writeAndSnap(t *testing.T, ds, file, name string, size int64) {
	t.Helper()
	writeRandomFile(t, filepath.Join(mountpoint(t, ds), file), size)
	zfs(t, "snapshot", ds+"@"+name)
}

func TestITRestoreNewest(t *testing.T) {
	begin(t)
	b, c := srcPool+"/b", srcPool+"/b/c"
	dst, dstC := dstPool+"/rn", dstPool+"/rn/c"
	zfs(t, "create", b)
	zfs(t, "create", c)
	writeAndSnap(t, b, "one", "s1", 4<<20)
	writeAndSnap(t, c, "cone", "s1", 4<<20)
	writeAndSnap(t, b, "two", "s2", 4<<20)
	srcBefore := snapsR(t, b)
	equal(t, "setup source snapshots", srcBefore, []string{b + "@s1", b + "@s2", c + "@s1"})

	syngoidOK(t, "restore", "-r", b, dst)

	equal(t, "restored snapshots", snapsR(t, dst), []string{dst + "@s2", dstC + "@s1"})
	sameGUID(t, b, dst, "s2")
	sameGUID(t, c, dstC, "s1")
	for _, f := range []string{"one", "two"} {
		sameFile(t, b, dst, f)
	}
	sameFile(t, c, dstC, "cone")

	// Anti-goals: nothing was created, held or destroyed on the source.
	equal(t, "source snapshots after the restore", snapsR(t, b), srcBefore)
	equal(t, "source bookmarks after the restore", bookmarksR(t, b), nil)
	noHolds(t, b)
	noHolds(t, dst)
}

func TestITRestoreSnapshotAndAt(t *testing.T) {
	begin(t)
	src := srcPool + "/pt"
	zfs(t, "create", src)
	writeAndSnap(t, src, "one", "s1", 1<<20)
	// Creation times have a resolution of one second.
	time.Sleep(1100 * time.Millisecond)
	writeAndSnap(t, src, "two", "s2", 1<<20)
	srcBefore := snapsR(t, src)

	bySnap := dstPool + "/by-snap"
	syngoidOK(t, "restore", "--snapshot", "s1", src, bySnap)
	equal(t, "snapshots restored with --snapshot", snaps(t, bySnap), full(bySnap, "s1"))
	sameGUID(t, src, bySnap, "s1")
	sameFile(t, src, bySnap, "one")

	// --at is inclusive: the creation second of s1 selects s1, not s2.
	out := zfs(t, "get", "-Hp", "-o", "value", "creation", src+"@s1")
	created, err := strconv.ParseInt(out, 10, 64)
	if err != nil {
		t.Fatalf("creation of %s@s1 = %q: %v", src, out, err)
	}
	at := time.Unix(created, 0).UTC().Format(time.RFC3339)
	byAt := dstPool + "/by-at"
	syngoidOK(t, "restore", "--at", at, src, byAt)
	equal(t, "snapshots restored with --at "+at, snaps(t, byAt), full(byAt, "s1"))
	sameGUID(t, src, byAt, "s1")

	equal(t, "source snapshots after the restores", snapsR(t, src), srcBefore)
	equal(t, "source bookmarks after the restores", bookmarksR(t, src), nil)
	noHolds(t, src)
}

func TestITRestoreRerunAndConflict(t *testing.T) {
	begin(t)
	b, c := srcPool+"/rr", srcPool+"/rr/c"
	dst := dstPool + "/rr"
	zfs(t, "create", b)
	zfs(t, "create", c)
	writeAndSnap(t, b, "one", "s1", 1<<20)
	writeAndSnap(t, c, "cone", "s1", 1<<20)

	syngoidOK(t, "restore", "-r", b, dst)
	restored := snapsR(t, dst)
	equal(t, "restored snapshots", restored, []string{dst + "@s1", dst + "/c@s1"})

	r := syngoidOK(t, "restore", "-r", b, dst)
	if !strings.Contains(r.stdout, "already restored at @") {
		t.Fatalf("a re-run of a finished restore did not report it:\nstdout: %s", r.stdout)
	}
	equal(t, "snapshots after the re-run", snapsR(t, dst), restored)

	// A snapshot of the new host's own makes the target no longer this
	// restore's work: a re-run must fail and touch nothing.
	zfs(t, "snapshot", dst+"@local")
	withLocal := snapsR(t, dst)
	r = syngoid(t, "restore", "-r", b, dst)
	if r.code != 2 {
		t.Fatalf("restore over a target with a local snapshot: exit %d, want 2", r.code)
	}
	if !strings.Contains(r.stderr, "CRITICAL ERROR: restore needs an empty target; these datasets already exist:") {
		t.Fatalf("conflict message missing from stderr:\n%s", r.stderr)
	}
	equal(t, "snapshots after the conflict", snapsR(t, dst), withLocal)
}

func TestITRestoreExistingTargetSendsNothing(t *testing.T) {
	begin(t)
	b, c, d := srcPool+"/ex", srcPool+"/ex/c", srcPool+"/ex/d"
	dst, dstC, dstD := dstPool+"/ex", dstPool+"/ex/c", dstPool+"/ex/d"
	for _, ds := range []string{b, c, d} {
		zfs(t, "create", ds)
		zfs(t, "snapshot", ds+"@s1")
	}
	srcBefore := snapsR(t, b)
	// An earlier, unrelated dataset at one target: the whole command fails
	// before anything is sent, including to the targets that do not exist.
	zfs(t, "create", dst)
	zfs(t, "create", dstC)

	r := syngoid(t, "restore", "-r", b, dst)
	if r.code != 2 {
		t.Fatalf("restore over existing target datasets: exit %d, want 2", r.code)
	}
	if !strings.Contains(r.stderr, "CRITICAL ERROR: restore needs an empty target; these datasets already exist:") {
		t.Fatalf("conflict message missing from stderr:\n%s", r.stderr)
	}
	if exists(t, dstD) {
		t.Fatalf("%s was created although the restore had a conflict", dstD)
	}
	for _, ds := range []string{dst, dstC} {
		equal(t, "snapshots of the existing "+ds, snaps(t, ds), nil)
	}
	equal(t, "source snapshots", snapsR(t, b), srcBefore)
	equal(t, "source bookmarks", bookmarksR(t, b), nil)
	noHolds(t, b)
}

func TestITRestoreInterruptThenResume(t *testing.T) {
	begin(t)
	src, dst := srcPool+"/ir", dstPool+"/ir"
	zfs(t, "create", src)
	writeAndSnap(t, src, "big", "s1", 300<<20)
	srcBefore := snapsR(t, src)

	interruptSync(t, "restore", src, dst)
	requireResumeToken(t, dst)

	r := syngoidOK(t, "restore", src, dst)
	if !strings.Contains(r.stdout, "Resuming interrupted zfs send/receive") {
		t.Fatalf("the re-run did not resume the interrupted transfer:\nstdout: %s", r.stdout)
	}
	if tok := resumeToken(t, dst); tok != "-" {
		t.Fatalf("receive_resume_token of %s = %q after the resumed restore, want -", dst, tok)
	}
	equal(t, "restored snapshots", snaps(t, dst), full(dst, "s1"))
	sameGUID(t, src, dst, "s1")
	sameFile(t, src, dst, "big")
	equal(t, "source snapshots", snapsR(t, src), srcBefore)
	noHolds(t, src)
}

func TestITRestoreClone(t *testing.T) {
	begin(t)
	b, o, cl := srcPool+"/cl", srcPool+"/cl/o", srcPool+"/cl/cl"
	dst, dstCl := dstPool+"/cl", dstPool+"/cl/cl"
	zfs(t, "create", b)
	zfs(t, "snapshot", b+"@s1")
	zfs(t, "create", o)
	writeAndSnap(t, o, "origin-data", "s1", 2<<20)
	zfs(t, "clone", o+"@s1", cl)
	writeAndSnap(t, cl, "clone-data", "s1", 2<<20)
	if got := prop(t, cl, "origin"); got != o+"@s1" {
		t.Fatalf("setup: origin of %s = %q", cl, got)
	}

	syngoidOK(t, "restore", "-r", b, dst)

	if !exists(t, dstCl) {
		t.Fatalf("%s was not restored", dstCl)
	}
	if got := prop(t, dstCl, "origin"); got != "-" {
		t.Fatalf("origin of %s = %q, want - (a restore sends clones in full, as independent datasets)", dstCl, got)
	}
	equal(t, "restored clone snapshots", snaps(t, dstCl), full(dstCl, "s1"))
	sameGUID(t, cl, dstCl, "s1")
	sameFile(t, cl, dstCl, "clone-data")
	sameFile(t, cl, dstCl, "origin-data")
}

func TestITRestoreUnprivileged(t *testing.T) {
	begin(t)
	b, c := srcPool+"/up", srcPool+"/up/c"
	parent := dstPool + "/up"
	dst, dstC := parent+"/t", parent+"/t/c"
	zfs(t, "create", b)
	zfs(t, "create", c)
	writeAndSnap(t, b, "one", "s1", 2<<20)
	writeAndSnap(t, c, "cone", "s1", 2<<20)
	zfs(t, "create", parent)
	// Design §7 item 2: only these delegations, no -F, no snapshot, no
	// destroy anywhere.
	zfs(t, "allow", "-u", itUser, "send", b)
	zfs(t, "allow", "-u", itUser, "receive,create,mount", parent)
	srcBefore := snapsR(t, b)

	requireOK(t, syngoidAs(t, itUser, "restore", "-r", b, dst))

	equal(t, "restored snapshots", snapsR(t, dst), []string{dst + "@s1", dstC + "@s1"})
	sameGUID(t, b, dst, "s1")
	sameGUID(t, c, dstC, "s1")
	sameFile(t, b, dst, "one")
	sameFile(t, c, dstC, "cone")
	equal(t, "source snapshots", snapsR(t, b), srcBefore)
	equal(t, "source bookmarks", bookmarksR(t, b), nil)
	noHolds(t, b)
}

// TestITRestoreAbortOwnPartial records what happens to a restore's own
// partial receive when its source snapshot is gone (design §7 items 1 and 3).
// It only fails on outcomes that break the anti-goals: anything on the target
// other than this restore's own dataset changed, or anything on the source
// changed by the restore.
func TestITRestoreAbortOwnPartial(t *testing.T) {
	begin(t)
	src, parent := srcPool+"/ab", dstPool+"/ab"
	dst := parent + "/t"
	other := dstPool + "/ab-other"
	zfs(t, "create", src)
	writeAndSnap(t, src, "big", "s1", 300<<20)
	zfs(t, "create", parent)
	// A canary on the target side that no restore may touch.
	zfs(t, "create", other)
	zfs(t, "snapshot", other+"@keep")
	otherBefore := snapsR(t, other)
	zfs(t, "allow", "-u", itUser, "send", src)
	zfs(t, "allow", "-u", itUser, "receive,create,mount", parent)

	// interruptSync runs syngoid as root; the partial receive state it
	// leaves is the same as an unprivileged restore's.
	interruptSync(t, "restore", src, dst)
	requireResumeToken(t, dst)
	t.Logf("after the interruption: %s has receive_resume_token set; snapshots %q", dst, snaps(t, dst))

	// Replace the snapshot the partial receive was for.
	zfs(t, "destroy", src+"@s1")
	zfs(t, "snapshot", src+"@s2")
	srcBefore := snapsR(t, src)
	equal(t, "setup source snapshots", srcBefore, []string{src + "@s2"})

	r := syngoidAs(t, itUser, "restore", src, dst)
	t.Logf("re-run after the source snapshot was replaced: exit %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
	if exists(t, dst) {
		t.Logf("after the re-run %s exists: snapshots %q, receive_resume_token %q", dst, snaps(t, dst), resumeToken(t, dst))
	} else {
		t.Logf("after the re-run %s does not exist", dst)
	}

	// Then the abort itself, as the unprivileged user with the restore's
	// delegations only, when a partial state is still there.
	if exists(t, dst) && resumeToken(t, dst) != "-" {
		cmd := zfsCommand(itUser, "receive", "-A", dst)
		ar, err := runCmd(cmd)
		if err != nil {
			t.Fatalf("running zfs receive -A: %v", err)
		}
		t.Logf("`zfs receive -A %s` as %s with receive,create,mount: exit %d, stderr %q", dst, itUser, ar.code, strings.TrimSpace(ar.stderr))
		if exists(t, dst) {
			t.Logf("after receive -A %s still exists: snapshots %q, receive_resume_token %q", dst, snaps(t, dst), resumeToken(t, dst))
		} else {
			t.Logf("after receive -A %s is gone", dst)
		}
	} else {
		t.Logf("no partial receive state left to abort with receive -A")
	}

	// Anti-goals.
	equal(t, "source snapshots", snapsR(t, src), srcBefore)
	equal(t, "source bookmarks", bookmarksR(t, src), nil)
	noHolds(t, src)
	equal(t, "snapshots of an unrelated target dataset", snapsR(t, other), otherBefore)
	if !exists(t, parent) {
		t.Fatalf("%s was destroyed: a restore may only abort its own partial dataset", parent)
	}
	var children []string
	for _, l := range lines(zfs(t, "list", "-H", "-o", "name", "-r", parent)) {
		if l != parent && l != dst {
			children = append(children, l)
		}
	}
	equal(t, "datasets under "+parent+" other than the restore's own", children, nil)
}

func TestITRestoreThenReplicateBack(t *testing.T) {
	begin(t)
	backup := srcPool + "/tb"
	restored := dstPool + "/tb"
	zfs(t, "create", backup)
	writeAndSnap(t, backup, "one", "s1", 2<<20)

	syngoidOK(t, "restore", backup, restored)
	equal(t, "restored snapshots", snaps(t, restored), full(restored, "s1"))

	// The new host goes back to work, then replicates to the backup.
	writeAndSnap(t, restored, "new-host", "s2", 2<<20)
	r := syngoidOK(t, restored, backup)
	if !strings.Contains(r.stdout, "Sending incremental") {
		t.Fatalf("replicating back did not send incrementally:\nstdout: %s", r.stdout)
	}
	if strings.Contains(r.stdout, "full snapshot") {
		t.Fatalf("replicating back sent a full stream:\nstdout: %s", r.stdout)
	}
	if !exists(t, backup+"@s2") {
		t.Fatalf("%s@s2 did not reach the backup", backup)
	}
	sameGUID(t, restored, backup, "s2")
	sameFile(t, restored, backup, "new-host")
}
