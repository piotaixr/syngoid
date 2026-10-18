//go:build integration

package integration

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/piotaixr/syngoid/internal/naming"
)

// short returns the part of a snapshot name after the '@'.
func short(snap string) string {
	_, s, _ := strings.Cut(snap, "@")
	return s
}

// holdTags returns the hold tags placed on snap.
func holdTags(t *testing.T, snap string) []string {
	t.Helper()
	var tags []string
	for _, l := range lines(zfs(t, "holds", "-H", snap)) {
		if f := strings.Split(l, "\t"); len(f) >= 2 {
			tags = append(tags, f[1])
		}
	}
	return tags
}

// oneSyncSnap returns the only sync snapshot of ds for identifier and host.
func oneSyncSnap(t *testing.T, ds, identifier, host string) string {
	t.Helper()
	got := syncSnaps(t, ds, naming.SyncSnapPattern(identifier, host))
	if len(got) != 1 {
		t.Fatalf("sync snapshots of %s with identifier %q: %q, want exactly one", ds, identifier, got)
	}
	return got[0]
}

func TestITInitialAndIncremental(t *testing.T) {
	begin(t)
	host := hostname(t)
	src, dst := srcPool+"/data", dstPool+"/data"
	zfs(t, "create", src)
	writeRandomFile(t, filepath.Join(mountpoint(t, src), "one"), 4<<20)
	zfs(t, "snapshot", src+"@s1")

	syngoidOK(t, src, dst)
	sync1 := short(oneSyncSnap(t, src, "", host))
	equal(t, "target snapshots after the initial sync", snaps(t, dst), full(dst, "s1", sync1))

	writeRandomFile(t, filepath.Join(mountpoint(t, src), "two"), 4<<20)
	zfs(t, "snapshot", src+"@s2")
	syngoidOK(t, src, dst)
	sync2 := short(oneSyncSnap(t, src, "", host))
	if sync2 == sync1 {
		t.Fatalf("second run reused sync snapshot %s", sync1)
	}
	// The previous sync snapshot is pruned on both sides.
	equal(t, "source snapshots", snaps(t, src), full(src, "s1", "s2", sync2))
	equal(t, "target snapshots", snaps(t, dst), full(dst, "s1", "s2", sync2))
	if g, w := prop(t, dst+"@"+sync2, "guid"), prop(t, src+"@"+sync2, "guid"); g != w {
		t.Fatalf("guid of %s@%s = %s, want %s", dst, sync2, g, w)
	}
	for _, f := range []string{"one", "two"} {
		if g, w := fileSHA256(t, filepath.Join(mountpoint(t, dst), f)), fileSHA256(t, filepath.Join(mountpoint(t, src), f)); g != w {
			t.Fatalf("%s differs on the target", f)
		}
	}

	// The per-dataset summary line (the totals line always mentions
	// "nothing to do").
	r := syngoidOK(t, "--no-sync-snap", src, dst)
	if !strings.Contains(r.stdout, src+" → "+dst+": nothing to do") {
		t.Fatal("a run without new snapshots should report nothing to do for the dataset")
	}
}

func TestITRecursiveWithClone(t *testing.T) {
	begin(t)
	root, troot := srcPool+"/rc", dstPool+"/rc"
	zfs(t, "create", root)
	zfs(t, "create", root+"/a")
	writeRandomFile(t, filepath.Join(mountpoint(t, root+"/a"), "data"), 8<<20)
	zfs(t, "snapshot", root+"/a@s1")
	zfs(t, "clone", root+"/a@s1", root+"/b")

	syngoidOK(t, "-r", root, troot)
	for _, ds := range []string{troot, troot + "/a", troot + "/b"} {
		if !exists(t, ds) {
			t.Fatalf("%s was not created", ds)
		}
	}
	if got, want := prop(t, troot+"/b", "origin"), troot+"/a@s1"; got != want {
		t.Fatalf("origin of %s/b = %q, want %q (clone not recreated as a clone)", troot, got, want)
	}
	syngoidOK(t, "-r", root, troot)
}

func TestITExcludeDatasets(t *testing.T) {
	begin(t)
	host := hostname(t)
	root, troot := srcPool+"/ex", dstPool+"/ex"
	zfs(t, "create", root)
	zfs(t, "create", root+"/keep")
	zfs(t, "create", root+"/skip")

	syngoidOK(t, "-r", "--exclude-datasets=/skip$", root, troot)
	if !exists(t, troot+"/keep") {
		t.Fatalf("%s/keep was not replicated", troot)
	}
	if exists(t, troot+"/skip") {
		t.Fatalf("%s/skip was replicated despite --exclude-datasets", troot)
	}
	if got := syncSnaps(t, root+"/skip", naming.SyncSnapPattern("", host)); len(got) != 0 {
		t.Fatalf("excluded dataset got sync snapshots %q", got)
	}
}

func TestITUseHold(t *testing.T) {
	begin(t)
	host := hostname(t)
	tag := naming.HoldTag("", host)
	src, dst := srcPool+"/hold", dstPool+"/hold"
	zfs(t, "create", src)

	syngoidOK(t, "--use-hold", src, dst)
	first := short(oneSyncSnap(t, src, "", host))
	for _, s := range []string{src + "@" + first, dst + "@" + first} {
		if !slices.Contains(holdTags(t, s), tag) {
			t.Fatalf("holds of %s = %q, want %s", s, holdTags(t, s), tag)
		}
	}

	syngoidOK(t, "--use-hold", src, dst)
	second := short(oneSyncSnap(t, src, "", host))
	for _, s := range []string{src + "@" + second, dst + "@" + second} {
		if !slices.Contains(holdTags(t, s), tag) {
			t.Fatalf("holds of %s = %q, want %s", s, holdTags(t, s), tag)
		}
	}
	// The previous sync snapshot was released, so pruning could destroy it.
	for _, s := range []string{src + "@" + first, dst + "@" + first} {
		if exists(t, s) {
			t.Fatalf("%s still exists: its hold was not released before pruning", s)
		}
	}
}

func TestITCreateBookmark(t *testing.T) {
	begin(t)
	src, dst := srcPool+"/bm", dstPool+"/bm"
	zfs(t, "create", src)
	zfs(t, "snapshot", src+"@s1")

	syngoidOK(t, "--no-sync-snap", "--create-bookmark", src, dst)
	zfs(t, "snapshot", src+"@s2")
	syngoidOK(t, "--no-sync-snap", "--create-bookmark", src, dst)

	equal(t, "source bookmarks", bookmarksR(t, src), []string{src + "#s1", src + "#s2"})
	for _, s := range []string{"s1", "s2"} {
		if g, w := prop(t, src+"#"+s, "guid"), prop(t, src+"@"+s, "guid"); g != w {
			t.Fatalf("guid of %s#%s = %s, want the snapshot's %s", src, s, g, w)
		}
	}
	equal(t, "target snapshots", snaps(t, dst), full(dst, "s1", "s2"))
}

func TestITPruneOnlyOwn(t *testing.T) {
	begin(t)
	host := hostname(t)
	src := srcPool + "/prune"
	zfs(t, "create", src)
	// A sync snapshot of another host whose name extends ours: syncoid's
	// prefix match pruned it (the nas/nas2 bug, spec §8.9 item 1).
	foreign := "syncoid_" + host + "2_2020-01-01:00:00:00-GMT00:00"
	zfs(t, "snapshot", src+"@"+foreign)

	runs := []struct{ id, target string }{
		{"a", dstPool + "/a"},
		{"b", dstPool + "/b"},
		{"", dstPool + "/plain"},
	}
	for range 2 {
		for _, r := range runs {
			args := []string{src, r.target}
			if r.id != "" {
				args = append([]string{"--identifier=" + r.id}, args...)
			}
			syngoidOK(t, args...)
		}
	}

	// Each identifier pruned only its own older sync snapshot.
	for _, r := range runs {
		oneSyncSnap(t, src, r.id, host)
		oneSyncSnap(t, r.target, r.id, host)
	}
	if n := len(snaps(t, src)); n != 4 {
		t.Fatalf("source snapshots = %q, want the foreign one and one per identifier", snaps(t, src))
	}
	for _, s := range []string{src + "@" + foreign, dstPool + "/plain@" + foreign} {
		if !exists(t, s) {
			t.Fatalf("foreign sync snapshot %s was pruned", s)
		}
	}
}

func TestITDeleteTargetSnapshots(t *testing.T) {
	begin(t)
	src, dst := srcPool+"/dts", dstPool+"/dts"
	zfs(t, "create", src)
	for _, s := range []string{"s1", "s2", "s3"} {
		zfs(t, "snapshot", src+"@"+s)
	}
	syngoidOK(t, "--no-sync-snap", src, dst)
	equal(t, "target snapshots", snaps(t, dst), full(dst, "s1", "s2", "s3"))

	zfs(t, "destroy", src+"@s2")
	zfs(t, "snapshot", src+"@s4")
	syngoidOK(t, "--no-sync-snap", "--delete-target-snapshots", src, dst)
	equal(t, "target snapshots", snaps(t, dst), full(dst, "s1", "s3", "s4"))
}

func TestITInterruptThenResume(t *testing.T) {
	begin(t)
	host := hostname(t)
	src, dst := srcPool+"/resume", dstPool+"/resume"
	zfs(t, "create", src)
	writeRandomFile(t, filepath.Join(mountpoint(t, src), "big"), 300<<20)

	interruptSync(t, src, dst)
	requireResumeToken(t, dst)

	r := syngoidOK(t, src, dst)
	if !strings.Contains(r.stdout, "Resuming interrupted zfs send/receive") {
		t.Fatal("the rerun did not resume the interrupted transfer")
	}
	if tok := resumeToken(t, dst); tok != "-" {
		t.Fatalf("receive_resume_token of %s = %q after the resumed run, want -", dst, tok)
	}
	newest := short(oneSyncSnap(t, src, "", host))
	if g, w := prop(t, dst+"@"+newest, "guid"), prop(t, src+"@"+newest, "guid"); g != w {
		t.Fatalf("guid of %s@%s = %s, want %s", dst, newest, g, w)
	}
	if g, w := fileSHA256(t, filepath.Join(mountpoint(t, dst), "big")), fileSHA256(t, filepath.Join(mountpoint(t, src), "big")); g != w {
		t.Fatal("the resumed file differs from the source")
	}
}

func TestITDryRunNoChanges(t *testing.T) {
	begin(t)
	src, dst := srcPool+"/dry", dstPool+"/dry"
	zfs(t, "create", src)
	zfs(t, "snapshot", src+"@s1")

	r := syngoidOK(t, "--dry-run", src, dst)
	if exists(t, dst) {
		t.Fatalf("--dry-run created %s", dst)
	}
	equal(t, "source snapshots after --dry-run", snaps(t, src), full(src, "s1"))
	for _, want := range []string{src + " → " + dst, "Total estimated transfer:"} {
		if !strings.Contains(r.stdout, want) {
			t.Fatalf("plan output lacks %q", want)
		}
	}

	syngoidOK(t, src, dst)
	zfs(t, "snapshot", src+"@s2")
	srcBefore, dstBefore := snaps(t, src), snaps(t, dst)
	r = syngoidOK(t, "--dry-run", src, dst)
	equal(t, "source snapshots after --dry-run", snaps(t, src), srcBefore)
	equal(t, "target snapshots after --dry-run", snaps(t, dst), dstBefore)
	if !strings.Contains(r.stdout, "create sync snapshot ") {
		t.Fatal("plan output does not announce the sync snapshot")
	}
}
