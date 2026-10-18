//go:build integration

package integration

// Ports of the syncoid regression scenarios of sanoid tests/syncoid/. Each
// test reproduces the upstream run.sh setup and assertions with syngoid
// instead of syncoid. Upstream uses one pool holding both POOL/src and
// POOL/dst; here the source lives in syngoid-src and the target in
// syngoid-dst, and the snapshot-list checksums are replaced by the explicit
// lists they encode (checked against the upstream checksums when porting).
//
// Upstream sleeps between `zfs snapshot` calls "so creation time can be used
// for proper sorting"; syngoid orders snapshots by createtxg (spec §8.1),
// so those sleeps are dropped. The helpers start each syngoid run in a later
// wall-clock second than the previous run ended in, so sync snapshot names
// differ.

import (
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// upstreamPair returns the source and target datasets used by the ports.
func upstreamPair() (src, dst string) {
	return srcPool + "/src", dstPool + "/dst"
}

// snapshotAll creates the snapshots ds@name for each name, in order.
func snapshotAll(t *testing.T, ds string, names ...string) {
	t.Helper()
	for _, n := range names {
		zfs(t, "snapshot", ds+"@"+n)
	}
}

// 001_bookmark_replication_intermediate: replication with fallback to
// bookmarks and all intermediate snapshots.
func TestUpstream001BookmarkReplicationIntermediate(t *testing.T) {
	begin(t)
	src, dst := upstreamPair()
	zfs(t, "create", src)
	zfs(t, "snapshot", src+"@snap1")
	zfs(t, "bookmark", src+"@snap1", src+"#snap1")

	// initial replication
	syngoidOK(t, "--no-sync-snap", "--debug", "--compress=none", src, dst)
	// destroy last common snapshot on source
	zfs(t, "destroy", src+"@snap1")
	// create intermediate snapshots
	snapshotAll(t, src, "snap2", "snap3", "snap4", "snap5")
	// replicate which should fallback to bookmarks
	syngoidOK(t, "--debug", "--compress=none", src, dst)

	equal(t, "target snapshots (sync snapshots ignored)", withoutSync(snapsR(t, dst)),
		full(dst, "snap1", "snap2", "snap3", "snap4", "snap5"))
	equal(t, "source snapshots (sync snapshots ignored)", withoutSync(snapsR(t, src)),
		full(src, "snap2", "snap3", "snap4", "snap5"))
}

// 002_bookmark_replication_no_intermediate: replication with fallback to
// bookmarks, without intermediate snapshots (--no-stream).
func TestUpstream002BookmarkReplicationNoIntermediate(t *testing.T) {
	begin(t)
	src, dst := upstreamPair()
	zfs(t, "create", src)
	zfs(t, "snapshot", src+"@snap1")
	zfs(t, "bookmark", src+"@snap1", src+"#snap1")

	// initial replication
	syngoidOK(t, "--no-sync-snap", "--debug", "--compress=none", src, dst)
	// destroy last common snapshot on source
	zfs(t, "destroy", src+"@snap1")
	// create intermediate snapshots
	snapshotAll(t, src, "snap2", "snap3", "snap4", "snap5")
	// replicate which should fallback to bookmarks
	syngoidOK(t, "--no-stream", "--no-sync-snap", "--debug", "--compress=none", src, dst)

	equal(t, "target snapshots", snapsR(t, dst), full(dst, "snap1", "snap5"))
	equal(t, "source snapshots", snapsR(t, src), full(src, "snap2", "snap3", "snap4", "snap5"))
}

// 003_force_delete: replication with deletion of the target if no matches
// are found.
func TestUpstream003ForceDelete(t *testing.T) {
	begin(t)
	src, dst := upstreamPair()
	zfs(t, "create", src)
	for _, c := range []string{"1", "2", "3"} {
		zfs(t, "create", src+"/"+c)
	}

	// initial replication
	syngoidOK(t, "-r", "--debug", "--compress=none", src, dst)
	// destroy last common snapshot on source
	zfs(t, "destroy", src+"/2@%")
	zfs(t, "snapshot", src+"/2@test")
	syngoidOK(t, "-r", "--force-delete", "--debug", "--compress=none", src, dst)

	for _, ds := range []string{src, dst} {
		want := []string{ds + "@syncoid_", ds + "/1@syncoid_", ds + "/2@test", ds + "/2@syncoid_", ds + "/3@syncoid_"}
		equal(t, ds+" snapshots (sync snapshot names normalized)", normalizeSync(snapsR(t, ds)), want)
	}
}

// 004_bookmark_replication_edge_case: the bookmark fallback stops when the
// snapshot after the bookmark is already the newest.
func TestUpstream004BookmarkReplicationEdgeCase(t *testing.T) {
	begin(t)
	src, dst := upstreamPair()
	zfs(t, "create", src)
	zfs(t, "snapshot", src+"@snap1")
	zfs(t, "bookmark", src+"@snap1", src+"#snap1")

	// initial replication
	syngoidOK(t, "--no-sync-snap", "--debug", "--compress=none", src, dst)
	// destroy last common snapshot on source
	zfs(t, "destroy", src+"@snap1")
	zfs(t, "snapshot", src+"@snap2")
	// replicate which should fallback to bookmarks and stop because it's
	// already on the latest snapshot
	syngoidOK(t, "--no-sync-snap", "--debug", "--compress=none", src, dst)

	equal(t, "target snapshots", withoutSync(snapsR(t, dst)), full(dst, "snap1", "snap2"))
	equal(t, "source snapshots", withoutSync(snapsR(t, src)), full(src, "snap2"))
}

// 005_reset_resume_state: replication with --no-resume to a target holding
// a partially received stream.
func TestUpstream005ResetResumeState(t *testing.T) {
	begin(t)
	src, dst := upstreamPair()
	zfs(t, "create", src)
	syngoidOK(t, "--debug", "--compress=none", src, dst)
	writeRandomFile(t, filepath.Join(mountpoint(t, src), "big_file"), 200<<20)

	// Upstream throttles with --source-bwlimit=2m (removed, spec §3.6) and
	// kills the processes after 5 s; interruptSync freezes the send instead.
	interruptSync(t, "--debug", "--compress=none", src, dst)
	requireResumeToken(t, dst)

	// Upstream greps for syncoid's "reset partial receive state of syncoid"
	// message. syngoid plans the normal send, and when zfs receive reports
	// the partial state it runs `zfs receive -A` and retries once (spec
	// §8.7, ErrPartialState with --no-resume); the command is logged at
	// DEBUG.
	r := syngoidOK(t, "--debug", "--compress=none", "--no-resume", src, dst)
	if !strings.Contains(r.stdout, "zfs receive -A "+dst) {
		t.Fatalf("the --no-resume run did not reset the partial receive state of %s", dst)
	}
	syngoidOK(t, "--debug", "--compress=none", src, dst)
}

// 006_reset_resume_state2: resumable replication where the snapshot the
// interrupted stream was sending no longer exists.
func TestUpstream006ResetResumeState2(t *testing.T) {
	begin(t)
	src, dst := upstreamPair()
	zfs(t, "create", src)
	syngoidOK(t, "--debug", "--compress=none", src, dst)
	writeRandomFile(t, filepath.Join(mountpoint(t, src), "big_file"), 200<<20)
	zfs(t, "snapshot", src+"@big")

	// See 005 for the replacement of --source-bwlimit.
	interruptSync(t, "--debug", "--no-sync-snap", "--compress=none", src, dst)
	requireResumeToken(t, dst)

	zfs(t, "destroy", src+"@big")
	syngoidOK(t, "--debug", "--compress=none", src, dst)
	syngoidOK(t, "--debug", "--compress=none", src, dst)
}

// 007_preserve_recordsize: preserving the recordsize of the source
// filesystems on the target.
func TestUpstream007PreserveRecordsize(t *testing.T) {
	begin(t)
	src, dst := upstreamPair()
	zfs(t, "create", src)
	zfs(t, "create", "-V", "100M", "-o", "volblocksize=4k", src+"/zvol4")
	zfs(t, "create", "-V", "100M", "-o", "volblocksize=16k", src+"/zvol16")
	zfs(t, "create", "-V", "100M", "-o", "volblocksize=64k", src+"/zvol64")
	zfs(t, "create", "-o", "recordsize=16k", src+"/16")
	zfs(t, "create", "-o", "recordsize=32k", src+"/32")
	zfs(t, "create", "-o", "recordsize=128k", src+"/128")

	syngoidOK(t, "--preserve-recordsize", "--recursive", "--debug", "--compress=none", src, dst)
	t.Log(zfs(t, "get", "-t", "filesystem", "-r", "recordsize", dst))
	t.Log(zfs(t, "get", "-t", "volume", "-r", "volblocksize", dst))

	for ds, want := range map[string]string{dst + "/16": "16K", dst + "/32": "32K", dst + "/128": "128K"} {
		if got := prop(t, ds, "recordsize"); got != want {
			t.Errorf("recordsize of %s = %s, want %s", ds, got, want)
		}
	}
}

// 008_force_delete_snapshot: replication with deletion of a conflicting
// snapshot on the target.
func TestUpstream008ForceDeleteSnapshot(t *testing.T) {
	begin(t)
	src, dst := upstreamPair()
	zfs(t, "create", src)
	zfs(t, "snapshot", src+"@duplicate")

	// initial replication
	syngoidOK(t, "-r", "--debug", "--compress=none", src, dst)
	// recreate snapshot with the same name on src
	zfs(t, "destroy", src+"@duplicate")
	zfs(t, "snapshot", src+"@duplicate")
	syngoidOK(t, "-r", "--force-delete", "--debug", "--compress=none", src, dst)

	equal(t, "target guid,snapshot list vs source", guidList(t, dst), guidList(t, src))
}

// guidList returns "guid name" for every snapshot of ds and its
// descendants, with ds replaced by "." and sync snapshot names normalized.
func guidList(t *testing.T, ds string) []string {
	t.Helper()
	out := lines(zfs(t, "list", "-t", "snapshot", "-r", "-H", "-o", "guid,name", ds))
	for i, l := range out {
		guid, name, _ := strings.Cut(l, "\t")
		out[i] = guid + " ." + strings.TrimPrefix(normalizeSync([]string{name})[0], ds)
	}
	return out
}

// 009_preserve_properties: preserving the locally set properties of the
// source datasets on the target. Ported from the later upstream revision,
// which uses xattr=sa (v2.2.0 used xattr=on) and adds a user property with
// whitespace and shell metacharacters.
func TestUpstream009PreserveProperties(t *testing.T) {
	begin(t)
	src, dst := upstreamPair()
	const userProp, userValue = "net.openoid:var-name", `with whitespace and !"§$%&/()= symbols`
	zfs(t, "create", "-o", "recordsize=16k", "-o", "xattr=sa", "-o", "mountpoint=none", "-o", "primarycache=none", src)
	zfs(t, "create", "-V", "100M", "-o", "volblocksize=8k", src+"/zvol8")
	zfs(t, "create", "-V", "100M", "-o", "volblocksize=16k", "-o", "primarycache=all", src+"/zvol16")
	zfs(t, "create", "-V", "100M", "-o", "volblocksize=64k", src+"/zvol64")
	zfs(t, "create", "-o", "recordsize=16k", "-o", "primarycache=none", src+"/16")
	zfs(t, "create", "-o", "recordsize=32k", "-o", "acltype=posixacl", src+"/32")
	zfs(t, "set", userProp+"="+userValue, src+"/32")

	syngoidOK(t, "--preserve-properties", "--recursive", "--debug", "--compress=none", src, dst)

	for _, c := range []struct{ ds, prop, want string }{
		{dst, "recordsize", "16K"},
		{dst, "mountpoint", "none"},
		{dst, "xattr", "sa"},
		{dst, "primarycache", "none"},
		{dst + "/16", "recordsize", "16K"},
		{dst + "/16", "primarycache", "none"},
		{dst + "/32", "recordsize", "32K"},
		{dst + "/32", userProp, userValue},
	} {
		if got := prop(t, c.ds, c.prop); got != c.want {
			t.Errorf("%s of %s = %q, want %q", c.prop, c.ds, got, c.want)
		}
	}
	// OpenZFS 2.2 prints posixacl as "posix" (upstream's expectation);
	// older releases print "posixacl".
	if got := prop(t, dst+"/32", "acltype"); got != "posix" && got != "posixacl" {
		t.Errorf("acltype of %s/32 = %q, want posix", dst, got)
	}
}

// 010_filter_snaps: filtering snapshot names with --include-snaps and
// --exclude-snaps.
func TestUpstream010FilterSnaps(t *testing.T) {
	begin(t)
	src, dst := upstreamPair()
	all := []string{"monthly1", "daily1", "daily2", "hourly1", "hourly2", "daily3", "hourly3", "hourly4"}
	zfs(t, "create", src)
	snapshotAll(t, src, all...)

	cases := []struct {
		name string
		args []string
		want []string // target snapshots
	}{
		// --exclude-snaps without --no-stream: the hourly snapshots are
		// missing, all other intermediate snapshots are present.
		{"1_exclude", []string{"--no-sync-snap", "--exclude-snaps=hourly"}, []string{"monthly1", "daily1", "daily2", "daily3"}},
		// --exclude-snaps with --no-stream: only daily3.
		{"2_exclude_no_stream", []string{"--no-sync-snap", "--exclude-snaps=hourly", "--no-stream"}, []string{"daily3"}},
		// --include-snaps without --no-stream: only the hourly snapshots.
		{"3_include", []string{"--no-sync-snap", "--include-snaps=hourly"}, []string{"hourly1", "hourly2", "hourly3", "hourly4"}},
		// --include-snaps with --no-stream: only hourly4.
		{"4_include_no_stream", []string{"--no-sync-snap", "--include-snaps=hourly", "--no-stream"}, []string{"hourly4"}},
		// both: the hourly snapshots except hourly3.
		{"5_include_exclude", []string{"--no-sync-snap", "--include-snaps=hourly", "--exclude-snaps=3"}, []string{"hourly1", "hourly2", "hourly4"}},
		// --exclude-snaps=syncoid with --no-stream and without
		// --no-sync-snap: only hourly4 is sent.
		{"6_exclude_sync_snap", []string{"--no-stream", "--exclude-snaps=syncoid"}, []string{"hourly4"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// clean_snaps: each case starts without a target.
			if exists(t, dst) {
				destroyWithRetry(t, dst)
			}
			args := append(append([]string{"--debug", "--compress=none"}, c.args...), src, dst)
			syngoidOK(t, args...)
			equal(t, "target snapshots", snapsR(t, dst), full(dst, c.want...))
			// Upstream's case 6 also expects a syncoid_ snapshot on the
			// source: syncoid creates the sync snapshot and then ignores
			// it. syngoid does not create a sync snapshot that the filters
			// exclude (spec §8.4), so the source is unchanged in every case.
			equal(t, "source snapshots", snapsR(t, src), full(src, all...))
		})
	}
}

// 011_sync_out-of-order_snapshots: snapshots whose creation times are out
// of order are still replicated (syngoid orders by createtxg, spec §8.1).
//
// Upstream sets the clock back to 2006 before creating the later snapshots
// and leaves it there (INVASIVE_TESTS=1). This port gets the same ordering
// less invasively: the first snapshot is created with the clock one hour
// ahead, the clock is restored, then the other two are created, so they
// are newer by createtxg but older by creation time.
func TestUpstream011SyncOutOfOrderSnapshots(t *testing.T) {
	requireInvasive(t)
	begin(t)
	before, after := srcPool+"/before", dstPool+"/after"
	const first = "this-snapshot-should-make-it-into-the-after-dataset"
	zfs(t, "create", before)

	start := time.Now()
	restored := false
	restore := func() {
		if !restored {
			restored = true
			// time.Since uses the monotonic clock, unaffected by setClock.
			setClock(t, start.Add(time.Since(start)))
		}
	}
	t.Cleanup(restore)
	setClock(t, start.Add(time.Hour))
	zfs(t, "snapshot", before+"@"+first)
	restore()
	snapshotAll(t, before, "oldest-snapshot", "another-snapshot-does-not-matter")

	c0, _ := strconv.ParseInt(zfs(t, "get", "-Hp", "-o", "value", "creation", before+"@"+first), 10, 64)
	c1, _ := strconv.ParseInt(zfs(t, "get", "-Hp", "-o", "value", "creation", before+"@oldest-snapshot"), 10, 64)
	if c0 <= c1 {
		t.Fatalf("setup: creation of @%s (%d) is not after @oldest-snapshot (%d)", first, c0, c1)
	}

	syngoidOK(t, "--sendoptions=Lec", before, after)

	got := snapsR(t, after)
	for _, s := range []string{first, "oldest-snapshot", "another-snapshot-does-not-matter"} {
		if !slices.Contains(got, after+"@"+s) {
			t.Errorf("%s@%s was not replicated; target snapshots: %q", after, s, got)
		}
	}
}

// 012_receive_resume_token: continuing a partial transfer left by an
// interrupted `zfs send -pwR | zfs recv -s`.
func TestUpstream012ReceiveResumeToken(t *testing.T) {
	begin(t)
	src, dst := srcPool+"/source", dstPool+"/destination"
	zfs(t, "create", src)
	zfs(t, "snapshot", src+"@empty")
	writeRandomFile(t, filepath.Join(mountpoint(t, src), "garbage.bin"), 16<<20)
	zfs(t, "snapshot", src+"@something")

	// Simulate interrupted transfer: zfs send -pwR ... | head --bytes=8M | zfs recv -s
	if _, err := pipeZFS(t, []string{"send", "-pwR", src + "@something"}, "", []string{"receive", "-s", dst}, 8<<20); err == nil {
		t.Fatal("setup: the truncated receive succeeded")
	}
	requireResumeToken(t, dst)

	// Using syngoid to continue interrupted transfer
	syngoidOK(t, "--sendoptions=pw", src, dst)

	// Verify data integrity
	if g, w := fileSHA256(t, filepath.Join(mountpoint(t, dst), "garbage.bin")), fileSHA256(t, filepath.Join(mountpoint(t, src), "garbage.bin")); g != w {
		t.Fatal("data integrity check failed")
	}
}

// 013_bookmark_replication_0_named: bookmark fallback with a snapshot and
// bookmark named "0" (falsy in Perl).
func TestUpstream013BookmarkReplication0Named(t *testing.T) {
	begin(t)
	src, dst := upstreamPair()
	zfs(t, "create", src)
	zfs(t, "snapshot", src+"@0")

	// initial replication
	syngoidOK(t, "--no-sync-snap", "--create-bookmark", "--debug", "--compress=none", src, dst)
	// destroy last common snapshot on source
	zfs(t, "destroy", src+"@0")
	zfs(t, "snapshot", src+"@1")
	// replicate which should fallback to bookmarks
	syngoidOK(t, "--no-sync-snap", "--create-bookmark", "--debug", "--compress=none", src, dst)

	equal(t, "target snapshots", withoutSync(snapsR(t, dst)), full(dst, "0", "1"))
	equal(t, "source snapshots", withoutSync(snapsR(t, src)), full(src, "1"))
	equal(t, "source bookmarks", bookmarksR(t, src), []string{src + "#0", src + "#1"})
	equal(t, "target bookmarks", bookmarksR(t, dst), nil)
}

// 014_bookmark_check_guid_on_existing: GUIDs of the bookmarks created by
// --create-bookmark, including the GUID-suffixed fallback name.
func TestUpstream014BookmarkCheckGuidOnExisting(t *testing.T) {
	begin(t)
	a, b, c := srcPool+"/a", dstPool+"/b", dstPool+"/c"
	flags := []string{"--debug", "--no-sync-snap", "--no-rollback", "--create-bookmark"}
	guid := func(name string) string { return prop(t, name, "guid") }
	zfs(t, "create", a)
	zfs(t, "snapshot", a+"@s0")

	// This fully replicates a to b
	syngoidOK(t, append(flags, a, b)...)
	// This fully replicates a to c
	syngoidOK(t, append(flags, a, c)...)

	// Bookmark guid should equal guid of all snapshots
	bm := guid(a + "#s0")
	for _, s := range []string{a + "@s0", b + "@s0", c + "@s0"} {
		if g := guid(s); g != bm {
			t.Fatalf("guid of %s = %s, want the bookmark's %s", s, g, bm)
		}
	}
	// Upstream (after v2.2.0) checks the GUID of an existing bookmark and
	// expects no fallback bookmark here. syngoid keeps syncoid 2.2.0's rule
	// (spec §8.1): when #s0 cannot be created, retry once with the first six
	// GUID characters appended. So the second run, finding #s0 taken,
	// creates the fallback, which must point at the same snapshot.
	if g := guid(a + "#s0" + bm[:6]); g != bm {
		t.Fatalf("guid of fallback bookmark %s#s0%s = %s, want %s", a, bm[:6], g, bm)
	}

	zfs(t, "snapshot", a+"@s1")
	// Create bookmark so syngoid is forced to create fallback bookmark
	zfs(t, "bookmark", a+"@s0", a+"#s1")
	// This incrementally replicates from a@s0 to a@s1 and should create a
	// bookmark with fallback suffix
	syngoidOK(t, append(flags, a, b)...)
	s1 := guid(a + "@s1")
	// Fallback bookmark guid should equal guid of snapshot
	if g := guid(a + "#s1" + s1[:6]); g != s1 {
		t.Fatalf("guid of fallback bookmark %s#s1%s = %s, want %s", a, s1[:6], g, s1)
	}

	zfs(t, "snapshot", a+"@s2")
	s2 := guid(a + "@s2")
	// Create bookmark and fallback bookmark so syngoid should fail
	zfs(t, "bookmark", a+"@s0", a+"#s2")
	zfs(t, "bookmark", a+"@s0", a+"#s2"+s2[:6])
	// This incrementally replicates from a@s1 to a@s2 and should fail to
	// create a bookmark with fallback suffix
	if r := syngoid(t, append(flags, a, b)...); r.code == 0 {
		t.Fatal("syngoid succeeded although neither bookmark name was free")
	}
}
