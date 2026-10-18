package fake_test

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/endpoint/fake"
	"github.com/piotaixr/syngoid/internal/stream"
	"github.com/piotaixr/syngoid/internal/zfs"
)

var _ endpoint.Endpoint = (*fake.Endpoint)(nil)

var ctx = context.Background()

// replicate pipes Send on src into Receive on dst and returns both errors.
func replicate(src, dst *fake.Endpoint, codec stream.Codec, s zfs.SendSpec, r zfs.RecvSpec) (sendErr, recvErr error) {
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		err := src.Send(ctx, s, endpoint.SendOpts{Codec: codec}, pw)
		pw.CloseWithError(err)
		done <- err
	}()
	recvErr = dst.Receive(ctx, r, codec, pr)
	_ = pr.Close()
	return <-done, recvErr
}

func mustReplicate(t *testing.T, src, dst *fake.Endpoint, s zfs.SendSpec, r zfs.RecvSpec) {
	t.Helper()
	serr, rerr := replicate(src, dst, stream.None, s, r)
	if serr != nil || rerr != nil {
		t.Fatalf("replicate: send=%v receive=%v", serr, rerr)
	}
}

func snapNames(ds *fake.Dataset) []string {
	var out []string
	for _, s := range ds.Snaps {
		out = append(out, s.Name)
	}
	return out
}

func wantStderr(t *testing.T, err error, want string) {
	t.Helper()
	var ce *zfs.CommandError
	if !errors.As(err, &ce) {
		t.Fatalf("err = %v (%T), want *zfs.CommandError", err, err)
	}
	if ce.Stderr != want {
		t.Errorf("stderr = %q, want %q", ce.Stderr, want)
	}
	if ce.ExitCode != 1 {
		t.Errorf("exit = %d, want 1", ce.ExitCode)
	}
	if k, w := ce.Kind, zfs.Classify(want); !errors.Is(k, w) && reflect.TypeOf(k) != reflect.TypeOf(w) {
		t.Errorf("Kind = %v, want Classify(stderr) = %v", k, w)
	}
}

// pair returns a source world holding tank/a with snapshots s1, s2, s3 and an
// empty target world, each behind an endpoint.
func pair() (sw, tw *fake.World, src, dst *fake.Endpoint) {
	sw, tw = fake.NewWorld(), fake.NewWorld()
	sw.Create("tank/a", "filesystem")
	for _, n := range []string{"s1", "s2", "s3"} {
		sw.Snap("tank/a", n)
	}
	tw.Create("backup", "filesystem")
	return sw, tw, fake.New(sw, endpoint.HostInfo{}), fake.New(tw, endpoint.HostInfo{User: "backup"})
}

func fullSpec(to string) zfs.SendSpec { return zfs.SendSpec{Dataset: "tank/a", To: to} }

func incSpec(from, to string, inter bool) zfs.SendSpec {
	return zfs.SendSpec{Dataset: "tank/a", To: to, FromSnap: from, Intermediates: inter}
}

func TestFakeFullThenIncremental(t *testing.T) {
	sw, tw, src, dst := pair()
	mustReplicate(t, src, dst, fullSpec("s1"), zfs.RecvSpec{Dataset: "backup/a"})
	got := tw.Dataset("backup/a")
	if got == nil || !reflect.DeepEqual(snapNames(got), []string{"s1"}) {
		t.Fatalf("after full: %+v", got)
	}
	if got.Snaps[0] != sw.Dataset("tank/a").Snaps[0] {
		t.Errorf("received snapshot %+v differs from source %+v (guid/txg/creation must be kept)", got.Snaps[0], sw.Dataset("tank/a").Snaps[0])
	}

	mustReplicate(t, src, dst, incSpec("s1", "s3", true), zfs.RecvSpec{Dataset: "backup/a"})
	if !reflect.DeepEqual(snapNames(got), []string{"s1", "s2", "s3"}) {
		t.Errorf("after -I: %v", snapNames(got))
	}

	// -i on a fresh copy only carries the target snapshot.
	tw2 := fake.NewWorld()
	tw2.Create("backup", "filesystem")
	dst2 := fake.New(tw2, endpoint.HostInfo{})
	mustReplicate(t, src, dst2, fullSpec("s1"), zfs.RecvSpec{Dataset: "backup/a"})
	mustReplicate(t, src, dst2, incSpec("s1", "s3", false), zfs.RecvSpec{Dataset: "backup/a"})
	if g := snapNames(tw2.Dataset("backup/a")); !reflect.DeepEqual(g, []string{"s1", "s3"}) {
		t.Errorf("after -i: %v", g)
	}
}

func TestFakeFullThroughCodecs(t *testing.T) {
	for _, c := range []stream.Codec{stream.None, stream.Gzip, stream.Zstd, stream.LZ4} {
		_, tw, src, dst := pair()
		serr, rerr := replicate(src, dst, c, fullSpec("s2"), zfs.RecvSpec{Dataset: "backup/a"})
		if serr != nil || rerr != nil {
			t.Fatalf("%s: send=%v recv=%v", c, serr, rerr)
		}
		if g := snapNames(tw.Dataset("backup/a")); !reflect.DeepEqual(g, []string{"s2"}) {
			t.Errorf("%s: snaps = %v", c, g)
		}
	}
}

func TestFakeIncrementalWrongBaseFails(t *testing.T) {
	_, tw, src, dst := pair()
	tw.Create("backup/a", "filesystem")
	tw.Snap("backup/a", "other") // different guid than any source snapshot
	serr, rerr := replicate(src, dst, stream.None, incSpec("s1", "s2", false), zfs.RecvSpec{Dataset: "backup/a"})
	if serr != nil {
		t.Fatalf("send: %v", serr)
	}
	wantStderr(t, rerr, "cannot receive incremental stream: most recent snapshot of backup/a does not match incremental source")
	var ce *zfs.CommandError
	if !errors.As(rerr, &ce) || ce.Kind != nil {
		t.Errorf("err = %v, want a CommandError with nil Kind (generic failure)", rerr)
	}
}

func TestFakeFullIntoExistingFails(t *testing.T) {
	_, tw, src, dst := pair()
	tw.Create("backup/a", "filesystem")
	_, rerr := replicate(src, dst, stream.None, fullSpec("s1"), zfs.RecvSpec{Dataset: "backup/a"})
	wantStderr(t, rerr, "cannot receive new filesystem stream: destination 'backup/a' exists\nmust specify -F to overwrite it")
}

func TestFakeForceRollback(t *testing.T) {
	sw, tw, src, dst := pair()
	mustReplicate(t, src, dst, fullSpec("s1"), zfs.RecvSpec{Dataset: "backup/a"})
	tw.Snap("backup/a", "extra")

	_, rerr := replicate(src, dst, stream.None, incSpec("s1", "s2", false), zfs.RecvSpec{Dataset: "backup/a"})
	wantStderr(t, rerr, "cannot receive incremental stream: most recent snapshot of backup/a does not match incremental source")
	if g := snapNames(tw.Dataset("backup/a")); !reflect.DeepEqual(g, []string{"s1", "extra"}) {
		t.Fatalf("failed receive changed target: %v", g)
	}

	mustReplicate(t, src, dst, incSpec("s1", "s2", false), zfs.RecvSpec{Dataset: "backup/a", Args: []string{"-F"}})
	if g := snapNames(tw.Dataset("backup/a")); !reflect.DeepEqual(g, []string{"s1", "s2"}) {
		t.Errorf("after -F: %v", g)
	}
	if tw.Dataset("backup/a").Snaps[1] != sw.Dataset("tank/a").Snaps[1] {
		t.Error("s2 differs from source")
	}

	// -F with a base the target never had still fails.
	tw.Create("backup/b", "filesystem")
	tw.Snap("backup/b", "x")
	_, rerr = replicate(src, dst, stream.None, incSpec("s1", "s2", false), zfs.RecvSpec{Dataset: "backup/b", Args: []string{"-F"}})
	wantStderr(t, rerr, "cannot receive incremental stream: most recent snapshot of backup/b does not match incremental source")
}

func TestFakeResumeToken(t *testing.T) {
	sw, tw, src, dst := pair()
	calls := 0
	dst.Faults.Receive = func(zfs.RecvSpec) error {
		calls++
		if calls == 1 {
			return zfs.ErrPartialState
		}
		return nil
	}
	recv := zfs.RecvSpec{Dataset: "backup/a", Args: []string{"-s"}}
	_, rerr := replicate(src, dst, stream.None, fullSpec("s1"), recv)
	if !errors.Is(rerr, zfs.ErrPartialState) {
		t.Fatalf("receive err = %v, want ErrPartialState", rerr)
	}
	got := tw.Dataset("backup/a")
	if got == nil || got.ResumeToken != "tok-1" || len(got.Snaps) != 0 {
		t.Fatalf("after interrupted receive: %+v", got)
	}
	props, err := dst.GetProps(ctx, "backup/a", "receive_resume_token")
	if err != nil || props["receive_resume_token"] != "tok-1" {
		t.Errorf("GetProps token = %v, %v", props, err)
	}

	// A plain receive is refused while the token exists.
	_, rerr = replicate(src, dst, stream.None, fullSpec("s1"), zfs.RecvSpec{Dataset: "backup/a"})
	wantStderr(t, rerr, "cannot receive new filesystem stream: destination backup/a contains partially-complete state from \"zfs receive -s\".")
	if !errors.Is(rerr, zfs.ErrPartialState) {
		t.Errorf("err = %v, want ErrPartialState", rerr)
	}

	serr, rerr := replicate(src, dst, stream.None, zfs.SendSpec{Dataset: "tank/a", ResumeToken: "tok-1"}, recv)
	if serr != nil || rerr != nil {
		t.Fatalf("resume: send=%v recv=%v", serr, rerr)
	}
	if got.ResumeToken != "" || !reflect.DeepEqual(snapNames(got), []string{"s1"}) {
		t.Errorf("after resume: token=%q snaps=%v", got.ResumeToken, snapNames(got))
	}
	if got.Snaps[0] != sw.Dataset("tank/a").Snaps[0] {
		t.Error("resumed snapshot differs from source")
	}
	if props, _ := dst.GetProps(ctx, "backup/a", "receive_resume_token"); props["receive_resume_token"] != "-" {
		t.Errorf("token prop after resume = %q, want -", props["receive_resume_token"])
	}
}

func TestFakeResumeIncremental(t *testing.T) {
	_, tw, src, dst := pair()
	mustReplicate(t, src, dst, fullSpec("s1"), zfs.RecvSpec{Dataset: "backup/a"})
	fail := true
	dst.Faults.Receive = func(zfs.RecvSpec) error {
		if fail {
			fail = false
			return errors.New("connection reset")
		}
		return nil
	}
	recv := zfs.RecvSpec{Dataset: "backup/a", Args: []string{"-s"}}
	_, rerr := replicate(src, dst, stream.None, incSpec("s1", "s3", true), recv)
	if rerr == nil {
		t.Fatal("want error")
	}
	got := tw.Dataset("backup/a")
	if got.ResumeToken != "tok-1" || len(got.Snaps) != 1 {
		t.Fatalf("after interrupted incremental: token=%q snaps=%v", got.ResumeToken, snapNames(got))
	}
	// Second interruption would be tok-2; here the resume succeeds.
	serr, rerr := replicate(src, dst, stream.None, zfs.SendSpec{Dataset: "tank/a", ResumeToken: "tok-1"}, recv)
	if serr != nil || rerr != nil {
		t.Fatalf("resume: %v %v", serr, rerr)
	}
	if got.ResumeToken != "" || !reflect.DeepEqual(snapNames(got), []string{"s1", "s2", "s3"}) {
		t.Errorf("after resume: token=%q snaps=%v", got.ResumeToken, snapNames(got))
	}
}

func TestFakeResumeSourceGone(t *testing.T) {
	_, _, src, dst := pair()
	dst.Faults.Receive = func(zfs.RecvSpec) error { return errors.New("boom") }
	_, _ = replicate(src, dst, stream.None, fullSpec("s2"), zfs.RecvSpec{Dataset: "backup/a", Args: []string{"-s"}})
	if err := src.Destroy(ctx, "tank/a", []string{"s2"}); err != nil {
		t.Fatal(err)
	}
	err := src.Send(ctx, zfs.SendSpec{Dataset: "tank/a", ResumeToken: "tok-1"}, endpoint.SendOpts{Codec: stream.None}, io.Discard)
	wantStderr(t, err, "cannot resume send: 'tank/a@s2' used in the initial send no longer exists")
	if !errors.Is(err, zfs.ErrResumeSourceGone) {
		t.Errorf("err = %v, want ErrResumeSourceGone", err)
	}
}

func TestFakeAbortResume(t *testing.T) {
	_, tw, src, dst := pair()
	dst.Faults.Receive = func(zfs.RecvSpec) error { return errors.New("boom") }
	recv := zfs.RecvSpec{Dataset: "backup/a", Args: []string{"-s"}}
	_, _ = replicate(src, dst, stream.None, fullSpec("s1"), recv)
	if tw.Dataset("backup/a") == nil {
		t.Fatal("partial dataset not created")
	}
	if err := dst.AbortResume(ctx, "backup/a"); err != nil {
		t.Fatal(err)
	}
	if tw.Dataset("backup/a") != nil {
		t.Error("dataset created only by the partial receive must be removed")
	}

	// An existing dataset keeps its snapshots and loses only the token.
	dst.Faults.Receive = nil
	mustReplicate(t, src, dst, fullSpec("s1"), zfs.RecvSpec{Dataset: "backup/a"})
	dst.Faults.Receive = func(zfs.RecvSpec) error { return errors.New("boom") }
	_, _ = replicate(src, dst, stream.None, incSpec("s1", "s2", false), recv)
	got := tw.Dataset("backup/a")
	if got.ResumeToken != "tok-2" {
		t.Fatalf("token = %q, want tok-2 (counter per target world)", got.ResumeToken)
	}
	if err := dst.AbortResume(ctx, "backup/a"); err != nil {
		t.Fatal(err)
	}
	if got.ResumeToken != "" || len(got.Snaps) != 1 {
		t.Errorf("after abort: token=%q snaps=%v", got.ResumeToken, snapNames(got))
	}
}

func TestFakeNoTokenWithoutS(t *testing.T) {
	_, tw, src, dst := pair()
	dst.Faults.Receive = func(zfs.RecvSpec) error { return errors.New("boom") }
	_, rerr := replicate(src, dst, stream.None, fullSpec("s1"), zfs.RecvSpec{Dataset: "backup/a"})
	if rerr == nil || rerr.Error() != "boom" {
		t.Fatalf("err = %v, want boom", rerr)
	}
	if tw.Dataset("backup/a") != nil {
		t.Error("nothing must be applied")
	}
}

func TestFakeBookmarkSend(t *testing.T) {
	sw, tw, src, dst := pair()
	mustReplicate(t, src, dst, fullSpec("s1"), zfs.RecvSpec{Dataset: "backup/a"})
	sw.MarkBookmark("tank/a", "s1", "bm1")
	if err := src.Destroy(ctx, "tank/a", []string{"s1"}); err != nil {
		t.Fatal(err)
	}
	spec := zfs.SendSpec{Dataset: "tank/a", To: "s3", FromBookmark: "bm1"}
	mustReplicate(t, src, dst, spec, zfs.RecvSpec{Dataset: "backup/a"})
	if g := snapNames(tw.Dataset("backup/a")); !reflect.DeepEqual(g, []string{"s1", "s3"}) {
		t.Errorf("snaps = %v", g)
	}
	if c := src.Calls(); c[len(c)-1] != "Send tank/a@s3" {
		t.Errorf("calls = %v", c)
	}
}

func TestFakeBookmarkMismatch(t *testing.T) {
	sw, tw, src, dst := pair()
	mustReplicate(t, src, dst, fullSpec("s1"), zfs.RecvSpec{Dataset: "backup/a"})
	sw.MarkBookmark("tank/a", "s2", "bm2") // target newest is s1
	_, rerr := replicate(src, dst, stream.None, zfs.SendSpec{Dataset: "tank/a", To: "s3", FromBookmark: "bm2"}, zfs.RecvSpec{Dataset: "backup/a"})
	wantStderr(t, rerr, "cannot receive incremental stream: most recent snapshot of backup/a does not match incremental source")
	if len(tw.Dataset("backup/a").Snaps) != 1 {
		t.Error("target changed")
	}
}

func TestFakeReceiveMissingParent(t *testing.T) {
	_, _, src, dst := pair()
	_, rerr := replicate(src, dst, stream.None, fullSpec("s1"), zfs.RecvSpec{Dataset: "backup/x/b"})
	wantStderr(t, rerr, "cannot open 'backup/x': dataset does not exist")
	if !errors.Is(rerr, zfs.ErrNoDataset) {
		t.Errorf("err = %v, want ErrNoDataset", rerr)
	}
}

func TestFakeIncrementalIntoMissingTarget(t *testing.T) {
	_, _, src, dst := pair()
	_, rerr := replicate(src, dst, stream.None, incSpec("s1", "s2", false), zfs.RecvSpec{Dataset: "backup/a"})
	wantStderr(t, rerr, "cannot open 'backup/a': dataset does not exist")
}

func TestFakeSnapshotNameConflict(t *testing.T) {
	_, tw, src, dst := pair()
	mustReplicate(t, src, dst, fullSpec("s1"), zfs.RecvSpec{Dataset: "backup/a"})
	tw.Snap("backup/a", "s2") // same name as a source snapshot, different guid
	// Base s1 is no longer the newest, so roll back via -F first: the
	// conflicting s2 is removed by the rollback and the receive succeeds...
	mustReplicate(t, src, dst, incSpec("s1", "s2", false), zfs.RecvSpec{Dataset: "backup/a", Args: []string{"-F"}})

	// ...but a same-named older snapshot with another guid is a conflict.
	tw2 := fake.NewWorld()
	tw2.Create("backup", "filesystem")
	dst2 := fake.New(tw2, endpoint.HostInfo{})
	mustReplicate(t, src, dst2, fullSpec("s1"), zfs.RecvSpec{Dataset: "backup/a"})
	d := tw2.Dataset("backup/a")
	d.Snaps = append([]zfs.Snapshot{{Name: "s3", GUID: "999999999", CreateTxg: 1, Creation: 1}}, d.Snaps...)
	_, rerr := replicate(src, dst2, stream.None, incSpec("s1", "s3", false), zfs.RecvSpec{Dataset: "backup/a"})
	wantStderr(t, rerr, "cannot restore to backup/a@s3: destination already exists")
	var de *zfs.DestinationExistsError
	if !errors.As(rerr, &de) || de.Name != "backup/a@s3" {
		t.Errorf("err = %v, want DestinationExistsError backup/a@s3", rerr)
	}
}

func TestFakeCloneStream(t *testing.T) {
	sw, tw, src, dst := pair()
	// Origin lives on the target as the replicated tank/a@s1.
	mustReplicate(t, src, dst, fullSpec("s1"), zfs.RecvSpec{Dataset: "backup/a"})
	sw.Create("tank/c", "filesystem")
	sw.Dataset("tank/c").Origin = "tank/a@s1"
	sw.Snap("tank/c", "c1")

	spec := zfs.SendSpec{Dataset: "tank/c", To: "c1", FromOrigin: "tank/a@s1"}
	mustReplicate(t, src, dst, spec, zfs.RecvSpec{Dataset: "backup/c"})
	c := tw.Dataset("backup/c")
	if c == nil || c.Origin != "backup/a@s1" || !reflect.DeepEqual(snapNames(c), []string{"c1"}) {
		t.Fatalf("clone = %+v", c)
	}

	// The origin snapshot is unknown to a fresh target.
	tw2 := fake.NewWorld()
	tw2.Create("backup", "filesystem")
	dst2 := fake.New(tw2, endpoint.HostInfo{})
	_, rerr := replicate(src, dst2, stream.None, spec, zfs.RecvSpec{Dataset: "backup/c"})
	if rerr == nil || tw2.Dataset("backup/c") != nil {
		t.Errorf("clone without origin: err=%v ds=%v", rerr, tw2.Dataset("backup/c"))
	}
	// Existing target is refused.
	_, rerr = replicate(src, dst, stream.None, spec, zfs.RecvSpec{Dataset: "backup/c"})
	wantStderr(t, rerr, "cannot receive new filesystem stream: destination 'backup/c' exists\nmust specify -F to overwrite it")
}

func TestFakeBlockSendCancel(t *testing.T) {
	_, _, src, _ := pair()
	src.Faults.BlockSend = true
	c, cancel := context.WithCancel(ctx)
	errc := make(chan error, 1)
	go func() {
		errc <- src.Send(c, fullSpec("s1"), endpoint.SendOpts{Codec: stream.None}, io.Discard)
	}()
	select {
	case err := <-errc:
		t.Fatalf("Send returned early: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Send did not return after cancel")
	}
}

func TestFakeSendFaultAndMissing(t *testing.T) {
	_, _, src, dst := pair()
	want := errors.New("send boom")
	src.Faults.Send = func(s zfs.SendSpec) error {
		if s.To != "s1" {
			t.Errorf("fault got %+v", s)
		}
		return want
	}
	serr, rerr := replicate(src, dst, stream.None, fullSpec("s1"), zfs.RecvSpec{Dataset: "backup/a"})
	if !errors.Is(serr, want) || rerr == nil {
		t.Errorf("send=%v recv=%v", serr, rerr)
	}
	src.Faults.Send = nil
	err := src.Send(ctx, zfs.SendSpec{Dataset: "tank/a", To: "nope"}, endpoint.SendOpts{Codec: stream.None}, io.Discard)
	wantStderr(t, err, "cannot open 'tank/a@nope': dataset does not exist")
	err = src.Send(ctx, zfs.SendSpec{Dataset: "tank/zzz", To: "s1"}, endpoint.SendOpts{Codec: stream.None}, io.Discard)
	wantStderr(t, err, "cannot open 'tank/zzz': dataset does not exist")
}

func TestFakeSendProgress(t *testing.T) {
	_, _, src, _ := pair()
	var last int64
	err := src.Send(ctx, fullSpec("s1"), endpoint.SendOpts{Codec: stream.Gzip, Progress: func(n int64) { last = n }}, io.Discard)
	if err != nil || last <= 0 {
		t.Errorf("err=%v progress=%d", err, last)
	}
}

func TestFakeSnapshotAtomic(t *testing.T) {
	sw, _, src, _ := pair()
	sw.Create("tank/b", "filesystem")
	err := src.Snapshot(ctx, []string{"tank/a@n1", "tank/b@n1", "tank/missing@n1"})
	wantStderr(t, err, "cannot open 'tank/missing': dataset does not exist")
	if len(sw.Dataset("tank/a").Snaps) != 3 || len(sw.Dataset("tank/b").Snaps) != 0 {
		t.Error("partial snapshot creation")
	}
	if err := src.Snapshot(ctx, []string{"tank/a@n1", "tank/b@n1"}); err != nil {
		t.Fatal(err)
	}
	a, b := sw.Dataset("tank/a").Snaps, sw.Dataset("tank/b").Snaps
	if len(a) != 4 || len(b) != 1 {
		t.Fatalf("snaps a=%v b=%v", a, b)
	}
	if a[3].GUID == b[0].GUID || a[3].CreateTxg <= a[2].CreateTxg || a[3].Creation <= a[2].Creation {
		t.Errorf("guid/txg/creation not unique/increasing: %+v %+v %+v", a[2], a[3], b[0])
	}
	if a[3].Creation%1000 != 0 {
		t.Errorf("creation %d is not seconds*1000", a[3].Creation)
	}
	if _, err := strconv.ParseUint(a[3].GUID, 10, 64); err != nil {
		t.Errorf("guid %q not decimal", a[3].GUID)
	}
	if c := src.Calls(); c[0] != "Snapshot tank/a@n1,tank/b@n1,tank/missing@n1" || c[1] != "Snapshot tank/a@n1,tank/b@n1" {
		t.Errorf("calls = %v", c)
	}
}

func TestFakeDestroyAndHolds(t *testing.T) {
	sw, _, src, _ := pair()
	if err := src.Hold(ctx, "keep", "tank/a@s2"); err != nil {
		t.Fatal(err)
	}
	if got := sw.Dataset("tank/a").Holds["s2"]; !reflect.DeepEqual(got, []string{"keep"}) {
		t.Fatalf("holds = %v", got)
	}
	err := src.Destroy(ctx, "tank/a", []string{"s1", "s2"})
	wantStderr(t, err, "cannot destroy snapshot tank/a@s2: dataset is busy")
	if len(sw.Dataset("tank/a").Snaps) != 3 {
		t.Error("destroy not atomic")
	}
	if err := src.Release(ctx, "keep", "tank/a@s2"); err != nil {
		t.Fatal(err)
	}
	if err := src.Destroy(ctx, "tank/a", []string{"s1", "s2"}); err != nil {
		t.Fatal(err)
	}
	if g := snapNames(sw.Dataset("tank/a")); !reflect.DeepEqual(g, []string{"s3"}) {
		t.Errorf("snaps = %v", g)
	}
	err = src.Destroy(ctx, "nope/x", []string{"s1"})
	wantStderr(t, err, "cannot open 'nope/x': dataset does not exist")
	want := []string{"Hold keep tank/a@s2", "Destroy tank/a@s1,s2", "Release keep tank/a@s2", "Destroy tank/a@s1,s2", "Destroy nope/x@s1"}
	if c := src.Calls(); !reflect.DeepEqual(c, want) {
		t.Errorf("calls = %v, want %v", c, want)
	}
}

func TestFakeDestroyDataset(t *testing.T) {
	sw, _, src, _ := pair()
	sw.Create("tank/a/child", "filesystem")
	if err := src.DestroyDataset(ctx, "tank/a", false); err == nil {
		t.Error("non-recursive destroy of a dataset with children must fail")
	}
	if err := src.DestroyDataset(ctx, "tank/a", true); err != nil {
		t.Fatal(err)
	}
	if sw.Dataset("tank/a") != nil || sw.Dataset("tank/a/child") != nil {
		t.Error("datasets remain")
	}
	err := src.DestroyDataset(ctx, "tank/a", false)
	wantStderr(t, err, "cannot open 'tank/a': dataset does not exist")
}

func TestFakeBookmarkAndListing(t *testing.T) {
	sw, _, src, _ := pair()
	if err := src.Bookmark(ctx, "tank/a@s2", "tank/a#b2"); err != nil {
		t.Fatal(err)
	}
	bms, err := src.ListBookmarks(ctx, "tank/a", false)
	if err != nil {
		t.Fatal(err)
	}
	s2 := sw.Dataset("tank/a").Snaps[1]
	if got := bms["tank/a"]; len(got) != 1 || got[0].Name != "b2" || got[0].GUID != s2.GUID || got[0].CreateTxg != s2.CreateTxg {
		t.Errorf("bookmarks = %+v", got)
	}
	sw.Create("tank", "filesystem")
	snaps, err := src.ListSnapshots(ctx, "tank", true)
	if err != nil || len(snaps["tank/a"]) != 3 {
		t.Errorf("snapshots = %v, %v", snaps, err)
	}
	if _, err := src.ListSnapshots(ctx, "tank/zzz", true); !errors.Is(err, zfs.ErrNoDataset) {
		t.Errorf("err = %v", err)
	}
	if _, err := src.ListBookmarks(ctx, "tank/zzz", false); !errors.Is(err, zfs.ErrNoDataset) {
		t.Errorf("err = %v", err)
	}
}

func TestFakeTreePropsExists(t *testing.T) {
	sw, _, src, _ := pair()
	sw.Create("tank", "filesystem")
	sw.Create("tank/a/child", "filesystem")
	sw.Create("tank/vol", "volume")
	sw.SetProp("tank/a", "syncoid:sync", "true")
	sw.SetProp("tank/a", "compression", "lz4")
	sw.Dataset("tank/a").Used = 12345
	sw.Dataset("tank/a").ResumeToken = "tok-9"

	tree, err := src.ListTree(ctx, "tank", true)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, d := range tree {
		names = append(names, d.Name)
	}
	if want := []string{"tank", "tank/a", "tank/a/child", "tank/vol"}; !reflect.DeepEqual(names, want) {
		t.Errorf("tree = %v, want %v", names, want)
	}
	if tree[1].SyncProp != "true" || tree[1].Used != 12345 || tree[1].ResumeToken != "tok-9" || tree[3].Type != "volume" {
		t.Errorf("tree entries = %+v", tree)
	}
	one, err := src.ListTree(ctx, "tank/a", false)
	if err != nil || len(one) != 1 || one[0].Name != "tank/a" {
		t.Errorf("non-recursive = %v, %v", one, err)
	}
	if _, err := src.ListTree(ctx, "nope", true); !errors.Is(err, zfs.ErrNoDataset) {
		t.Errorf("err = %v", err)
	}

	p, err := src.GetProps(ctx, "tank/a", "name", "type", "used", "referenced", "recordsize", "receive_resume_token", "written@s1", "written@s3", "compression", "nothing")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"name": "tank/a", "type": "filesystem", "used": "12345", "referenced": "12345", "recordsize": "131072",
		"receive_resume_token": "tok-9", "written@s1": strconv.Itoa(2 << 20), "written@s3": "0",
		"compression": "lz4", "nothing": "-",
	}
	if !reflect.DeepEqual(p, want) {
		t.Errorf("props = %v\nwant %v", p, want)
	}
	if _, err := src.GetProps(ctx, "nope", "name"); !errors.Is(err, zfs.ErrNoDataset) {
		t.Errorf("err = %v", err)
	}

	lp, err := src.LocalProps(ctx, "tank/a")
	if err != nil || !reflect.DeepEqual(lp, map[string]string{"syncoid:sync": "true", "compression": "lz4"}) {
		t.Errorf("local props = %v, %v", lp, err)
	}
	lp["x"] = "y"
	if _, ok := sw.Dataset("tank/a").Props["x"]; ok {
		t.Error("LocalProps must return a copy")
	}

	if ok, err := src.Exists(ctx, "tank/a"); !ok || err != nil {
		t.Errorf("Exists = %v, %v", ok, err)
	}
	if ok, err := src.Exists(ctx, "tank/q"); ok || err != nil {
		t.Errorf("Exists missing = %v, %v", ok, err)
	}
}

func TestFakeEstimateInfoAllowBusyLabel(t *testing.T) {
	_, _, src, dst := pair()
	n, err := src.SendEstimate(ctx, incSpec("s1", "s3", true))
	if err != nil || n != 2<<20 {
		t.Errorf("estimate -I = %d, %v", n, err)
	}
	if n, _ := src.SendEstimate(ctx, incSpec("s1", "s3", false)); n != 1<<20 {
		t.Errorf("estimate -i = %d", n)
	}
	if n, _ := src.SendEstimate(ctx, zfs.SendSpec{Dataset: "tank/a", To: "gone"}); n != 0 {
		t.Errorf("estimate of missing = %d, want 0", n)
	}

	info, err := dst.Info(ctx)
	if err != nil || info.User != "backup" {
		t.Errorf("info = %+v, %v", info, err)
	}
	if dst.Label() != "" {
		t.Errorf("label = %q", dst.Label())
	}
	dst.LabelValue = "backup@host"
	if dst.Label() != "backup@host" {
		t.Errorf("label = %q", dst.Label())
	}

	dst.Faults.Allow = map[string]string{"backup/a": "send,snapshot"}
	if s, _ := dst.Allow(ctx, "backup/a"); s != "send,snapshot" {
		t.Errorf("allow = %q", s)
	}
	if s, _ := dst.Allow(ctx, "other"); s != "" {
		t.Errorf("allow = %q", s)
	}
	dst.Faults.Busy = map[string]bool{"backup/a": true}
	busy, err := dst.ReceiveBusy(ctx, []string{"backup/a", "backup/b"})
	if err != nil || !busy["backup/a"] || busy["backup/b"] {
		t.Errorf("busy = %v, %v", busy, err)
	}
}

func TestFakeCallsReceiveAndCopy(t *testing.T) {
	_, _, src, dst := pair()
	mustReplicate(t, src, dst, fullSpec("s1"), zfs.RecvSpec{Dataset: "backup/a"})
	if c := src.Calls(); !reflect.DeepEqual(c, []string{"Send tank/a@s1"}) {
		t.Errorf("src calls = %v", c)
	}
	c := dst.Calls()
	if !reflect.DeepEqual(c, []string{"Receive backup/a"}) {
		t.Errorf("dst calls = %v", c)
	}
	c[0] = "mutated"
	if dst.Calls()[0] != "Receive backup/a" {
		t.Error("Calls must return a copy")
	}
}

func TestFakeConcurrent(t *testing.T) {
	_, _, src, dst := pair()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = replicate(src, dst, stream.None, fullSpec("s1"), zfs.RecvSpec{Dataset: "backup/a" + strconv.Itoa(i)})
			_, _ = src.ListSnapshots(ctx, "tank", true)
			_ = src.Snapshot(ctx, []string{"tank/a@c" + strconv.Itoa(i)})
		}()
	}
	wg.Wait()
	if !strings.HasPrefix(dst.Calls()[0], "Receive ") {
		t.Errorf("calls = %v", dst.Calls())
	}
}

func TestFakePartialStateMessageIncremental(t *testing.T) {
	_, _, src, dst := pair()
	mustReplicate(t, src, dst, fullSpec("s1"), zfs.RecvSpec{Dataset: "backup/a"})
	dst.Faults.Receive = func(zfs.RecvSpec) error { return errors.New("boom") }
	_, _ = replicate(src, dst, stream.None, incSpec("s1", "s2", false), zfs.RecvSpec{Dataset: "backup/a", Args: []string{"-s"}})
	dst.Faults.Receive = nil
	_, rerr := replicate(src, dst, stream.None, incSpec("s1", "s2", false), zfs.RecvSpec{Dataset: "backup/a"})
	wantStderr(t, rerr, "cannot receive incremental stream: destination backup/a contains partially-complete state from \"zfs receive -s\".")
	if !errors.Is(rerr, zfs.ErrPartialState) {
		t.Errorf("err = %v, want ErrPartialState", rerr)
	}
}

func TestFakeReceiveAdvancesClock(t *testing.T) {
	sw, tw, src, dst := pair()
	// The source clock runs far ahead of the (empty) target's.
	for i := 0; i < 50; i++ {
		sw.Snap("tank/a", "extra"+strconv.Itoa(i))
	}
	mustReplicate(t, src, dst, fullSpec("extra49"), zfs.RecvSpec{Dataset: "backup/a"})
	recv := tw.Dataset("backup/a").Snaps[0]

	local := tw.Snap("backup/a", "local")
	if local.CreateTxg <= recv.CreateTxg || local.Creation <= recv.Creation {
		t.Errorf("World.Snap %+v not after received %+v", local, recv)
	}
	if err := dst.Snapshot(ctx, []string{"backup/a@local2"}); err != nil {
		t.Fatal(err)
	}
	l2 := tw.Dataset("backup/a").Snaps[2]
	if l2.CreateTxg <= local.CreateTxg || l2.Creation <= local.Creation {
		t.Errorf("Endpoint.Snapshot %+v not after %+v", l2, local)
	}
}

func TestFakeSnapshotWithoutAt(t *testing.T) {
	_, _, src, _ := pair()
	err := src.Snapshot(ctx, []string{"tank/a"})
	wantStderr(t, err, "cannot create snapshot 'tank/a': missing '@' delimiter in snapshot name")
}

// interruptedTargets returns two target endpoints whose full receive of s1
// from src was interrupted, both holding "tok-1".
func interruptedTargets(t *testing.T, src *fake.Endpoint) (tw1, tw2 *fake.World, dst1, dst2 *fake.Endpoint) {
	t.Helper()
	mk := func() (*fake.World, *fake.Endpoint) {
		w := fake.NewWorld()
		w.Create("backup", "filesystem")
		e := fake.New(w, endpoint.HostInfo{})
		e.Faults.Receive = func(zfs.RecvSpec) error { return errors.New("boom") }
		_, _ = replicate(src, e, stream.None, fullSpec("s1"), zfs.RecvSpec{Dataset: "backup/a", Args: []string{"-s"}})
		if w.Dataset("backup/a").ResumeToken != "tok-1" {
			t.Fatalf("token = %q", w.Dataset("backup/a").ResumeToken)
		}
		e.Faults.Receive = nil
		return w, e
	}
	tw1, dst1 = mk()
	tw2, dst2 = mk()
	return tw1, tw2, dst1, dst2
}

func TestFakeResumeTokenAmbiguous(t *testing.T) {
	_, _, src, _ := pair()
	_, _, _, _ = interruptedTargets(t, src)
	err := src.Send(ctx, zfs.SendSpec{Dataset: "tank/a", ResumeToken: "tok-1"}, endpoint.SendOpts{Codec: stream.None}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Errorf("err = %v, want ambiguous resume token error", err)
	}
}

func TestFakeResumeRegistryCleanup(t *testing.T) {
	_, _, src, _ := pair()
	tw1, _, dst1, dst2 := interruptedTargets(t, src)
	send := zfs.SendSpec{Dataset: "tank/a", ResumeToken: "tok-1"}
	recv := zfs.RecvSpec{Dataset: "backup/a", Args: []string{"-s"}}

	// Destroying one target's dataset unregisters its token: no longer ambiguous.
	if err := dst1.DestroyDataset(ctx, "backup/a", false); err != nil {
		t.Fatal(err)
	}
	if tw1.Dataset("backup/a") != nil {
		t.Fatal("dataset remains")
	}
	if serr, rerr := replicate(src, dst2, stream.None, send, recv); serr != nil || rerr != nil {
		t.Fatalf("resume after cleanup: send=%v recv=%v", serr, rerr)
	}

	// A successful resume also unregisters: the token is now unknown.
	err := src.Send(ctx, send, endpoint.SendOpts{Codec: stream.None}, io.Discard)
	wantStderr(t, err, "cannot resume send: invalid resume token 'tok-1'")
}
