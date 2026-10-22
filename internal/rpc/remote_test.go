package rpc_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/piotaixr/syngoid/internal/agent"
	"github.com/piotaixr/syngoid/internal/agent/agenttest"
	"github.com/piotaixr/syngoid/internal/buildinfo"
	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/endpoint/fake"
	"github.com/piotaixr/syngoid/internal/logx"
	"github.com/piotaixr/syngoid/internal/rpc"
	"github.com/piotaixr/syngoid/internal/zfs"
)

const (
	label = "backup@nas"
	wait  = 5 * time.Second
)

var hostInfo = endpoint.HostInfo{
	OS: "fakeos", Arch: "fakearch", User: "backup", UID: 1234,
	Groups: []string{"backup", "zfs"}, ZFSVersion: "zfs-2.2.0",
}

// testEP wraps a fake endpoint for the agent side. Exists on "block/ds"
// waits for its context, on "deaf/ds" ignores cancellation until release is
// closed, on "tank/x" fails like zfs on a missing dataset, on "partial/ds"
// fails with partial receive state and on "log/ds" logs through the agent's
// logger; everything else is the fake's.
type testEP struct {
	*fake.Endpoint
	log *slog.Logger

	started   chan struct{} // receives once per blocked Exists or started Send
	cancelled chan struct{} // receives once per "block/ds" whose ctx ended
	release   chan struct{} // closed to let "deaf/ds" return
	sendCtx   chan error    // receives ctx.Err() as each Send returns
	recvCtx   chan error    // receives ctx.Err() as each Receive returns
}

func newTestEP(w *fake.World) *testEP {
	return &testEP{
		Endpoint:  fake.New(w, hostInfo),
		started:   make(chan struct{}, 8),
		cancelled: make(chan struct{}, 8),
		release:   make(chan struct{}),
		sendCtx:   make(chan error, 8),
		recvCtx:   make(chan error, 8),
	}
}

func (e *testEP) Exists(ctx context.Context, ds string) (bool, error) {
	switch ds {
	case "block/ds":
		e.started <- struct{}{}
		<-ctx.Done()
		e.cancelled <- struct{}{}
		return false, ctx.Err()
	case "deaf/ds":
		e.started <- struct{}{}
		<-e.release
		return true, nil
	case "tank/x":
		stderr := "cannot open 'tank/x': dataset does not exist"
		return false, &zfs.CommandError{Argv: []string{"zfs", "list", ds}, ExitCode: 1, Stderr: stderr, Kind: zfs.Classify(stderr)}
	case "partial/ds":
		stderr := "cannot receive incremental stream: destination partial/ds contains partially-complete state from \"zfs receive -s\"."
		return false, &zfs.CommandError{Argv: []string{"zfs", "receive", ds}, ExitCode: 1, Stderr: stderr, Kind: zfs.Classify(stderr)}
	case "log/ds":
		e.log.Debug("zfs receive -s -F tank/b")
		e.log.Warn("careful")
		e.log.Error("broken")
		return true, nil
	}
	return e.Endpoint.Exists(ctx, ds)
}

// startTest serves ep through an agent and returns the connected Remote
// and the initiator's end of the pipe.
func startTest(t *testing.T, ep *testEP, debug bool, log *slog.Logger) (*rpc.Remote, net.Conn) {
	t.Helper()
	return agenttest.StartConn(t, func(_ bool, l *slog.Logger) endpoint.Endpoint {
		ep.log = l
		return ep
	}, label, debug, log)
}

// populated returns a World with a small tree, snapshots, bookmarks and
// properties.
func populated() *fake.World {
	w := fake.NewWorld()
	w.Create("tank", "")
	w.Create("tank/a", "")
	w.Create("tank/a/b", "")
	w.Create("tank/vol", "volume")
	for _, s := range []string{"s1", "s2", "s3"} {
		w.Snap("tank/a", s)
	}
	w.Snap("tank/a/b", "s1")
	w.MarkBookmark("tank/a", "s1", "s1")
	w.SetProp("tank/a", "syncoid:sync", "true")
	w.SetProp("tank/a", "compression", "lz4")
	return w
}

func TestRemoteQueriesMatchFake(t *testing.T) {
	ctx := context.Background()
	f := fake.New(populated(), hostInfo)
	f.Faults.Busy = map[string]bool{"tank/a": true}
	f.Faults.Allow = map[string]string{"tank/a": "---- Permissions on tank/a ----\n"}
	r := agenttest.Start(t, f, label, false, nil)

	if got := r.Label(); got != label {
		t.Errorf("Label() = %q, want %q", got, label)
	}
	same := func(name string, got, want any, gerr, werr error) {
		t.Helper()
		if gerr != nil || werr != nil {
			t.Fatalf("%s: remote err %v, direct err %v", name, gerr, werr)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s:\nremote %#v\ndirect %#v", name, got, want)
		}
	}

	for _, rec := range []bool{false, true} {
		g1, e1 := r.ListTree(ctx, "tank", rec)
		w1, f1 := f.ListTree(ctx, "tank", rec)
		same(fmt.Sprint("ListTree ", rec), g1, w1, e1, f1)
		g2, e2 := r.ListSnapshots(ctx, "tank", rec)
		w2, f2 := f.ListSnapshots(ctx, "tank", rec)
		same(fmt.Sprint("ListSnapshots ", rec), g2, w2, e2, f2)
		g3, e3 := r.ListBookmarks(ctx, "tank", rec)
		w3, f3 := f.ListBookmarks(ctx, "tank", rec)
		same(fmt.Sprint("ListBookmarks ", rec), g3, w3, e3, f3)
	}
	props := []string{"name", "type", "compression", "syncoid:sync", "receive_resume_token", "recordsize"}
	g4, e4 := r.GetProps(ctx, "tank/a", props...)
	w4, f4 := f.GetProps(ctx, "tank/a", props...)
	same("GetProps", g4, w4, e4, f4)
	for _, ds := range []string{"tank/a", "tank"} {
		g5, e5 := r.LocalProps(ctx, ds)
		w5, f5 := f.LocalProps(ctx, ds)
		same("LocalProps "+ds, g5, w5, e5, f5)
	}
	for _, ds := range []string{"tank/a", "tank/nope"} {
		g6, e6 := r.Exists(ctx, ds)
		w6, f6 := f.Exists(ctx, ds)
		same("Exists "+ds, g6, w6, e6, f6)
	}
	for _, ds := range []string{"tank/a", "tank"} {
		g7, e7 := r.Allow(ctx, ds)
		w7, f7 := f.Allow(ctx, ds)
		same("Allow "+ds, g7, w7, e7, f7)
	}
	busyOn := []string{"tank/a", "tank/a/b"}
	g8, e8 := r.ReceiveBusy(ctx, busyOn)
	w8, f8 := f.ReceiveBusy(ctx, busyOn)
	same("ReceiveBusy", g8, w8, e8, f8)
	for _, spec := range []zfs.SendSpec{
		{Dataset: "tank/a", To: "s3", FromSnap: "s1", Intermediates: true},
		{Dataset: "tank/a", To: "s1"},
		{Dataset: "tank/a", To: "missing"},
	} {
		g9, e9 := r.SendEstimate(ctx, spec)
		w9, f9 := f.SendEstimate(ctx, spec)
		same(fmt.Sprintf("SendEstimate %+v", spec), g9, w9, e9, f9)
	}
	g10, e10 := r.Info(ctx)
	w10, f10 := f.Info(ctx)
	same("Info", g10, w10, e10, f10)
}

func TestRemoteConcurrentCalls(t *testing.T) {
	w := fake.NewWorld()
	const n = 32
	for i := range n {
		w.Create(fmt.Sprintf("tank%d", i), "")
	}
	r := agenttest.Start(t, fake.New(w, hostInfo), label, false, nil)

	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := range n {
		wg.Go(func() {
			ds := fmt.Sprintf("tank%d", i)
			got, err := r.GetProps(context.Background(), ds, "name")
			switch {
			case err != nil:
				errs <- err
			case got["name"] != ds:
				errs <- fmt.Errorf("GetProps(%s) = %v", ds, got)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// state is a comparable summary of a World: GUIDs differ between Worlds,
// so only names, holds and resume tokens are kept.
func state(w *fake.World, names []string) []string {
	var out []string
	for _, name := range names {
		d := w.Dataset(name)
		if d == nil {
			out = append(out, name+" absent")
			continue
		}
		var snaps, bms, holds []string
		for _, s := range d.Snaps {
			snaps = append(snaps, s.Name)
		}
		for _, b := range d.Bookmarks {
			bms = append(bms, b.Name)
		}
		for s, tags := range d.Holds {
			holds = append(holds, s+"="+strings.Join(tags, "+"))
		}
		sort.Strings(holds)
		out = append(out, fmt.Sprintf("%s snaps=%v bookmarks=%v holds=%v token=%q", name, snaps, bms, holds, d.ResumeToken))
	}
	return out
}

func TestRemoteMutations(t *testing.T) {
	ctx := context.Background()
	newWorld := func() *fake.World {
		w := populated()
		w.Create("tank/c", "")
		w.Dataset("tank/a/b").ResumeToken = "1-abc-def"
		return w
	}
	ops := func(ep endpoint.Endpoint) error {
		return errors.Join(
			ep.Snapshot(ctx, []string{"tank/a@s4", "tank/a/b@s4"}),
			ep.Hold(ctx, "syncoid_host", "tank/a@s4"),
			ep.Hold(ctx, "keep", "tank/a@s2"),
			ep.Release(ctx, "keep", "tank/a@s2"),
			ep.Bookmark(ctx, "tank/a@s4", "tank/a#s4"),
			ep.Destroy(ctx, "tank/a", []string{"s1", "s2"}),
			ep.DestroyDataset(ctx, "tank/c", false),
			ep.AbortResume(ctx, "tank/a/b"),
		)
	}
	names := []string{"tank", "tank/a", "tank/a/b", "tank/c", "tank/vol"}

	directWorld, remoteWorld := newWorld(), newWorld()
	before := state(remoteWorld, names)
	direct := fake.New(directWorld, hostInfo)
	if err := ops(direct); err != nil {
		t.Fatalf("direct: %v", err)
	}
	remoteFake := fake.New(remoteWorld, hostInfo)
	r := agenttest.Start(t, remoteFake, label, false, nil)
	if err := ops(r); err != nil {
		t.Fatalf("remote: %v", err)
	}

	got, want := state(remoteWorld, names), state(directWorld, names)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("remote World:\n%s\ndirect World:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if reflect.DeepEqual(got, before) {
		t.Error("mutations left the World unchanged")
	}
	if g, w := remoteFake.Calls(), direct.Calls(); !reflect.DeepEqual(g, w) {
		t.Errorf("calls:\nremote %q\ndirect %q", g, w)
	}
	if d := remoteWorld.Dataset("tank/a"); len(d.Holds["s4"]) != 1 || len(d.Bookmarks) != 2 {
		t.Errorf("tank/a holds %v bookmarks %v, want a hold on s4 and two bookmarks", d.Holds, d.Bookmarks)
	}
}

func TestRemoteErrorsClassified(t *testing.T) {
	ctx := context.Background()
	r, _ := startTest(t, newTestEP(populated()), false, nil)

	_, err := r.Exists(ctx, "tank/x")
	if !errors.Is(err, zfs.ErrNoDataset) {
		t.Errorf("Exists(tank/x) err = %v, want ErrNoDataset", err)
	}
	var ce *zfs.CommandError
	if !errors.As(err, &ce) {
		t.Fatalf("Exists(tank/x) err = %T %v, want *zfs.CommandError", err, err)
	}
	if want := "cannot open 'tank/x': dataset does not exist"; ce.Stderr != want || ce.ExitCode != 1 {
		t.Errorf("CommandError = %+v, want stderr %q exit 1", ce, want)
	}

	// The fake's own error for a missing dataset survives the trip too.
	if _, err := r.ListTree(ctx, "tank/x", false); !errors.Is(err, zfs.ErrNoDataset) {
		t.Errorf("ListTree(tank/x) err = %v, want ErrNoDataset", err)
	}

	if _, err := r.Exists(ctx, "partial/ds"); !errors.Is(err, zfs.ErrPartialState) {
		t.Errorf("Exists(partial/ds) err = %v, want ErrPartialState", err)
	}
}

func TestRemoteInvalidParams(t *testing.T) {
	r := agenttest.Start(t, fake.New(populated(), hostInfo), label, false, nil)
	_, err := r.Exists(context.Background(), "-x")
	var ie *rpc.InvalidError
	if !errors.As(err, &ie) {
		t.Fatalf("Exists(-x) err = %T %v, want *rpc.InvalidError", err, err)
	}
	if ie.Field != "dataset" || ie.Value != "-x" {
		t.Errorf("InvalidError = %+v, want dataset -x", ie)
	}
}

// callAsync runs Exists(ds) in the background and returns its error channel.
func callAsync(ctx context.Context, r *rpc.Remote, ds string) <-chan error {
	errc := make(chan error, 1)
	go func() {
		_, err := r.Exists(ctx, ds)
		errc <- err
	}()
	return errc
}

func recv[T any](t *testing.T, c <-chan T, d time.Duration, what string) T {
	t.Helper()
	select {
	case v := <-c:
		return v
	case <-time.After(d):
		t.Fatalf("timed out waiting for %s", what)
	}
	panic("unreachable")
}

func TestRemoteCancel(t *testing.T) {
	ep := newTestEP(populated())
	r, _ := startTest(t, ep, false, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := callAsync(ctx, r, "block/ds")
	recv(t, ep.started, wait, "the blocked call to start")
	cancel()
	err := recv(t, errc, time.Second, "the cancelled call to return")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	recv(t, ep.cancelled, wait, "the agent handler's context to end")

	// The Remote is still usable.
	if ok, err := r.Exists(context.Background(), "tank/a"); err != nil || !ok {
		t.Errorf("Exists after cancel = %v, %v", ok, err)
	}
}

func TestRemoteCancelGrace(t *testing.T) {
	ep := newTestEP(populated())
	r, _ := startTest(t, ep, false, nil)
	t.Cleanup(func() { close(ep.release) })
	rpc.SetCancelGrace(t, 100*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := callAsync(ctx, r, "deaf/ds")
	recv(t, ep.started, wait, "the deaf call to start")
	start := time.Now()
	cancel()
	err := recv(t, errc, wait, "the cancelled call to return")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if d := time.Since(start); d < 100*time.Millisecond {
		t.Errorf("returned after %v, want it to wait the 100ms grace for the agent", d)
	}
}

func TestRemoteConnectionLost(t *testing.T) {
	ep := newTestEP(populated())
	r, conn := startTest(t, ep, false, nil)

	errc := callAsync(context.Background(), r, "block/ds")
	recv(t, ep.started, wait, "the blocked call to start")
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	check := func(what string, err error) {
		t.Helper()
		if !errors.Is(err, rpc.ErrConnectionLost) {
			t.Errorf("%s err = %v, want ErrConnectionLost", what, err)
		}
		if err != nil && !strings.Contains(err.Error(), label) {
			t.Errorf("%s err = %q, want the label %q in it", what, err, label)
		}
	}
	check("in-flight call", recv(t, errc, wait, "the in-flight call to fail"))
	_, err := r.ListTree(context.Background(), "tank", true)
	check("later ListTree", err)
	check("later Snapshot", r.Snapshot(context.Background(), []string{"tank/a@x"}))
}

func TestRemoteClose(t *testing.T) {
	r := agenttest.Start(t, fake.New(populated(), hostInfo), label, false, nil)
	if err := r.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if _, err := r.Exists(context.Background(), "tank/a"); !errors.Is(err, rpc.ErrConnectionLost) {
		t.Errorf("Exists after Close err = %v, want ErrConnectionLost", err)
	}
}

// syncBuffer is a bytes.Buffer safe for concurrent use.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestRemoteLogEvents(t *testing.T) {
	var stdout, stderr syncBuffer
	log := logx.New(logx.Config{Stdout: &stdout, Stderr: &stderr, Debug: true})
	r, _ := startTest(t, newTestEP(populated()), true, log)

	if _, err := r.Exists(context.Background(), "log/ds"); err != nil {
		t.Fatal(err)
	}
	if got, want := stdout.String(), "DEBUG: [backup@nas] zfs receive -s -F tank/b\n"; got != want {
		t.Errorf("stdout = %q, want %q", got, want)
	}
	if got, want := stderr.String(), "WARNING: [backup@nas] careful\nCRITICAL ERROR: [backup@nas] broken\n"; got != want {
		t.Errorf("stderr = %q, want %q", got, want)
	}
}

func TestHandshakeTimeout(t *testing.T) {
	a, b := net.Pipe()
	defer func() { _ = b.Close() }()
	peerDone := make(chan struct{})
	go func() {
		// The peer reads everything and never answers.
		_, _ = io.Copy(io.Discard, b)
		close(peerDone)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	hello := rpc.Hello{Protocol: buildinfo.ProtocolVersion, Version: buildinfo.Version, BuildHash: buildinfo.BuildHash()}
	start := time.Now()
	r, _, err := rpc.Handshake(ctx, a, hello, label, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Handshake err = %v, want context.DeadlineExceeded", err)
	}
	if r != nil {
		t.Error("Handshake returned a Remote on failure")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("Handshake took %v", d)
	}
	recv(t, peerDone, wait, "the peer to see the connection closed")
	if _, err := a.Write([]byte("x")); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("write on conn after failed Handshake err = %v, want io.ErrClosedPipe", err)
	}
}

func TestHandshakeReply(t *testing.T) {
	a, b := net.Pipe()
	served := make(chan error, 1)
	go func() {
		served <- agent.Serve(context.Background(), b, agent.Config{
			NewEndpoint: func(bool, *slog.Logger) endpoint.Endpoint { return fake.New(populated(), hostInfo) },
			Home:        "/home/backup",
			ExeSHA256:   "abc123",
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	hello := rpc.Hello{Protocol: buildinfo.ProtocolVersion, Version: buildinfo.Version, BuildHash: buildinfo.BuildHash()}
	r, reply, err := rpc.Handshake(ctx, a, hello, label, nil)
	if err != nil {
		t.Fatalf("Handshake: %v", err)
	}
	want := rpc.HelloReply{
		Protocol: buildinfo.ProtocolVersion, Version: buildinfo.Version, BuildHash: buildinfo.BuildHash(),
		ExeSHA256: "abc123", OS: runtime.GOOS, Arch: runtime.GOARCH, User: "backup", UID: 1234,
		Groups: []string{"backup", "zfs"}, ZFSVersion: "zfs-2.2.0", Home: "/home/backup",
	}
	if !reflect.DeepEqual(reply, want) {
		t.Errorf("reply = %+v\nwant    %+v", reply, want)
	}
	if ok, err := r.Exists(ctx, "tank/a"); err != nil || !ok {
		t.Errorf("Exists = %v, %v", ok, err)
	}
	if err := r.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if err := recv(t, served, wait, "agent.Serve to return"); err != nil {
		t.Errorf("Serve = %v, want nil after shutdown", err)
	}
}
