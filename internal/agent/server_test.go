package agent

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/yamux"

	"github.com/piotaixr/syngoid/internal/buildinfo"
	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/endpoint/fake"
	"github.com/piotaixr/syngoid/internal/rpc"
	"github.com/piotaixr/syngoid/internal/zfs"
)

const wait = 5 * time.Second

var hostInfo = endpoint.HostInfo{
	OS: "fakeos", Arch: "fakearch", User: "backup", UID: 1234,
	Groups: []string{"backup", "zfs"}, ZFSVersion: "zfs-2.2.0",
}

// testEP wraps a fake endpoint with the hooks the tests need. Exists on
// "block/ds" waits for its context, on "slow/ds" keeps running after its
// context ended until release is closed (like a zfs child still being
// reaped), on "killed/ds" fails with a CommandError once its context ended
// (like a killed zfs child), on "fail/ds" fails with a CommandError
// and on "log/ds" logs; everything else is the fake's.
type testEP struct {
	*fake.Endpoint
	log *slog.Logger

	started   chan struct{} // receives once per blocked Exists
	cancelled chan struct{} // receives once per blocked Exists whose ctx ended
	release   chan struct{} // closed to let "slow/ds" return after its ctx ended
	payloads  chan []byte   // receives the whole input of every Receive
	receiving chan struct{} // receives once per Receive, on entry
	readErrs  chan error    // receives the input's read error of every Receive
}

func (e *testEP) Exists(ctx context.Context, ds string) (bool, error) {
	switch ds {
	case "block/ds":
		e.started <- struct{}{}
		<-ctx.Done()
		e.cancelled <- struct{}{}
		return false, ctx.Err()
	case "slow/ds":
		e.started <- struct{}{}
		<-ctx.Done()
		<-e.release
		return false, ctx.Err()
	case "killed/ds":
		e.started <- struct{}{}
		<-ctx.Done()
		return false, &zfs.CommandError{Argv: []string{"zfs", "list", ds}, ExitCode: -1, Stderr: "signal: killed"}
	case "fail/ds":
		return false, &zfs.CommandError{Argv: []string{"zfs", "list", ds}, ExitCode: 1, Stderr: "cannot open 'fail/ds': permission denied"}
	case "log/ds":
		e.log.Debug("zfs list", "dataset", ds)
		e.log.Info("hello info")
		e.log.Warn("careful")
		e.log.Error("broken")
		return true, nil
	case "panic/ds":
		panic("boom")
	}
	return e.Endpoint.Exists(ctx, ds)
}

type harness struct {
	t      *testing.T
	world  *fake.World
	ep     *testEP
	cfg    Config
	client net.Conn
	cli    *yamux.Session
	codec  *rpc.Codec
	done   chan error // Serve's result

	msgs    chan rpc.Message
	pending map[uint64]rpc.Message
	events  []rpc.Event
	nextID  uint64
}

// newHarness starts Serve on one end of a pipe and a raw client on the
// other, with the control stream open but no hello sent.
func newHarness(t *testing.T, tweak func(*Config)) *harness {
	t.Helper()
	h := &harness{t: t, world: fake.NewWorld(), pending: map[uint64]rpc.Message{}}
	h.ep = &testEP{
		Endpoint:  fake.New(h.world, hostInfo),
		started:   make(chan struct{}, 8),
		cancelled: make(chan struct{}, 8),
		release:   make(chan struct{}),
		payloads:  make(chan []byte, 8),
		receiving: make(chan struct{}, 8),
		readErrs:  make(chan error, 8),
	}
	h.cfg = Config{
		NewEndpoint: func(_ bool, l *slog.Logger) endpoint.Endpoint { h.ep.log = l; return h.ep },
		Home:        "/home/backup",
		Exe:         "/home/backup/.syngoid/syngoid-abc",
		ExeSHA256:   "abc123",
	}
	if tweak != nil {
		tweak(&h.cfg)
	}
	a, b := net.Pipe()
	h.client = a
	h.done = make(chan error, 1)
	go func() { h.done <- Serve(context.Background(), b, h.cfg) }()

	var err error
	if h.cli, err = rpc.MuxClient(a); err != nil {
		t.Fatalf("MuxClient: %v", err)
	}
	ctrl, err := h.cli.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	h.codec = rpc.NewCodec(ctrl)
	h.msgs = make(chan rpc.Message, 64)
	go func() {
		for {
			m, err := h.codec.Read()
			if err != nil {
				close(h.msgs)
				return
			}
			h.msgs <- m
		}
	}()
	t.Cleanup(func() { _ = h.cli.Close(); _ = a.Close() })
	return h
}

func (h *harness) send(method string, params any) uint64 {
	h.t.Helper()
	h.nextID++
	m := rpc.Message{ID: h.nextID, Method: method}
	if params != nil {
		raw, err := json.Marshal(params)
		if err != nil {
			h.t.Fatal(err)
		}
		m.Params = raw
	}
	if err := h.codec.Write(m); err != nil {
		h.t.Fatalf("write %s: %v", method, err)
	}
	return m.ID
}

// await returns the response to request id, collecting events on the way.
func (h *harness) await(id uint64) rpc.Message {
	h.t.Helper()
	if m, ok := h.pending[id]; ok {
		delete(h.pending, id)
		return m
	}
	timeout := time.After(wait)
	for {
		select {
		case m, ok := <-h.msgs:
			if !ok {
				h.t.Fatalf("control stream closed while waiting for response %d", id)
			}
			switch {
			case m.Event != nil:
				h.events = append(h.events, *m.Event)
			case m.ID == id:
				return m
			default:
				h.pending[m.ID] = m
			}
		case <-timeout:
			h.t.Fatalf("no response to request %d", id)
		}
	}
}

func (h *harness) call(method string, params any) rpc.Message {
	h.t.Helper()
	return h.await(h.send(method, params))
}

// ok calls method, requires success and returns the raw result.
func (h *harness) ok(method string, params any) json.RawMessage {
	h.t.Helper()
	m := h.call(method, params)
	if m.Error != nil {
		h.t.Fatalf("%s: error %+v", method, m.Error)
	}
	return m.Result
}

func (h *harness) hello(hello rpc.Hello) rpc.HelloReply {
	h.t.Helper()
	var r rpc.HelloReply
	decode(h.t, h.ok(rpc.MethodHello, hello), &r)
	return r
}

func (h *harness) serveResult() error {
	h.t.Helper()
	select {
	case err := <-h.done:
		return err
	case <-time.After(2 * time.Second):
		h.t.Fatal("Serve did not return")
		return nil
	}
}

func decode(t *testing.T, raw json.RawMessage, v any) {
	t.Helper()
	if err := json.Unmarshal(raw, v); err != nil {
		t.Fatalf("decoding %s: %v", raw, err)
	}
}

func matchingHello() rpc.Hello {
	return rpc.Hello{Protocol: buildinfo.ProtocolVersion, Version: buildinfo.Version, BuildHash: buildinfo.BuildHash()}
}

func TestServeHello(t *testing.T) {
	h := newHarness(t, nil)
	got := h.hello(rpc.Hello{Protocol: 1, Version: "dev", BuildHash: "x"})
	want := rpc.HelloReply{
		Protocol: 1, Version: "dev", BuildHash: buildinfo.BuildHash(), ExeSHA256: "abc123",
		OS: runtime.GOOS, Arch: runtime.GOARCH,
		User: "backup", UID: 1234, Groups: []string{"backup", "zfs"}, ZFSVersion: "zfs-2.2.0",
		Home: "/home/backup",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("HelloReply = %+v, want %+v", got, want)
	}
}

func TestServeHelloPassesSudo(t *testing.T) {
	var sudo atomic.Bool
	h := newHarness(t, func(c *Config) {
		inner := c.NewEndpoint
		c.NewEndpoint = func(s bool, l *slog.Logger) endpoint.Endpoint { sudo.Store(s); return inner(s, l) }
	})
	h.hello(rpc.Hello{Protocol: 1, Sudo: true})
	if !sudo.Load() {
		t.Error("NewEndpoint did not get sudo=true")
	}
}

func TestServeFirstMessageMustBeHello(t *testing.T) {
	h := newHarness(t, nil)
	m := h.call(rpc.MethodExists, rpc.DatasetParams{Dataset: "tank"})
	if m.Error == nil || m.Error.Code != rpc.CodeInvalid {
		t.Errorf("response = %+v, want an %q error", m, rpc.CodeInvalid)
	}
	if err := h.serveResult(); err == nil {
		t.Error("Serve returned nil, want an error")
	}
}

func TestServeDispatch(t *testing.T) {
	h := newHarness(t, nil)
	w := h.world
	w.Create("tank", "")
	for _, s := range []string{"a", "b", "c"} {
		w.Snap("tank", s)
	}
	w.MarkBookmark("tank", "a", "bm")
	w.SetProp("tank", "syncoid:sync", "true")
	w.Create("tank/child", "")
	w.Snap("tank/child", "s")
	w.Create("tank/r", "")
	w.Dataset("tank/r").ResumeToken = "1-abc-def"
	h.hello(rpc.Hello{Protocol: 1})
	ref := fake.New(w, hostInfo) // answers the same queries directly
	ctx := context.Background()

	// expect decodes the result into T and compares it with want.
	expect := func(t *testing.T, raw json.RawMessage, want any) {
		t.Helper()
		got := reflect.New(reflect.TypeOf(want))
		decode(t, raw, got.Interface())
		if !reflect.DeepEqual(got.Elem().Interface(), want) {
			t.Errorf("result = %+v, want %+v", got.Elem().Interface(), want)
		}
	}
	must := func(t *testing.T) func(v any, err error) any {
		return func(v any, err error) any {
			t.Helper()
			if err != nil {
				t.Fatal(err)
			}
			return v
		}
	}

	queries := []struct {
		name, method string
		params       any
		want         func(t *testing.T) any
	}{
		{"info", rpc.MethodInfo, nil, func(t *testing.T) any { return must(t)(ref.Info(ctx)) }},
		{
			"list_tree", rpc.MethodListTree,
			rpc.DatasetParams{Dataset: "tank", Recursive: true},
			func(t *testing.T) any { return must(t)(ref.ListTree(ctx, "tank", true)) },
		},
		{
			"list_snapshots", rpc.MethodListSnapshots,
			rpc.DatasetParams{Dataset: "tank", Recursive: true},
			func(t *testing.T) any { return must(t)(ref.ListSnapshots(ctx, "tank", true)) },
		},
		{
			"list_bookmarks", rpc.MethodListBookmarks,
			rpc.DatasetParams{Dataset: "tank"},
			func(t *testing.T) any { return must(t)(ref.ListBookmarks(ctx, "tank", false)) },
		},
		{
			"get_props", rpc.MethodGetProps,
			rpc.GetPropsParams{Dataset: "tank", Props: []string{"type", "syncoid:sync"}},
			func(t *testing.T) any { return must(t)(ref.GetProps(ctx, "tank", "type", "syncoid:sync")) },
		},
		{
			"local_props", rpc.MethodLocalProps,
			rpc.DatasetParams{Dataset: "tank"},
			func(t *testing.T) any { return must(t)(ref.LocalProps(ctx, "tank")) },
		},
		{
			"exists", rpc.MethodExists,
			rpc.DatasetParams{Dataset: "tank"},
			func(t *testing.T) any { return must(t)(ref.Exists(ctx, "tank")) },
		},
		{
			"exists missing", rpc.MethodExists,
			rpc.DatasetParams{Dataset: "nope"},
			func(t *testing.T) any { return must(t)(ref.Exists(ctx, "nope")) },
		},
		{
			"send_estimate", rpc.MethodSendEstimate,
			rpc.SendEstimateParams{Spec: zfs.SendSpec{Dataset: "tank", To: "c", FromSnap: "a"}},
			func(t *testing.T) any {
				return must(t)(ref.SendEstimate(ctx, zfs.SendSpec{Dataset: "tank", To: "c", FromSnap: "a"}))
			},
		},
		{
			"receive_busy", rpc.MethodReceiveBusy,
			rpc.ReceiveBusyParams{Datasets: []string{"tank", "tank/child"}},
			func(t *testing.T) any { return must(t)(ref.ReceiveBusy(ctx, []string{"tank", "tank/child"})) },
		},
		{
			"allow", rpc.MethodAllow,
			rpc.DatasetParams{Dataset: "tank"},
			func(t *testing.T) any { return must(t)(ref.Allow(ctx, "tank")) },
		},
	}
	for _, q := range queries {
		t.Run(q.name, func(t *testing.T) { expect(t, h.ok(q.method, q.params), q.want(t)) })
	}

	// Mutations: the response carries no result and the World changes.
	snaps := func(ds string) []string {
		var out []string
		for _, s := range w.Dataset(ds).Snaps {
			out = append(out, s.Name)
		}
		return out
	}
	mutations := []struct {
		name, method string
		params       any
		check        func() bool
	}{
		{
			"snapshot", rpc.MethodSnapshot,
			rpc.SnapshotParams{Names: []string{"tank@d", "tank/child@d"}},
			func() bool {
				return reflect.DeepEqual(snaps("tank"), []string{"a", "b", "c", "d"}) && len(snaps("tank/child")) == 2
			},
		},
		{
			"hold", rpc.MethodHold,
			rpc.HoldParams{Tag: "keep", Snapshot: "tank@b"},
			func() bool { return reflect.DeepEqual(w.Dataset("tank").Holds["b"], []string{"keep"}) },
		},
		{
			"release", rpc.MethodRelease,
			rpc.HoldParams{Tag: "keep", Snapshot: "tank@b"},
			func() bool { return len(w.Dataset("tank").Holds["b"]) == 0 },
		},
		{
			"bookmark", rpc.MethodBookmark,
			rpc.BookmarkParams{Snapshot: "tank@c", Bookmark: "tank#bm2"},
			func() bool { return len(w.Dataset("tank").Bookmarks) == 2 },
		},
		{
			"destroy", rpc.MethodDestroy,
			rpc.DestroyParams{Dataset: "tank", Snaps: []string{"a", "d"}},
			func() bool { return reflect.DeepEqual(snaps("tank"), []string{"b", "c"}) },
		},
		{
			"abort_resume", rpc.MethodAbortResume,
			rpc.DatasetParams{Dataset: "tank/r"},
			func() bool { return w.Dataset("tank/r").ResumeToken == "" },
		},
		{
			"destroy_dataset", rpc.MethodDestroyDataset,
			rpc.DatasetParams{Dataset: "tank/child", Recursive: true},
			func() bool { return w.Dataset("tank/child") == nil },
		},
	}
	for _, m := range mutations {
		t.Run(m.name, func(t *testing.T) {
			if res := h.ok(m.method, m.params); len(res) != 0 {
				t.Errorf("result = %s, want none", res)
			}
			if !m.check() {
				t.Errorf("World state wrong after %s; calls: %v", m.method, h.ep.Calls())
			}
		})
	}
}

func TestServeCommandError(t *testing.T) {
	h := newHarness(t, nil)
	h.hello(rpc.Hello{Protocol: 1})
	m := h.call(rpc.MethodExists, rpc.DatasetParams{Dataset: "fail/ds"})
	if m.Error == nil || m.Error.Code != rpc.CodeCommand {
		t.Fatalf("response = %+v, want a %q error", m, rpc.CodeCommand)
	}
	if want := "cannot open 'fail/ds': permission denied"; m.Error.Stderr != want {
		t.Errorf("Stderr = %q, want %q", m.Error.Stderr, want)
	}
}

func TestServeRejectsInvalidParams(t *testing.T) {
	h := newHarness(t, nil)
	h.world.Create("tank", "")
	h.hello(rpc.Hello{Protocol: 1})
	m := h.call(rpc.MethodDestroy, rpc.DestroyParams{Dataset: "-r", Snaps: []string{"x"}})
	if m.Error == nil || m.Error.Code != rpc.CodeInvalid {
		t.Fatalf("response = %+v, want an %q error", m, rpc.CodeInvalid)
	}
	if calls := h.ep.Calls(); len(calls) != 0 {
		t.Errorf("endpoint calls = %v, want none", calls)
	}
}

func TestServeRejectsUndecodableParams(t *testing.T) {
	h := newHarness(t, nil)
	h.hello(rpc.Hello{Protocol: 1})
	m := h.call(rpc.MethodExists, []int{1, 2})
	if m.Error == nil || m.Error.Code != rpc.CodeInvalid {
		t.Errorf("response = %+v, want an %q error", m, rpc.CodeInvalid)
	}
}

func TestServeUnknownMethod(t *testing.T) {
	h := newHarness(t, nil)
	h.hello(rpc.Hello{Protocol: 1})
	// hello is accepted only once.
	for _, method := range []string{"frobnicate", rpc.MethodHello} {
		m := h.call(method, nil)
		if m.Error == nil || m.Error.Code != rpc.CodeUnknownMethod {
			t.Errorf("%s: response = %+v, want %q", method, m, rpc.CodeUnknownMethod)
		}
	}
}

func TestServeRecoversFromPanic(t *testing.T) {
	h := newHarness(t, nil)
	h.hello(rpc.Hello{Protocol: 1})
	m := h.call(rpc.MethodExists, rpc.DatasetParams{Dataset: "panic/ds"})
	if m.Error == nil || m.Error.Code != rpc.CodeInternal {
		t.Fatalf("response = %+v, want an %q error", m, rpc.CodeInternal)
	}
	var ok bool
	decode(t, h.ok(rpc.MethodExists, rpc.DatasetParams{Dataset: "nope"}), &ok) // still serving
}

func TestServeLogEvents(t *testing.T) {
	for _, debug := range []bool{true, false} {
		h := newHarness(t, nil)
		h.hello(rpc.Hello{Protocol: 1, Debug: debug})
		h.ok(rpc.MethodExists, rpc.DatasetParams{Dataset: "log/ds"})
		var want []rpc.Event
		if debug {
			want = append(want, rpc.Event{Kind: rpc.EventLog, Level: "DEBUG", Msg: "zfs list"})
		}
		want = append(want,
			rpc.Event{Kind: rpc.EventLog, Level: "INFO", Msg: "hello info"},
			rpc.Event{Kind: rpc.EventLog, Level: "WARN", Msg: "careful"},
			rpc.Event{Kind: rpc.EventLog, Level: "ERROR", Msg: "broken"},
		)
		if !reflect.DeepEqual(h.events, want) {
			t.Errorf("debug=%v: events = %+v, want %+v", debug, h.events, want)
		}
	}
}

// blockRequest starts an Exists that blocks in the endpoint until cancelled.
func (h *harness) blockRequest() uint64 { return h.blockOn("block/ds") }

// blockOn starts an Exists on ds, one of the testEP datasets that wait for
// their context, and returns once it is inside the endpoint.
func (h *harness) blockOn(ds string) uint64 {
	h.t.Helper()
	id := h.send(rpc.MethodExists, rpc.DatasetParams{Dataset: ds})
	select {
	case <-h.ep.started:
	case <-time.After(wait):
		h.t.Fatal("blocked request never reached the endpoint")
	}
	return id
}

func TestServeConcurrentRequests(t *testing.T) {
	h := newHarness(t, nil)
	h.world.Create("tank", "")
	h.hello(rpc.Hello{Protocol: 1})
	blocked := h.blockRequest()
	h.ok(rpc.MethodListTree, rpc.DatasetParams{Dataset: "tank"}) // not delayed by the first
	if _, ok := h.pending[blocked]; ok {
		t.Fatal("blocked request already answered")
	}
}

func TestServeCancel(t *testing.T) {
	h := newHarness(t, nil)
	h.hello(rpc.Hello{Protocol: 1})
	blocked := h.blockRequest()
	h.ok(rpc.MethodCancel, rpc.CancelParams{ID: blocked})
	m := h.await(blocked)
	if m.Error == nil || m.Error.Code != rpc.CodeCanceled {
		t.Fatalf("response = %+v, want a %q error", m, rpc.CodeCanceled)
	}
	if m.Error.Message != context.Canceled.Error() {
		t.Errorf("Message = %q, want %q (no doubled wrap)", m.Error.Message, context.Canceled.Error())
	}
	// Cancelling a request that is gone is not an error.
	h.ok(rpc.MethodCancel, rpc.CancelParams{ID: blocked})
}

// A command killed by the cancellation reports its exit status, not the
// context error; the response is still "canceled".
func TestServeCancelKilledCommand(t *testing.T) {
	h := newHarness(t, nil)
	h.hello(rpc.Hello{Protocol: 1})
	killed := h.blockOn("killed/ds")
	h.ok(rpc.MethodCancel, rpc.CancelParams{ID: killed})
	m := h.await(killed)
	if m.Error == nil || m.Error.Code != rpc.CodeCanceled {
		t.Errorf("response = %+v, want a %q error", m, rpc.CodeCanceled)
	}
}

// An id may be reused as soon as its response has been received.
func TestServeFinishedRequestIDIsReusable(t *testing.T) {
	h := newHarness(t, nil)
	h.hello(rpc.Hello{Protocol: 1})
	for range 50 {
		id := h.send(rpc.MethodExists, rpc.DatasetParams{Dataset: "nope"})
		if m := h.await(id); m.Error != nil {
			t.Fatalf("response = %+v", m)
		}
		// Reuse the id at once: the registry must already have forgotten it.
		if err := h.codec.Write(rpc.Message{ID: id, Method: rpc.MethodExists, Params: json.RawMessage(`{"dataset":"nope"}`)}); err != nil {
			t.Fatal(err)
		}
		if m := h.await(id); m.Error != nil {
			t.Fatalf("reused id %d: response = %+v", id, m)
		}
	}
}

// Serve must not return while a handler is still running, even though its
// context has been cancelled.
func TestServeWaitsForHandlers(t *testing.T) {
	h := newHarness(t, nil)
	h.hello(rpc.Hello{Protocol: 1})
	h.blockOn("slow/ds")
	_ = h.cli.Close()
	_ = h.client.Close()
	select {
	case err := <-h.done:
		t.Fatalf("Serve returned (%v) while a handler was still running", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(h.ep.release)
	if err := h.serveResult(); err != nil {
		t.Errorf("Serve = %v, want nil", err)
	}
}

func TestServeEOFCancelsInFlight(t *testing.T) {
	h := newHarness(t, nil)
	h.hello(rpc.Hello{Protocol: 1})
	h.blockRequest()
	_ = h.cli.Close()
	_ = h.client.Close()
	select {
	case <-h.ep.cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked request's context was not cancelled")
	}
	if err := h.serveResult(); err != nil {
		t.Errorf("Serve = %v, want nil on a clean end of session", err)
	}
}

func TestServeContextEndCancelsInFlight(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Own wiring: the harness always uses context.Background().
	world := fake.NewWorld()
	ep := &testEP{Endpoint: fake.New(world, hostInfo), started: make(chan struct{}, 1), cancelled: make(chan struct{}, 1)}
	a, b := net.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, b, Config{NewEndpoint: func(bool, *slog.Logger) endpoint.Endpoint { return ep }})
	}()
	cli, err := rpc.MuxClient(a)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()
	ctrl, err := cli.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	c := rpc.NewCodec(ctrl)
	hello, _ := json.Marshal(rpc.Hello{Protocol: 1})
	req, _ := json.Marshal(rpc.DatasetParams{Dataset: "block/ds"})
	_ = c.Write(rpc.Message{ID: 1, Method: rpc.MethodHello, Params: hello})
	if m, err := c.Read(); err != nil || m.Error != nil {
		t.Fatalf("hello: %+v, %v", m, err)
	}
	_ = c.Write(rpc.Message{ID: 2, Method: rpc.MethodExists, Params: req})
	<-ep.started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Serve = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after ctx ended")
	}
	select {
	case <-ep.cancelled:
	default:
		t.Error("request context was not cancelled before Serve returned")
	}
}

func TestServeShutdown(t *testing.T) {
	h := newHarness(t, nil)
	h.hello(rpc.Hello{Protocol: 1})
	h.ok(rpc.MethodShutdown, nil)
	if err := h.serveResult(); err != nil {
		t.Errorf("Serve = %v, want nil", err)
	}
}

type pruneCall struct{ exe, home string }

func TestServePrunePanicIsContained(t *testing.T) {
	h := newHarness(t, func(c *Config) {
		c.Prune = func(string, string, *slog.Logger) { panic("prune boom") }
	})
	h.hello(matchingHello())
	var ok bool
	decode(t, h.ok(rpc.MethodExists, rpc.DatasetParams{Dataset: "nope"}), &ok) // still serving
	h.ok(rpc.MethodShutdown, nil)
	if err := h.serveResult(); err != nil {
		t.Errorf("Serve = %v, want nil", err)
	}
}

func TestServePrunesOnlyOnMatch(t *testing.T) {
	cases := []struct {
		name  string
		hello func() rpc.Hello
		want  []pruneCall
	}{
		{"match", matchingHello, []pruneCall{{"/home/backup/.syngoid/syngoid-abc", "/home/backup"}}},
		{"version differs", func() rpc.Hello { h := matchingHello(); h.Version = "v9.9.9"; return h }, nil},
		{"protocol differs", func() rpc.Hello { h := matchingHello(); h.Protocol++; return h }, nil},
	}
	if buildinfo.IsDev() {
		cases = append(cases, struct {
			name  string
			hello func() rpc.Hello
			want  []pruneCall
		}{"dev build hash differs", func() rpc.Hello { h := matchingHello(); h.BuildHash = "other"; return h }, nil})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var calls []pruneCall
			h := newHarness(t, func(c *Config) {
				c.Prune = func(exe, home string, _ *slog.Logger) {
					mu.Lock()
					defer mu.Unlock()
					calls = append(calls, pruneCall{exe, home})
				}
			})
			h.hello(tc.hello())
			h.ok(rpc.MethodShutdown, nil)
			if err := h.serveResult(); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			if !reflect.DeepEqual(calls, tc.want) {
				t.Errorf("Prune calls = %v, want %v", calls, tc.want)
			}
		})
	}
}
