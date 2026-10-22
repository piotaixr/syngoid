package rpc_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/piotaixr/syngoid/internal/agent/agenttest"
	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/endpoint/fake"
	"github.com/piotaixr/syngoid/internal/rpc"
	"github.com/piotaixr/syngoid/internal/stream"
	"github.com/piotaixr/syngoid/internal/zfs"
)

// progressTotal is where the progress of "progress/ds" ends.
const progressTotal = 5 << 20

// Send of "progress/ds" reports progress 1000 times over 1.2 s, ending at
// progressTotal, and writes nothing; Send of "endless/ds" writes until w
// fails; everything else is the fake's. Every Send signals started (when
// there is room) and reports its ctx.Err() on sendCtx as it returns.
func (e *testEP) Send(ctx context.Context, s zfs.SendSpec, o endpoint.SendOpts, w io.Writer) error {
	select {
	case e.started <- struct{}{}:
	default:
	}
	defer func() { e.sendCtx <- ctx.Err() }()
	switch s.Dataset {
	case "progress/ds":
		start := time.Now()
		for i := int64(1); i <= 1000; i++ {
			time.Sleep(time.Until(start.Add(time.Duration(i) * 1200 * time.Microsecond)))
			o.Progress(i * progressTotal / 1000)
		}
		return nil
	case "endless/ds":
		chunk := make([]byte, 64<<10)
		for {
			if _, err := w.Write(chunk); err != nil {
				return err
			}
		}
	}
	return e.Endpoint.Send(ctx, s, o, w)
}

// Receive fails at once, without reading r, when the target's parent is
// missing, as zfs does; otherwise the fake receives. Every Receive reports
// its ctx.Err() on recvCtx as it returns.
func (e *testEP) Receive(ctx context.Context, s zfs.RecvSpec, codec stream.Codec, r io.Reader) error {
	defer func() { e.recvCtx <- ctx.Err() }()
	if i := strings.LastIndexByte(s.Dataset, '/'); i > 0 {
		parent := s.Dataset[:i]
		if ok, err := e.Endpoint.Exists(ctx, parent); err != nil || !ok {
			stderr := "cannot open '" + parent + "': dataset does not exist"
			return &zfs.CommandError{Argv: []string{"zfs", "receive", s.Dataset}, ExitCode: 1, Stderr: stderr, Kind: zfs.Classify(stderr)}
		}
	}
	return e.Endpoint.Receive(ctx, s, codec, r)
}

// relay moves one send from src into a receive of target on dst the way the
// engine does: Send into a pipe, a plain io.Copy into a second pipe, and
// Receive from it.
func relay(ctx context.Context, src, dst endpoint.Endpoint, s zfs.SendSpec, target string, codec stream.Codec) error {
	aR, aW := io.Pipe()
	bR, bW := io.Pipe()
	var sendErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		sendErr = src.Send(ctx, s, endpoint.SendOpts{Codec: codec}, aW)
		_ = aW.CloseWithError(sendErr)
	}()
	go func() {
		defer wg.Done()
		_, err := io.Copy(bW, aR)
		_ = aR.CloseWithError(err)
		_ = bW.CloseWithError(err)
	}()
	recvErr := dst.Receive(ctx, zfs.RecvSpec{Dataset: target}, codec, bR)
	_ = bR.CloseWithError(recvErr)
	wg.Wait()
	return errors.Join(sendErr, recvErr)
}

// snapIDs lists the name and GUID of each snapshot of ds.
func snapIDs(w *fake.World, ds string) []string {
	d := w.Dataset(ds)
	if d == nil {
		return nil
	}
	var out []string
	for _, s := range d.Snaps {
		out = append(out, s.Name+"="+s.GUID)
	}
	return out
}

func TestRemoteTransferCodecs(t *testing.T) {
	ctx := context.Background()
	local := func(_ *testing.T, w *fake.World) endpoint.Endpoint { return fake.New(w, hostInfo) }
	remote := func(t *testing.T, w *fake.World) endpoint.Endpoint {
		return agenttest.Start(t, fake.New(w, hostInfo), label, false, nil)
	}
	topologies := []struct {
		name     string
		src, dst func(*testing.T, *fake.World) endpoint.Endpoint
	}{
		{"local to remote", local, remote},
		{"remote to local", remote, local},
		{"remote to remote", remote, remote},
	}
	for _, codec := range []stream.Codec{stream.None, stream.ZstdFast, stream.Zstd, stream.LZ4, stream.Gzip} {
		for _, tp := range topologies {
			t.Run(string(codec)+"/"+tp.name, func(t *testing.T) {
				srcW, dstW := populated(), fake.NewWorld()
				dstW.Create("pool", "")
				src, dst := tp.src(t, srcW), tp.dst(t, dstW)
				if err := relay(ctx, src, dst, zfs.SendSpec{Dataset: "tank/a", To: "s1"}, "pool/a", codec); err != nil {
					t.Fatalf("full send: %v", err)
				}
				incr := zfs.SendSpec{Dataset: "tank/a", FromSnap: "s1", To: "s3", Intermediates: true}
				if err := relay(ctx, src, dst, incr, "pool/a", codec); err != nil {
					t.Fatalf("incremental send: %v", err)
				}
				got, want := snapIDs(dstW, "pool/a"), snapIDs(srcW, "tank/a")
				if len(want) != 3 || !reflect.DeepEqual(got, want) {
					t.Errorf("target snapshots = %v, want %v", got, want)
				}
			})
		}
	}
}

func TestRemoteSendProgress(t *testing.T) {
	r, _ := startTest(t, newTestEP(populated()), false, nil)
	var mu sync.Mutex
	var calls []int64
	o := endpoint.SendOpts{Codec: stream.None, Progress: func(n int64) {
		mu.Lock()
		calls = append(calls, n)
		mu.Unlock()
	}}
	if err := r.Send(context.Background(), zfs.SendSpec{Dataset: "progress/ds", To: "s1"}, o, io.Discard); err != nil {
		t.Fatalf("Send: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(calls) == 0 || len(calls) > 4 || calls[len(calls)-1] != progressTotal {
		t.Errorf("Progress calls = %v, want at most 4 ending with %d", calls, progressTotal)
	}
}

// endless is an io.Reader producing zeros forever.
type endless struct{}

func (endless) Read(p []byte) (int, error) { return len(p), nil }

func TestRemoteReceiveEarlyFailureUnblocks(t *testing.T) {
	r, _ := startTest(t, newTestEP(fake.NewWorld()), false, nil)
	errc := make(chan error, 1)
	go func() {
		errc <- r.Receive(context.Background(), zfs.RecvSpec{Dataset: "pool/a"}, stream.None, endless{})
	}()
	err := recv(t, errc, 2*time.Second, "Receive to fail")
	var ce *zfs.CommandError
	if !errors.As(err, &ce) || !strings.Contains(ce.Stderr, "dataset does not exist") || !errors.Is(err, zfs.ErrNoDataset) {
		t.Errorf("err = %v, want the dataset does not exist *zfs.CommandError", err)
	}
}

// failingReader yields data, then fails with err.
func failingReader(data []byte, err error) io.Reader {
	return io.MultiReader(bytes.NewReader(data), readerFunc(func([]byte) (int, error) { return 0, err }))
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

func TestRemoteReceiveUpstreamError(t *testing.T) {
	ctx := context.Background()
	var payload bytes.Buffer
	if err := fake.New(populated(), hostInfo).Send(ctx, zfs.SendSpec{Dataset: "tank/a", To: "s1"}, endpoint.SendOpts{Codec: stream.None}, &payload); err != nil {
		t.Fatal(err)
	}
	// A complete fake stream padded to 64 KiB: only the cancel can stop the
	// agent from applying it.
	payload.Write(bytes.Repeat([]byte{'\n'}, 64<<10-payload.Len()))

	w := fake.NewWorld()
	w.Create("pool", "")
	ep := newTestEP(w)
	r, _ := startTest(t, ep, false, nil)
	before := state(w, []string{"pool", "pool/a"})
	errX := errors.New("upstream broke")
	err := r.Receive(ctx, zfs.RecvSpec{Dataset: "pool/a"}, stream.None, failingReader(payload.Bytes(), errX))
	if !errors.Is(err, errX) {
		t.Fatalf("err = %v, want errX", err)
	}
	if err := recv(t, ep.recvCtx, wait, "the agent's receive to return"); !errors.Is(err, context.Canceled) {
		t.Errorf("agent receive ctx err = %v, want context.Canceled", err)
	}
	if after := state(w, []string{"pool", "pool/a"}); !reflect.DeepEqual(after, before) {
		t.Errorf("target World = %v, want it unchanged: %v", after, before)
	}
}

func TestRemoteSendCancel(t *testing.T) {
	ep := newTestEP(populated())
	ep.Faults.BlockSend = true
	r, _ := startTest(t, ep, false, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() {
		errc <- r.Send(ctx, zfs.SendSpec{Dataset: "tank/a", To: "s1"}, endpoint.SendOpts{Codec: stream.None}, io.Discard)
	}()
	recv(t, ep.started, wait, "the agent's send to start")
	cancel()
	if err := recv(t, errc, 2*time.Second, "the cancelled Send to return"); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	// Send waited for the agent's answer (well within cancelGrace), so the
	// handler has returned.
	select {
	case err := <-ep.sendCtx:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("agent send ctx err = %v, want context.Canceled", err)
		}
	default:
		t.Error("the agent's send had not returned when Send did")
	}
}

// failAfter accepts n writes, then fails every write with err.
type failAfter struct {
	n   int
	err error
}

func (f *failAfter) Write(p []byte) (int, error) {
	if f.n == 0 {
		return 0, f.err
	}
	f.n--
	return len(p), nil
}

func TestRemoteSendDownstreamError(t *testing.T) {
	ep := newTestEP(populated())
	r, _ := startTest(t, ep, false, nil)
	errW := errors.New("downstream broke")
	err := r.Send(context.Background(), zfs.SendSpec{Dataset: "endless/ds", To: "s1"}, endpoint.SendOpts{Codec: stream.None}, &failAfter{n: 1, err: errW})
	if !errors.Is(err, errW) {
		t.Fatalf("err = %v, want the writer's error", err)
	}
	if err := recv(t, ep.sendCtx, wait, "the agent's send to return"); !errors.Is(err, context.Canceled) {
		t.Errorf("agent send ctx err = %v, want context.Canceled", err)
	}
}

func TestRemoteSendError(t *testing.T) {
	r, _ := startTest(t, newTestEP(populated()), false, nil)
	var w bytes.Buffer
	err := r.Send(context.Background(), zfs.SendSpec{Dataset: "tank/a", To: "nope"}, endpoint.SendOpts{Codec: stream.Zstd}, &w)
	var ce *zfs.CommandError
	if !errors.As(err, &ce) || !errors.Is(err, zfs.ErrNoDataset) {
		t.Errorf("err = %v, want the agent's dataset does not exist *zfs.CommandError", err)
	}
	if w.Len() != 0 {
		t.Errorf("w got %d bytes from a failed send, want none", w.Len())
	}
}

// stallWriter signals entered on its first Write, which then blocks until
// unstall.
type stallWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newStallWriter(t *testing.T) *stallWriter {
	w := &stallWriter{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { close(w.release) })
	return w
}

func (w *stallWriter) Write([]byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return 0, io.ErrClosedPipe
}

// sendAsync runs Send of "endless/ds" into w in the background.
func sendAsync(ctx context.Context, r endpoint.Endpoint, w io.Writer) <-chan error {
	errc := make(chan error, 1)
	go func() {
		errc <- r.Send(ctx, zfs.SendSpec{Dataset: "endless/ds", To: "s1"}, endpoint.SendOpts{Codec: stream.None}, w)
	}()
	return errc
}

func TestRemoteSendStalledWriterCancel(t *testing.T) {
	rpc.SetCancelGrace(t, 100*time.Millisecond)
	r, _ := startTest(t, newTestEP(populated()), false, nil)
	w := newStallWriter(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := sendAsync(ctx, r, w)
	recv(t, w.entered, wait, "the copy to block in w.Write")
	cancel()
	if err := recv(t, errc, time.Second, "Send to return despite the stalled w"); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

func TestRemoteSendConnectionLostMidStream(t *testing.T) {
	r, conn := startTest(t, newTestEP(populated()), false, nil)
	w := newStallWriter(t)
	errc := sendAsync(context.Background(), r, w)
	recv(t, w.entered, wait, "the stream to flow")
	_ = conn.Close()
	if err := recv(t, errc, 2*time.Second, "Send to return after the connection died"); !errors.Is(err, rpc.ErrConnectionLost) {
		t.Errorf("err = %v, want ErrConnectionLost", err)
	}
}

// signalReader closes flowing once n bytes have been read through it.
type signalReader struct {
	r       io.Reader
	n       int
	flowing chan struct{}
}

func (s *signalReader) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	if s.n > 0 {
		if s.n -= n; s.n <= 0 {
			close(s.flowing)
		}
	}
	return n, err
}

func TestRemoteReceiveConnectionLostMidStream(t *testing.T) {
	var header bytes.Buffer
	if err := fake.New(populated(), hostInfo).Send(context.Background(), zfs.SendSpec{Dataset: "tank/a", To: "s1"}, endpoint.SendOpts{Codec: stream.None}, &header); err != nil {
		t.Fatal(err)
	}
	w := fake.NewWorld()
	w.Create("pool", "")
	r, conn := startTest(t, newTestEP(w), false, nil)
	// A valid fake stream header, then padding the agent drains forever.
	rd := &signalReader{r: io.MultiReader(&header, endless{}), n: 1 << 20, flowing: make(chan struct{})}
	errc := make(chan error, 1)
	go func() {
		errc <- r.Receive(context.Background(), zfs.RecvSpec{Dataset: "pool/a"}, stream.None, rd)
	}()
	recv(t, rd.flowing, wait, "the stream to flow")
	_ = conn.Close()
	if err := recv(t, errc, 2*time.Second, "Receive to return after the connection died"); !errors.Is(err, rpc.ErrConnectionLost) {
		t.Errorf("err = %v, want ErrConnectionLost", err)
	}
}
