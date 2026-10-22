package agent

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/yamux"

	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/endpoint/fake"
	"github.com/piotaixr/syngoid/internal/rpc"
	"github.com/piotaixr/syngoid/internal/stream"
	"github.com/piotaixr/syngoid/internal/zfs"
)

// Receive hands the whole input to payloads, then lets the fake receive it.
func (e *testEP) Receive(ctx context.Context, s zfs.RecvSpec, codec stream.Codec, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	e.payloads <- data
	return e.Endpoint.Receive(ctx, s, codec, bytes.NewReader(data))
}

// newTimeoutHarness is newHarness with a dataStreamTimeout of d.
func newTimeoutHarness(t *testing.T, d time.Duration) *harness {
	old := dataStreamTimeout
	dataStreamTimeout = d
	t.Cleanup(func() { dataStreamTimeout = old })
	return newHarness(t, nil)
}

// openData opens a data stream and writes its header for request id and
// payload in a single write.
func (h *harness) openData(id uint64, payload []byte) *yamux.Stream {
	h.t.Helper()
	st, err := h.cli.OpenStream()
	if err != nil {
		h.t.Fatalf("OpenStream: %v", err)
	}
	var b bytes.Buffer
	if err := rpc.WriteDataHeader(&b, id); err != nil {
		h.t.Fatal(err)
	}
	b.Write(payload)
	if _, err := st.Write(b.Bytes()); err != nil {
		h.t.Fatalf("writing data stream: %v", err)
	}
	return st
}

// readAll reads st to its end, failing the test if that takes too long.
func readAll(t *testing.T, st *yamux.Stream, what string) []byte {
	t.Helper()
	type result struct {
		data []byte
		err  error
	}
	c := make(chan result, 1)
	go func() {
		data, err := io.ReadAll(st)
		c <- result{data, err}
	}()
	select {
	case r := <-c:
		if r.err != nil {
			t.Fatalf("reading %s: %v", what, r.err)
		}
		return r.data
	case <-time.After(wait):
		t.Fatalf("%s was not closed", what)
	}
	return nil
}

// sendStream returns the fake stream of tank/a@s1 sent from w.
func sendStream(t *testing.T, w *fake.World) []byte {
	t.Helper()
	var b bytes.Buffer
	spec := zfs.SendSpec{Dataset: "tank/a", To: "s1"}
	if err := fake.New(w, hostInfo).Send(context.Background(), spec, endpoint.SendOpts{Codec: stream.None}, &b); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

// sourceWorld returns a World holding tank/a@s1.
func sourceWorld() *fake.World {
	w := fake.NewWorld()
	w.Create("tank", "")
	w.Create("tank/a", "")
	w.Snap("tank/a", "s1")
	return w
}

func snapNames(d *fake.Dataset) []string {
	if d == nil {
		return nil
	}
	var out []string
	for _, s := range d.Snaps {
		out = append(out, s.Name)
	}
	return out
}

func TestAgentDataStreamTimeout(t *testing.T) {
	h := newTimeoutHarness(t, 100*time.Millisecond)
	h.hello(matchingHello())
	m := h.call(rpc.MethodSend, rpc.SendParams{Spec: zfs.SendSpec{Dataset: "tank/a", To: "s1"}, Codec: stream.None})
	if m.Error == nil || m.Error.Code != rpc.CodeInternal || !strings.Contains(m.Error.Message, "no data stream") {
		t.Fatalf("response = %+v, want an %q error about no data stream", m, rpc.CodeInternal)
	}
}

func TestAgentStreamBeforeRequest(t *testing.T) {
	h := newHarness(t, nil)
	h.hello(matchingHello())
	h.world.Create("pool", "")
	// More than a bufio buffer's worth of payload travels in the same write
	// as the header: the receive must get exactly the bytes after it.
	payload := append(sendStream(t, sourceWorld()), bytes.Repeat([]byte{'\n'}, 3*4096)...)
	id := h.nextID + 1
	st := h.openData(id, payload)
	_ = st.Close()
	// Let the agent park the stream before the request exists.
	time.Sleep(50 * time.Millisecond)

	if got := h.send(rpc.MethodReceive, rpc.ReceiveParams{Spec: zfs.RecvSpec{Dataset: "pool/a"}, Codec: stream.None}); got != id {
		t.Fatalf("request id = %d, want %d", got, id)
	}
	if m := h.await(id); m.Error != nil {
		t.Fatalf("receive: %+v", m.Error)
	}
	if got := <-h.ep.payloads; !bytes.Equal(got, payload) {
		t.Errorf("receive got %d bytes, want exactly the %d payload bytes after the header", len(got), len(payload))
	}
	if got := snapNames(h.world.Dataset("pool/a")); len(got) != 1 || got[0] != "s1" {
		t.Errorf("pool/a snapshots = %v, want [s1]", got)
	}
}

func TestAgentSendStreamsThenProgressThenResponse(t *testing.T) {
	h := newHarness(t, nil)
	h.hello(matchingHello())
	h.world.Create("tank", "")
	h.world.Create("tank/a", "")
	h.world.Snap("tank/a", "s1")
	want := sendStream(t, h.world)

	id := h.nextID + 1
	st := h.openData(id, nil)
	h.ok(rpc.MethodSend, rpc.SendParams{Spec: zfs.SendSpec{Dataset: "tank/a", To: "s1"}, Codec: stream.None})
	if got := readAll(t, st, "the data stream"); !bytes.Equal(got, want) {
		t.Errorf("data stream = %q, want %q", got, want)
	}
	// await collected the events that preceded the response.
	var last *rpc.Event
	for i := range h.events {
		if ev := &h.events[i]; ev.Kind == rpc.EventProgress && ev.RequestID == id {
			last = ev
		}
	}
	if last == nil || last.Bytes != int64(len(want)) {
		t.Errorf("last progress event before the response = %+v, want %d bytes", last, len(want))
	}
}

func TestAgentClosesStrayDataStreams(t *testing.T) {
	h := newTimeoutHarness(t, 200*time.Millisecond)
	h.hello(matchingHello())
	h.world.Create("pool", "")

	bad, err := h.cli.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bad.Write([]byte("not json\n")); err != nil {
		t.Fatal(err)
	}
	readAll(t, bad, "a stream with a bad header")
	readAll(t, h.openData(999, []byte("x")), "a stream no request claims")

	payload := sendStream(t, sourceWorld())
	id := h.send(rpc.MethodReceive, rpc.ReceiveParams{Spec: zfs.RecvSpec{Dataset: "pool/a"}, Codec: stream.None})
	first := h.openData(id, payload)
	time.Sleep(50 * time.Millisecond) // let the request claim the first stream
	readAll(t, h.openData(id, []byte("junk")), "a second stream for the same request")
	_ = first.Close()
	if m := h.await(id); m.Error != nil {
		t.Fatalf("receive: %+v", m.Error)
	}
	if got := <-h.ep.payloads; !bytes.Equal(got, payload) {
		t.Errorf("receive got %q, want the first stream's payload", got)
	}
	// Other requests are unaffected.
	h.ok(rpc.MethodExists, rpc.DatasetParams{Dataset: "pool/a"})
}
