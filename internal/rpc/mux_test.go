package rpc

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
)

// muxPair returns a client and a server session over a net.Pipe whose client
// side is wrapped by wrap (nil for no wrapping).
func muxPair(t *testing.T, wrap func(io.ReadWriteCloser) io.ReadWriteCloser) (cli, srv *yamux.Session) {
	t.Helper()
	a, b := net.Pipe()
	var ca io.ReadWriteCloser = a
	if wrap != nil {
		ca = wrap(a)
	}
	var err error
	if cli, err = MuxClient(ca); err != nil {
		t.Fatalf("MuxClient: %v", err)
	}
	if srv, err = MuxServer(b); err != nil {
		t.Fatalf("MuxServer: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close(); _ = srv.Close() })
	return cli, srv
}

func TestMuxStreamsOverPipe(t *testing.T) {
	cli, srv := muxPair(t, nil)

	const size = 8 << 20
	payloads := [2][]byte{make([]byte, size), make([]byte, size)}
	for _, p := range payloads {
		if _, err := rand.Read(p); err != nil {
			t.Fatal(err)
		}
	}

	var got [2][]byte
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		cs, err := cli.OpenStream()
		if err != nil {
			t.Fatalf("OpenStream: %v", err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := cs.Write(payloads[i]); err != nil {
				t.Errorf("stream %d write: %v", i, err)
			}
			_ = cs.Close()
		}()
	}
	for i := 0; i < 2; i++ {
		ss, err := srv.AcceptStream()
		if err != nil {
			t.Fatalf("AcceptStream: %v", err)
		}
		// Streams are accepted in the order they were opened.
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, err := io.ReadAll(ss)
			if err != nil {
				t.Errorf("accepted stream %d read: %v", i, err)
			}
			got[i] = b
		}()
	}
	wg.Wait()

	for i := range got {
		if !bytes.Equal(got[i], payloads[i]) {
			t.Fatalf("stream %d: received %d bytes, content differs from what was sent", i, len(got[i]))
		}
	}
}

// throttledConn delays each Write in proportion to the bytes written, to
// model a slow link. Close unblocks any Write in progress.
type throttledConn struct {
	io.ReadWriteCloser
	bytesPerSec int
	done        chan struct{}
	once        sync.Once
}

func newThrottledConn(c io.ReadWriteCloser, bytesPerSec int) *throttledConn {
	return &throttledConn{ReadWriteCloser: c, bytesPerSec: bytesPerSec, done: make(chan struct{})}
}

func (c *throttledConn) Write(p []byte) (int, error) {
	const piece = 32 << 10
	total := 0
	for len(p) > 0 {
		n := min(len(p), piece)
		select {
		case <-time.After(time.Duration(n) * time.Second / time.Duration(c.bytesPerSec)):
		case <-c.done:
			return total, io.ErrClosedPipe
		}
		w, err := c.ReadWriteCloser.Write(p[:n])
		total += w
		if err != nil {
			return total, err
		}
		p = p[n:]
	}
	return total, nil
}

func (c *throttledConn) Close() error {
	c.once.Do(func() { close(c.done) })
	return c.ReadWriteCloser.Close()
}

func TestMuxSlowLinkNoWriteTimeout(t *testing.T) {
	old := connWriteTimeout
	connWriteTimeout = 500 * time.Millisecond
	t.Cleanup(func() { connWriteTimeout = old })

	const rate = 2 << 20
	const size = 6 << 20

	slowWrite := func(chunked bool) error {
		cli, srv := muxPair(t, func(c io.ReadWriteCloser) io.ReadWriteCloser { return newThrottledConn(c, rate) })
		go func() {
			s, err := srv.AcceptStream()
			if err != nil {
				return
			}
			_, _ = io.Copy(io.Discard, s)
		}()
		cs, err := cli.OpenStream()
		if err != nil {
			t.Fatalf("OpenStream: %v", err)
		}
		var w io.Writer = cs
		if chunked {
			w = ChunkWriter(cs)
		}
		_, err = w.Write(make([]byte, size))
		_ = cli.Close()
		_ = srv.Close()
		return err
	}

	// Control: a single large Write is sent as frames as big as the stream
	// window allows, one of which outlasts the write timeout on this link.
	if err := slowWrite(false); !errors.Is(err, yamux.ErrConnectionWriteTimeout) {
		t.Fatalf("control write without ChunkWriter: got %v, want %v", err, yamux.ErrConnectionWriteTimeout)
	}
	if err := slowWrite(true); err != nil {
		t.Fatalf("write through ChunkWriter: %v", err)
	}
}

func TestChunkWriterPieces(t *testing.T) {
	var sizes []int
	w := ChunkWriter(writerFunc(func(p []byte) (int, error) {
		sizes = append(sizes, len(p))
		return len(p), nil
	}))
	n, err := w.Write(make([]byte, 2*MaxFrameWrite+5))
	if err != nil || n != 2*MaxFrameWrite+5 {
		t.Fatalf("Write = %d, %v", n, err)
	}
	want := []int{MaxFrameWrite, MaxFrameWrite, 5}
	if len(sizes) != len(want) {
		t.Fatalf("pieces = %v, want %v", sizes, want)
	}
	for i := range want {
		if sizes[i] != want[i] {
			t.Fatalf("pieces = %v, want %v", sizes, want)
		}
	}

	wantErr := errors.New("boom")
	w = ChunkWriter(writerFunc(func(p []byte) (int, error) { return 3, wantErr }))
	if n, err := w.Write(make([]byte, 10)); n != 3 || !errors.Is(err, wantErr) {
		t.Fatalf("error Write = %d, %v; want 3, boom", n, err)
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func TestDataHeader(t *testing.T) {
	payload := []byte("raw zfs stream bytes\nwith a newline")

	a, b := net.Pipe()
	go func() {
		defer func() { _ = a.Close() }()
		if err := WriteDataHeader(a, 42); err != nil {
			t.Errorf("WriteDataHeader: %v", err)
		}
		_, _ = a.Write(payload)
	}()
	r := bufio.NewReader(b)
	id, err := ReadDataHeader(r)
	if err != nil {
		t.Fatalf("ReadDataHeader: %v", err)
	}
	if id != 42 {
		t.Fatalf("id = %d, want 42", id)
	}
	rest, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rest, payload) {
		t.Fatalf("payload after header = %q, want %q", rest, payload)
	}

	var buf bytes.Buffer
	if err := WriteDataHeader(&buf, 7); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "{\"request_id\":7}\n" {
		t.Fatalf("header bytes = %q", buf.String())
	}

	for _, tc := range []struct{ name, in string }{
		{"too long", strings.Repeat("x", 5000) + "\n"},
		{"id zero", "{\"request_id\":0}\n"},
		{"missing id", "{}\n"},
		{"bad json", "not json\n"},
		{"no newline", "{\"request_id\":3}"},
		{"empty", ""},
	} {
		if _, err := ReadDataHeader(bufio.NewReader(strings.NewReader(tc.in))); err == nil {
			t.Errorf("%s: expected an error", tc.name)
		}
	}
}

func TestDataHeaderLeavesPayloadInReader(t *testing.T) {
	// Header and more than a buffer's worth of payload arrive in one write:
	// the reader must hold on to what it read past the header.
	payload := bytes.Repeat([]byte("0123456789abcdef"), 600)
	var b bytes.Buffer
	if err := WriteDataHeader(&b, 9); err != nil {
		t.Fatal(err)
	}
	b.Write(payload)

	a, c := net.Pipe()
	go func() {
		defer func() { _ = a.Close() }()
		_, _ = a.Write(b.Bytes())
	}()
	r := bufio.NewReader(c)
	if id, err := ReadDataHeader(r); err != nil || id != 9 {
		t.Fatalf("ReadDataHeader = %d, %v, want 9", id, err)
	}
	if r.Buffered() == 0 {
		t.Fatal("no payload bytes buffered after the header")
	}
	rest, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rest, payload) {
		t.Fatalf("got %d bytes after the header, want exactly the %d payload bytes", len(rest), len(payload))
	}
}

func TestMuxConfigSilent(t *testing.T) {
	cfg := muxConfig()
	if cfg.LogOutput != io.Discard {
		t.Errorf("LogOutput = %v, want io.Discard", cfg.LogOutput)
	}
	if cfg.EnableKeepAlive {
		t.Error("EnableKeepAlive = true, want false")
	}
	if cfg.MaxStreamWindowSize != MaxStreamWindow {
		t.Errorf("MaxStreamWindowSize = %d, want %d", cfg.MaxStreamWindowSize, MaxStreamWindow)
	}
	if cfg.ConnectionWriteTimeout != connWriteTimeout {
		t.Errorf("ConnectionWriteTimeout = %v, want %v", cfg.ConnectionWriteTimeout, connWriteTimeout)
	}
	if err := yamux.VerifyConfig(cfg); err != nil {
		t.Errorf("VerifyConfig: %v", err)
	}
}
