package rpc

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/hashicorp/yamux"
)

// benchBytes is what one benchmark iteration moves through one stream.
const benchBytes = 256 << 20

// osPipeConn joins the read end of one os.Pipe and the write end of another
// into the ReadWriteCloser a yamux session runs on, as ssh's stdin and
// stdout do.
type osPipeConn struct {
	r *os.File
	w *os.File
}

func (c osPipeConn) Read(p []byte) (int, error)  { return c.r.Read(p) }
func (c osPipeConn) Write(p []byte) (int, error) { return c.w.Write(p) }
func (c osPipeConn) Close() error {
	err := c.r.Close()
	if e := c.w.Close(); err == nil {
		err = e
	}
	return err
}

// benchSessions returns a client and a server yamux session joined by two
// os.Pipe pairs (one per direction), both using a window-byte stream window.
func benchSessions(b *testing.B, window uint32) (cli, srv *yamux.Session) {
	b.Helper()
	c2sR, c2sW, err := os.Pipe()
	if err != nil {
		b.Fatal(err)
	}
	s2cR, s2cW, err := os.Pipe()
	if err != nil {
		b.Fatal(err)
	}
	if cli, err = yamux.Client(osPipeConn{r: s2cR, w: c2sW}, muxConfigWindow(window)); err != nil {
		b.Fatal(err)
	}
	if srv, err = yamux.Server(osPipeConn{r: c2sR, w: s2cW}, muxConfigWindow(window)); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = cli.Close(); _ = srv.Close() })
	return cli, srv
}

// BenchmarkMuxThroughput sends 256 MiB from a client to a server through one
// stream over os.Pipe pairs, the writer wrapped in ChunkWriter as production
// does, for three stream window sizes. It resolves spec §14 item 2 for the
// in-process part: the pipes have no latency, so it measures the mux's own
// overhead, not a network.
func BenchmarkMuxThroughput(b *testing.B) {
	for _, mib := range []uint32{4, 16, 64} {
		b.Run(fmt.Sprintf("window=%dMiB", mib), func(b *testing.B) {
			cli, srv := benchSessions(b, mib<<20)
			chunk := make([]byte, 1<<20)
			b.SetBytes(benchBytes)
			b.ResetTimer()
			for range b.N {
				recv := make(chan error, 1)
				go func() {
					st, err := srv.AcceptStream()
					if err != nil {
						recv <- err
						return
					}
					n, err := io.Copy(io.Discard, st)
					if err == nil && n != benchBytes {
						err = fmt.Errorf("server read %d bytes, want %d", n, benchBytes)
					}
					_ = st.Close()
					recv <- err
				}()
				st, err := cli.OpenStream()
				if err != nil {
					b.Fatal(err)
				}
				w := ChunkWriter(st)
				for sent := 0; sent < benchBytes; sent += len(chunk) {
					if _, err := w.Write(chunk); err != nil {
						b.Fatal(err)
					}
				}
				if err := st.Close(); err != nil {
					b.Fatal(err)
				}
				if err := <-recv; err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
