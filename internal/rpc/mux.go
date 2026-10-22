package rpc

import (
	"io"
	"time"

	"github.com/hashicorp/yamux"
)

const (
	// MaxStreamWindow is the yamux per-stream receive window.
	MaxStreamWindow = 16 << 20
	// MaxFrameWrite bounds the bytes ChunkWriter hands to yamux at once.
	MaxFrameWrite = 256 << 10
)

// connWriteTimeout is yamux's safety valve on a single frame write. It is a
// variable so tests can shorten it.
var connWriteTimeout = 2 * time.Minute

// muxConfig returns the yamux configuration shared by both ends. Keepalive
// is off (ssh already detects a dead link) and logging is discarded, as the
// agent must never write to its stderr once the mux is up.
func muxConfig() *yamux.Config {
	cfg := yamux.DefaultConfig()
	cfg.EnableKeepAlive = false
	cfg.MaxStreamWindowSize = MaxStreamWindow
	cfg.ConnectionWriteTimeout = connWriteTimeout
	cfg.LogOutput = io.Discard
	return cfg
}

// MuxClient starts the initiating end of a yamux session over conn.
func MuxClient(conn io.ReadWriteCloser) (*yamux.Session, error) {
	return yamux.Client(conn, muxConfig())
}

// MuxServer starts the accepting end of a yamux session over conn.
func MuxServer(conn io.ReadWriteCloser) (*yamux.Session, error) {
	return yamux.Server(conn, muxConfig())
}

// ChunkWriter returns a writer that forwards each Write to w in pieces of at
// most MaxFrameWrite bytes. yamux sends a large Write as a single frame as
// big as the stream window allows, and its connection write timeout covers
// the whole frame, so on a slow link big frames time out.
func ChunkWriter(w io.Writer) io.Writer { return chunkWriter{w} }

type chunkWriter struct{ w io.Writer }

func (c chunkWriter) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		n, err := c.w.Write(p[:min(len(p), MaxFrameWrite)])
		total += n
		if err != nil {
			return total, err
		}
		p = p[n:]
	}
	return total, nil
}
