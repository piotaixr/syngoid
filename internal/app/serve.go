package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/piotaixr/syngoid/internal/agent"
	"github.com/piotaixr/syngoid/internal/buildinfo"
	"github.com/piotaixr/syngoid/internal/endpoint"
)

// runServe implements the serve command: the remote agent, speaking the RPC
// protocol on stdin and stdout. Nothing but the mux writes to stdout, and
// stderr is used only for failures before the mux is up.
func runServe(ctx context.Context, env Env) int {
	// A dropped connection must surface as a failed write, not kill the
	// agent before it has cleaned up its children.
	signal.Ignore(syscall.SIGPIPE)

	// On error exe stays empty and pruning does nothing.
	exe, _ := os.Executable()
	// Without a hash the initiator's check fails and it pushes again.
	sum, err := buildinfo.ExeSHA256()
	if err != nil {
		sum = ""
	}
	cfg := agent.Config{
		NewEndpoint: func(sudo bool, log *slog.Logger) endpoint.Endpoint {
			if env.LoggerReady != nil {
				env.LoggerReady(log)
			}
			return env.NewLocal(sudo)
		},
		Home:      os.Getenv("HOME"),
		Exe:       exe,
		ExeSHA256: sum,
		Prune:     agent.PruneSiblings,
	}
	err = agent.Serve(ctx, newStdioConn(env.Stdin, env.Stdout), cfg)
	if err == nil {
		return 0
	}
	if errors.Is(err, agent.ErrSetup) {
		return criticalf(env, "serve: %v", err)
	}
	return 2
}

// stdioConn is the agent's connection: it reads the initiator's bytes from
// in and writes its own to out.
//
// Close must unblock a pending Read, which closing a blocking file such as
// os.Stdin does not do, and the mux waits for its reader before it shuts
// down. So Read is served from a pump goroutine that does the real reads; a
// closed conn abandons the pump, which ends with its read or the process.
type stdioConn struct {
	in  io.Reader
	out io.Writer

	startPump sync.Once
	chunks    chan chunk
	done      chan struct{}
	closeOnce sync.Once
	buf       []byte // rest of the last chunk
	err       error  // the pump's final error, once delivered
}

type chunk struct {
	data []byte
	err  error
}

func newStdioConn(in io.Reader, out io.Writer) *stdioConn {
	return &stdioConn{in: in, out: out, chunks: make(chan chunk), done: make(chan struct{})}
}

func (c *stdioConn) pump() {
	for {
		buf := make([]byte, 32*1024)
		n, err := c.in.Read(buf)
		if n > 0 && !c.deliver(chunk{data: buf[:n]}) {
			return
		}
		if err != nil {
			c.deliver(chunk{err: err})
			return
		}
	}
}

// deliver hands ch to Read; it reports false when the conn was closed first.
func (c *stdioConn) deliver(ch chunk) bool {
	select {
	case c.chunks <- ch:
		return true
	case <-c.done:
		return false
	}
}

// Read must not be called concurrently.
func (c *stdioConn) Read(p []byte) (int, error) {
	c.startPump.Do(func() { go c.pump() })
	for len(c.buf) == 0 {
		if c.err != nil {
			return 0, c.err
		}
		select {
		case ch := <-c.chunks:
			c.buf, c.err = ch.data, ch.err
		case <-c.done:
			return 0, io.ErrClosedPipe
		}
	}
	n := copy(p, c.buf)
	c.buf = c.buf[n:]
	return n, nil
}

func (c *stdioConn) Write(p []byte) (int, error) { return c.out.Write(p) }

// Close closes whichever of in and out can be closed and makes pending and
// later Reads fail.
func (c *stdioConn) Close() error {
	c.closeOnce.Do(func() { close(c.done) })
	var errs []error
	for _, v := range []any{c.in, c.out} {
		if cl, ok := v.(io.Closer); ok {
			errs = append(errs, cl.Close())
		}
	}
	return errors.Join(errs...)
}
