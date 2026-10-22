package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/signal"
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

	exe, err := os.Executable()
	if err != nil {
		exe = ""
	}
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
	err = agent.Serve(ctx, stdioConn{in: env.Stdin, out: env.Stdout}, cfg)
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
type stdioConn struct {
	in  io.Reader
	out io.Writer
}

func (c stdioConn) Read(p []byte) (int, error)  { return c.in.Read(p) }
func (c stdioConn) Write(p []byte) (int, error) { return c.out.Write(p) }

// Close closes whichever of in and out can be closed, which unblocks a
// pending Read on a pipe.
func (c stdioConn) Close() error {
	var errs []error
	for _, v := range []any{c.in, c.out} {
		if cl, ok := v.(io.Closer); ok {
			errs = append(errs, cl.Close())
		}
	}
	return errors.Join(errs...)
}
