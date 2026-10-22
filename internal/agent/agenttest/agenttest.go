// Package agenttest connects an rpc.Remote to an in-process agent for tests.
package agenttest

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/piotaixr/syngoid/internal/agent"
	"github.com/piotaixr/syngoid/internal/buildinfo"
	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/rpc"
)

// wait bounds the handshake and the agent's exit at cleanup.
const wait = 5 * time.Second

// Start serves ep through agent.Serve on one end of a net.Pipe and returns
// the Remote handshaken on the other end, labelled label. debug asks the
// agent for debug log events, which log receives (nil discards them).
// Cleanup closes the Remote and waits for Serve to return.
func Start(t testing.TB, ep endpoint.Endpoint, label string, debug bool, log *slog.Logger) *rpc.Remote {
	t.Helper()
	r, _ := StartConn(t, func(bool, *slog.Logger) endpoint.Endpoint { return ep }, label, debug, log)
	return r
}

// StartConn is Start with an agent-side endpoint factory (which receives the
// agent's event logger) that also returns the initiator's end of the pipe,
// for tests that break the connection.
func StartConn(t testing.TB, newEP func(sudo bool, log *slog.Logger) endpoint.Endpoint, label string, debug bool, log *slog.Logger) (*rpc.Remote, net.Conn) {
	t.Helper()
	a, b := net.Pipe()
	served := make(chan error, 1)
	go func() { served <- agent.Serve(context.Background(), b, agent.Config{NewEndpoint: newEP}) }()

	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	hello := rpc.Hello{
		Protocol:  buildinfo.ProtocolVersion,
		Version:   buildinfo.Version,
		BuildHash: buildinfo.BuildHash(),
		Debug:     debug,
	}
	r, _, err := rpc.Handshake(ctx, a, hello, label, log)
	if err != nil {
		_ = b.Close()
		t.Fatalf("agenttest: handshake: %v", err)
	}
	t.Cleanup(func() {
		_ = r.Close()
		select {
		case err := <-served:
			if err != nil {
				t.Errorf("agenttest: agent.Serve: %v", err)
			}
		case <-time.After(wait):
			t.Errorf("agenttest: agent.Serve did not return")
		}
	})
	return r, a
}
