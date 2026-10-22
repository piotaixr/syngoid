package app

import (
	"context"
	"errors"
	"log/slog"

	"github.com/piotaixr/syngoid/internal/cli"
	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/stream"
)

// Connector reaches remote hosts for one run.
type Connector interface {
	// Endpoint returns the endpoint of loc's host, running zfs through
	// sudo there when sudo is set. Errors are final and already name the
	// host.
	Endpoint(ctx context.Context, loc cli.Location, sudo bool) (endpoint.Endpoint, error)
	// Close ends every connection. It is called once, after the run.
	Close() error
}

// errNoConnector is the failure of a remote side when Env.NewConnector is nil.
var errNoConnector = errors.New("remote endpoints are not configured")

// sides holds the endpoints of a run.
type sides struct {
	src, dst endpoint.Endpoint
	// close releases the connections; it is never nil.
	close func()
}

// openSides returns the endpoints of both sides of o: local ones from
// env.NewLocal and remote ones from a Connector, which is created only when a
// side is remote. Two remote sides on the same user, host and sudo share one
// endpoint.
func openSides(ctx context.Context, env Env, o *cli.Options, log *slog.Logger) (sides, error) {
	if !o.Source.IsRemote() && !o.Target.IsRemote() {
		return sides{env.NewLocal(o.SourceSudo), env.NewLocal(o.TargetSudo), func() {}}, nil
	}
	if env.NewConnector == nil {
		return sides{}, errNoConnector
	}
	conn, err := env.NewConnector(o, log)
	if err != nil {
		return sides{}, err
	}
	s := sides{close: func() {
		if err := conn.Close(); err != nil {
			log.Debug("closing remote connections: " + err.Error())
		}
	}}
	type key struct {
		user, host string
		sudo       bool
	}
	opened := map[key]endpoint.Endpoint{}
	open := func(loc cli.Location, sudo bool) (endpoint.Endpoint, error) {
		if !loc.IsRemote() {
			return env.NewLocal(sudo), nil
		}
		k := key{loc.User, loc.Host, sudo}
		if ep, ok := opened[k]; ok {
			return ep, nil
		}
		ep, err := conn.Endpoint(ctx, loc, sudo)
		if err != nil {
			return nil, err
		}
		opened[k] = ep
		return ep, nil
	}
	if s.src, err = open(o.Source, o.SourceSudo); err == nil {
		s.dst, err = open(o.Target, o.TargetSudo)
	}
	if err != nil {
		s.close()
		return sides{}, err
	}
	return s, nil
}

// streamCodec is the codec for the zfs stream (§9.2): --compress applies
// only when the stream crosses the network.
func streamCodec(o *cli.Options) stream.Codec {
	if o.Source.IsRemote() || o.Target.IsRemote() {
		return o.Compress
	}
	return stream.None
}
