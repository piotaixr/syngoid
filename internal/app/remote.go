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

// side is one location to open and whether zfs runs through sudo there.
type side struct {
	loc  cli.Location
	sudo bool
}

// openSides returns the endpoints of both sides of o: local ones from
// env.NewLocal and remote ones from a Connector, which is created only when a
// side is remote. Two remote sides on the same user, host and sudo share one
// endpoint.
func openSides(ctx context.Context, env Env, o *cli.Options, log *slog.Logger) (sides, error) {
	eps, closeAll, err := openEach(ctx, env, o, log, side{o.Source, o.SourceSudo}, side{o.Target, o.TargetSudo})
	if err != nil {
		return sides{}, err
	}
	return sides{eps[0], eps[1], closeAll}, nil
}

// openSource opens only the source side of o, as openSides does.
func openSource(ctx context.Context, env Env, o *cli.Options, log *slog.Logger) (endpoint.Endpoint, func(), error) {
	eps, closeAll, err := openEach(ctx, env, o, log, side{o.Source, o.SourceSudo})
	if err != nil {
		return nil, nil, err
	}
	return eps[0], closeAll, nil
}

// openEach opens one endpoint per side, in order. The Connector is created
// only when a side is remote. The returned close is never nil on success.
func openEach(ctx context.Context, env Env, o *cli.Options, log *slog.Logger, want ...side) ([]endpoint.Endpoint, func(), error) {
	out := make([]endpoint.Endpoint, len(want))
	anyRemote := false
	for _, w := range want {
		anyRemote = anyRemote || w.loc.IsRemote()
	}
	if !anyRemote {
		for i, w := range want {
			out[i] = env.NewLocal(w.sudo)
		}
		return out, func() {}, nil
	}
	if env.NewConnector == nil {
		return nil, nil, errNoConnector
	}
	conn, err := env.NewConnector(o, log)
	if err != nil {
		return nil, nil, err
	}
	closeAll := func() {
		if err := conn.Close(); err != nil {
			log.Debug("closing remote connections: " + err.Error())
		}
	}
	type key struct {
		user, host string
		sudo       bool
	}
	opened := map[key]endpoint.Endpoint{}
	for i, w := range want {
		if !w.loc.IsRemote() {
			out[i] = env.NewLocal(w.sudo)
			continue
		}
		k := key{w.loc.User, w.loc.Host, w.sudo}
		if ep, ok := opened[k]; ok {
			out[i] = ep
			continue
		}
		ep, err := conn.Endpoint(ctx, w.loc, w.sudo)
		if err != nil {
			closeAll()
			return nil, nil, err
		}
		opened[k] = ep
		out[i] = ep
	}
	return out, closeAll, nil
}

// streamCodec is the codec for the zfs stream (§9.2): --compress applies
// only when the stream crosses the network.
func streamCodec(o *cli.Options) stream.Codec {
	if o.Source.IsRemote() || o.Target.IsRemote() {
		return o.Compress
	}
	return stream.None
}
