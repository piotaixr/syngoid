// Package agent implements the remote side of the syngoid protocol: it
// serves RPC requests from an initiator over a multiplexed connection and
// runs each of them on an endpoint.Endpoint.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"sync"

	"github.com/hashicorp/yamux"

	"github.com/piotaixr/syngoid/internal/buildinfo"
	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/rpc"
)

// Config configures Serve.
type Config struct {
	// NewEndpoint builds the endpoint requests run on; sudo is Hello.Sudo
	// and log sends its records to the initiator as log events.
	NewEndpoint func(sudo bool, log *slog.Logger) endpoint.Endpoint
	// Home is the agent user's home directory, Exe the path of the running
	// executable and ExeSHA256 its hex SHA-256; the handshake reports Home
	// and ExeSHA256.
	Home, Exe, ExeSHA256 string
	// Prune, when non-nil, removes stale uploaded agent binaries. It runs
	// once, after a handshake with a matching initiator.
	Prune func(exe, home string, log *slog.Logger)
}

// Serve speaks the protocol on conn until the initiator asks for shutdown
// (nil), the connection or ctx ends, or the handshake fails. Every
// in-flight request is cancelled and has returned when Serve does.
func Serve(ctx context.Context, conn io.ReadWriteCloser, cfg Config) error {
	sess, err := rpc.MuxServer(conn)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("agent: mux: %w", err)
	}
	defer func() { _ = sess.Close() }()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Closing the session unblocks AcceptStream and the control reads.
	go func() {
		<-ctx.Done()
		_ = sess.Close()
	}()

	ctrl, err := sess.AcceptStream()
	if err != nil {
		return sessionErr(ctx, err)
	}
	s := &server{cfg: cfg, sess: sess, codec: rpc.NewCodec(ctrl), inflight: map[uint64]context.CancelFunc{}}
	// Deferred after cancel, so it runs first: stop everything, then wait.
	defer s.wg.Wait()
	defer cancel()

	if err := s.handshake(ctx); err != nil {
		return err
	}
	return s.loop(ctx)
}

// sessionErr turns a read or accept error into Serve's result: nil when the
// initiator went away cleanly, ctx's error when ctx ended.
func sessionErr(ctx context.Context, err error) error {
	switch {
	case ctx.Err() != nil:
		return ctx.Err()
	case errors.Is(err, io.EOF), errors.Is(err, yamux.ErrSessionShutdown), errors.Is(err, yamux.ErrStreamClosed):
		return nil
	}
	return err
}

type server struct {
	cfg   Config
	sess  *yamux.Session
	codec *rpc.Codec
	log   *slog.Logger
	ep    endpoint.Endpoint

	wg       sync.WaitGroup
	mu       sync.Mutex
	inflight map[uint64]context.CancelFunc
}

// handshake reads the hello request and answers it.
func (s *server) handshake(ctx context.Context) error {
	req, err := s.codec.Read()
	if err != nil {
		return sessionErr(ctx, err)
	}
	if req.Method != rpc.MethodHello {
		err := &rpc.InvalidError{Field: "method", Value: req.Method}
		s.respond(req.ID, nil, err)
		return fmt.Errorf("agent: first message must be hello: %w", err)
	}
	var hello rpc.Hello
	if err := decodeParams(req.Params, &hello); err != nil {
		s.respond(req.ID, nil, err)
		return fmt.Errorf("agent: bad hello: %w", err)
	}

	level := slog.LevelInfo
	if hello.Debug {
		level = slog.LevelDebug
	}
	s.log = newEventLogger(s.codec, level)
	s.ep = s.cfg.NewEndpoint(hello.Sudo, s.log)
	info, err := s.ep.Info(ctx)
	if err != nil {
		s.respond(req.ID, nil, err)
		return fmt.Errorf("agent: host info: %w", err)
	}
	reply := rpc.HelloReply{
		Protocol:   buildinfo.ProtocolVersion,
		Version:    buildinfo.Version,
		BuildHash:  buildinfo.BuildHash(),
		ExeSHA256:  s.cfg.ExeSHA256,
		OS:         runtime.GOOS,
		Arch:       runtime.GOARCH,
		User:       info.User,
		UID:        info.UID,
		Groups:     info.Groups,
		ZFSVersion: info.ZFSVersion,
		Home:       s.cfg.Home,
	}
	s.respond(req.ID, reply, nil)

	if s.cfg.Prune != nil && matches(hello, reply) {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() {
				// Pruning is housekeeping: a panic must neither crash the
				// agent nor reach its stderr.
				if r := recover(); r != nil {
					s.log.Debug(fmt.Sprintf("prune panicked: %v", r))
				}
			}()
			s.cfg.Prune(s.cfg.Exe, s.cfg.Home, s.log)
		}()
	}
	return nil
}

// matches reports whether the initiator is the same build as this agent:
// same protocol and version and, for development builds, same build hash.
func matches(h rpc.Hello, r rpc.HelloReply) bool {
	if h.Protocol != r.Protocol || h.Version != r.Version {
		return false
	}
	return !buildinfo.IsDev() || h.BuildHash == r.BuildHash
}

// loop reads requests until the session ends or shutdown is requested.
func (s *server) loop(ctx context.Context) error {
	for {
		req, err := s.codec.Read()
		if err != nil {
			return sessionErr(ctx, err)
		}
		if req.Event != nil {
			continue
		}
		switch req.Method {
		case rpc.MethodShutdown:
			s.respond(req.ID, nil, nil)
			return nil
		case rpc.MethodCancel:
			s.cancelRequest(req)
		default:
			s.start(ctx, req)
		}
	}
}

// cancelRequest cancels the request named by a cancel request and answers it.
// Cancelling a request that already finished is not an error.
func (s *server) cancelRequest(req rpc.Message) {
	var p rpc.CancelParams
	if err := decodeParams(req.Params, &p); err != nil {
		s.respond(req.ID, nil, err)
		return
	}
	if err := rpc.Validate(rpc.MethodCancel, p); err != nil {
		s.respond(req.ID, nil, err)
		return
	}
	s.mu.Lock()
	cancel := s.inflight[p.ID]
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	s.respond(req.ID, nil, nil)
}

// start runs req in its own goroutine with its own context.
func (s *server) start(ctx context.Context, req rpc.Message) {
	h, ok := handlers[req.Method]
	if !ok {
		s.respond(req.ID, nil, &methodError{req.Method})
		return
	}
	reqCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	_, dup := s.inflight[req.ID]
	if !dup {
		s.inflight[req.ID] = cancel
	}
	s.mu.Unlock()
	if dup {
		cancel()
		s.respond(req.ID, nil, &rpc.InvalidError{Field: "id", Value: fmt.Sprint(req.ID)})
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer cancel()
		res, err := s.run(reqCtx, h, req)
		if err != nil && reqCtx.Err() != nil && !errors.Is(err, context.Canceled) {
			// A killed command reports its exit status, not the context
			// error; the request was cancelled all the same.
			err = fmt.Errorf("%w: %w", context.Canceled, err)
		}
		// Forget the id before answering, so a client that reuses it as
		// soon as it has the response is not rejected as a duplicate.
		s.mu.Lock()
		delete(s.inflight, req.ID)
		s.mu.Unlock()
		s.respond(req.ID, res, err)
	}()
}

// run calls h, turning a panic into an error so one bad request cannot take
// the agent down.
func (s *server) run(ctx context.Context, h handler, req rpc.Message) (res any, err error) {
	defer func() {
		if r := recover(); r != nil {
			res, err = nil, &panicError{r}
		}
	}()
	return h(ctx, s, req)
}

// respond writes the response to request id. A failed write means the
// session is gone, which is not worth reporting.
func (s *server) respond(id uint64, result any, err error) {
	m := rpc.Message{ID: id}
	switch {
	case err != nil:
		m.Error = encodeError(err)
	case result != nil:
		raw, merr := json.Marshal(result)
		if merr != nil {
			m.Error = &rpc.Error{Code: rpc.CodeInternal, Message: "encoding result: " + merr.Error()}
			break
		}
		m.Result = raw
	}
	_ = s.codec.Write(m)
}

// methodError is the error of a request for a method the agent lacks.
type methodError struct{ method string }

func (e *methodError) Error() string { return "unknown method " + e.method }

// panicError is the error of a request whose handler panicked.
type panicError struct{ value any }

func (e *panicError) Error() string { return fmt.Sprintf("internal error: %v", e.value) }

// encodeError is rpc.EncodeError plus the agent's own error kinds.
func encodeError(err error) *rpc.Error {
	var me *methodError
	if errors.As(err, &me) {
		return &rpc.Error{Code: rpc.CodeUnknownMethod, Message: err.Error()}
	}
	return rpc.EncodeError(err)
}

// decodeParams decodes raw into p; absent params leave p at its zero value.
// A failure is an *rpc.InvalidError on "params".
func decodeParams(raw json.RawMessage, p any) error {
	if len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, p); err != nil {
		return &rpc.InvalidError{Field: "params", Value: err.Error()}
	}
	return nil
}
