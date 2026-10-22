package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/yamux"

	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/logx"
	"github.com/piotaixr/syngoid/internal/zfs"
)

// ErrConnectionLost is wrapped by every error a Remote returns because the
// connection to its agent failed.
var ErrConnectionLost = errors.New("connection to agent lost")

// cancelGrace is how long a call whose context ended waits for the agent to
// answer the cancelled request. It is a variable so tests can shorten it.
var cancelGrace = 15 * time.Second

// shutdownWait bounds how long Close waits for the shutdown reply.
const shutdownWait = 5 * time.Second

// Remote is an endpoint.Endpoint served by an agent over the protocol. It is
// safe for concurrent use.
type Remote struct {
	label string
	log   *slog.Logger
	sess  *yamux.Session
	codec *Codec

	nextID atomic.Uint64

	mu sync.Mutex
	// pending maps the id of each request awaiting its response to the
	// channel the response goes to; nil once the reader has stopped.
	pending map[uint64]chan Message
	// progress maps the id of each pending send to its progress callback.
	progress map[uint64]*progressSink
	readErr  error         // why the reader stopped; set before done closes
	done     chan struct{} // closed when the reader stops

	closeOnce sync.Once
}

var _ endpoint.Endpoint = (*Remote)(nil)

// Handshake starts a multiplexed session over conn, opens the control
// stream and exchanges hello with the agent, waiting for the reply under
// ctx. Any failure closes conn. The returned Remote, labelled label, logs
// the agent's log events to log (nil discards them); the caller must Close
// it, also when it rejects the reply.
func Handshake(ctx context.Context, conn io.ReadWriteCloser, h Hello, label string, log *slog.Logger) (*Remote, HelloReply, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	sess, err := MuxClient(conn)
	if err != nil {
		_ = conn.Close()
		return nil, HelloReply{}, fmt.Errorf("%s: handshake: %w", label, err)
	}
	// Until the reply is in, ctx ending closes the session (and conn), which
	// unblocks every step below.
	stop := context.AfterFunc(ctx, func() { _ = sess.Close() })
	defer stop()

	var reply HelloReply
	r, err := handshake(sess, h, label, log, &reply)
	if err == nil && !stop() {
		err = ctx.Err()
	}
	if err != nil {
		_ = sess.Close()
		if r != nil {
			<-r.done
		}
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		if !errors.Is(err, ErrConnectionLost) {
			err = fmt.Errorf("%s: handshake: %w", label, err)
		}
		return nil, HelloReply{}, err
	}
	return r, reply, nil
}

// handshake opens the control stream, starts the reader and sends hello.
// The Remote is non-nil whenever its reader runs.
func handshake(sess *yamux.Session, h Hello, label string, log *slog.Logger, reply *HelloReply) (*Remote, error) {
	ctrl, err := sess.OpenStream()
	if err != nil {
		return nil, err
	}
	r := &Remote{
		label:    label,
		log:      log,
		sess:     sess,
		codec:    NewCodec(ctrl),
		pending:  map[uint64]chan Message{},
		progress: map[uint64]*progressSink{},
		done:     make(chan struct{}),
	}
	go r.read()

	_, ch, err := r.request(MethodHello, h, nil)
	if err != nil {
		return r, err
	}
	select {
	case m := <-ch:
		return r, r.decode(MethodHello, m, reply)
	case <-r.done:
		return r, r.lost(nil)
	}
}

// read routes each control message to its pending call or to the logger
// until the stream fails, then stops the Remote.
func (r *Remote) read() {
	for {
		m, err := r.codec.Read()
		if err != nil {
			r.mu.Lock()
			r.pending, r.progress = nil, nil
			r.readErr = err
			r.mu.Unlock()
			close(r.done)
			// The control stream is gone or garbled: the session is useless.
			_ = r.sess.Close()
			return
		}
		if m.Event != nil {
			r.event(m.Event)
			continue
		}
		r.mu.Lock()
		ch := r.pending[m.ID]
		delete(r.pending, m.ID)
		delete(r.progress, m.ID)
		r.mu.Unlock()
		if ch != nil {
			ch <- m
		}
	}
}

// event re-logs an agent log event under the Remote's label, or hands a
// progress event to the callback of its pending send.
func (r *Remote) event(ev *Event) {
	switch ev.Kind {
	case EventLog:
		r.log.Log(context.Background(), eventLevel(ev.Level), ev.Msg, logx.KeyHost, r.label)
	case EventProgress:
		r.mu.Lock()
		p := r.progress[ev.RequestID]
		r.mu.Unlock()
		p.report(ev.Bytes)
	}
}

// eventLevel maps an Event level name to its slog level; ERROR is critical.
func eventLevel(name string) slog.Level {
	switch name {
	case "DEBUG":
		return slog.LevelDebug
	case "WARN":
		return slog.LevelWarn
	case "ERROR":
		return logx.LevelCritical
	}
	return slog.LevelInfo
}

// lost returns the error of a call that failed because the connection did;
// cause defaults to the reader's error once it has stopped.
func (r *Remote) lost(cause error) error {
	if cause == nil {
		r.mu.Lock()
		cause = r.readErr
		r.mu.Unlock()
	}
	if cause == nil || errors.Is(cause, io.EOF) {
		return fmt.Errorf("%s: %w", r.label, ErrConnectionLost)
	}
	return fmt.Errorf("%s: %w: %w", r.label, ErrConnectionLost, cause)
}

// request sends a request and returns its id and the channel its response
// will arrive on. progress, when non-nil, receives the request's progress
// events until the response or forget.
func (r *Remote) request(method string, params any, progress func(int64)) (uint64, chan Message, error) {
	m := Message{ID: r.nextID.Add(1), Method: method}
	if params != nil {
		raw, err := json.Marshal(params)
		if err != nil {
			return 0, nil, fmt.Errorf("%s: %s: encoding params: %w", r.label, method, err)
		}
		m.Params = raw
	}
	ch := make(chan Message, 1)
	r.mu.Lock()
	if r.pending == nil {
		r.mu.Unlock()
		return 0, nil, r.lost(nil)
	}
	r.pending[m.ID] = ch
	if progress != nil {
		r.progress[m.ID] = &progressSink{fn: progress}
	}
	r.mu.Unlock()
	if err := r.codec.Write(m); err != nil {
		r.forget(m.ID)
		return 0, nil, r.lost(err)
	}
	return m.ID, ch, nil
}

// forget stops waiting for the response and progress of request id.
func (r *Remote) forget(id uint64) {
	r.mu.Lock()
	p := r.progress[id]
	delete(r.pending, id)
	delete(r.progress, id)
	r.mu.Unlock()
	if p != nil {
		p.stop()
	}
}

// decode turns a response into the call's error, or decodes its result
// into result (left untouched when the response carries none).
func (r *Remote) decode(method string, m Message, result any) error {
	if m.Error != nil {
		return m.Error.Err()
	}
	if result == nil || len(m.Result) == 0 {
		return nil
	}
	if err := json.Unmarshal(m.Result, result); err != nil {
		return fmt.Errorf("%s: %s: decoding result: %w", r.label, method, err)
	}
	return nil
}

// call runs one request and decodes its result into result. When ctx ends
// first, the agent is told to cancel the request and given cancelGrace to
// answer, and the call returns ctx's error (unless the response was already
// in).
func (r *Remote) call(ctx context.Context, method string, params, result any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	id, ch, err := r.request(method, params, nil)
	if err != nil {
		return err
	}
	select {
	case m := <-ch:
		return r.decode(method, m, result)
	case <-r.done:
		// The response may have been routed just before the reader stopped.
		if m, ok := ready(ch); ok {
			return r.decode(method, m, result)
		}
		return r.lost(nil)
	case <-ctx.Done():
		if m, ok := ready(ch); ok {
			return r.decode(method, m, result)
		}
	}
	r.cancelRequest(id, ch)
	return ctx.Err()
}

// ready returns the response on ch if it has already arrived.
func ready(ch chan Message) (Message, bool) {
	select {
	case m := <-ch:
		return m, true
	default:
		return Message{}, false
	}
}

// cancelRequest tells the agent to cancel request id, waits up to
// cancelGrace for its response (or the end of the connection) and forgets
// the request.
func (r *Remote) cancelRequest(id uint64, ch chan Message) {
	cancel := Message{ID: r.nextID.Add(1), Method: MethodCancel}
	cancel.Params, _ = json.Marshal(CancelParams{ID: id})
	// Nobody waits for the cancel's own response (the reader drops it), and
	// the write runs aside so a stuck connection cannot outlast the grace.
	go func() { _ = r.codec.Write(cancel) }()
	grace := time.NewTimer(cancelGrace)
	defer grace.Stop()
	select {
	case <-ch:
	case <-r.done:
	case <-grace.C:
	}
	r.forget(id)
}

// query is call for a method with a result of type T.
func query[T any](ctx context.Context, r *Remote, method string, params any) (T, error) {
	var out T
	err := r.call(ctx, method, params, &out)
	return out, err
}

// Close asks the agent to shut down, waiting up to 5 s for its reply, and
// closes the session. It is idempotent; calls after it fail with
// ErrConnectionLost.
func (r *Remote) Close() error {
	r.closeOnce.Do(func() {
		if _, ch, err := r.request(MethodShutdown, nil, nil); err == nil {
			t := time.NewTimer(shutdownWait)
			select {
			case <-ch:
			case <-r.done:
			case <-t.C:
			}
			t.Stop()
		}
		_ = r.sess.Close()
		<-r.done
	})
	return nil
}

// Label returns the label given to Handshake.
func (r *Remote) Label() string { return r.label }

// Info returns the agent host's description.
func (r *Remote) Info(ctx context.Context) (endpoint.HostInfo, error) {
	return query[endpoint.HostInfo](ctx, r, MethodInfo, nil)
}

// ListTree lists the datasets under root, root first.
func (r *Remote) ListTree(ctx context.Context, root string, recursive bool) ([]zfs.DatasetInfo, error) {
	return query[[]zfs.DatasetInfo](ctx, r, MethodListTree, DatasetParams{Dataset: root, Recursive: recursive})
}

// ListSnapshots lists snapshots keyed by dataset.
func (r *Remote) ListSnapshots(ctx context.Context, root string, recursive bool) (map[string][]zfs.Snapshot, error) {
	return query[map[string][]zfs.Snapshot](ctx, r, MethodListSnapshots, DatasetParams{Dataset: root, Recursive: recursive})
}

// ListBookmarks lists bookmarks keyed by dataset.
func (r *Remote) ListBookmarks(ctx context.Context, root string, recursive bool) (map[string][]zfs.Bookmark, error) {
	return query[map[string][]zfs.Bookmark](ctx, r, MethodListBookmarks, DatasetParams{Dataset: root, Recursive: recursive})
}

// GetProps returns the named properties of ds.
func (r *Remote) GetProps(ctx context.Context, ds string, props ...string) (map[string]string, error) {
	return query[map[string]string](ctx, r, MethodGetProps, GetPropsParams{Dataset: ds, Props: props})
}

// LocalProps returns the local properties of ds.
func (r *Remote) LocalProps(ctx context.Context, ds string) (map[string]string, error) {
	return query[map[string]string](ctx, r, MethodLocalProps, DatasetParams{Dataset: ds})
}

// Exists reports whether ds exists.
func (r *Remote) Exists(ctx context.Context, ds string) (bool, error) {
	return query[bool](ctx, r, MethodExists, DatasetParams{Dataset: ds})
}

// Snapshot creates the named snapshots.
func (r *Remote) Snapshot(ctx context.Context, names []string) error {
	return r.call(ctx, MethodSnapshot, SnapshotParams{Names: names}, nil)
}

// Destroy destroys snaps of ds.
func (r *Remote) Destroy(ctx context.Context, ds string, snaps []string) error {
	return r.call(ctx, MethodDestroy, DestroyParams{Dataset: ds, Snaps: snaps}, nil)
}

// DestroyDataset destroys ds.
func (r *Remote) DestroyDataset(ctx context.Context, ds string, recursive bool) error {
	return r.call(ctx, MethodDestroyDataset, DatasetParams{Dataset: ds, Recursive: recursive}, nil)
}

// Hold places hold tag on snapshot.
func (r *Remote) Hold(ctx context.Context, tag, snapshot string) error {
	return r.call(ctx, MethodHold, HoldParams{Tag: tag, Snapshot: snapshot}, nil)
}

// Release removes hold tag from snapshot.
func (r *Remote) Release(ctx context.Context, tag, snapshot string) error {
	return r.call(ctx, MethodRelease, HoldParams{Tag: tag, Snapshot: snapshot}, nil)
}

// Bookmark creates bookmark from snapshot.
func (r *Remote) Bookmark(ctx context.Context, snapshot, bookmark string) error {
	return r.call(ctx, MethodBookmark, BookmarkParams{Snapshot: snapshot, Bookmark: bookmark}, nil)
}

// AbortResume discards the partial receive state of ds.
func (r *Remote) AbortResume(ctx context.Context, ds string) error {
	return r.call(ctx, MethodAbortResume, DatasetParams{Dataset: ds}, nil)
}

// SendEstimate returns the estimated stream size of s.
func (r *Remote) SendEstimate(ctx context.Context, s zfs.SendSpec) (int64, error) {
	return query[int64](ctx, r, MethodSendEstimate, SendEstimateParams{Spec: s})
}

// ReceiveBusy reports which of datasets are the target of a running receive.
func (r *Remote) ReceiveBusy(ctx context.Context, datasets []string) (map[string]bool, error) {
	return query[map[string]bool](ctx, r, MethodReceiveBusy, ReceiveBusyParams{Datasets: datasets})
}

// Allow returns the raw output of `zfs allow ds`.
func (r *Remote) Allow(ctx context.Context, ds string) (string, error) {
	return query[string](ctx, r, MethodAllow, DatasetParams{Dataset: ds})
}
