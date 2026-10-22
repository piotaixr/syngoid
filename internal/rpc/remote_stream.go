package rpc

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/yamux"

	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/stream"
	"github.com/piotaixr/syngoid/internal/zfs"
)

// Send runs zfs send for s on the agent, which compresses the stream with
// o.Codec, and copies the stream into w. o.Progress, when set, receives the
// agent's progress reports on the Remote's reader goroutine, so it must be
// quick and must not call the Remote. Send returns once the agent has
// answered (or cancelGrace has passed after a cancel), so the remote zfs
// send has exited. It never starts a write to w after returning; a Write in
// progress may outlive Send, and the caller unblocks it by closing w, so a
// stalled w cannot block the return. If w fails, the agent is told to
// cancel and Send returns w's error; if ctx ends first, the agent is told
// to cancel and Send returns ctx's error.
func (r *Remote) Send(ctx context.Context, s zfs.SendSpec, o endpoint.SendOpts, w io.Writer) error {
	t, err := r.openTransfer(ctx, MethodSend, SendParams{Spec: s, Codec: o.Codec}, o.Progress)
	if err != nil {
		return err
	}
	defer t.close()
	sink := &sendSink{w: w}
	var copyErr error // the copy's error; read once copied is closed
	copied := make(chan struct{})
	go func() {
		defer close(copied)
		_, copyErr = io.CopyBuffer(sink, t.st, make([]byte, MaxFrameWrite))
	}()
	// halt ends the copy early: it starts no further write to w and its
	// reads fail. It is not waited for, as it may be stuck in w.Write.
	halt := func() {
		sink.halted.Store(true)
		t.unblock()
	}

	var m Message
	select {
	case <-copied:
		if sink.err != nil {
			t.abandon()
			return sink.err
		}
		// The agent closed the stream, or the stream broke: the response
		// says which.
		return t.await(ctx, copyErr)
	case m = <-t.ch:
	case <-r.done:
		var ok bool
		if m, ok = ready(t.ch); !ok {
			halt()
			return r.lost(nil)
		}
	case <-ctx.Done():
		halt()
		t.abandon()
		return ctx.Err()
	}
	if m.Error != nil {
		halt()
		return m.Error.Err()
	}
	// The agent closed the stream before answering, so the rest of it is
	// already here: deliver it, for as long as ctx allows.
	select {
	case <-copied:
		if sink.err != nil {
			return sink.err
		}
		if copyErr != nil {
			return r.lost(copyErr)
		}
		return nil
	case <-ctx.Done():
		halt()
		return ctx.Err()
	}
}

// Receive runs zfs receive for s on the agent, which decodes the stream
// with codec, feeding it what it reads from rd. It returns once the agent
// has answered (or cancelGrace has passed after a cancel), so the remote
// zfs receive has exited. If reading rd fails, the agent is told to cancel
// and Receive returns the read error unchanged; if ctx ends first, the
// agent is told to cancel and Receive returns ctx's error. As with
// endpoint.Local, when Receive fails a Read of rd may still be in progress;
// the caller unblocks it by closing rd.
func (r *Remote) Receive(ctx context.Context, s zfs.RecvSpec, codec stream.Codec, rd io.Reader) error {
	t, err := r.openTransfer(ctx, MethodReceive, ReceiveParams{Spec: s, Codec: codec}, nil)
	if err != nil {
		return err
	}
	defer t.close()
	src := &recvSource{r: rd}
	copied := make(chan struct{})
	go func() {
		defer close(copied)
		if _, err := io.CopyBuffer(ChunkWriter(t.st), src, make([]byte, MaxFrameWrite)); err == nil {
			_ = t.st.Close() // the agent reads the end of the stream
		}
	}()

	var m Message
	select {
	case <-copied:
		if src.err != nil {
			t.abandon()
			return src.err
		}
		// A failed write means the agent or the connection is done: the
		// response says which.
		return t.await(ctx, nil)
	case m = <-t.ch:
	case <-r.done:
		var ok bool
		if m, ok = ready(t.ch); !ok {
			return r.lost(nil)
		}
	case <-ctx.Done():
		t.unblock()
		t.abandon()
		return ctx.Err()
	}
	// The agent no longer reads: end the copy's writes.
	t.close()
	if m.Error != nil {
		return m.Error.Err()
	}
	// As with endpoint.Local, a genuine read error wins over success.
	<-copied
	return src.err
}

// transfer is a send or receive request with its data stream.
type transfer struct {
	r  *Remote
	id uint64
	ch chan Message // the response
	st *yamux.Stream
}

// openTransfer sends the request, then opens its data stream and writes the
// header. progress receives the request's progress events.
func (r *Remote) openTransfer(ctx context.Context, method string, params any, progress func(int64)) (*transfer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	id, ch, err := r.request(method, params, progress)
	if err != nil {
		return nil, err
	}
	t := &transfer{r: r, id: id, ch: ch}
	if t.st, err = r.sess.OpenStream(); err == nil {
		if err = WriteDataHeader(t.st, id); err != nil {
			_ = t.st.Close()
		}
	}
	if err != nil {
		t.abandon()
		return nil, r.lost(err)
	}
	return t, nil
}

// await waits for the response once the copy has ended and returns the
// agent's error, or the copy's stream error copyErr as a lost connection.
func (t *transfer) await(ctx context.Context, copyErr error) error {
	var m Message
	select {
	case m = <-t.ch:
	case <-t.r.done:
		var ok bool
		if m, ok = ready(t.ch); !ok {
			return t.r.lost(nil)
		}
	case <-ctx.Done():
		var ok bool
		if m, ok = ready(t.ch); !ok {
			t.abandon()
			return ctx.Err()
		}
	}
	if m.Error != nil {
		return m.Error.Err()
	}
	if copyErr != nil {
		return t.r.lost(copyErr)
	}
	return nil
}

// abandon tells the agent to cancel the request and waits up to
// cancelGrace for its response. The data stream stays open meanwhile, so
// the agent sees the cancel rather than what could pass for the end of a
// complete stream.
func (t *transfer) abandon() { t.r.cancelRequest(t.id, t.ch) }

// unblock makes blocked and future reads and writes on the data stream
// fail, without signalling its end to the agent.
func (t *transfer) unblock() { _ = t.st.SetDeadline(time.Now()) }

// close unblocks and closes the data stream and forgets the request.
func (t *transfer) close() {
	t.unblock()
	_ = t.st.Close()
	t.r.forget(t.id)
}

// progressSink is the progress callback of one send. The reader calls it,
// so it must be quick; it is never called once the request is forgotten.
type progressSink struct {
	mu sync.Mutex
	fn func(int64)
}

// report calls the callback; p may be nil.
func (p *progressSink) report(n int64) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fn != nil {
		p.fn(n)
	}
}

func (p *progressSink) stop() {
	p.mu.Lock()
	p.fn = nil
	p.mu.Unlock()
}

// errHalted ends Send's copy once Send has decided to return.
var errHalted = errors.New("rpc: send halted")

// sendSink is the w of Send as its copy writes to it: it records w's
// error and, once halted, refuses every write.
type sendSink struct {
	w      io.Writer
	halted atomic.Bool
	err    error // w's error; read once the copy has returned
}

func (s *sendSink) Write(p []byte) (int, error) {
	if s.halted.Load() {
		return 0, errHalted
	}
	n, err := s.w.Write(p)
	if err == nil && n < len(p) {
		err = io.ErrShortWrite
	}
	if err != nil {
		s.err = err
	}
	return n, err
}

// recvSource is the rd of Receive as its copy reads it: it records the
// read error, EOF aside.
type recvSource struct {
	r   io.Reader
	err error // read once the copy has returned
}

func (s *recvSource) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	if err != nil && err != io.EOF {
		s.err = err
	}
	return n, err
}
