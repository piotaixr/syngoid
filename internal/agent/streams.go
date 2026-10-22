package agent

import (
	"bufio"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/hashicorp/yamux"

	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/rpc"
)

// dataStreamTimeout bounds how long a send or receive request waits for its
// data stream, and how long an accepted data stream may take to send its
// header and then waits for its request. It is a variable so tests can
// shorten it.
var dataStreamTimeout = 30 * time.Second

// progressInterval is the minimum time between two progress events of one
// send.
var progressInterval = 500 * time.Millisecond

// offerHook, when set as Serve starts, is told what became of each data
// stream past its header: taken is false for a stream closed as a
// duplicate, and parked is true for one that waits for its request. Tests
// use it to order streams and requests.
var offerHook func(id uint64, taken, parked bool)

// errNoDataStream fails a send or receive whose data stream never came.
var errNoDataStream = errors.New("no data stream")

// dataStream is an accepted data stream past its header; r reads the bytes
// that follow the header.
type dataStream struct {
	st *yamux.Stream
	r  *bufio.Reader
}

// unblock makes blocked and future reads and writes on the stream fail,
// without signalling its end to the initiator.
func (d *dataStream) unblock() { _ = d.st.SetDeadline(time.Now()) }

// close unblocks the stream and closes it.
func (d *dataStream) close() {
	d.unblock()
	_ = d.st.Close()
}

// slot pairs a request with its data stream, whichever arrives first.
type slot struct {
	stream     chan *dataStream // holds the stream until the request takes it
	claimed    chan struct{}    // closed when the request arrives
	hasStream  bool
	hasRequest bool
}

// rendezvous holds the slot of every request id that has a data stream or
// a send or receive request waiting for its counterpart, or in progress.
type rendezvous struct {
	mu    sync.Mutex
	slots map[uint64]*slot
}

// slotLocked returns the slot of id, creating it. rv.mu must be held.
func (rv *rendezvous) slotLocked(id uint64) *slot {
	if rv.slots == nil {
		rv.slots = map[uint64]*slot{}
	}
	sl := rv.slots[id]
	if sl == nil {
		sl = &slot{stream: make(chan *dataStream, 1), claimed: make(chan struct{})}
		rv.slots[id] = sl
	}
	return sl
}

// drop removes sl if it is still the slot of id.
func (rv *rendezvous) drop(id uint64, sl *slot) {
	rv.mu.Lock()
	if rv.slots[id] == sl {
		delete(rv.slots, id)
	}
	rv.mu.Unlock()
}

// acceptStreams accepts the data streams that follow the control stream
// until the session ends, pairing each in its own tracked goroutine.
func (s *server) acceptStreams(ctx context.Context) {
	for {
		st, err := s.sess.AcceptStream()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.offer(ctx, st)
		}()
	}
}

// offer reads the header of data stream st and hands the stream to its
// request, waiting up to dataStreamTimeout for the request to arrive. A
// stream with a bad header, for a request that already has one, or that no
// request claims in time is closed.
func (s *server) offer(ctx context.Context, st *yamux.Stream) {
	_ = st.SetReadDeadline(time.Now().Add(s.dataTimeout))
	ds := &dataStream{st: st, r: bufio.NewReader(st)}
	id, err := rpc.ReadDataHeader(ds.r)
	if err != nil {
		ds.close()
		return
	}
	_ = st.SetReadDeadline(time.Time{})

	rv := &s.streams
	rv.mu.Lock()
	sl := rv.slotLocked(id)
	if sl.hasStream {
		rv.mu.Unlock()
		ds.close()
		s.offered(id, false, false)
		return
	}
	sl.hasStream = true
	sl.stream <- ds
	claimed := sl.hasRequest
	rv.mu.Unlock()
	s.offered(id, true, !claimed)
	if claimed {
		return
	}

	t := time.NewTimer(s.dataTimeout)
	defer t.Stop()
	select {
	case <-sl.claimed:
		return
	case <-t.C:
	case <-ctx.Done():
	}
	rv.mu.Lock()
	claimed = sl.hasRequest
	if !claimed {
		delete(rv.slots, id)
	}
	rv.mu.Unlock()
	if !claimed {
		ds.close()
	}
}

func (s *server) offered(id uint64, taken, parked bool) {
	if s.offerHook != nil {
		s.offerHook(id, taken, parked)
	}
}

// claim waits up to dataStreamTimeout for the data stream of request id.
// On success the caller must call release once done with the stream.
func (s *server) claim(ctx context.Context, id uint64) (ds *dataStream, release func(), err error) {
	rv := &s.streams
	rv.mu.Lock()
	sl := rv.slotLocked(id)
	if sl.hasRequest {
		// Unreachable: the server rejects an id already in flight.
		rv.mu.Unlock()
		return nil, nil, errors.New("data stream already claimed")
	}
	sl.hasRequest = true
	close(sl.claimed)
	rv.mu.Unlock()
	release = func() { rv.drop(id, sl) }

	t := time.NewTimer(s.dataTimeout)
	defer t.Stop()
	select {
	case ds := <-sl.stream:
		return ds, release, nil
	case <-t.C:
		err = errNoDataStream
	case <-ctx.Done():
		err = ctx.Err()
	}
	release()
	// The stream may have been handed over just before the slot was dropped.
	select {
	case ds := <-sl.stream:
		ds.close()
	default:
	}
	return nil, nil, err
}

// send serves a send request: zfs send writes into the data stream, and
// progress events go out at most every progressInterval plus a final one.
// The stream is closed before the handler returns and the response is
// written, so the final progress event precedes the response.
func (s *server) send(ctx context.Context, id uint64, p rpc.SendParams) error {
	ds, release, err := s.claim(ctx, id)
	if err != nil {
		return err
	}
	defer release()
	defer ds.close()
	stop := context.AfterFunc(ctx, ds.unblock)
	defer stop()

	pr := &progress{emit: func(n int64) { s.progressEvent(id, n) }, last: time.Now()}
	err = s.ep.Send(ctx, p.Spec, endpoint.SendOpts{Codec: p.Codec, Progress: pr.report}, rpc.ChunkWriter(ds.st))
	pr.flush()
	return err
}

// receive serves a receive request: zfs receive reads the data stream from
// just past its header. The stream is closed before the response.
func (s *server) receive(ctx context.Context, id uint64, p rpc.ReceiveParams) error {
	ds, release, err := s.claim(ctx, id)
	if err != nil {
		return err
	}
	defer release()
	defer ds.close()
	stop := context.AfterFunc(ctx, ds.unblock)
	defer stop()
	return s.ep.Receive(ctx, p.Spec, p.Codec, ds.r)
}

// progressEvent sends a progress event for request id. A failed write
// means the session is gone.
func (s *server) progressEvent(id uint64, n int64) {
	_ = s.codec.Write(rpc.Message{Event: &rpc.Event{Kind: rpc.EventProgress, RequestID: id, Bytes: n}})
}

// progress throttles the progress reports of one send to one emit per
// progressInterval, counted from last.
type progress struct {
	mu     sync.Mutex
	emit   func(int64)
	last   time.Time
	latest int64
	sent   int64
}

func (p *progress) report(n int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.latest = n
	if now := time.Now(); now.Sub(p.last) >= progressInterval {
		p.last, p.sent = now, n
		p.emit(n)
	}
}

// flush emits the latest report if it was held back.
func (p *progress) flush() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.latest != p.sent {
		p.sent = p.latest
		p.emit(p.latest)
	}
}
