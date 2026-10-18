package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/stream"
	"github.com/piotaixr/syngoid/internal/zfs"
)

// errUpstream marks the stream feeding zfs receive as closed because the
// send or the copy failed, so the resulting receive error is not the
// receive's own fault.
var errUpstream = errors.New("send stream failed")

// sideErr is a transfer error attributed to the side that caused it.
type sideErr struct {
	side Side
	err  error
}

func (e *sideErr) Error() string { return e.err.Error() }
func (e *sideErr) Unwrap() error { return e.err }

// errSides reports whether a transfer error is attributed to the source or
// to the target; an unattributed error is neither.
func errSides(err error) (source, target bool) {
	var se *sideErr
	if !errors.As(err, &se) {
		return false, false
	}
	return se.side == Source, se.side == Target
}

// transfer runs send action a: Src.Send writes into pipe A, a buffered
// copy moves A into pipe B, and Dst.Receive reads B. It returns the raw
// bytes sent and an error wrapped in a sideErr. A receive error that is
// not caused by the upstream failure is the target's and takes precedence.
func (e *Executor) transfer(ctx context.Context, st DatasetState, a Action) (int64, error) {
	spec := SendSpecFor(st, a, e.Opts.SendOpts)
	est, _ := EstimateAction(ctx, e.Src, st, a, e.Opts.SendOpts)
	e.Log.Info(sendMessage(st, a, est, e.Opts.NoStream))
	args, err := e.recvArgs(ctx, st, a)
	if err != nil {
		return 0, &sideErr{Source, err}
	}

	var raw atomic.Int64
	sendOpts := endpoint.SendOpts{Codec: e.SendCodec, Progress: func(n int64) {
		raw.Store(n)
		e.Progress.Update(n)
	}}
	e.Progress.Start(st.Source, est)
	start := time.Now()

	// Send and copy failures reach the receive through the pipes, so ctx2 is
	// only cancelled when the receive fails: cancelling it earlier could kill
	// the receive before it reads the upstream error and misattribute the
	// failure to the target.
	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()
	aR, aW := io.Pipe()
	bR, bW := io.Pipe()
	var sendErr, copyErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		sendErr = e.Src.Send(ctx2, spec, sendOpts, aW)
		_ = aW.CloseWithError(sendErr)
	}()
	go func() {
		defer wg.Done()
		_, copyErr = stream.Copy(ctx2, bW, aR, e.Opts.BufferSize)
		_ = aR.CloseWithError(copyErr)
		if copyErr != nil {
			_ = bW.CloseWithError(fmt.Errorf("%w: %w", errUpstream, copyErr))
			return
		}
		_ = bW.Close()
	}()
	recvErr := e.Dst.Receive(ctx2, zfs.RecvSpec{Dataset: st.Target, Args: args}, e.RecvCodec, bR)
	if recvErr != nil {
		cancel()
	}
	_ = bR.CloseWithError(recvErr)
	wg.Wait()

	switch {
	case recvErr != nil && !errors.Is(recvErr, errUpstream):
		err = &sideErr{Target, recvErr}
	case sendErr != nil:
		err = &sideErr{Source, sendErr}
	case copyErr != nil:
		err = &sideErr{Source, copyErr}
	case recvErr != nil:
		err = &sideErr{Target, recvErr}
	}
	n := raw.Load()
	e.Progress.Finish(n, time.Since(start), err)
	return n, err
}

// recvArgs builds the zfs receive flags for send action a.
func (e *Executor) recvArgs(ctx context.Context, st DatasetState, a Action) ([]string, error) {
	_, full := a.(SendFull)
	var local map[string]string
	if e.Opts.PreserveProperties {
		var err error
		if local, err = e.Src.LocalProps(ctx, st.Source); err != nil {
			return nil, err
		}
	}
	recordsize := ""
	if e.Opts.PreserveRecordsize && full {
		props, err := e.Src.GetProps(ctx, st.Source, "recordsize")
		if err != nil {
			return nil, err
		}
		recordsize = props["recordsize"]
	}
	return RecvArgs(e.Opts, full, st.SourceType, local, recordsize, e.UnprivLinuxTarget), nil
}

// sendMessage is syncoid's INFO line announcing send action a.
func sendMessage(st DatasetState, a Action, est int64, noStream bool) string {
	size := "~ UNKNOWN"
	if est > 0 {
		size = "~ " + stream.HumanBytes(est)
	}
	src, tgt := st.Source, st.Target
	switch a := a.(type) {
	case SendFull:
		if noStream {
			return fmt.Sprintf("--no-stream selected; sending newest full snapshot %s@%s to new target filesystem %s: (%s)", src, a.Snap, tgt, size)
		}
		return fmt.Sprintf("Sending oldest full snapshot %s@%s to new target filesystem %s (%s):", src, a.Snap, tgt, size)
	case SendIncremental:
		return fmt.Sprintf("Sending incremental %s@%s ... %s to %s (%s):", src, a.From, a.To, tgt, size)
	case SendFromBookmark:
		return fmt.Sprintf("Sending incremental %s#%s ... %s to %s:", src, a.Bookmark, a.To, tgt)
	case SendClone:
		return fmt.Sprintf("Clone is recreated on target %s based on %s (%s):", tgt, a.Origin, size)
	case Resume:
		return fmt.Sprintf("Resuming interrupted zfs send/receive from %s to %s (%s remaining):", src, tgt, size)
	}
	return a.Describe()
}
