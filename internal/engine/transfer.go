package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"

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
	e.transferStarted()
	h := e.Progress.Start(st.Source, est)
	sendOpts := endpoint.SendOpts{Codec: e.SendCodec, Progress: func(n int64) {
		raw.Store(n)
		h.Update(n)
	}}

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
		h.Sent()
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
	e.transferEnded(ctx)
	h.Finish(n, err)
	return n, err
}

// transferStarted counts a running transfer for the abort warning.
func (e *Executor) transferStarted() {
	e.abortMu.Lock()
	defer e.abortMu.Unlock()
	e.transfers++
}

// transferEnded stops counting a transfer. When ctx is cancelled, the abort
// is decided first, so a transfer ending before the dispatcher's
// cancellation callback ran still counts in the warning.
func (e *Executor) transferEnded(ctx context.Context) {
	e.abortMu.Lock()
	n := 0
	if ctx.Err() != nil {
		n = e.decideAbortLocked()
	}
	e.transfers--
	e.abortMu.Unlock()
	e.reportAbort(n)
}

// abortNow decides the abort of this run, when not yet decided, and
// reports it.
func (e *Executor) abortNow() {
	e.abortMu.Lock()
	n := e.decideAbortLocked()
	e.abortMu.Unlock()
	e.reportAbort(n)
}

// decideAbortLocked returns the number of running transfers the abort
// warning names the first time it is called in a run, and 0 afterwards.
// e.abortMu is held.
func (e *Executor) decideAbortLocked() int {
	if e.abortDecided {
		return 0
	}
	e.abortDecided = true
	return e.transfers
}

// reportAbort logs the abort warning for n running transfers and switches
// the progress rows to aborting; it does nothing when n is 0.
func (e *Executor) reportAbort(n int) {
	if n == 0 {
		return
	}
	e.Log.Warn(fmt.Sprintf("aborting %d running transfer(s); press Ctrl-C again to exit immediately", n))
	e.Progress.Aborting()
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
	if e.UnprivLinuxTarget {
		e.warnDroppedProps(st, local)
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

// warnDroppedProps warns, once per dataset, that the preserved properties
// mountpoint and canmount are dropped for an unprivileged Linux target
// (spec §6.1).
func (e *Executor) warnDroppedProps(st DatasetState, local map[string]string) {
	dropped := UnprivDroppedProps(local)
	if len(dropped) == 0 {
		return
	}
	e.mu.Lock()
	warned := e.warnedUnpriv[st.Source]
	if !warned {
		if e.warnedUnpriv == nil {
			e.warnedUnpriv = map[string]bool{}
		}
		e.warnedUnpriv[st.Source] = true
	}
	e.mu.Unlock()
	if warned {
		return
	}
	verb := "they cannot"
	if len(dropped) == 1 {
		verb = "it cannot"
	}
	e.Log.Warn(fmt.Sprintf("%s: not preserving %s: %s be set on unprivileged Linux target %s",
		st.Source, strings.Join(dropped, ", "), verb, st.Target))
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
