package engine

import (
	"context"
	"strconv"

	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/zfs"
)

// SendSpecFor returns the zfs send specification for send action a on s.
// The options are copied unfiltered; zfs.SendArgs and zfs.EstimateArgs
// apply the per-type allow-list. Non-send actions yield a zero spec.
func SendSpecFor(s DatasetState, a Action, opts []zfs.Opt) zfs.SendSpec {
	sp := zfs.SendSpec{Dataset: s.Source, Options: opts}
	switch a := a.(type) {
	case SendFull:
		sp.To = a.Snap
	case SendIncremental:
		sp.FromSnap, sp.To, sp.Intermediates = a.From, a.To, a.Intermediates
	case SendFromBookmark:
		sp.FromBookmark, sp.To = a.Bookmark, a.To
	case SendClone:
		sp.FromOrigin, sp.To = a.Origin, a.To
	case Resume:
		sp.ResumeToken = a.Token
	}
	return sp
}

// sendDest returns the destination snapshot of send action a. ok is false
// for non-send actions; the name is empty for a resume.
func sendDest(a Action) (name string, ok bool) {
	switch a := a.(type) {
	case SendFull:
		return a.Snap, true
	case SendIncremental:
		return a.To, true
	case SendFromBookmark:
		return a.To, true
	case SendClone:
		return a.To, true
	case Resume:
		return "", true
	}
	return "", false
}

// EstimateAction estimates the stream size of a. A send ending at an
// existing snapshot is measured with zfs send -nvP and is exact. A send
// ending at the not-yet-created sync snapshot is approximated from the
// written@ (incremental) or referenced (full, clone) property of the source
// and reports approx=true; failures give (0, true). Non-send actions give
// (0, false).
func EstimateAction(ctx context.Context, src endpoint.Endpoint, s DatasetState, a Action, opts []zfs.Opt) (bytes int64, approx bool) {
	dest, ok := sendDest(a)
	if !ok {
		return 0, false
	}
	if s.NewSyncSnap == "" || dest != s.NewSyncSnap {
		n, err := src.SendEstimate(ctx, SendSpecFor(s, a, opts))
		if err != nil {
			return 0, false
		}
		return n, false
	}
	prop := "referenced"
	switch a := a.(type) {
	case SendIncremental:
		prop = "written@" + a.From
	case SendFromBookmark:
		prop = "written#" + a.Bookmark
	}
	props, err := src.GetProps(ctx, s.Source, prop)
	if err != nil {
		return 0, true
	}
	n, err := strconv.ParseInt(props[prop], 10, 64)
	if err != nil {
		return 0, true
	}
	return n, true
}
