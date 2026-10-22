package agent

import (
	"context"

	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/rpc"
)

// handler runs one request and returns its result (nil for none).
type handler func(ctx context.Context, s *server, req rpc.Message) (any, error)

// typed adapts fn to a handler: it decodes the request's params into a P,
// validates that value, then calls fn with it.
func typed[P any](method string, fn func(ctx context.Context, ep endpoint.Endpoint, p P) (any, error)) handler {
	return func(ctx context.Context, s *server, req rpc.Message) (any, error) {
		var p P
		if err := decodeParams(req.Params, &p); err != nil {
			return nil, err
		}
		if err := rpc.Validate(method, p); err != nil {
			return nil, err
		}
		return fn(ctx, s.ep, p)
	}
}

// handlers maps each method served as a plain request/response to its
// handler. hello, cancel and shutdown are handled by the server loop.
var handlers = map[string]handler{
	rpc.MethodInfo: typed(rpc.MethodInfo, func(ctx context.Context, ep endpoint.Endpoint, _ struct{}) (any, error) {
		return ep.Info(ctx)
	}),
	rpc.MethodListTree: typed(rpc.MethodListTree, func(ctx context.Context, ep endpoint.Endpoint, p rpc.DatasetParams) (any, error) {
		return ep.ListTree(ctx, p.Dataset, p.Recursive)
	}),
	rpc.MethodListSnapshots: typed(rpc.MethodListSnapshots, func(ctx context.Context, ep endpoint.Endpoint, p rpc.DatasetParams) (any, error) {
		return ep.ListSnapshots(ctx, p.Dataset, p.Recursive)
	}),
	rpc.MethodListBookmarks: typed(rpc.MethodListBookmarks, func(ctx context.Context, ep endpoint.Endpoint, p rpc.DatasetParams) (any, error) {
		return ep.ListBookmarks(ctx, p.Dataset, p.Recursive)
	}),
	rpc.MethodGetProps: typed(rpc.MethodGetProps, func(ctx context.Context, ep endpoint.Endpoint, p rpc.GetPropsParams) (any, error) {
		return ep.GetProps(ctx, p.Dataset, p.Props...)
	}),
	rpc.MethodLocalProps: typed(rpc.MethodLocalProps, func(ctx context.Context, ep endpoint.Endpoint, p rpc.DatasetParams) (any, error) {
		return ep.LocalProps(ctx, p.Dataset)
	}),
	rpc.MethodExists: typed(rpc.MethodExists, func(ctx context.Context, ep endpoint.Endpoint, p rpc.DatasetParams) (any, error) {
		return ep.Exists(ctx, p.Dataset)
	}),
	rpc.MethodSnapshot: typed(rpc.MethodSnapshot, func(ctx context.Context, ep endpoint.Endpoint, p rpc.SnapshotParams) (any, error) {
		return nil, ep.Snapshot(ctx, p.Names)
	}),
	rpc.MethodDestroy: typed(rpc.MethodDestroy, func(ctx context.Context, ep endpoint.Endpoint, p rpc.DestroyParams) (any, error) {
		return nil, ep.Destroy(ctx, p.Dataset, p.Snaps)
	}),
	rpc.MethodDestroyDataset: typed(rpc.MethodDestroyDataset, func(ctx context.Context, ep endpoint.Endpoint, p rpc.DatasetParams) (any, error) {
		return nil, ep.DestroyDataset(ctx, p.Dataset, p.Recursive)
	}),
	rpc.MethodHold: typed(rpc.MethodHold, func(ctx context.Context, ep endpoint.Endpoint, p rpc.HoldParams) (any, error) {
		return nil, ep.Hold(ctx, p.Tag, p.Snapshot)
	}),
	rpc.MethodRelease: typed(rpc.MethodRelease, func(ctx context.Context, ep endpoint.Endpoint, p rpc.HoldParams) (any, error) {
		return nil, ep.Release(ctx, p.Tag, p.Snapshot)
	}),
	rpc.MethodBookmark: typed(rpc.MethodBookmark, func(ctx context.Context, ep endpoint.Endpoint, p rpc.BookmarkParams) (any, error) {
		return nil, ep.Bookmark(ctx, p.Snapshot, p.Bookmark)
	}),
	rpc.MethodAbortResume: typed(rpc.MethodAbortResume, func(ctx context.Context, ep endpoint.Endpoint, p rpc.DatasetParams) (any, error) {
		return nil, ep.AbortResume(ctx, p.Dataset)
	}),
	rpc.MethodSendEstimate: typed(rpc.MethodSendEstimate, func(ctx context.Context, ep endpoint.Endpoint, p rpc.SendEstimateParams) (any, error) {
		return ep.SendEstimate(ctx, p.Spec)
	}),
	rpc.MethodReceiveBusy: typed(rpc.MethodReceiveBusy, func(ctx context.Context, ep endpoint.Endpoint, p rpc.ReceiveBusyParams) (any, error) {
		return ep.ReceiveBusy(ctx, p.Datasets)
	}),
	rpc.MethodAllow: typed(rpc.MethodAllow, func(ctx context.Context, ep endpoint.Endpoint, p rpc.DatasetParams) (any, error) {
		return ep.Allow(ctx, p.Dataset)
	}),
	// Task 6 adds send and receive here; until then they are unknown methods.
}
