// Package endpoint abstracts a ZFS host (local or remote) behind the
// interface the replication engine talks to.
package endpoint

import (
	"context"
	"io"

	"github.com/piotaixr/syngoid/internal/stream"
	"github.com/piotaixr/syngoid/internal/zfs"
)

// HostInfo describes the host an Endpoint runs on.
type HostInfo struct {
	OS, Arch, User string
	UID            int
	Groups         []string
	// ZFSVersion is the first line of `zfs version`, "" when unavailable.
	ZFSVersion string
}

// SendOpts configures Endpoint.Send.
type SendOpts struct {
	// Codec compresses the stream written to the destination writer.
	Codec stream.Codec
	// Progress, when non-nil, receives the cumulative number of raw
	// (uncompressed) bytes produced by zfs send.
	Progress func(rawBytes int64)
}

// Endpoint is one ZFS host. The query and mutation methods mirror zfs.Client.
type Endpoint interface {
	// Label is "" for the local host and "user@host" for a remote one.
	Label() string
	Info(ctx context.Context) (HostInfo, error)

	ListTree(ctx context.Context, root string, recursive bool) ([]zfs.DatasetInfo, error)
	ListSnapshots(ctx context.Context, root string, recursive bool) (map[string][]zfs.Snapshot, error)
	ListBookmarks(ctx context.Context, root string, recursive bool) (map[string][]zfs.Bookmark, error)
	GetProps(ctx context.Context, ds string, props ...string) (map[string]string, error)
	LocalProps(ctx context.Context, ds string) (map[string]string, error)
	Exists(ctx context.Context, ds string) (bool, error)

	Snapshot(ctx context.Context, names []string) error
	Destroy(ctx context.Context, ds string, snaps []string) error
	DestroyDataset(ctx context.Context, ds string, recursive bool) error
	Hold(ctx context.Context, tag, snapshot string) error
	Release(ctx context.Context, tag, snapshot string) error
	Bookmark(ctx context.Context, snapshot, bookmark string) error
	AbortResume(ctx context.Context, ds string) error
	SendEstimate(ctx context.Context, s zfs.SendSpec) (int64, error)

	// ReceiveBusy reports which of datasets are the target of a running
	// zfs receive.
	ReceiveBusy(ctx context.Context, datasets []string) (map[string]bool, error)
	// Send runs zfs send for s, writing the stream compressed with o.Codec to w.
	Send(ctx context.Context, s zfs.SendSpec, o SendOpts, w io.Writer) error
	// Receive decodes r with codec and feeds it to zfs receive.
	Receive(ctx context.Context, s zfs.RecvSpec, codec stream.Codec, r io.Reader) error
	// Allow returns the raw output of `zfs allow ds`.
	Allow(ctx context.Context, ds string) (string, error)
}
