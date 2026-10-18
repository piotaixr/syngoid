package zfs

import (
	"context"
	"strings"
)

// run executes a zfs command whose output is not needed.
func (c *Client) run(ctx context.Context, args ...string) error {
	return c.Exec.Run(ctx, Cmd{Argv: c.Argv(args...)})
}

// Snapshot creates all the named snapshots in one invocation.
func (c *Client) Snapshot(ctx context.Context, names []string) error {
	if len(names) == 0 {
		return nil
	}
	return c.run(ctx, append([]string{"snapshot"}, names...)...)
}

// Destroy destroys the snapshots snaps (short names) of ds in one
// invocation as ds@a,b,c.
func (c *Client) Destroy(ctx context.Context, ds string, snaps []string) error {
	if len(snaps) == 0 {
		return nil
	}
	return c.run(ctx, "destroy", ds+"@"+strings.Join(snaps, ","))
}

// DestroyDataset destroys ds, and its descendants when recursive.
func (c *Client) DestroyDataset(ctx context.Context, ds string, recursive bool) error {
	if recursive {
		return c.run(ctx, "destroy", "-r", ds)
	}
	return c.run(ctx, "destroy", ds)
}

// Hold places hold tag on the full snapshot name.
func (c *Client) Hold(ctx context.Context, tag, snapshot string) error {
	return c.run(ctx, "hold", tag, snapshot)
}

// Release removes hold tag from the full snapshot name.
func (c *Client) Release(ctx context.Context, tag, snapshot string) error {
	return c.run(ctx, "release", tag, snapshot)
}

// Bookmark creates bookmark (full name, ds#name) from snapshot (full name).
func (c *Client) Bookmark(ctx context.Context, snapshot, bookmark string) error {
	return c.run(ctx, "bookmark", snapshot, bookmark)
}

// AbortResume discards the partial receive state of ds.
func (c *Client) AbortResume(ctx context.Context, ds string) error {
	return c.run(ctx, "receive", "-A", ds)
}
