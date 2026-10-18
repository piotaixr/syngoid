package zfs

import (
	"context"
	"strings"
)

// Client builds and runs zfs command lines through an Exec.
type Client struct {
	Exec Exec
	Sudo bool
}

// Argv returns the full argv for a zfs invocation, prefixed with
// "sudo -n" when Sudo is set.
func (c *Client) Argv(args ...string) []string {
	prefix := []string{"zfs"}
	if c.Sudo {
		prefix = []string{"sudo", "-n", "zfs"}
	}
	return append(prefix, args...)
}

// Output runs zfs with args and returns its stdout.
func (c *Client) Output(ctx context.Context, args ...string) (string, error) {
	var out strings.Builder
	if err := c.Exec.Run(ctx, Cmd{Argv: c.Argv(args...), Stdout: &out}); err != nil {
		return "", err
	}
	return out.String(), nil
}
