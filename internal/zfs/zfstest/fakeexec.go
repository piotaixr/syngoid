// Package zfstest provides test doubles for the zfs package.
package zfstest

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/piotaixr/syngoid/internal/zfs"
)

// Response is the canned result for one argv.
type Response struct {
	Stdout string
	// StdoutBytes is written to the command's stdout after Stdout; use it
	// for binary or large streams.
	StdoutBytes []byte
	Stderr      string
	Exit        int
	// Block makes Run wait until the context is done and return ctx.Err().
	Block bool
}

// FakeExec is a zfs.Exec returning canned responses keyed by the argv joined
// with single spaces. It is safe for concurrent use.
type FakeExec struct {
	mu        sync.Mutex
	Responses map[string]Response
	Calls     [][]string
	// Stdin holds everything each command read from its Stdin, keyed like
	// Responses. Repeated calls with the same key are concatenated.
	Stdin map[string][]byte
}

var _ zfs.Exec = (*FakeExec)(nil)

// Run records the call and replays the matching Response.
func (f *FakeExec) Run(ctx context.Context, c zfs.Cmd) error {
	key := strings.Join(c.Argv, " ")
	f.mu.Lock()
	f.Calls = append(f.Calls, append([]string(nil), c.Argv...))
	resp, ok := f.Responses[key]
	f.mu.Unlock()
	if !ok {
		return fmt.Errorf("zfstest: unexpected command: %s", key)
	}
	if resp.Block {
		<-ctx.Done()
		return ctx.Err()
	}
	if c.Stdin != nil {
		data, err := io.ReadAll(c.Stdin)
		f.mu.Lock()
		if f.Stdin == nil {
			f.Stdin = make(map[string][]byte)
		}
		f.Stdin[key] = append(f.Stdin[key], data...)
		f.mu.Unlock()
		if err != nil {
			return err
		}
	}
	if c.Stdout != nil && resp.Stdout != "" {
		if _, err := io.WriteString(c.Stdout, resp.Stdout); err != nil {
			return err
		}
	}
	if c.Stdout != nil && len(resp.StdoutBytes) > 0 {
		if _, err := c.Stdout.Write(resp.StdoutBytes); err != nil {
			return err
		}
	}
	if resp.Exit != 0 {
		return &zfs.CommandError{Argv: c.Argv, ExitCode: resp.Exit, Stderr: resp.Stderr, Kind: zfs.Classify(resp.Stderr)}
	}
	return nil
}
