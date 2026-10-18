// Package app wires the syngoid command line to its implementation.
package app

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/piotaixr/syngoid/internal/buildinfo"
)

// Env carries the process environment so Main can be tested without os.
type Env struct {
	Stdin       io.Reader
	Stdout      io.Writer
	Stderr      io.Writer
	StdinIsTTY  bool
	StderrIsTTY bool
	Hostname    func() (string, error)
	Now         func() time.Time
}

// Main runs syngoid with args (excluding the program name) and returns the
// process exit code.
func Main(_ context.Context, args []string, env Env) int {
	if len(args) == 0 {
		_, _ = fmt.Fprintln(env.Stderr, "CRITICAL ERROR: missing command; try 'syngoid version'")
		return 2
	}
	switch args[0] {
	case "version":
		return runVersion(env)
	}
	if strings.HasPrefix(args[0], "-") {
		_, _ = fmt.Fprintf(env.Stderr, "CRITICAL ERROR: unknown flag %q\n", args[0])
	} else {
		_, _ = fmt.Fprintf(env.Stderr, "CRITICAL ERROR: unknown command %q\n", args[0])
	}
	return 2
}

func runVersion(env Env) int {
	dirty := ""
	if buildinfo.Dirty == "true" {
		dirty = ", dirty"
	}
	_, _ = fmt.Fprintf(env.Stdout, "syngoid %s (commit %s%s) protocol %d\n",
		buildinfo.Version, buildinfo.Commit, dirty, buildinfo.ProtocolVersion)
	return 0
}
