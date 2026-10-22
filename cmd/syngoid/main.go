// Command syngoid replicates ZFS datasets; it is a Go rewrite of syncoid.
package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"golang.org/x/term"

	"github.com/piotaixr/syngoid/internal/app"
	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/zfs"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer stop()
	go forceExitOnSecondSignal(ctx)

	var logger *slog.Logger
	env := app.Env{
		Stdin:       os.Stdin,
		Stdout:      os.Stdout,
		Stderr:      os.Stderr,
		StdinIsTTY:  isTTY(os.Stdin),
		StderrIsTTY: isTTY(os.Stderr),
		Hostname:    os.Hostname,
		Now:         time.Now,
		NewLocal: func(sudo bool) endpoint.Endpoint {
			x := zfs.OSExec{Log: logger}
			return endpoint.NewLocal(&zfs.Client{Exec: x, Sudo: sudo}, x)
		},
		LoggerReady: func(l *slog.Logger) { logger = l },
		PoolExists:  (&pools{exec: zfs.OSExec{}, stderr: os.Stderr}).exists,
	}
	code := app.Main(ctx, os.Args[1:], env)
	stop()
	os.Exit(code)
}

// forceExitOnSecondSignal waits for the first SIGINT or SIGTERM (which
// cancels ctx and starts the graceful shutdown), then exits with code 2 on
// the next SIGINT.
func forceExitOnSecondSignal(ctx context.Context) {
	<-ctx.Done()
	again := make(chan os.Signal, 1)
	signal.Notify(again, syscall.SIGINT)
	<-again
	os.Exit(2)
}

// pools answers whether local ZFS pools exist, for telling host:dataset
// from a local dataset whose pool name contains a colon. It runs before the
// logger exists, so its warning goes straight to stderr.
type pools struct {
	exec   zfs.Exec
	stderr io.Writer
	warn   sync.Once
}

// exists reports whether a local ZFS pool called name exists. Any error,
// such as zfs not being installed, counts as no and warns once.
func (p *pools) exists(name string) bool {
	var out bytes.Buffer
	err := p.exec.Run(context.Background(), zfs.Cmd{
		Argv:   []string{"zfs", "list", "-H", "-o", "name", "-d", "0"},
		Stdout: &out,
	})
	if err != nil {
		p.warn.Do(func() {
			_, _ = io.WriteString(p.stderr, "WARNING: Unable to enumerate pools (is zfs available?)\n")
		})
		return false
	}
	for _, line := range bytes.Split(out.Bytes(), []byte("\n")) {
		if string(bytes.TrimSpace(line)) == name {
			return true
		}
	}
	return false
}

// isTTY reports whether f is a terminal; /dev/null and pipes are not.
func isTTY(f *os.File) bool {
	return term.IsTerminal(int(f.Fd()))
}
