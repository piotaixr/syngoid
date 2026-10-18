// Command syngoid replicates ZFS datasets; it is a Go rewrite of syncoid.
package main

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/piotaixr/syngoid/internal/app"
	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/zfs"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
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
		PoolExists:  poolExists,
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

// poolExists reports whether a local ZFS pool called name exists. Any error,
// such as zfs not being installed, counts as no.
func poolExists(name string) bool {
	var out bytes.Buffer
	err := zfs.OSExec{}.Run(context.Background(), zfs.Cmd{
		Argv:   []string{"zfs", "list", "-H", "-o", "name", "-d", "0"},
		Stdout: &out,
	})
	if err != nil {
		return false
	}
	for _, line := range bytes.Split(out.Bytes(), []byte("\n")) {
		if string(bytes.TrimSpace(line)) == name {
			return true
		}
	}
	return false
}

// isTTY reports whether f is a character device (a terminal).
func isTTY(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}
