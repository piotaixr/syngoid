// Command syngoid replicates ZFS datasets; it is a Go rewrite of syncoid.
package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"time"

	"golang.org/x/term"

	"github.com/piotaixr/syngoid/internal/app"
	"github.com/piotaixr/syngoid/internal/bootstrap"
	"github.com/piotaixr/syngoid/internal/buildinfo"
	"github.com/piotaixr/syngoid/internal/cli"
	"github.com/piotaixr/syngoid/internal/dist"
	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/sigctl"
	"github.com/piotaixr/syngoid/internal/sshx"
	"github.com/piotaixr/syngoid/internal/zfs"
)

func main() {
	ctl, ctx := sigctl.New(context.Background(), os.Exit)
	ctl.Start()
	defer ctl.Stop()

	var logger *slog.Logger
	stdinIsTTY := isTTY(os.Stdin)
	env := app.Env{
		Stdin:       os.Stdin,
		Stdout:      os.Stdout,
		Stderr:      os.Stderr,
		StdinIsTTY:  stdinIsTTY,
		StderrIsTTY: isTTY(os.Stderr),
		TermSize:    stderrSize,
		Interrupts:  ctl,
		Hostname:    os.Hostname,
		Now:         time.Now,
		NewLocal: func(sudo bool) endpoint.Endpoint {
			x := zfs.OSExec{Log: logger}
			return endpoint.NewLocal(&zfs.Client{Exec: x, Sudo: sudo}, x)
		},
		NewConnector: func(o *cli.Options, log *slog.Logger) (app.Connector, error) {
			return newConnector(o, log, stdinIsTTY)
		},
		LoggerReady: func(l *slog.Logger) { logger = l },
		PoolExists:  (&pools{exec: zfs.OSExec{}, stderr: os.Stderr}).exists,
	}
	code := app.Main(ctx, os.Args[1:], env)
	ctl.Stop()
	os.Exit(code)
}

// poolConnector adapts a bootstrap.Pool to app.Connector.
type poolConnector struct{ pool *bootstrap.Pool }

// newConnector returns a Connector of ssh masters and self-pushed agents
// configured from o.
func newConnector(o *cli.Options, log *slog.Logger, stdinIsTTY bool) (app.Connector, error) {
	res := bootstrap.Resolver{BinStores: o.BinStores, NoDownload: o.NoDownload}
	if !buildinfo.IsDev() {
		res.Cache, res.CacheErr = dist.NewUserCache(buildinfo.Version, o.DownloadURL, log)
	}
	pool, err := bootstrap.NewSSHPool(
		sshx.Config{Options: o.SSHOptions, ConfigFile: o.SSHConfig, StdinIsTTY: stdinIsTTY, Log: log},
		bootstrap.Options{
			RemoteBinDir: o.RemoteBinDir,
			Resolver:     res,
			Debug:        o.Debug,
			Log:          log,
		})
	if err != nil {
		return nil, err
	}
	return poolConnector{pool}, nil
}

func (c poolConnector) Endpoint(ctx context.Context, loc cli.Location, sudo bool) (endpoint.Endpoint, error) {
	r, err := c.pool.Endpoint(ctx, loc.User, loc.Host, sudo)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (c poolConnector) Close() error { return c.pool.Close() }

// pools answers whether local ZFS pools exist, for telling host:dataset
// from a local dataset whose pool name contains a colon. It runs before the
// logger exists, so its warning goes straight to stderr.
type pools struct {
	exec   zfs.Exec
	stderr io.Writer
	warn   sync.Once
}

// exists reports whether a local ZFS pool called name exists. Any error
// counts as no; one other than zfs not being installed warns once (a host
// without ZFS, e.g. a laptop relaying between two remotes, has no pools).
func (p *pools) exists(name string) bool {
	var out bytes.Buffer
	err := p.exec.Run(context.Background(), zfs.Cmd{
		Argv:   []string{"zfs", "list", "-H", "-o", "name", "-d", "0"},
		Stdout: &out,
	})
	if errors.Is(err, exec.ErrNotFound) {
		return false
	}
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

// stderrSize returns the width and height of the stderr terminal: width 80
// and height 0 when unknown.
func stderrSize() (width, height int) {
	w, h, err := term.GetSize(int(os.Stderr.Fd()))
	if err != nil {
		return 80, 0
	}
	if w <= 0 {
		w = 80
	}
	return w, max(h, 0)
}

// isTTY reports whether f is a terminal; /dev/null and pipes are not.
func isTTY(f *os.File) bool {
	return term.IsTerminal(int(f.Fd()))
}
