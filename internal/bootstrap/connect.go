//go:build unix

package bootstrap

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/piotaixr/syngoid/internal/buildinfo"
	"github.com/piotaixr/syngoid/internal/logx"
	"github.com/piotaixr/syngoid/internal/rpc"
)

// ErrMismatch is wrapped by CheckHello's errors: the agent is not the
// initiator's build or not the binary just pushed.
var ErrMismatch = errors.New("agent mismatch")

const defaultHandshakeTimeout = 30 * time.Second

// devHashHint explains a build-hash mismatch: without a stamped hash every
// development build differs, so an agent built for another platform never
// matches.
const devHashHint = `development builds match only when every platform's binary is built with the same ` +
	`-ldflags "-X github.com/piotaixr/syngoid/internal/buildinfo.Hash=<value>", or with a release Version`

// Transport runs commands on one remote host.
type Transport interface {
	// Label names the host in logs and errors, e.g. "user@host".
	Label() string
	// Run executes cmd with stdin and returns its stdout.
	Run(ctx context.Context, cmd string, stdin io.Reader) ([]byte, error)
	// Spawn starts cmd; ctx only bounds the start.
	Spawn(ctx context.Context, cmd string) (Proc, error)
}

// Proc is a spawned remote command: Read returns its stdout, Write feeds its
// stdin and Close ends it.
type Proc interface {
	io.ReadWriteCloser
	// Stderr returns what the command wrote to stderr so far.
	Stderr() string
}

// Options configures Connect.
type Options struct {
	// RemoteBinDir is the agent directory; "" means DefaultRemoteBinDir.
	// A leading "~/" is expanded on the remote.
	RemoteBinDir string
	Resolver     Resolver
	// Sudo and Debug are sent in Hello.
	Sudo, Debug bool
	// Log receives the push notice and the agent's log events; nil
	// discards them.
	Log *slog.Logger
	// HandshakeTimeout bounds each handshake; 0 means 30 s.
	HandshakeTimeout time.Duration
}

// CheckHello reports whether the agent's hello reply matches this build:
// same protocol and version, same build hash for development builds and,
// when pushedSHA is set, an executable with that SHA-256. Errors wrap
// ErrMismatch.
func CheckHello(got rpc.HelloReply, pushedSHA string) error {
	switch {
	case got.Protocol != buildinfo.ProtocolVersion:
		return fmt.Errorf("%w: protocol %d, want %d", ErrMismatch, got.Protocol, buildinfo.ProtocolVersion)
	case got.Version != buildinfo.Version:
		return fmt.Errorf("%w: version %q, want %q", ErrMismatch, got.Version, buildinfo.Version)
	case buildinfo.IsDev() && got.BuildHash != buildinfo.BuildHash():
		return fmt.Errorf("%w: build hash %q, want %q (%s)", ErrMismatch, got.BuildHash, buildinfo.BuildHash(), devHashHint)
	case pushedSHA != "" && got.ExeSHA256 != pushedSHA:
		return fmt.Errorf("%w: executable SHA-256 %q, want the pushed %q", ErrMismatch, got.ExeSHA256, pushedSHA)
	}
	return nil
}

// Connect brings up an agent over t (§5.2–§5.5): it probes the remote,
// pushes the agent when it is missing, starts `syngoid serve` and
// handshakes. A failed or mismatched first handshake is followed by one
// push and a second attempt.
func Connect(ctx context.Context, t Transport, o Options) (*rpc.Remote, error) {
	c := &connector{t: t, o: o, label: t.Label(), log: o.Log}
	if c.log == nil {
		c.log = slog.New(slog.DiscardHandler)
	}
	dir := o.RemoteBinDir
	if dir == "" {
		dir = DefaultRemoteBinDir()
	}
	if err := checkRemoteDir(dir); err != nil {
		return nil, fmt.Errorf("%s: %w", c.label, err)
	}
	out, err := t.Run(ctx, ProbeCommand(dir), nil)
	if err != nil {
		return nil, fmt.Errorf("%s: probe: %w", c.label, err)
	}
	pr, err := ParseProbe(string(out))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", c.label, err)
	}
	if strings.HasPrefix(dir, "~/") && !strings.HasPrefix(pr.Home, "/") {
		return nil, fmt.Errorf("%s: remote $HOME %q is not absolute; pass an absolute --remote-bin-dir", c.label, pr.Home)
	}

	pushed := "" // SHA-256 of the binary pushed in this run
	if !pr.Executable {
		if pushed, err = c.push(ctx, pr); err != nil {
			return nil, err
		}
	}
	for attempt := 1; ; attempt++ {
		r, stderr, err := c.start(ctx, pr.Dir, pushed)
		if err == nil {
			return r, nil
		}
		if attempt == 2 || ctx.Err() != nil {
			if s := strings.TrimRight(stderr, "\n"); s != "" {
				return nil, fmt.Errorf("%s: agent handshake failed: %w\nremote stderr:\n%s", c.label, err, s)
			}
			return nil, fmt.Errorf("%s: agent handshake failed: %w", c.label, err)
		}
		c.log.Debug(fmt.Sprintf("agent handshake failed, pushing again: %v", err), logx.KeyHost, c.label)
		if pushed, err = c.push(ctx, pr); err != nil {
			return nil, err
		}
	}
}

type connector struct {
	t     Transport
	o     Options
	label string
	log   *slog.Logger
}

// push resolves the agent binary for the probed platform and installs it in
// pr.Dir, returning its SHA-256.
func (c *connector) push(ctx context.Context, pr ProbeResult) (string, error) {
	path, err := c.o.Resolver.Resolve(pr.Platform)
	if err != nil {
		return "", fmt.Errorf("%s: %w", c.label, err)
	}
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("%s: push: %w", c.label, err)
	}
	defer func() { _ = f.Close() }()
	// Hash the whole file up front: what the transport consumed could be
	// short of it and still succeed.
	h := sha256.New()
	size, err := io.Copy(h, f)
	if err != nil {
		return "", fmt.Errorf("%s: push: %w", c.label, err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("%s: push: %w", c.label, err)
	}
	c.log.Info(fmt.Sprintf("pushing syngoid agent to %s:%s/syngoid", c.label, pr.Dir), logx.KeyHost, c.label)
	if _, err := c.t.Run(ctx, PushCommand(pr.Dir, size), f); err != nil {
		return "", fmt.Errorf("%s: push %s: %w", c.label, path, err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// start spawns the agent in dir and handshakes with it. On failure the
// process is closed and its stderr returned with the error.
func (c *connector) start(ctx context.Context, dir, pushed string) (*rpc.Remote, string, error) {
	proc, err := c.t.Spawn(ctx, ServeCommand(dir))
	if err != nil {
		return nil, "", err
	}
	// Handshake and Remote.Close close the proc too; once makes every Close
	// wait for the first one to finish.
	p := &onceProc{Proc: proc}
	timeout := c.o.HandshakeTimeout
	if timeout <= 0 {
		timeout = defaultHandshakeTimeout
	}
	hctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	hello := rpc.Hello{
		Protocol:  buildinfo.ProtocolVersion,
		Version:   buildinfo.Version,
		BuildHash: buildinfo.BuildHash(),
		Debug:     c.o.Debug,
		Sudo:      c.o.Sudo,
	}
	r, reply, err := rpc.Handshake(hctx, p, hello, c.label, c.log)
	if err == nil {
		if err = CheckHello(reply, pushed); err == nil {
			return r, "", nil
		}
		_ = r.Close()
	}
	_ = p.Close()
	return nil, p.Stderr(), err
}

// onceProc makes Close idempotent; later calls wait for the first.
type onceProc struct {
	Proc
	once sync.Once
	err  error
}

func (p *onceProc) Close() error {
	p.once.Do(func() { p.err = p.Proc.Close() })
	return p.err
}
