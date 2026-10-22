//go:build unix

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/piotaixr/syngoid/internal/rpc"
	"github.com/piotaixr/syngoid/internal/sshx"
)

// MasterTransport is a Transport over a connection that is started once
// and closed at the end of the run, such as an ssh ControlMaster.
type MasterTransport interface {
	Transport
	Start(ctx context.Context) error
	Close() error
}

// Pool shares connections for a run (R-M2-3): one started master per
// [user@]host and one agent session per ([user@]host, sudo). It is safe
// for concurrent use; connects are serialized.
type Pool struct {
	// NewTransport builds the master for user@host; n is unique per Pool.
	NewTransport func(user, host string, n int) (MasterTransport, error)
	// Opts configures every Connect; Sudo is set per session.
	Opts Options

	controlDir string // removed by Close when set

	mu       sync.Mutex
	closed   bool
	next     int
	masters  map[string]MasterTransport
	sessions map[sessionKey]*rpc.Remote
	// Creation order, for Close.
	masterList  []MasterTransport
	sessionList []*rpc.Remote
}

type sessionKey struct {
	dest string
	sudo bool
}

// NewSSHPool returns a Pool of sshx ControlMasters whose sockets live in a
// new private control directory.
func NewSSHPool(cfg sshx.Config, o Options) (*Pool, error) {
	dir, err := sshx.NewControlDir()
	if err != nil {
		return nil, fmt.Errorf("ssh control dir: %w", err)
	}
	return &Pool{
		NewTransport: func(user, host string, n int) (MasterTransport, error) {
			m, err := sshx.NewMaster(cfg, user, host, dir, n)
			if err != nil {
				return nil, err
			}
			return sshMaster{m}, nil
		},
		Opts:       o,
		controlDir: dir,
	}, nil
}

// sshMaster adapts *sshx.Master to MasterTransport.
type sshMaster struct{ *sshx.Master }

func (m sshMaster) Spawn(ctx context.Context, cmd string) (Proc, error) {
	p, err := m.Master.Spawn(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// Endpoint returns the agent session for ([user@]host, sudo), starting the
// host's master and connecting the agent on first use. Failures are not
// cached.
func (p *Pool) Endpoint(ctx context.Context, user, host string, sudo bool) (*rpc.Remote, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errors.New("bootstrap: pool is closed")
	}
	dest := host
	if user != "" {
		dest = user + "@" + host
	}
	key := sessionKey{dest: dest, sudo: sudo}
	if r := p.sessions[key]; r != nil {
		return r, nil
	}
	m, err := p.master(ctx, user, host, dest)
	if err != nil {
		return nil, err
	}
	o := p.Opts
	o.Sudo = sudo
	r, err := Connect(ctx, m, o)
	if err != nil {
		return nil, err
	}
	if p.sessions == nil {
		p.sessions = map[sessionKey]*rpc.Remote{}
	}
	p.sessions[key] = r
	p.sessionList = append(p.sessionList, r)
	return r, nil
}

// master returns the started master for dest. p.mu is held.
func (p *Pool) master(ctx context.Context, user, host, dest string) (MasterTransport, error) {
	if m := p.masters[dest]; m != nil {
		return m, nil
	}
	m, err := p.NewTransport(user, host, p.next)
	p.next++
	if err != nil {
		return nil, err
	}
	if err := m.Start(ctx); err != nil {
		_ = m.Close()
		return nil, err
	}
	if p.masters == nil {
		p.masters = map[string]MasterTransport{}
	}
	p.masters[dest] = m
	p.masterList = append(p.masterList, m)
	return m, nil
}

// Close closes every session, then every master, then removes the control
// directory. It is idempotent; the Pool is unusable afterwards.
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	var errs []error
	for _, r := range p.sessionList {
		errs = append(errs, r.Close())
	}
	for _, m := range p.masterList {
		errs = append(errs, m.Close())
	}
	if p.controlDir != "" {
		errs = append(errs, os.RemoveAll(p.controlDir))
	}
	p.masters, p.sessions, p.masterList, p.sessionList = nil, nil, nil, nil
	return errors.Join(errs...)
}
