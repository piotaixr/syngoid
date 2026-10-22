//go:build unix

package bootstrap

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piotaixr/syngoid/internal/rpc"
	"github.com/piotaixr/syngoid/internal/sshx"
)

// fakeMaster is a MasterTransport over an shTransport that counts Start
// and Close; onClose, when set, runs in Close.
type fakeMaster struct {
	*shTransport
	starts, closes atomic.Int32
	onClose        func()
}

func (m *fakeMaster) Start(context.Context) error { m.starts.Add(1); return nil }

func (m *fakeMaster) Close() error {
	m.closes.Add(1)
	if m.onClose != nil {
		m.onClose()
	}
	return nil
}

// closed reports whether r's connection is gone.
func closed(ctx context.Context, r *rpc.Remote) bool {
	_, err := r.Info(ctx)
	return errors.Is(err, rpc.ErrConnectionLost)
}

func TestPoolReuse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ctl := t.TempDir()
	var masters []*fakeMaster
	p := &Pool{
		NewTransport: func(user, host string, n int) (MasterTransport, error) {
			m := &fakeMaster{shTransport: newShTransport(t)}
			m.label = user + "@" + host
			masters = append(masters, m)
			return m, nil
		},
		controlDir: ctl,
	}
	r1, err := p.Endpoint(ctx, "u", "h", false)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := p.Endpoint(ctx, "u", "h", false)
	if err != nil {
		t.Fatal(err)
	}
	if r1 != r2 {
		t.Error("same (host, sudo) gave two remotes")
	}
	r3, err := p.Endpoint(ctx, "u", "h", true)
	if err != nil {
		t.Fatal(err)
	}
	if r3 == r1 {
		t.Error("sudo session reused the non-sudo one")
	}
	if len(masters) != 1 {
		t.Fatalf("masters = %d, want 1", len(masters))
	}
	if n := masters[0].starts.Load(); n != 1 {
		t.Fatalf("starts = %d, want 1", n)
	}

	remotesClosedFirst := false
	masters[0].onClose = func() { remotesClosedFirst = closed(ctx, r1) && closed(ctx, r3) }
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if !remotesClosedFirst {
		t.Error("a master was closed before the sessions")
	}
	for _, r := range []*rpc.Remote{r1, r3} {
		if !closed(ctx, r) {
			t.Error("session still usable after Close")
		}
	}
	if n := masters[0].closes.Load(); n != 1 {
		t.Errorf("master closes = %d, want 1", n)
	}
	if _, err := os.Stat(ctl); !os.IsNotExist(err) {
		t.Errorf("control dir still there: %v", err)
	}
	if _, err := p.Endpoint(ctx, "u", "h", false); err == nil {
		t.Error("Endpoint after Close succeeded")
	}
}

func TestPoolSeparateHosts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var ns []int
	p := &Pool{NewTransport: func(user, host string, n int) (MasterTransport, error) {
		ns = append(ns, n)
		m := &fakeMaster{shTransport: newShTransport(t)}
		m.label = host
		return m, nil
	}}
	t.Cleanup(func() { _ = p.Close() })
	for _, host := range []string{"a", "b"} {
		if _, err := p.Endpoint(ctx, "", host, false); err != nil {
			t.Fatal(err)
		}
	}
	if len(ns) != 2 || ns[0] == ns[1] {
		t.Errorf("master numbers = %v, want two distinct", ns)
	}
}

func TestPoolStartFailure(t *testing.T) {
	ctx := context.Background()
	calls := 0
	p := &Pool{NewTransport: func(user, host string, n int) (MasterTransport, error) {
		calls++
		return &failingMaster{fakeMaster{shTransport: newShTransport(t)}}, nil
	}}
	t.Cleanup(func() { _ = p.Close() })
	for range 2 {
		if _, err := p.Endpoint(ctx, "", "h", false); err == nil || err.Error() != "start failed" {
			t.Errorf("Endpoint = %v, want the start error", err)
		}
	}
	if calls != 2 {
		t.Errorf("a failed master was reused: %d transports built", calls)
	}
}

type failingMaster struct{ fakeMaster }

func (*failingMaster) Start(context.Context) error { return errors.New("start failed") }

func TestNewSSHPool(t *testing.T) {
	p, err := NewSSHPool(sshx.Config{SSH: "/nonexistent/ssh"}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(p.controlDir)
	if err != nil || !fi.IsDir() || fi.Mode().Perm() != 0o700 {
		t.Fatalf("control dir %q: %v, %v", p.controlDir, fi, err)
	}
	mt, err := p.NewTransport("backup", "nas", 3)
	if err != nil {
		t.Fatal(err)
	}
	if mt.Label() != "backup@nas" {
		t.Errorf("Label = %q", mt.Label())
	}
	if _, err := p.NewTransport("", "-oProxyCommand=x", 4); err == nil {
		t.Error("option-like host accepted")
	}
	if _, err := p.Endpoint(context.Background(), "", "nas", false); err == nil {
		t.Error("Endpoint with a missing ssh succeeded")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(p.controlDir); !os.IsNotExist(err) {
		t.Errorf("control dir still there: %v", err)
	}
}
