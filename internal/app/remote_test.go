package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/piotaixr/syngoid/internal/agent/agenttest"
	"github.com/piotaixr/syngoid/internal/cli"
	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/endpoint/fake"
	"github.com/piotaixr/syngoid/internal/rpc"
	"github.com/piotaixr/syngoid/internal/stream"
	"github.com/piotaixr/syngoid/internal/zfs"
)

// codecLog records the codec of every Send and Receive.
type codecLog struct {
	mu           sync.Mutex
	sends, recvs []stream.Codec
}

// codecRecorder wraps an endpoint to record the codecs it is given.
type codecRecorder struct {
	endpoint.Endpoint
	log *codecLog
}

func (c codecRecorder) Send(ctx context.Context, s zfs.SendSpec, o endpoint.SendOpts, w io.Writer) error {
	c.log.mu.Lock()
	c.log.sends = append(c.log.sends, o.Codec)
	c.log.mu.Unlock()
	return c.Endpoint.Send(ctx, s, o, w)
}

func (c codecRecorder) Receive(ctx context.Context, s zfs.RecvSpec, codec stream.Codec, r io.Reader) error {
	c.log.mu.Lock()
	c.log.recvs = append(c.log.recvs, codec)
	c.log.mu.Unlock()
	return c.Endpoint.Receive(ctx, s, codec, r)
}

// endpointCall is one Connector.Endpoint call.
type endpointCall struct {
	Loc  cli.Location
	Sudo bool
}

// testConnector serves each host from its own fake.World through a real
// agent and RPC session over net.Pipe.
type testConnector struct {
	t      *testing.T
	worlds map[string]*fake.World
	// uid is the uid of every remote user; 0 skips delegation checks.
	uid int
	// setup, when non-nil, configures a host's fake endpoint before use.
	setup func(host string, ep *fake.Endpoint)

	mu      sync.Mutex
	calls   []endpointCall
	eps     []*fake.Endpoint
	remotes []*rpc.Remote
	codecs  codecLog
	closed  bool
}

func newTestConnector(t *testing.T, hosts ...string) *testConnector {
	c := &testConnector{t: t, worlds: map[string]*fake.World{}}
	for _, h := range hosts {
		c.worlds[h] = fake.NewWorld()
	}
	return c
}

func (c *testConnector) Endpoint(_ context.Context, loc cli.Location, sudo bool) (endpoint.Endpoint, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, endpointCall{loc, sudo})
	w := c.worlds[loc.Host]
	if w == nil {
		return nil, fmt.Errorf("%s: unknown test host", loc.Host)
	}
	user := loc.User
	if user == "" {
		user = "backup"
	}
	ep := fake.New(w, endpoint.HostInfo{OS: "linux", User: user, UID: c.uid})
	if c.setup != nil {
		c.setup(loc.Host, ep)
	}
	label := loc.Host
	if loc.User != "" {
		label = loc.User + "@" + loc.Host
	}
	r := agenttest.Start(c.t, ep, label, false, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.eps = append(c.eps, ep)
	c.remotes = append(c.remotes, r)
	return codecRecorder{Endpoint: r, log: &c.codecs}, nil
}

func (c *testConnector) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	for _, r := range c.remotes {
		_ = r.Close()
	}
	return nil
}

// remoteEnv is a syncEnv whose NewConnector returns a testConnector.
type remoteEnv struct {
	*syncEnv
	conn    *testConnector
	created int
}

func newRemoteEnv(t *testing.T, hosts ...string) *remoteEnv {
	t.Helper()
	re := &remoteEnv{syncEnv: newSyncEnv(t, endpoint.HostInfo{}), conn: newTestConnector(t, hosts...)}
	re.NewConnector = func(*cli.Options, *slog.Logger) (Connector, error) {
		re.created++
		return re.conn, nil
	}
	return re
}

// populate gives host a pool and, when dataset is not empty, a dataset with
// one snapshot.
func (re *remoteEnv) populate(host, pool, dataset string) {
	w := re.conn.worlds[host]
	w.Create(pool, "")
	if dataset != "" {
		w.Create(dataset, "")
		w.Snap(dataset, "one")
	}
}

func (re *remoteEnv) mutatingCalls() []string {
	var out []string
	out = append(out, re.ep.Calls()...)
	for _, ep := range re.conn.eps {
		out = append(out, ep.Calls()...)
	}
	return out
}

func TestMainRemoteNotConfigured(t *testing.T) {
	se := newSyncEnv(t, endpoint.HostInfo{})
	if got := se.run("sync", "nas:tank/a", "tank/b"); got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
	want := "CRITICAL ERROR: remote endpoints are not configured\n"
	if se.stderr.String() != want {
		t.Errorf("stderr = %q, want %q", se.stderr, want)
	}
}

func TestMainRemoteTargetEndToEnd(t *testing.T) {
	re := newRemoteEnv(t, "nas")
	re.populate("nas", "pool", "")
	if got := re.run("sync", "-r", "tank/data", "backup@nas:pool/data"); got != 0 {
		t.Fatalf("exit code = %d, want 0; stderr %q", got, re.stderr)
	}
	for _, ds := range []string{"pool/data", "pool/data/vm"} {
		if d := re.conn.worlds["nas"].Dataset(ds); d == nil || len(d.Snaps) < 2 {
			t.Errorf("target %s not populated: %+v", ds, d)
		}
	}
	if !strings.Contains(re.stdout.String(), "Summary:\n") || !strings.Contains(re.stdout.String(), "  tank/data → pool/data: ok, ") {
		t.Errorf("no summary in stdout:\n%s", re.stdout)
	}
	if want := []endpointCall{{cli.Location{User: "backup", Host: "nas", Dataset: "pool/data"}, false}}; !slices.Equal(re.conn.calls, want) {
		t.Errorf("Endpoint calls = %v, want %v", re.conn.calls, want)
	}
	if want := []bool{false}; !slices.Equal(re.newLocalCalls, want) {
		t.Errorf("NewLocal sudo args = %v, want %v", re.newLocalCalls, want)
	}
	if !re.conn.closed {
		t.Error("connector not closed")
	}
}

func TestMainRemoteSourceEndToEnd(t *testing.T) {
	re := newRemoteEnv(t, "db1")
	re.populate("db1", "tank", "tank/data")
	if got := re.run("sync", "db1:tank/data", "backup/data"); got != 0 {
		t.Fatalf("exit code = %d, want 0; stderr %q", got, re.stderr)
	}
	if d := re.world.Dataset("backup/data"); d == nil || len(d.Snaps) < 2 {
		t.Errorf("target not populated: %+v", d)
	}
	if want := []endpointCall{{cli.Location{Host: "db1", Dataset: "tank/data"}, false}}; !slices.Equal(re.conn.calls, want) {
		t.Errorf("Endpoint calls = %v, want %v", re.conn.calls, want)
	}
	if want := []bool{false}; !slices.Equal(re.newLocalCalls, want) {
		t.Errorf("NewLocal sudo args = %v, want %v", re.newLocalCalls, want)
	}
	if !re.conn.closed {
		t.Error("connector not closed")
	}
}

func TestMainRemoteToRemoteSameHost(t *testing.T) {
	re := newRemoteEnv(t, "nas")
	re.populate("nas", "tank", "tank/a")
	re.conn.worlds["nas"].Create("pool", "")
	if got := re.run("nas:tank/a", "nas:pool/a"); got != 0 {
		t.Fatalf("exit code = %d, want 0; stderr %q", got, re.stderr)
	}
	if n := len(re.conn.calls); n != 1 {
		t.Errorf("Endpoint calls = %v, want one", re.conn.calls)
	}
	if d := re.conn.worlds["nas"].Dataset("pool/a"); d == nil || len(d.Snaps) < 2 {
		t.Errorf("target not populated: %+v", d)
	}
	if len(re.newLocalCalls) != 0 {
		t.Errorf("NewLocal called: %v", re.newLocalCalls)
	}

	re = newRemoteEnv(t, "nas")
	re.populate("nas", "tank", "tank/a")
	re.conn.worlds["nas"].Create("pool", "")
	if got := re.run("--target-sudo", "nas:tank/a", "nas:pool/a"); got != 0 {
		t.Fatalf("--target-sudo: exit code = %d, want 0; stderr %q", got, re.stderr)
	}
	var sudos []bool
	for _, c := range re.conn.calls {
		sudos = append(sudos, c.Sudo)
	}
	if want := []bool{false, true}; !slices.Equal(sudos, want) {
		t.Errorf("Endpoint sudo args = %v, want %v", sudos, want)
	}
}

func TestMainCodecPlacement(t *testing.T) {
	// want checks that l holds at least one send and one receive, all
	// using codec.
	want := func(t *testing.T, name string, l *codecLog, nSends, nRecvs bool, codec stream.Codec) {
		t.Helper()
		l.mu.Lock()
		defer l.mu.Unlock()
		if nSends && len(l.sends) == 0 || nRecvs && len(l.recvs) == 0 {
			t.Errorf("%s: sends %v, receives %v, want some", name, l.sends, l.recvs)
		}
		for _, c := range append(slices.Clone(l.sends), l.recvs...) {
			if c != codec {
				t.Errorf("%s: codec %v, want %v everywhere (sends %v, receives %v)", name, c, codec, l.sends, l.recvs)
				break
			}
		}
	}

	// Local to remote: the remote receive sees lz4, and so does the local
	// send.
	re := newRemoteEnv(t, "nas")
	re.populate("nas", "pool", "")
	var localSends codecLog
	re.NewLocal = func(bool) endpoint.Endpoint { return codecRecorder{Endpoint: re.ep, log: &localSends} }
	if got := re.run("--compress=lz4", "tank/data", "nas:pool/data"); got != 0 {
		t.Fatalf("local to remote: exit code = %d; stderr %q", got, re.stderr)
	}
	want(t, "local to remote (local side)", &localSends, true, false, stream.LZ4)
	want(t, "local to remote (remote side)", &re.conn.codecs, false, true, stream.LZ4)

	// Remote to remote.
	re = newRemoteEnv(t, "db1", "nas")
	re.populate("db1", "tank", "tank/data")
	re.populate("nas", "pool", "")
	if got := re.run("--compress=lz4", "db1:tank/data", "nas:pool/data"); got != 0 {
		t.Fatalf("remote to remote: exit code = %d; stderr %q", got, re.stderr)
	}
	want(t, "remote to remote", &re.conn.codecs, true, true, stream.LZ4)

	// Local to local: no connector, no compression.
	se := newSyncEnv(t, endpoint.HostInfo{})
	var local codecLog
	se.NewLocal = func(bool) endpoint.Endpoint { return codecRecorder{Endpoint: se.ep, log: &local} }
	created := false
	se.NewConnector = func(*cli.Options, *slog.Logger) (Connector, error) {
		created = true
		return nil, errors.New("unexpected")
	}
	if got := se.run("--compress=lz4", "tank/data", "backup/data"); got != 0 {
		t.Fatalf("local to local: exit code = %d; stderr %q", got, se.stderr)
	}
	if created {
		t.Error("a connector was created for a local sync")
	}
	want(t, "local to local", &local, true, true, stream.None)
}

func TestMainRemoteDryRun(t *testing.T) {
	re := newRemoteEnv(t, "nas")
	re.populate("nas", "pool", "")
	if got := re.run("--dry-run", "-r", "tank/data", "backup@nas:pool/data"); got != 0 {
		t.Fatalf("exit code = %d, want 0; stderr %q", got, re.stderr)
	}
	out := re.stdout.String()
	for _, want := range []string{"tank/data → pool/data\n", "tank/data/vm → pool/data/vm\n"} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout does not contain %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "Summary:") {
		t.Errorf("dry run printed a summary:\n%s", out)
	}
	if calls := re.mutatingCalls(); len(calls) != 0 {
		t.Errorf("dry run made mutating calls: %q", calls)
	}
	if re.conn.worlds["nas"].Dataset("pool/data") != nil {
		t.Error("dry run created the target")
	}
	if !re.conn.closed {
		t.Error("connector not closed")
	}
}

func TestMainRemotePermissions(t *testing.T) {
	re := newRemoteEnv(t, "nas")
	re.populate("nas", "pool", "")
	re.conn.uid = 1000
	re.conn.setup = func(_ string, ep *fake.Endpoint) {
		ep.Faults.Allow = map[string]string{"pool": allowOutput("pool", "backup", "create,mount,receive,destroy")}
	}
	if got := re.run("permissions", "tank/data", "backup@nas:pool/data"); got != 0 {
		t.Fatalf("complete: exit code = %d, want 0; stderr %q", got, re.stderr)
	}
	for _, want := range []string{"Full minimal grants:", "[nas]  zfs allow -u backup"} {
		if !strings.Contains(re.stdout.String(), want) {
			t.Errorf("stdout does not contain %q:\n%s", want, re.stdout)
		}
	}
	if calls := re.mutatingCalls(); len(calls) != 0 {
		t.Errorf("permissions made calls: %q", calls)
	}

	re = newRemoteEnv(t, "nas")
	re.populate("nas", "pool", "")
	re.conn.uid = 1000
	if got := re.run("permissions", "tank/data", "backup@nas:pool/data"); got != 2 {
		t.Fatalf("missing: exit code = %d, want 2", got)
	}
	for _, want := range []string{
		"CRITICAL ERROR: missing ZFS permissions\n",
		"  target backup@nas:pool/data: ",
		"  [nas]  zfs allow -u backup ",
		"Full minimal grants:",
	} {
		if !strings.Contains(re.stdout.String(), want) {
			t.Errorf("stdout does not contain %q:\n%s", want, re.stdout)
		}
	}
	if !re.conn.closed {
		t.Error("connector not closed")
	}
}

func TestMainRemoteSyncPreflightFailureNamesHost(t *testing.T) {
	re := newRemoteEnv(t, "nas")
	re.populate("nas", "pool", "")
	re.conn.uid = 1000
	if got := re.run("tank/data", "backup@nas:pool/data"); got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
	for _, want := range []string{"  target backup@nas:pool/data: ", "  [nas]  zfs allow -u backup "} {
		if !strings.Contains(re.stderr.String(), want) {
			t.Errorf("stderr does not contain %q:\n%s", want, re.stderr)
		}
	}
	if calls := re.mutatingCalls(); len(calls) != 0 {
		t.Errorf("failed preflight made calls: %q", calls)
	}
}

func TestMainRemoteConnectFailureExit2(t *testing.T) {
	re := newRemoteEnv(t, "nas")
	closed := false
	re.NewConnector = func(*cli.Options, *slog.Logger) (Connector, error) {
		return failingConnector{err: errors.New("nas: agent handshake failed: boom"), closed: &closed}, nil
	}
	if got := re.run("tank/data", "nas:pool/data"); got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
	if want := "CRITICAL ERROR: nas: agent handshake failed: boom\n"; re.stderr.String() != want {
		t.Errorf("stderr = %q, want %q", re.stderr, want)
	}
	if !closed {
		t.Error("connector not closed after the failed connect")
	}
	if re.stdout.Len() != 0 {
		t.Errorf("stdout = %q, want empty", re.stdout)
	}
}

func TestMainRemoteConnectorCreationFailureExit2(t *testing.T) {
	re := newRemoteEnv(t, "nas")
	re.NewConnector = func(*cli.Options, *slog.Logger) (Connector, error) { return nil, errors.New("ssh control dir: boom") }
	if got := re.run("tank/data", "nas:pool/data"); got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
	if want := "CRITICAL ERROR: ssh control dir: boom\n"; re.stderr.String() != want {
		t.Errorf("stderr = %q, want %q", re.stderr, want)
	}
}

type failingConnector struct {
	err    error
	closed *bool
}

func (f failingConnector) Endpoint(context.Context, cli.Location, bool) (endpoint.Endpoint, error) {
	return nil, f.err
}

func (f failingConnector) Close() error { *f.closed = true; return nil }

func TestMainRemoteCancelledExit2(t *testing.T) {
	re := newRemoteEnv(t, "db1")
	re.populate("db1", "tank", "tank/data")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	re.conn.setup = func(_ string, ep *fake.Endpoint) {
		ep.Faults.BlockSend = true
		ep.Faults.Send = func(zfs.SendSpec) error { cancel(); return nil }
	}
	if got := Main(ctx, []string{"db1:tank/data", "backup/data"}, re.Env); got != 2 {
		t.Fatalf("exit code = %d, want 2; stderr %q", got, re.stderr)
	}
	out := re.stdout.String()
	if !strings.Contains(out, "Summary:") || !strings.Contains(out, "failed (cancelled)") {
		t.Errorf("stdout lacks the cancelled summary:\n%s", out)
	}
	if !re.conn.closed {
		t.Error("connector not closed")
	}
}
