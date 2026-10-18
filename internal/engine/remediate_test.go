package engine

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/endpoint/fake"
	"github.com/piotaixr/syngoid/internal/zfs"
)

// seededEnv has tank/data with snapshots a and b, and backup/data holding a.
func seededEnv(t *testing.T) *execEnv {
	env := newExecEnv(t, "tank/data", "backup/data")
	env.sw.Create("tank", "")
	env.sw.Create("tank/data", "")
	env.sw.Snap("tank/data", "a")
	env.sw.Snap("tank/data", "b")
	env.tw.Create("backup", "")
	mustTransfer(t, env.src, env.dst, zfs.SendSpec{Dataset: "tank/data", To: "a"}, "backup/data")
	return env
}

func TestExecutorResumeThenReplan(t *testing.T) {
	env := newExecEnv(t, "tank/data", "backup/data")
	env.sw.Create("tank", "")
	env.sw.Create("tank/data", "")
	env.sw.Snap("tank/data", "a")
	env.sw.Snap("tank/data", "b")
	env.tw.Create("backup", "")
	env.dst.Faults.Receive = func(zfs.RecvSpec) error { return errors.New("connection lost") }
	if err := transfer(env.src, env.dst, zfs.SendSpec{Dataset: "tank/data", To: "a"}, "backup/data", "-s"); err == nil {
		t.Fatal("seeding interrupted receive succeeded")
	}
	env.dst.Faults.Receive = nil
	if tok := env.tw.Dataset("backup/data").ResumeToken; tok != "tok-1" {
		t.Fatalf("resume token %q", tok)
	}
	ns, nd := len(env.src.Calls()), len(env.dst.Calls())
	ts := syncName(10)

	res := env.run(ts)
	wantStatuses(t, res, StatusOK)
	wantCalls(t, "source", callsFrom(env.src, ns), []string{"Snapshot tank/data@" + ts, "Send tok-1", "Send tank/data@" + ts})
	wantCalls(t, "target", callsFrom(env.dst, nd), []string{"Receive backup/data", "Receive backup/data"})
	if got, want := dsSnaps(env.tw, "backup/data"), []string{"a", "b", ts}; !eqStrings(got, want) {
		t.Errorf("target snaps %v, want %v", got, want)
	}
	if want := "Resuming interrupted zfs send/receive from tank/data to backup/data (~ 1.0 MiB remaining):"; !containsMsg(env.logs.at(slog.LevelInfo), want) {
		t.Errorf("missing INFO %q in %q", want, env.logs.at(slog.LevelInfo))
	}
}

func TestExecutorPartialStateNoResume(t *testing.T) {
	ts := syncName(10)
	t.Run("no-resume aborts and retries", func(t *testing.T) {
		env := seededEnv(t)
		env.opts.NoResume = true
		env.tw.Dataset("backup/data").ResumeToken = "tok-9"
		nd := len(env.dst.Calls())
		res := env.run(ts)
		wantStatuses(t, res, StatusOK)
		wantCalls(t, "target", callsFrom(env.dst, nd), []string{"Receive backup/data", "AbortResume backup/data", "Receive backup/data"})
		if got, want := dsSnaps(env.tw, "backup/data"), []string{"a", "b", ts}; !eqStrings(got, want) {
			t.Errorf("target snaps %v, want %v", got, want)
		}
	})
	t.Run("resume enabled is critical", func(t *testing.T) {
		env := seededEnv(t)
		env.dst.Faults.Receive = func(zfs.RecvSpec) error {
			return zfsErr(`cannot receive incremental stream: destination backup/data contains partially-complete state from "zfs receive -s".`)
		}
		res := env.run(ts)
		wantStatuses(t, res, StatusCritical)
		if n := countPrefix(env.dst.Calls(), "AbortResume"); n != 0 {
			t.Errorf("AbortResume called %d times", n)
		}
		if !strings.Contains(res[0].Message, "partially-complete state") {
			t.Errorf("message %q", res[0].Message)
		}
	})
}

func TestExecutorResumeSourceGone(t *testing.T) {
	env := seededEnv(t)
	env.tw.Dataset("backup/data").ResumeToken = "tok-7"
	env.src.Faults.Send = func(s zfs.SendSpec) error {
		if s.ResumeToken != "" {
			return zfsErr("cannot resume send: 'tank/data@b' used in the initial send no longer exists")
		}
		return nil
	}
	ns, nd := len(env.src.Calls()), len(env.dst.Calls())
	ts := syncName(10)
	res := env.run(ts)
	wantStatuses(t, res, StatusOK)
	wantCalls(t, "source", callsFrom(env.src, ns), []string{"Snapshot tank/data@" + ts, "Send tok-7", "Send tank/data@" + ts})
	wantCalls(t, "target", callsFrom(env.dst, nd), []string{"Receive backup/data", "AbortResume backup/data", "Receive backup/data"})
	if got, want := dsSnaps(env.tw, "backup/data"), []string{"a", "b", ts}; !eqStrings(got, want) {
		t.Errorf("target snaps %v, want %v", got, want)
	}
}

func TestExecutorDestinationExistsForceDelete(t *testing.T) {
	ts := syncName(10)
	setup := func(t *testing.T) *execEnv {
		env := newExecEnv(t, "tank/data", "backup/data")
		env.sw.Create("tank", "")
		env.sw.Create("tank/data", "")
		a := env.sw.Snap("tank/data", "a")
		env.sw.Snap("tank/data", "b")
		env.sw.Snap("tank/data", "x")
		env.tw.Create("backup", "")
		env.tw.Create("backup/data", "")
		env.tw.Snap("backup/data", "x") // same name, different GUID
		d := env.tw.Dataset("backup/data")
		d.Snaps = append(d.Snaps, a)
		return env
	}
	t.Run("force delete", func(t *testing.T) {
		env := setup(t)
		env.opts.ForceDelete = true
		res := env.run(ts)
		wantStatuses(t, res, StatusOK)
		wantCalls(t, "target", env.dst.Calls(), []string{"Receive backup/data", "Destroy backup/data@x", "Receive backup/data"})
		if got, want := dsSnaps(env.tw, "backup/data"), []string{"a", "b", "x", ts}; !eqStrings(got, want) {
			t.Errorf("target snaps %v, want %v", got, want)
		}
	})
	t.Run("force delete declined", func(t *testing.T) {
		env := setup(t)
		env.opts.ForceDelete = true
		var asked []string
		env.confirm = func(q string) bool {
			asked = append(asked, q)
			return false
		}
		res := env.run(ts)
		wantStatuses(t, res, StatusCritical)
		if res[0].Message != "destructive action declined" {
			t.Errorf("message %q", res[0].Message)
		}
		if want := []string{"Destroy conflicting backup/data@x (--force-delete)?"}; !eqStrings(asked, want) {
			t.Errorf("asked %q, want %q", asked, want)
		}
		if n := countPrefix(env.dst.Calls(), "Destroy"); n != 0 {
			t.Errorf("%d Destroy calls", n)
		}
		if got := dsSnaps(env.tw, "backup/data"); !eqStrings(got, []string{"x", "a"}) {
			t.Errorf("target snaps %v", got)
		}
	})
	t.Run("without force delete", func(t *testing.T) {
		env := setup(t)
		res := env.run(ts)
		wantStatuses(t, res, StatusCritical)
		if !strings.Contains(res[0].Message, "destination already exists") {
			t.Errorf("message %q", res[0].Message)
		}
		if n := countPrefix(env.dst.Calls(), "Destroy"); n != 0 {
			t.Errorf("%d Destroy calls", n)
		}
	})
}

// conflictingTarget has tank/data@a and an existing backup/data sharing no
// snapshot with it.
func conflictingTarget(t *testing.T) *execEnv {
	env := newExecEnv(t, "tank/data", "backup/data")
	env.opts.ForceDelete = true
	env.sw.Create("tank", "")
	env.sw.Create("tank/data", "")
	env.sw.Snap("tank/data", "a")
	env.tw.Create("backup", "")
	env.tw.Create("backup/data", "")
	env.tw.Snap("backup/data", "z")
	return env
}

func TestExecutorForceDestroyTarget(t *testing.T) {
	env := conflictingTarget(t)
	ts := syncName(10)
	states, plans := env.plan(ts)
	if len(plans[0].Actions) != 1 || plans[0].Actions[0] != (ForceDestroyTarget{}) {
		t.Fatalf("plan %+v", plans[0])
	}
	res := env.exec(context.Background(), ts, states, plans)
	wantStatuses(t, res, StatusOK)
	wantCalls(t, "source", env.src.Calls(), []string{"Snapshot tank/data@" + ts, "Send tank/data@a", "Send tank/data@" + ts})
	wantCalls(t, "target", env.dst.Calls(), []string{"DestroyDataset -r backup/data", "Receive backup/data", "Receive backup/data"})
	if got, want := dsSnaps(env.tw, "backup/data"), []string{"a", ts}; !eqStrings(got, want) {
		t.Errorf("target snaps %v, want %v", got, want)
	}
}

func TestExecutorConfirmDestructive(t *testing.T) {
	env := conflictingTarget(t)
	var asked []string
	env.confirm = func(q string) bool {
		asked = append(asked, q)
		return false
	}
	res := env.run(syncName(10))
	wantStatuses(t, res, StatusCritical)
	if res[0].Message != "destructive action declined" {
		t.Errorf("message %q", res[0].Message)
	}
	if want := []string{"Destroy backup/data recursively (--force-delete)?"}; !eqStrings(asked, want) {
		t.Errorf("asked %q, want %q", asked, want)
	}
	wantCalls(t, "target", env.dst.Calls(), nil)
	if got := dsSnaps(env.tw, "backup/data"); !eqStrings(got, []string{"z"}) {
		t.Errorf("target snaps %v", got)
	}
}

func TestExecutorCloneFallback(t *testing.T) {
	env := newExecEnv(t, "tank/data", "backup/data")
	env.opts.Recursive = true
	env.sw.Create("tank", "")
	env.sw.Create("tank/data", "")
	env.sw.Snap("tank/data", "a")
	env.sw.Create("tank/data/clone", "")
	env.sw.Dataset("tank/data/clone").Origin = "tank/data@a"
	env.sw.Snap("tank/data/clone", "b")
	env.tw.Create("backup", "")
	env.src.Faults.Send = func(s zfs.SendSpec) error {
		if s.FromOrigin != "" {
			return errors.New("injected clone send failure")
		}
		return nil
	}
	ts := syncName(10)
	states, plans := env.plan(ts)
	if got := plans[1].Actions[0]; got != (SendClone{Origin: "tank/data@a", To: "b"}) {
		t.Fatalf("clone plan starts with %v", got)
	}
	res := env.exec(context.Background(), ts, states, plans)
	wantStatuses(t, res, StatusOK, StatusOK)
	if n := countPrefix(env.src.Calls(), "Send tank/data/clone@b"); n != 2 {
		t.Errorf("clone base sent %d times, want 2 (clone then full): %q", n, env.src.Calls())
	}
	if n := countPrefix(env.src.Calls(), "Snapshot "); n != 1 {
		t.Errorf("%d Snapshot calls, want 1", n)
	}
	d := env.tw.Dataset("backup/data/clone")
	if d == nil || d.Origin != "" || !eqStrings(names(d.Snaps), []string{"b", ts}) {
		t.Errorf("target clone %+v", d)
	}
}

func TestExecutorSourceVanished(t *testing.T) {
	env := newExecEnv(t, "tank/data", "backup/data")
	env.sw.Create("tank", "")
	env.sw.Create("tank/data", "")
	env.sw.Snap("tank/data", "a")
	env.tw.Create("backup", "")
	remover := fake.New(env.sw, endpoint.HostInfo{})
	var once sync.Once
	env.src.Faults.Send = func(zfs.SendSpec) error {
		var err error
		once.Do(func() { err = remover.DestroyDataset(context.Background(), "tank/data", true) })
		return err
	}
	res := env.run(syncName(10))
	wantStatuses(t, res, StatusSkipped)
	want := "Skipping dataset (dataset no longer exists): tank/data..."
	if res[0].Message != want {
		t.Errorf("message %q, want %q", res[0].Message, want)
	}
	if !containsMsg(env.logs.at(slog.LevelWarn), want) {
		t.Errorf("missing WARN %q in %q", want, env.logs.at(slog.LevelWarn))
	}
}

func TestExecutorTargetMissingParentIsCritical(t *testing.T) {
	env := newExecEnv(t, "tank/a", "backup/a")
	env.sw.Create("tank", "")
	env.sw.Create("tank/a", "")
	env.sw.Snap("tank/a", "a")
	env.dst.Faults.Receive = func(zfs.RecvSpec) error {
		return zfsErr("cannot open 'backup/a': dataset does not exist")
	}
	res := env.run(syncName(10))
	wantStatuses(t, res, StatusCritical)
	if !strings.Contains(res[0].Message, "dataset does not exist") {
		t.Errorf("message %q", res[0].Message)
	}
	if !errors.Is(res[0].Err, zfs.ErrNoDataset) {
		t.Errorf("Err %v does not wrap ErrNoDataset", res[0].Err)
	}
	for _, w := range env.logs.at(slog.LevelWarn) {
		if strings.HasPrefix(w, "Skipping dataset") {
			t.Errorf("unexpected skip warning %q", w)
		}
	}
}

// stuckResume reports a resume token on every target dataset and pretends
// to abort it, so a resume can fail forever.
type stuckResume struct {
	*fake.Endpoint
	mu     sync.Mutex
	aborts int
}

func (s *stuckResume) ListTree(ctx context.Context, root string, recursive bool) ([]zfs.DatasetInfo, error) {
	tree, err := s.Endpoint.ListTree(ctx, root, recursive)
	for i := range tree {
		tree[i].ResumeToken = "tok-stuck"
	}
	return tree, err
}

func (s *stuckResume) AbortResume(context.Context, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.aborts++
	return nil
}

func TestExecutorRemediationBound(t *testing.T) {
	env := seededEnv(t)
	stuck := &stuckResume{Endpoint: env.dst}
	env.dstEP = stuck
	env.src.Faults.Send = func(s zfs.SendSpec) error {
		if s.ResumeToken != "" {
			return zfsErr("cannot resume send: 'tank/data@b' used in the initial send no longer exists")
		}
		return nil
	}
	res := env.run(syncName(10))
	wantStatuses(t, res, StatusCritical)
	if want := "giving up after 3 re-plan rounds: "; !strings.HasPrefix(res[0].Message, want) {
		t.Errorf("message %q, want prefix %q", res[0].Message, want)
	}
	if n := countPrefix(env.src.Calls(), "Send tok-stuck"); n != 4 {
		t.Errorf("%d resume sends, want 4 (initial + 3 rounds)", n)
	}
	if stuck.aborts != 3 {
		t.Errorf("%d aborts, want 3", stuck.aborts)
	}
}

// noopDestroy pretends every snapshot destroy succeeds.
type noopDestroy struct {
	*fake.Endpoint
	mu        sync.Mutex
	destroyed []string
}

func (n *noopDestroy) Destroy(_ context.Context, ds string, snaps []string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.destroyed = append(n.destroyed, ds+"@"+strings.Join(snaps, ","))
	return nil
}

func TestExecutorRemediationBoundMixedKinds(t *testing.T) {
	// Every receive fails with a conflicting destination (and leaves a resume
	// token), every resume fails because its source is gone: the rounds
	// alternate destroy-conflict and abort-resume and still stop at 3.
	env := seededEnv(t)
	env.opts.ForceDelete = true
	nd := &noopDestroy{Endpoint: env.dst}
	env.dstEP = nd
	env.dst.Faults.Receive = func(zfs.RecvSpec) error {
		return zfsErr("cannot restore to backup/data@x: destination already exists")
	}
	env.src.Faults.Send = func(s zfs.SendSpec) error {
		if s.ResumeToken != "" {
			return zfsErr("cannot resume send: 'tank/data@b' used in the initial send no longer exists")
		}
		return nil
	}
	res := env.run(syncName(10))
	wantStatuses(t, res, StatusCritical)
	if want := "giving up after 3 re-plan rounds: "; !strings.HasPrefix(res[0].Message, want) {
		t.Errorf("message %q, want prefix %q", res[0].Message, want)
	}
	if !errors.Is(res[0].Err, zfs.ErrResumeSourceGone) {
		t.Errorf("last error %v, want ErrResumeSourceGone", res[0].Err)
	}
	if want := []string{"backup/data@x", "backup/data@x"}; !eqStrings(nd.destroyed, want) {
		t.Errorf("destroyed %q, want %q", nd.destroyed, want)
	}
	if n := countPrefix(env.dst.Calls(), "AbortResume"); n != 1 {
		t.Errorf("%d AbortResume calls, want 1", n)
	}
	if n := countPrefix(env.src.Calls(), "Send tok-"); n != 2 {
		t.Errorf("%d resume sends, want 2", n)
	}
}

func TestExecutorForceDestroyGuards(t *testing.T) {
	ts := syncName(10)
	// run executes the ForceDestroyTarget plan directly on a dsRun prepared
	// by mutate, so the guards are reached in states Run cannot easily set up.
	run := func(env *execEnv, mutate func(d *dsRun)) Result {
		states, plans := env.plan(ts)
		if len(plans[0].Actions) != 1 || plans[0].Actions[0] != (ForceDestroyTarget{}) {
			t.Fatalf("plan %+v", plans[0])
		}
		d := &dsRun{e: env.executor(ts), st: states[0], plan: plans[0], res: Result{Source: "tank/data", Target: "backup/data"}}
		mutate(d)
		d.run(context.Background())
		return d.res
	}
	t.Run("budget exhausted", func(t *testing.T) {
		env := conflictingTarget(t)
		res := run(env, func(d *dsRun) { d.rounds = maxReplans })
		if res.Status != StatusCritical || res.Message != "giving up after 3 re-plan rounds" {
			t.Errorf("result %v %q", res.Status, res.Message)
		}
		wantCalls(t, "target", env.dst.Calls(), nil)
	})
	t.Run("busy target", func(t *testing.T) {
		env := conflictingTarget(t)
		env.dst.Faults.Busy = map[string]bool{"backup/data": true}
		res := run(env, func(*dsRun) {})
		if res.Status != StatusSoft || res.Message != busyMessage("backup/data") {
			t.Errorf("result %v %q", res.Status, res.Message)
		}
		wantCalls(t, "target", env.dst.Calls(), nil)
	})
	t.Run("pool root", func(t *testing.T) {
		env := conflictingTarget(t)
		res := run(env, func(d *dsRun) { d.st.TargetIsPoolRoot = true })
		if res.Status != StatusCritical || !strings.Contains(res.Message, "refusing to destroy pool root dataset backup/data") {
			t.Errorf("result %v %q", res.Status, res.Message)
		}
		wantCalls(t, "target", env.dst.Calls(), nil)
	})
}

// cancellingBusy cancels the run during its at-th ReceiveBusy call.
type cancellingBusy struct {
	*fake.Endpoint
	cancel context.CancelFunc
	at, n  int
}

func (c *cancellingBusy) ReceiveBusy(ctx context.Context, ds []string) (map[string]bool, error) {
	c.n++
	if c.n == c.at {
		c.cancel()
		return nil, ctx.Err()
	}
	return c.Endpoint.ReceiveBusy(ctx, ds)
}

func TestExecutorCancelDuringBusyCheck(t *testing.T) {
	ts := syncName(10)
	tests := []struct {
		name      string
		at        int
		wantSrc   []string
		situation string
	}{
		{"batch check", 1, nil, "before the sync snapshots"},
		{"per-dataset check", 2, []string{"Snapshot tank/data@" + ts}, "before the first send"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newExecEnv(t, "tank/data", "backup/data")
			env.sw.Create("tank", "")
			env.sw.Create("tank/data", "")
			env.sw.Snap("tank/data", "a")
			env.tw.Create("backup", "")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			env.dstEP = &cancellingBusy{Endpoint: env.dst, cancel: cancel, at: tt.at}
			states, plans := env.plan(ts)
			res := env.exec(ctx, ts, states, plans)
			wantStatuses(t, res, StatusCritical)
			if res[0].Message != "cancelled" {
				t.Errorf("message %q, want cancelled (%s)", res[0].Message, tt.situation)
			}
			if w := env.logs.at(slog.LevelWarn); len(w) > 0 {
				t.Errorf("unexpected warnings %q", w)
			}
			wantCalls(t, "source", env.src.Calls(), tt.wantSrc)
			wantCalls(t, "target", env.dst.Calls(), nil)
		})
	}
}

func TestExecutorSourceVanishedBeforeRun(t *testing.T) {
	t.Run("before the sync snapshots", func(t *testing.T) {
		ts := syncName(10)
		env := threeDatasets(t)
		states, plans := env.plan(ts)
		if err := fake.New(env.sw, endpoint.HostInfo{}).DestroyDataset(context.Background(), "tank/data/b", true); err != nil {
			t.Fatal(err)
		}
		res := env.exec(context.Background(), ts, states, plans)
		wantStatuses(t, res, StatusOK, StatusOK, StatusSkipped)
		want := "Skipping dataset (dataset no longer exists): tank/data/b..."
		if res[2].Message != want || !containsMsg(env.logs.at(slog.LevelWarn), want) {
			t.Errorf("message %q, warnings %q; want %q", res[2].Message, env.logs.at(slog.LevelWarn), want)
		}
	})
	t.Run("before the refresh", func(t *testing.T) {
		env := newExecEnv(t, "tank/data", "backup/data")
		env.opts.NoSyncSnap = true
		env.sw.Create("tank", "")
		env.sw.Create("tank/data", "")
		env.sw.Snap("tank/data", "a")
		env.tw.Create("backup", "")
		states, plans := env.plan("")
		if err := fake.New(env.sw, endpoint.HostInfo{}).DestroyDataset(context.Background(), "tank/data", true); err != nil {
			t.Fatal(err)
		}
		res := env.exec(context.Background(), "", states, plans)
		wantStatuses(t, res, StatusSkipped)
		want := "Skipping dataset (dataset no longer exists): tank/data..."
		if res[0].Message != want || !containsMsg(env.logs.at(slog.LevelWarn), want) {
			t.Errorf("message %q, warnings %q; want %q", res[0].Message, env.logs.at(slog.LevelWarn), want)
		}
		wantCalls(t, "target", env.dst.Calls(), nil)
	})
}
