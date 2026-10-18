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
