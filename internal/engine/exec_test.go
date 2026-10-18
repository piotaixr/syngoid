package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/piotaixr/syngoid/internal/cli"
	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/endpoint/fake"
	"github.com/piotaixr/syngoid/internal/naming"
	"github.com/piotaixr/syngoid/internal/stream"
	"github.com/piotaixr/syngoid/internal/zfs"
)

// logRecord is one captured log line.
type logRecord struct {
	level slog.Level
	msg   string
}

// captureHandler records every log message.
type captureHandler struct {
	mu   sync.Mutex
	recs []logRecord
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.recs = append(h.recs, logRecord{r.Level, r.Message})
	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }

// at returns the messages logged at level.
func (h *captureHandler) at(level slog.Level) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, r := range h.recs {
		if r.level == level {
			out = append(out, r.msg)
		}
	}
	return out
}

// execEnv is a source host and a target host replicating Source to Target.
type execEnv struct {
	t        *testing.T
	sw, tw   *fake.World
	src, dst *fake.Endpoint
	// srcEP and dstEP are the endpoints used by plan and exec; they default
	// to src and dst and may be replaced by wrappers.
	srcEP, dstEP endpoint.Endpoint
	opts         *cli.Options
	logs         *captureHandler
	confirm      func(string) bool
}

func newExecEnv(t *testing.T, source, target string) *execEnv {
	t.Helper()
	sw, tw := fake.NewWorld(), fake.NewWorld()
	src, dst := fake.New(sw, endpoint.HostInfo{}), fake.New(tw, endpoint.HostInfo{})
	return &execEnv{
		t: t, sw: sw, tw: tw, src: src, dst: dst, srcEP: src, dstEP: dst,
		opts: &cli.Options{Source: cli.Location{Dataset: source}, Target: cli.Location{Dataset: target}},
		logs: &captureHandler{},
	}
}

// syncName returns a valid sync snapshot name for host nas at hour:00 UTC.
func syncName(hour int) string {
	return naming.SyncSnapName("", "nas", time.Date(2026, 10, 18, hour, 0, 0, 0, time.UTC))
}

// plan gathers and plans the replication as the app would.
func (e *execEnv) plan(syncSnap string) ([]DatasetState, []Plan) {
	e.t.Helper()
	states, err := Gather(context.Background(), e.srcEP, e.dstEP, e.opts, syncSnap)
	if err != nil {
		e.t.Fatal(err)
	}
	po := PlanOptionsFrom(e.opts, "nas")
	plans := make([]Plan, len(states))
	for i, s := range states {
		plans[i] = PlanDataset(s, po)
	}
	return states, plans
}

func (e *execEnv) executor(syncSnap string) *Executor {
	return &Executor{
		Src: e.srcEP, Dst: e.dstEP, Opts: e.opts,
		PlanOpts:  PlanOptionsFrom(e.opts, "nas"),
		Log:       slog.New(e.logs),
		Progress:  stream.NewRenderer(stream.ProgressNone, io.Discard, false, nil),
		Confirm:   e.confirm,
		SendCodec: stream.None, RecvCodec: stream.None,
		SyncSnap: syncSnap,
	}
}

func (e *execEnv) exec(ctx context.Context, syncSnap string, states []DatasetState, plans []Plan) []Result {
	return e.executor(syncSnap).Run(ctx, states, plans)
}

func (e *execEnv) run(syncSnap string) []Result {
	e.t.Helper()
	states, plans := e.plan(syncSnap)
	return e.exec(context.Background(), syncSnap, states, plans)
}

// transfer replicates s from src into target with receive args, the way
// earlier runs would have, so tests can seed a target.
func transfer(src, dst *fake.Endpoint, s zfs.SendSpec, target string, args ...string) error {
	pr, pw := io.Pipe()
	go func() {
		pw.CloseWithError(src.Send(context.Background(), s, endpoint.SendOpts{Codec: stream.None}, pw))
	}()
	err := dst.Receive(context.Background(), zfs.RecvSpec{Dataset: target, Args: args}, stream.None, pr)
	_ = pr.Close()
	return err
}

func mustTransfer(t *testing.T, src, dst *fake.Endpoint, s zfs.SendSpec, target string, args ...string) {
	t.Helper()
	if err := transfer(src, dst, s, target, args...); err != nil {
		t.Fatal(err)
	}
}

// zfsErr builds the error a zfs command failing with stderr returns.
func zfsErr(stderr string) error {
	return &zfs.CommandError{Argv: []string{"zfs"}, ExitCode: 1, Stderr: stderr, Kind: zfs.Classify(stderr)}
}

func dsSnaps(w *fake.World, ds string) []string {
	d := w.Dataset(ds)
	if d == nil {
		return nil
	}
	return names(d.Snaps)
}

// callsFrom returns the calls of ep made after the first n.
func callsFrom(ep *fake.Endpoint, n int) []string { return ep.Calls()[n:] }

func countPrefix(list []string, prefix string) int {
	n := 0
	for _, s := range list {
		if strings.HasPrefix(s, prefix) {
			n++
		}
	}
	return n
}

func containsMsg(msgs []string, want string) bool {
	for _, m := range msgs {
		if m == want {
			return true
		}
	}
	return false
}

func wantCalls(t *testing.T, what string, got, want []string) {
	t.Helper()
	if !eqStrings(got, want) {
		t.Errorf("%s calls:\n got  %q\n want %q", what, got, want)
	}
}

func wantStatuses(t *testing.T, res []Result, want ...Status) {
	t.Helper()
	if len(res) != len(want) {
		t.Fatalf("got %d results, want %d: %+v", len(res), len(want), res)
	}
	for i, r := range res {
		if r.Status != want[i] {
			t.Errorf("result %d (%s): status %v (%q), want %v", i, r.Source, r.Status, r.Message, want[i])
		}
	}
}

func TestExecutorFirstAndSecondRun(t *testing.T) {
	env := newExecEnv(t, "tank/data", "backup/data")
	env.sw.Create("tank", "")
	env.sw.Create("tank/data", "")
	for _, s := range []string{"a", "b", "c"} {
		env.sw.Snap("tank/data", s)
	}
	env.tw.Create("backup", "")

	t1 := syncName(10)
	res := env.run(t1)
	wantStatuses(t, res, StatusOK)
	if got, want := dsSnaps(env.tw, "backup/data"), []string{"a", "b", "c", t1}; !eqStrings(got, want) {
		t.Errorf("target after run 1: %v, want %v", got, want)
	}
	wantCalls(t, "source", env.src.Calls(), []string{"Snapshot tank/data@" + t1, "Send tank/data@a", "Send tank/data@" + t1})
	wantCalls(t, "target", env.dst.Calls(), []string{"Receive backup/data", "Receive backup/data"})
	info := env.logs.at(slog.LevelInfo)
	for _, want := range []string{
		"Sending oldest full snapshot tank/data@a to new target filesystem backup/data (~ 1.0 MiB):",
		"Sending incremental tank/data@a ... " + t1 + " to backup/data (~ 3.0 MiB):",
	} {
		if !containsMsg(info, want) {
			t.Errorf("missing INFO %q in %q", want, info)
		}
	}
	if res[0].Bytes <= 0 {
		t.Errorf("Bytes = %d, want > 0", res[0].Bytes)
	}

	env.sw.Snap("tank/data", "d")
	t2 := syncName(11)
	ns, nd := len(env.src.Calls()), len(env.dst.Calls())
	res = env.run(t2)
	wantStatuses(t, res, StatusOK)
	if got, want := dsSnaps(env.tw, "backup/data"), []string{"a", "b", "c", "d", t2}; !eqStrings(got, want) {
		t.Errorf("target after run 2: %v, want %v", got, want)
	}
	if got, want := dsSnaps(env.sw, "tank/data"), []string{"a", "b", "c", "d", t2}; !eqStrings(got, want) {
		t.Errorf("source after run 2: %v, want %v", got, want)
	}
	wantCalls(t, "source", callsFrom(env.src, ns), []string{"Snapshot tank/data@" + t2, "Send tank/data@" + t2, "Destroy tank/data@" + t1})
	wantCalls(t, "target", callsFrom(env.dst, nd), []string{"Receive backup/data", "Destroy backup/data@" + t1})
	if !containsMsg(env.logs.at(slog.LevelInfo), "Sending incremental tank/data@"+t1+" ... "+t2+" to backup/data (~ 2.0 MiB):") {
		t.Errorf("missing incremental INFO in %q", env.logs.at(slog.LevelInfo))
	}
	if w := env.logs.at(slog.LevelWarn); len(w) > 0 {
		t.Errorf("unexpected warnings %q", w)
	}
}

// threeDatasets sets up tank/data with children a and b, one snapshot each,
// replicated recursively to the absent backup/data.
func threeDatasets(t *testing.T) *execEnv {
	env := newExecEnv(t, "tank/data", "backup/data")
	env.opts.Recursive = true
	env.sw.Create("tank", "")
	for _, ds := range []string{"tank/data", "tank/data/a", "tank/data/b"} {
		env.sw.Create(ds, "")
		env.sw.Snap(ds, "s1")
	}
	env.tw.Create("backup", "")
	return env
}

func TestExecutorBatchedSyncSnaps(t *testing.T) {
	ts := syncName(10)
	t.Run("one batch", func(t *testing.T) {
		env := threeDatasets(t)
		res := env.run(ts)
		wantStatuses(t, res, StatusOK, StatusOK, StatusOK)
		calls := env.src.Calls()
		if n := countPrefix(calls, "Snapshot "); n != 1 {
			t.Errorf("%d Snapshot calls, want 1: %q", n, calls)
		}
		if want := "Snapshot tank/data@" + ts + ",tank/data/a@" + ts + ",tank/data/b@" + ts; calls[0] != want {
			t.Errorf("first call %q, want %q", calls[0], want)
		}
	})
	t.Run("fallback per dataset", func(t *testing.T) {
		env := threeDatasets(t)
		states, plans := env.plan(ts)
		env.sw.Snap("tank/data/b", ts) // makes the batch and b's own call fail
		res := env.exec(context.Background(), ts, states, plans)
		wantStatuses(t, res, StatusOK, StatusOK, StatusCritical)
		var snaps []string
		for _, c := range env.src.Calls() {
			if strings.HasPrefix(c, "Snapshot ") {
				snaps = append(snaps, c)
			}
		}
		wantCalls(t, "snapshot", snaps, []string{
			"Snapshot tank/data@" + ts + ",tank/data/a@" + ts + ",tank/data/b@" + ts,
			"Snapshot tank/data@" + ts, "Snapshot tank/data/a@" + ts, "Snapshot tank/data/b@" + ts,
		})
		if want := "creating sync snapshot tank/data/b@" + ts + " failed: "; !strings.HasPrefix(res[2].Message, want) {
			t.Errorf("message %q, want prefix %q", res[2].Message, want)
		}
		if got := dsSnaps(env.tw, "backup/data/a"); !eqStrings(got, []string{"s1", ts}) {
			t.Errorf("backup/data/a snaps %v", got)
		}
	})
}

func TestExecutorBusyTarget(t *testing.T) {
	ts := syncName(10)
	env := threeDatasets(t)
	env.dst.Faults.Busy = map[string]bool{"backup/data/a": true}
	res := env.run(ts)
	wantStatuses(t, res, StatusOK, StatusSoft, StatusOK)
	if want := "Cannot sync now: backup/data/a is already target of a zfs receive process."; res[1].Message != want {
		t.Errorf("message %q, want %q", res[1].Message, want)
	}
	if want := "Snapshot tank/data@" + ts + ",tank/data/b@" + ts; env.src.Calls()[0] != want {
		t.Errorf("first call %q, want %q", env.src.Calls()[0], want)
	}
	if n := countPrefix(env.dst.Calls(), "Receive backup/data/a"); n != 0 {
		t.Errorf("busy target received %d times", n)
	}
	if env.tw.Dataset("backup/data/b") == nil {
		t.Error("backup/data/b not replicated")
	}
}

func TestExecutorCancelMidTransfer(t *testing.T) {
	env := newExecEnv(t, "tank", "backup")
	env.opts.Recursive, env.opts.SkipParent = true, true
	env.sw.Create("tank", "")
	for _, ds := range []string{"tank/a", "tank/b"} {
		env.sw.Create(ds, "")
		env.sw.Snap(ds, "s1")
	}
	env.tw.Create("backup", "")
	ts := syncName(10)
	states, plans := env.plan(ts)

	started := make(chan struct{})
	var once sync.Once
	env.src.Faults.BlockSend = true
	env.src.Faults.Send = func(zfs.SendSpec) error {
		once.Do(func() { close(started) })
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan []Result, 1)
	go func() { done <- env.exec(ctx, ts, states, plans) }()
	<-started
	cancel()
	var res []Result
	select {
	case res = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within 2s of cancellation")
	}
	wantStatuses(t, res, StatusCritical, StatusCritical)
	for _, r := range res {
		if r.Message != "cancelled" {
			t.Errorf("%s: message %q, want cancelled", r.Source, r.Message)
		}
	}
	if n := countPrefix(env.src.Calls(), "Send "); n != 1 {
		t.Errorf("%d sends started, want 1: %q", n, env.src.Calls())
	}
	if n := countPrefix(env.dst.Calls(), "Receive "); n != 1 {
		t.Errorf("%d receives started, want 1: %q", n, env.dst.Calls())
	}
}

func TestExecutorDependencySkip(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(env *execEnv)
		parent Status
	}{
		{"critical parent", func(env *execEnv) {
			env.src.Faults.Send = func(s zfs.SendSpec) error {
				if s.Dataset == "tank/data" {
					return errors.New("injected send failure")
				}
				return nil
			}
		}, StatusCritical},
		{"busy parent", func(env *execEnv) {
			env.dst.Faults.Busy = map[string]bool{"backup/data": true}
		}, StatusSoft},
		{"parent skipped by property", func(env *execEnv) {
			env.sw.SetProp("tank/data", "syncoid:sync", "false")
		}, StatusSkipped},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newExecEnv(t, "tank", "backup")
			env.opts.Recursive, env.opts.SkipParent = true, true
			env.sw.Create("tank", "")
			for _, ds := range []string{"tank/data", "tank/data/a", "tank/other"} {
				env.sw.Create(ds, "")
				env.sw.Snap(ds, "s1")
			}
			env.tw.Create("backup", "")
			tt.setup(env)
			res := env.run(syncName(10))
			wantStatuses(t, res, tt.parent, StatusSoft, StatusOK)
			if want := "skipped: dependency tank/data failed"; res[1].Message != want {
				t.Errorf("message %q, want %q", res[1].Message, want)
			}
			if n := countPrefix(env.src.Calls(), "Send tank/data/a@"); n != 0 {
				t.Errorf("dependent dataset was sent %d times", n)
			}
			if n := countPrefix(env.dst.Calls(), "Receive backup/data/a"); n != 0 {
				t.Errorf("dependent dataset was received %d times", n)
			}
		})
	}
}

func TestExecutorWarningsOnly(t *testing.T) {
	t.Run("post actions", func(t *testing.T) {
		env := newExecEnv(t, "tank/data", "backup/data")
		env.opts.UseHold, env.opts.DeleteTargetSnapshots = true, true
		t1, t2 := syncName(10), syncName(11)
		env.sw.Create("tank", "")
		env.sw.Create("tank/data", "")
		for _, s := range []string{"a", "z", t1} {
			env.sw.Snap("tank/data", s)
		}
		env.tw.Create("backup", "")
		mustTransfer(t, env.src, env.dst, zfs.SendSpec{Dataset: "tank/data", To: "a"}, "backup/data")
		mustTransfer(t, env.src, env.dst, zfs.SendSpec{Dataset: "tank/data", FromSnap: "a", To: t1, Intermediates: true}, "backup/data")
		if err := env.src.Destroy(context.Background(), "tank/data", []string{"z"}); err != nil {
			t.Fatal(err)
		}
		env.srcEP, env.dstEP = failingMutations{env.src}, failingMutations{env.dst}

		res := env.run(t2)
		wantStatuses(t, res, StatusOK)
		warns := env.logs.at(slog.LevelWarn)
		for _, a := range []Action{
			Hold{Source, t2},
			Release{Source, t1},
			Hold{Target, t2},
			Release{Target, t1},
			PruneSyncSnaps{Source, []string{t1}},
			PruneSyncSnaps{Target, []string{t1}},
			DestroyTargetSnaps{},
		} {
			want := "tank/data: " + a.Describe() + ": " + errInjected.Error()
			if !containsMsg(warns, want) {
				t.Errorf("missing WARN %q in %q", want, warns)
			}
		}
		if len(warns) != 7 {
			t.Errorf("%d warnings, want 7: %q", len(warns), warns)
		}
	})

	bookmarkEnv := func(t *testing.T) (*execEnv, string) {
		env := newExecEnv(t, "tank/data", "backup/data")
		env.opts.NoSyncSnap, env.opts.CreateBookmark = true, true
		env.sw.Create("tank", "")
		env.sw.Create("tank/data", "")
		env.sw.Snap("tank/data", "a")
		c := env.sw.Snap("tank/data", "c")
		env.tw.Create("backup", "")
		mustTransfer(t, env.src, env.dst, zfs.SendSpec{Dataset: "tank/data", To: "a"}, "backup/data")
		env.sw.MarkBookmark("tank/data", "c", "c") // the first attempt fails
		return env, naming.BookmarkFallback("c", c.GUID)
	}
	t.Run("bookmark fallback name", func(t *testing.T) {
		env, alt := bookmarkEnv(t)
		n := len(env.src.Calls())
		res := env.run("")
		wantStatuses(t, res, StatusOK)
		wantCalls(t, "source", callsFrom(env.src, n), []string{
			"Send tank/data@c", "Bookmark tank/data@c tank/data#c", "Bookmark tank/data@c tank/data#" + alt,
		})
	})
	t.Run("bookmark fails twice", func(t *testing.T) {
		env, alt := bookmarkEnv(t)
		env.sw.MarkBookmark("tank/data", "c", alt)
		res := env.run("")
		wantStatuses(t, res, StatusCritical)
		if want := "tank/data: create bookmark #c: "; !strings.HasPrefix(res[0].Message, want) {
			t.Errorf("message %q, want prefix %q", res[0].Message, want)
		}
		if n := countPrefix(env.src.Calls(), "Bookmark "); n != 2 {
			t.Errorf("%d Bookmark calls, want 2", n)
		}
	})
}

var errInjected = errors.New("injected failure")

// failingMutations fails every hold, release and snapshot destroy.
type failingMutations struct{ *fake.Endpoint }

func (failingMutations) Hold(context.Context, string, string) error      { return errInjected }
func (failingMutations) Release(context.Context, string, string) error   { return errInjected }
func (failingMutations) Destroy(context.Context, string, []string) error { return errInjected }

func TestExecutorDeleteTargetSnapshots(t *testing.T) {
	env := newExecEnv(t, "tank/data", "backup/data")
	env.opts.DeleteTargetSnapshots = true
	env.sw.Create("tank", "")
	env.sw.Create("tank/data", "")
	env.sw.Snap("tank/data", "base")
	var gone []string
	for i := range 120 {
		gone = append(gone, fmt.Sprintf("s%03d", i))
		env.sw.Snap("tank/data", gone[i])
	}
	env.sw.Snap("tank/data", "last")
	env.tw.Create("backup", "")
	mustTransfer(t, env.src, env.dst, zfs.SendSpec{Dataset: "tank/data", To: "base"}, "backup/data")
	mustTransfer(t, env.src, env.dst, zfs.SendSpec{Dataset: "tank/data", FromSnap: "base", To: "last", Intermediates: true}, "backup/data")
	if err := env.src.Destroy(context.Background(), "tank/data", gone); err != nil {
		t.Fatal(err)
	}
	n := len(env.dst.Calls())
	ts := syncName(10)

	res := env.run(ts)
	wantStatuses(t, res, StatusOK)
	wantCalls(t, "target", callsFrom(env.dst, n), []string{
		"Receive backup/data",
		"Destroy backup/data@" + strings.Join(gone[:50], ","),
		"Destroy backup/data@" + strings.Join(gone[50:100], ","),
		"Destroy backup/data@" + strings.Join(gone[100:], ","),
	})
	if got, want := dsSnaps(env.tw, "backup/data"), []string{"base", "last", ts}; !eqStrings(got, want) {
		t.Errorf("target snaps %v, want %v", got, want)
	}
}

func TestExecutorStaleBase(t *testing.T) {
	env := newExecEnv(t, "tank/data", "backup/data")
	env.sw.Create("tank", "")
	env.sw.Create("tank/data", "")
	for _, s := range []string{"a", "b", "c"} {
		env.sw.Snap("tank/data", s)
	}
	env.tw.Create("backup", "")
	mustTransfer(t, env.src, env.dst, zfs.SendSpec{Dataset: "tank/data", To: "a"}, "backup/data")
	mustTransfer(t, env.src, env.dst, zfs.SendSpec{Dataset: "tank/data", FromSnap: "a", To: "b", Intermediates: true}, "backup/data")
	ts := syncName(10)
	states, plans := env.plan(ts)
	if got := plans[0].Actions[0]; got != (SendIncremental{From: "b", To: ts, Intermediates: true}) {
		t.Fatalf("initial plan starts with %v", got)
	}
	env.tw.Dataset("backup/data").Snaps[1].GUID = "123" // target b is no longer the source's b

	res := env.exec(context.Background(), ts, states, plans)
	wantStatuses(t, res, StatusOK)
	if want := "plan for tank/data changed since planning; executing updated plan"; !containsMsg(env.logs.at(slog.LevelWarn), want) {
		t.Errorf("missing WARN %q in %q", want, env.logs.at(slog.LevelWarn))
	}
	if want := "Sending incremental tank/data@a ... " + ts + " to backup/data (~ 3.0 MiB):"; !containsMsg(env.logs.at(slog.LevelInfo), want) {
		t.Errorf("missing INFO %q in %q", want, env.logs.at(slog.LevelInfo))
	}
	tb := env.tw.Dataset("backup/data").Snaps[1]
	sb := env.sw.Dataset("tank/data").Snaps[1]
	if tb.Name != "b" || tb.GUID != sb.GUID {
		t.Errorf("target b = %+v, want guid %s", tb, sb.GUID)
	}
}

func TestSendMessage(t *testing.T) {
	st := DatasetState{Source: "tank/a", Target: "backup/a"}
	const gib = 1 << 30
	tests := []struct {
		a        Action
		est      int64
		noStream bool
		want     string
	}{
		{SendFull{Snap: "s"}, 0, false, "Sending oldest full snapshot tank/a@s to new target filesystem backup/a (~ UNKNOWN):"},
		{SendFull{Snap: "s"}, gib, true, "--no-stream selected; sending newest full snapshot tank/a@s to new target filesystem backup/a: (~ 1.0 GiB)"},
		{SendIncremental{From: "b", To: "T"}, 1288490189, false, "Sending incremental tank/a@b ... T to backup/a (~ 1.2 GiB):"},
		{SendFromBookmark{Bookmark: "bm", To: "T"}, gib, false, "Sending incremental tank/a#bm ... T to backup/a:"},
		{SendClone{Origin: "tank/o@x", To: "s"}, gib, false, "Clone is recreated on target backup/a based on tank/o@x (~ 1.0 GiB):"},
		{Resume{Token: "tok"}, 0, false, "Resuming interrupted zfs send/receive from tank/a to backup/a (~ UNKNOWN remaining):"},
	}
	for _, tt := range tests {
		if got := sendMessage(st, tt.a, tt.est, tt.noStream); got != tt.want {
			t.Errorf("%T:\n got  %q\n want %q", tt.a, got, tt.want)
		}
	}
}
