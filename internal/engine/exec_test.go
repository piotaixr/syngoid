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
		Progress:  stream.NewRenderer(stream.ProgressNone, nil, nil, time.Now),
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
	for _, jobs := range []int{1, 3} {
		for _, tt := range tests {
			t.Run(fmt.Sprintf("jobs=%d/%s", jobs, tt.name), func(t *testing.T) {
				env := newExecEnv(t, "tank", "backup")
				env.opts.Recursive, env.opts.SkipParent = true, true
				env.opts.Jobs = jobs
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

func TestExecutorLogsPlanOutcomes(t *testing.T) {
	env := newExecEnv(t, "tank/data", "backup/data")
	env.opts.Recursive = true
	env.opts.NoSyncSnap = true
	env.sw.Create("tank", "")
	env.sw.Create("tank/data", "")
	env.sw.Snap("tank/data", "a")
	env.sw.Create("tank/data/off", "")
	env.sw.Snap("tank/data/off", "a")
	env.sw.SetProp("tank/data/off", "syncoid:sync", "false")
	env.sw.Create("tank/data/empty", "")
	env.tw.Create("backup", "")
	mustTransfer(t, env.src, env.dst, zfs.SendSpec{Dataset: "tank/data", To: "a"}, "backup/data")
	env.tw.Create("backup/data/empty", "")
	env.tw.Snap("backup/data/empty", "z")

	res := env.run("")
	wantStatuses(t, res, StatusNothing, StatusSoft, StatusSkipped)
	if got, want := env.logs.at(slog.LevelInfo), []string{
		"no snapshots on source newer than a on target. Nothing to do, not syncing.",
		"Skipping dataset (syncoid:sync=false): tank/data/off...",
	}; !eqStrings(got, want) {
		t.Errorf("INFO %q, want %q", got, want)
	}
	if got, want := env.logs.at(slog.LevelWarn), []string{
		"no snapshots exist on source tank/data/empty, and you asked for --no-sync-snap.",
	}; !eqStrings(got, want) {
		t.Errorf("WARN %q, want %q", got, want)
	}
}

func TestExecutorWarnsDroppedUnprivProps(t *testing.T) {
	for _, tt := range []struct {
		name  string
		props map[string]string
		want  []string
	}{
		{
			"both",
			map[string]string{"mountpoint": "/x", "canmount": "noauto", "compression": "lz4"},
			[]string{"tank/data: not preserving canmount, mountpoint: they cannot be set on unprivileged Linux target backup/data"},
		},
		{
			"mountpoint only",
			map[string]string{"mountpoint": "/x"},
			[]string{"tank/data: not preserving mountpoint: it cannot be set on unprivileged Linux target backup/data"},
		},
		{"neither", map[string]string{"compression": "lz4"}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			env := newExecEnv(t, "tank/data", "backup/data")
			env.opts.PreserveProperties = true
			env.sw.Create("tank", "")
			env.sw.Create("tank/data", "")
			for k, v := range tt.props {
				env.sw.SetProp("tank/data", k, v)
			}
			env.sw.Snap("tank/data", "a")
			env.tw.Create("backup", "")
			ts := syncName(10)
			states, plans := env.plan(ts)
			ex := env.executor(ts)
			ex.UnprivLinuxTarget = true
			res := ex.Run(context.Background(), states, plans)
			wantStatuses(t, res, StatusOK)
			if n := countPrefix(env.dst.Calls(), "Receive "); n != 2 {
				t.Fatalf("%d receives, want 2 (full + incremental)", n)
			}
			if got := env.logs.at(slog.LevelWarn); !eqStrings(got, tt.want) {
				t.Errorf("WARN %q, want %q", got, tt.want)
			}
		})
	}
}

// recordingRenderer records the renderer calls the executor makes.
type recordingRenderer struct {
	mu     sync.Mutex
	events []string
}

func (r *recordingRenderer) record(ev string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
}

func (r *recordingRenderer) got() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

func (r *recordingRenderer) Start(label string, _ int64) stream.Transfer {
	r.record("start " + label)
	return &recordingTransfer{r: r, label: label}
}
func (r *recordingRenderer) Aborting() { r.record("aborting") }
func (r *recordingRenderer) Close()    {}

type recordingTransfer struct {
	r     *recordingRenderer
	label string
}

func (t *recordingTransfer) Update(int64) {}
func (t *recordingTransfer) Sent()        { t.r.record("sent " + t.label) }
func (t *recordingTransfer) Finish(_ int64, err error) {
	t.r.record(fmt.Sprintf("finish %s %v", t.label, err))
}

func TestExecutorTransferPhases(t *testing.T) {
	setup := func() (*execEnv, *recordingRenderer, []DatasetState, []Plan) {
		env := newExecEnv(t, "tank/a", "backup/a")
		env.opts.NoSyncSnap = true
		env.sw.Create("tank", "")
		env.sw.Create("tank/a", "")
		env.sw.Snap("tank/a", "s1")
		env.tw.Create("backup", "")
		mustTransfer(t, env.src, env.dst, zfs.SendSpec{Dataset: "tank/a", To: "s1"}, "backup/a")
		env.sw.Snap("tank/a", "s2")
		states, plans := env.plan("")
		return env, &recordingRenderer{}, states, plans
	}

	env, rec, states, plans := setup()
	ex := env.executor("")
	ex.Progress = rec
	wantStatuses(t, ex.Run(context.Background(), states, plans), StatusOK)
	wantCalls(t, "renderer", rec.got(), []string{"start tank/a", "sent tank/a", "finish tank/a <nil>"})

	env, rec, states, plans = setup()
	env.src.Faults.Send = func(zfs.SendSpec) error { return errors.New("injected") }
	ex = env.executor("")
	ex.Progress = rec
	ex.Run(context.Background(), states, plans)
	got := rec.got()
	if len(got) != 2 || got[0] != "start tank/a" || !strings.HasPrefix(got[1], "finish tank/a ") || !strings.Contains(got[1], "injected") {
		t.Errorf("renderer events on send failure = %q, want start then finish with injected", got)
	}
}

// waitTimeout bounds every wait of the parallel executor tests.
const waitTimeout = 5 * time.Second

// sendGate holds each dataset in its first send until the test releases it,
// recording which datasets are inside a send at the same time.
type sendGate struct {
	t       *testing.T
	mu      sync.Mutex
	running map[string]bool
	max     int
	entered map[string]bool
	release map[string]chan struct{}
	// onEnter, when set, is called under mu with the running set before the
	// dataset joins it.
	onEnter func(ds string, running map[string]bool)
	started chan string
}

// gateSends installs a sendGate as env.src's Send fault hook. Every gate
// left closed is released when the test ends.
func gateSends(t *testing.T, env *execEnv) *sendGate {
	g := &sendGate{
		t: t, running: map[string]bool{}, entered: map[string]bool{},
		release: map[string]chan struct{}{}, started: make(chan string, 16),
	}
	env.src.Faults.Send = g.hook
	t.Cleanup(func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		for ds, ch := range g.release {
			select {
			case <-ch:
			default:
				close(ch)
			}
			delete(g.release, ds)
		}
	})
	return g
}

func (g *sendGate) gate(ds string) chan struct{} {
	ch, ok := g.release[ds]
	if !ok {
		ch = make(chan struct{})
		g.release[ds] = ch
	}
	return ch
}

func (g *sendGate) hook(s zfs.SendSpec) error {
	ds := s.Dataset
	g.mu.Lock()
	if g.entered[ds] {
		g.mu.Unlock()
		return nil
	}
	g.entered[ds] = true
	if g.onEnter != nil {
		g.onEnter(ds, g.running)
	}
	g.running[ds] = true
	g.max = max(g.max, len(g.running))
	ch := g.gate(ds)
	g.mu.Unlock()
	g.started <- ds
	<-ch
	g.mu.Lock()
	delete(g.running, ds)
	g.mu.Unlock()
	return nil
}

// waitStarted returns the next dataset to enter its send.
func (g *sendGate) waitStarted() string {
	g.t.Helper()
	select {
	case ds := <-g.started:
		return ds
	case <-time.After(waitTimeout):
		g.t.Fatalf("no send started within %v", waitTimeout)
		return ""
	}
}

// noneStarted fails when a dataset enters its send within d.
func (g *sendGate) noneStarted(d time.Duration) {
	g.t.Helper()
	select {
	case ds := <-g.started:
		g.t.Fatalf("%s started its send while the running ones are held", ds)
	case <-time.After(d):
	}
}

// open lets dataset ds finish its send.
func (g *sendGate) open(ds string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	close(g.gate(ds))
}

func (g *sendGate) maxRunning() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.max
}

// startExec runs the executor in the background on a state planned now;
// each tweak adjusts the executor first.
func (e *execEnv) startExec(syncSnap string, tweaks ...func(*Executor)) <-chan []Result {
	e.t.Helper()
	states, plans := e.plan(syncSnap)
	ex := e.executor(syncSnap)
	for _, f := range tweaks {
		f(ex)
	}
	done := make(chan []Result, 1)
	go func() { done <- ex.Run(context.Background(), states, plans) }()
	return done
}

func waitResults(t *testing.T, done <-chan []Result) []Result {
	t.Helper()
	select {
	case res := <-done:
		return res
	case <-time.After(waitTimeout):
		t.Fatalf("Run did not return within %v", waitTimeout)
		return nil
	}
}

// waitFor polls cond until it holds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", waitTimeout, what)
		}
		time.Sleep(time.Millisecond)
	}
}

// flatDatasets sets up tank/<name> for each name, one snapshot each,
// replicated recursively without the parent into the existing backup.
func flatDatasets(t *testing.T, jobs int, names ...string) *execEnv {
	env := newExecEnv(t, "tank", "backup")
	env.opts.Recursive, env.opts.SkipParent = true, true
	env.opts.Jobs = jobs
	env.sw.Create("tank", "")
	for _, n := range names {
		env.sw.Create("tank/"+n, "")
		env.sw.Snap("tank/"+n, "s1")
	}
	env.tw.Create("backup", "")
	return env
}

func TestExecutorJobsLimit(t *testing.T) {
	env := flatDatasets(t, 3, "a", "b", "c", "d", "e")
	g := gateSends(t, env)
	done := env.startExec(syncName(10))
	var first []string
	for range 3 {
		first = append(first, g.waitStarted())
	}
	g.noneStarted(100 * time.Millisecond)
	for _, ds := range first {
		g.open(ds)
	}
	for range 2 {
		g.open(g.waitStarted())
	}
	res := waitResults(t, done)
	wantStatuses(t, res, StatusOK, StatusOK, StatusOK, StatusOK, StatusOK)
	if m := g.maxRunning(); m != 3 {
		t.Errorf("max running = %d, want 3", m)
	}
}

func TestExecutorJobsMoreThanDatasets(t *testing.T) {
	env := flatDatasets(t, 10, "a", "b")
	g := gateSends(t, env)
	done := env.startExec(syncName(10))
	a, b := g.waitStarted(), g.waitStarted()
	g.open(a)
	g.open(b)
	wantStatuses(t, waitResults(t, done), StatusOK, StatusOK)
	if m := g.maxRunning(); m != 2 {
		t.Errorf("max running = %d, want 2", m)
	}
}

func TestExecutorJobsParentFirst(t *testing.T) {
	env := newExecEnv(t, "tank/data", "backup/data")
	env.opts.Recursive, env.opts.Jobs = true, 2
	env.sw.Create("tank", "")
	for _, ds := range []string{"tank/data", "tank/data/a"} {
		env.sw.Create(ds, "")
		env.sw.Snap(ds, "s1")
	}
	env.tw.Create("backup", "")
	g := gateSends(t, env)
	childSaw := "not sent"
	g.onEnter = func(ds string, _ map[string]bool) {
		if ds == "tank/data/a" {
			childSaw = "no parent target"
			if env.tw.Dataset("backup/data") != nil {
				childSaw = "parent target"
			}
		}
	}
	done := env.startExec(syncName(10))
	if ds := g.waitStarted(); ds != "tank/data" {
		t.Fatalf("first send %s, want tank/data", ds)
	}
	g.noneStarted(100 * time.Millisecond)
	g.open("tank/data")
	g.open(g.waitStarted())
	wantStatuses(t, waitResults(t, done), StatusOK, StatusOK)
	g.mu.Lock()
	defer g.mu.Unlock()
	if childSaw != "parent target" {
		t.Errorf("child send saw %s, want parent target", childSaw)
	}
}

func TestExecutorJobsForceDeleteSerializesChild(t *testing.T) {
	env := newExecEnv(t, "tank/data", "backup/data")
	env.opts.Recursive, env.opts.ForceDelete, env.opts.Jobs = true, true, 2
	env.sw.Create("tank", "")
	env.tw.Create("backup", "")
	for _, ds := range []string{"tank/data", "tank/data/a"} {
		env.sw.Create(ds, "")
		env.sw.Snap(ds, "s1")
		mustTransfer(t, env.src, env.dst, zfs.SendSpec{Dataset: ds, To: "s1"}, "backup"+strings.TrimPrefix(ds, "tank"))
	}
	g := gateSends(t, env)
	overlap := false
	g.onEnter = func(ds string, running map[string]bool) {
		if ds == "tank/data/a" && running["tank/data"] {
			overlap = true
		}
	}
	done := env.startExec(syncName(10))
	if ds := g.waitStarted(); ds != "tank/data" {
		t.Fatalf("first send %s, want tank/data", ds)
	}
	g.noneStarted(100 * time.Millisecond)
	g.open("tank/data")
	g.open(g.waitStarted())
	wantStatuses(t, waitResults(t, done), StatusOK, StatusOK)
	g.mu.Lock()
	defer g.mu.Unlock()
	if overlap {
		t.Error("child sent while its parent was sending")
	}
}

func TestExecutorJobsResultOrder(t *testing.T) {
	env := flatDatasets(t, 3, "a", "b", "c")
	g := gateSends(t, env)
	rec := &recordingRenderer{}
	done := env.startExec(syncName(10), func(ex *Executor) { ex.Progress = rec })
	for range 3 {
		g.waitStarted()
	}
	for _, n := range []string{"c", "b", "a"} {
		g.open("tank/" + n)
		// full s1, then incremental to the sync snapshot
		waitFor(t, "tank/"+n+"'s transfers to finish", func() bool {
			return countPrefix(rec.got(), "finish tank/"+n+" <nil>") == 2
		})
	}
	res := waitResults(t, done)
	wantStatuses(t, res, StatusOK, StatusOK, StatusOK)
	for i, n := range []string{"a", "b", "c"} {
		if res[i].Source != "tank/"+n {
			t.Errorf("result %d is %s, want tank/%s", i, res[i].Source, n)
		}
		if res[i].Bytes <= 0 {
			t.Errorf("result %d (%s): Bytes = %d, want > 0", i, res[i].Source, res[i].Bytes)
		}
	}
}
