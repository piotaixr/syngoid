package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/piotaixr/syngoid/internal/cli"
	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/logx"
	"github.com/piotaixr/syngoid/internal/naming"
	"github.com/piotaixr/syngoid/internal/stream"
	"github.com/piotaixr/syngoid/internal/zfs"
)

const (
	// maxReplans bounds the re-gather/re-plan rounds of one dataset.
	maxReplans = 3
	// destroyBatch is the number of snapshots destroyed per zfs destroy.
	destroyBatch = 50
	// msgCancelled is the result message of datasets stopped by cancellation.
	msgCancelled = "cancelled"
	// msgDeclined is the result message when Confirm refuses a destructive step.
	msgDeclined = "destructive action declined"
)

// Executor runs the planned replication of every dataset from Src to Dst
// (spec §8.5, §8.7).
type Executor struct {
	Src, Dst endpoint.Endpoint
	Opts     *cli.Options
	PlanOpts PlanOptions
	Log      *slog.Logger
	Progress stream.Renderer
	// Confirm, when non-nil, is asked before every destructive step; false
	// declines it.
	Confirm              func(question string) bool
	SendCodec, RecvCodec stream.Codec
	UnprivLinuxTarget    bool
	// SyncSnap is this run's sync snapshot name ("" with --no-sync-snap).
	SyncSnap string
}

// Run executes plans, where plans[i] is the plan of states[i], and returns
// one Result per state in the same order. It first excludes busy targets,
// creates the sync snapshots in one batch per pool, refreshes and re-plans
// every dataset, then runs the datasets sequentially in dependency order.
// A dataset whose dependency ended other than ok or nothing to do is
// skipped as a soft failure. After ctx
// is cancelled no new action starts and every unfinished dataset ends
// critical with message "cancelled". Every outcome is logged: critical
// failures as CRITICAL ERROR, soft failures as WARNING, skipped datasets
// and nothing to do as INFO; plan-time outcomes are logged when their
// dataset's turn comes.
func (e *Executor) Run(ctx context.Context, states []DatasetState, plans []Plan) []Result {
	if e.Log == nil {
		e.Log = slog.New(slog.DiscardHandler)
	}
	if e.Progress == nil {
		e.Progress = stream.NewRenderer(stream.ProgressNone, nil, false, nil)
	}
	r := &runState{
		e:        e,
		states:   append([]DatasetState(nil), states...),
		plans:    append([]Plan(nil), plans...),
		results:  make([]Result, len(states)),
		done:     make([]bool, len(states)),
		logLater: make([]bool, len(states)),
		stale:    make([]bool, len(states)),
		visited:  make([]bool, len(states)),
	}
	for i, s := range r.states {
		r.results[i] = Result{Source: s.Source, Target: s.Target}
		if p := r.plans[i]; p.Status != StatusOK {
			r.planned(i, p)
		}
	}

	for _, phase := range []func(context.Context){r.checkBusy, r.createSyncSnaps, r.refresh} {
		if ctx.Err() != nil {
			break
		}
		phase(ctx)
	}

	order, deps := Order(r.states)
	failed := make([]bool, len(r.states))
	for _, i := range order {
		r.visited[i] = true
		switch {
		case r.logLater[i]:
			r.logLater[i] = false
			r.e.logOutcome(r.results[i].Status, r.results[i].Message)
		case !r.done[i]:
			r.runOne(ctx, i, deps[i], failed)
		}
		st := r.results[i].Status
		failed[i] = st != StatusOK && st != StatusNothing
	}
	return r.results
}

// runState is the bookkeeping of one Run.
type runState struct {
	e       *Executor
	states  []DatasetState
	plans   []Plan
	results []Result
	done    []bool
	// logLater marks final outcomes decided by a plan, logged when the
	// dataset's turn comes in the execution order.
	logLater []bool
	// stale marks datasets whose target was destroyed by an ancestor's
	// --force-delete: they are re-gathered and re-planned before running.
	stale []bool
	// visited marks the datasets the execution loop has reached.
	visited []bool
}

// runOne executes dataset i unless ctx is cancelled or a dependency failed.
func (r *runState) runOne(ctx context.Context, i int, deps []int, failed []bool) {
	if err := ctx.Err(); err != nil {
		r.finish(i, StatusCritical, msgCancelled, err)
		return
	}
	for _, d := range deps {
		if failed[d] {
			r.finish(i, StatusSoft, "skipped: dependency "+r.states[d].Source+" failed", nil)
			return
		}
	}
	if r.stale[i] && !r.regather(ctx, i) {
		return
	}
	d := r.e.runDataset(ctx, r.states[i], r.plans[i])
	if d.logged {
		r.record(i, d.res.Status, d.res.Message, d.res.Err)
	} else {
		r.finish(i, d.res.Status, d.res.Message, d.res.Err)
	}
	r.results[i].Bytes, r.results[i].Duration = d.res.Bytes, d.res.Duration
	if d.destroyed != "" {
		r.invalidate(d.destroyed)
	}
}

// invalidate marks every dataset not yet reached whose target is under the
// recursively destroyed target as stale. One that had nothing to do is
// pending again: its target no longer exists.
func (r *runState) invalidate(destroyed string) {
	for j, s := range r.states {
		if r.visited[j] || (s.Target != destroyed && !strings.HasPrefix(s.Target, destroyed+"/")) {
			continue
		}
		if r.done[j] {
			if r.results[j].Status != StatusNothing {
				continue
			}
			r.done[j], r.logLater[j] = false, false
			r.results[j] = Result{Source: s.Source, Target: s.Target}
		}
		r.stale[j] = true
	}
}

// regather refreshes the state and plan of stale dataset i. It reports
// false when the dataset ended instead. The sync snapshot already exists,
// so none is created.
func (r *runState) regather(ctx context.Context, i int) bool {
	s := r.states[i]
	st, err := GatherOne(ctx, r.e.Src, r.e.Dst, r.e.Opts, s.Source, s.Target, r.e.SyncSnap, s.OriginInScope)
	switch {
	case errors.Is(err, zfs.ErrNoDataset):
		r.vanished(i)
		return false
	case err != nil:
		r.finish(i, StatusCritical, fmt.Sprintf("%s: re-gather failed: %v", s.Source, err), err)
		return false
	}
	st = stripSyncSnap(st)
	p := PlanDataset(st, r.e.PlanOpts)
	r.states[i], r.plans[i] = st, p
	if p.Status != StatusOK {
		r.finish(i, p.Status, p.Message, nil)
		return false
	}
	return true
}

// pending returns the indices of the datasets without a final result.
func (r *runState) pending() []int {
	var out []int
	for i, d := range r.done {
		if !d {
			out = append(out, i)
		}
	}
	return out
}

// record sets the final status of dataset i without logging it.
func (r *runState) record(i int, st Status, msg string, err error) {
	res := &r.results[i]
	res.Status, res.Message, res.Err = st, msg, err
	r.done[i] = true
}

// finish records the final status of dataset i and logs it.
func (r *runState) finish(i int, st Status, msg string, err error) {
	r.record(i, st, msg, err)
	r.e.logOutcome(st, msg)
}

// planned records the non-ok outcome of plan p for dataset i, to be logged
// when the execution loop reaches the dataset.
func (r *runState) planned(i int, p Plan) {
	r.record(i, p.Status, p.Message, nil)
	r.logLater[i] = true
}

// logOutcome logs the final outcome of a dataset at the level matching st.
// Cancellations are not logged per dataset.
func (e *Executor) logOutcome(st Status, msg string) {
	if msg == "" {
		return
	}
	switch st {
	case StatusCritical:
		if msg != msgCancelled {
			logx.Critical(e.Log, msg)
		}
	case StatusSoft:
		e.Log.Warn(msg)
	case StatusSkipped, StatusNothing:
		e.Log.Info(msg)
	case StatusOK:
	}
}

// checkBusy marks datasets whose target is being received into as soft
// failures, with one ReceiveBusy call for all targets.
func (r *runState) checkBusy(ctx context.Context) {
	idx := r.pending()
	if len(idx) == 0 {
		return
	}
	targets := make([]string, len(idx))
	for k, i := range idx {
		targets[k] = r.states[i].Target
	}
	busy, err := r.e.Dst.ReceiveBusy(ctx, targets)
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		r.e.Log.Warn("cannot check for running zfs receive processes: " + err.Error())
		return
	}
	for _, i := range idx {
		if t := r.states[i].Target; busy[t] {
			r.finish(i, StatusSoft, busyMessage(t), nil)
		}
	}
}

// vanished ends dataset i as skipped because its source no longer exists.
func (r *runState) vanished(i int) {
	msg := vanishedMessage(r.states[i].Source)
	r.e.Log.Warn(msg)
	r.record(i, StatusSkipped, msg, nil)
}

func busyMessage(target string) string {
	return "Cannot sync now: " + target + " is already target of a zfs receive process."
}

// createSyncSnaps creates the sync snapshots with one zfs snapshot per pool,
// falling back to one call per dataset when the batch fails.
func (r *runState) createSyncSnaps(ctx context.Context) {
	type entry struct {
		i    int
		name string
	}
	byPool := map[string][]entry{}
	var pools []string
	for _, i := range r.pending() {
		s := r.states[i]
		if s.NewSyncSnap == "" {
			continue
		}
		pool, _, _ := strings.Cut(s.Source, "/")
		if _, ok := byPool[pool]; !ok {
			pools = append(pools, pool)
		}
		byPool[pool] = append(byPool[pool], entry{i, s.Source + "@" + s.NewSyncSnap})
	}
	for _, pool := range pools {
		es := byPool[pool]
		names := make([]string, len(es))
		for k, en := range es {
			names[k] = en.name
		}
		err := r.e.Src.Snapshot(ctx, names)
		if err == nil {
			continue
		}
		if ctx.Err() != nil {
			return
		}
		r.e.Log.Debug("creating sync snapshots in one batch failed, retrying per dataset: " + err.Error())
		for _, en := range es {
			err := r.e.Src.Snapshot(ctx, []string{en.name})
			switch {
			case err == nil || ctx.Err() != nil:
			case errors.Is(err, zfs.ErrNoDataset):
				r.vanished(en.i)
			default:
				r.finish(en.i, StatusCritical, fmt.Sprintf("creating sync snapshot %s failed: %v", en.name, err), err)
			}
		}
	}
}

// refresh re-gathers every dataset once now that the sync snapshots exist,
// and re-plans each pending dataset from the fresh state.
func (r *runState) refresh(ctx context.Context) {
	idx := r.pending()
	if len(idx) == 0 {
		return
	}
	fresh, err := Gather(ctx, r.e.Src, r.e.Dst, r.e.Opts, r.e.SyncSnap)
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		for _, i := range idx {
			if errors.Is(err, zfs.ErrNoDataset) { // only the source root can be missing
				r.vanished(i)
			} else {
				r.finish(i, StatusCritical, "refreshing dataset state failed: "+err.Error(), err)
			}
		}
		return
	}
	bySource := make(map[string]DatasetState, len(fresh))
	for _, s := range fresh {
		bySource[s.Source] = s
	}
	for _, i := range idx {
		s, ok := bySource[r.states[i].Source]
		if !ok {
			r.vanished(i)
			continue
		}
		s = stripSyncSnap(s)
		p := PlanDataset(s, r.e.PlanOpts)
		if !samePlan(p, r.plans[i]) {
			r.e.Log.Warn("plan for " + s.Source + " changed since planning; executing updated plan")
		}
		r.states[i], r.plans[i] = s, p
		if p.Status != StatusOK {
			r.planned(i, p)
		}
	}
}

// stripSyncSnap removes the already-created sync snapshot from the source
// snapshots: the planner appends NewSyncSnap itself.
func stripSyncSnap(s DatasetState) DatasetState {
	if s.NewSyncSnap == "" {
		return s
	}
	out := make([]zfs.Snapshot, 0, len(s.SourceSnaps))
	for _, sn := range s.SourceSnaps {
		if sn.Name != s.NewSyncSnap {
			out = append(out, sn)
		}
	}
	s.SourceSnaps = out
	return s
}

// samePlan reports whether a and b have the same outcome and actions.
func samePlan(a, b Plan) bool {
	if a.Status != b.Status || len(a.Actions) != len(b.Actions) {
		return false
	}
	for i := range a.Actions {
		if a.Actions[i].Describe() != b.Actions[i].Describe() {
			return false
		}
	}
	return true
}

func vanishedMessage(source string) string {
	return "Skipping dataset (dataset no longer exists): " + source + "..."
}

// step tells the dataset loop what to do after an action.
type step int

const (
	stepNext    step = iota // continue with the next action
	stepRetry               // run the same action again
	stepRestart             // the plan was replaced; start it from the beginning
	stepStop                // the dataset is finished; res holds the outcome
)

// dsRun executes the plan of one dataset.
type dsRun struct {
	e    *Executor
	st   DatasetState
	plan Plan
	res  Result
	// rounds counts re-gather/re-plan rounds; worked records that a
	// transfer or destroy succeeded.
	rounds      int
	worked      bool
	busyChecked bool
	retried     bool
	// logged records that the outcome was already logged; destroyed is the
	// target destroyed recursively by --force-delete, if any.
	logged    bool
	destroyed string
}

// runDataset executes plan p for state st; the outcome is in the
// returned dsRun's res.
func (e *Executor) runDataset(ctx context.Context, st DatasetState, p Plan) *dsRun {
	start := time.Now()
	d := &dsRun{e: e, st: st, plan: p, res: Result{Source: st.Source, Target: st.Target}}
	d.run(ctx)
	d.res.Duration = time.Since(start)
	return d
}

func (d *dsRun) run(ctx context.Context) {
	for i := 0; i < len(d.plan.Actions); {
		if err := ctx.Err(); err != nil {
			d.end(StatusCritical, msgCancelled, err)
			return
		}
		var next step
		switch a := d.plan.Actions[i].(type) {
		case SendFull, SendIncremental, SendFromBookmark, SendClone, Resume:
			next = d.send(ctx, a)
		case ForceDestroyTarget:
			next = d.forceDestroy(ctx)
		default:
			next = d.post(ctx, a)
		}
		switch next {
		case stepNext:
			i++
			d.retried = false
		case stepRestart:
			i = 0
			d.retried = false
		case stepRetry:
		case stepStop:
			return
		}
	}
}

// end sets the final outcome of the dataset.
func (d *dsRun) end(st Status, msg string, err error) step {
	d.res.Status, d.res.Message, d.res.Err = st, msg, err
	return stepStop
}

// fail ends the dataset critically with the error of action a.
func (d *dsRun) fail(a Action, err error) step {
	return d.end(StatusCritical, fmt.Sprintf("%s: %s: %v", d.st.Source, a.Describe(), err), err)
}

// warn logs a failure that does not affect the result.
func (d *dsRun) warn(a Action, err error) {
	d.e.Log.Warn(fmt.Sprintf("%s: %s: %v", d.st.Source, a.Describe(), err))
}

// vanished ends the dataset as skipped because its source is gone.
func (d *dsRun) vanished() step {
	msg := vanishedMessage(d.st.Source)
	d.e.Log.Warn(msg)
	d.logged = true
	return d.end(StatusSkipped, msg, nil)
}

// send runs one send action, checking first that the target is not busy.
func (d *dsRun) send(ctx context.Context, a Action) step {
	if stop, ok := d.checkBusy(ctx); !ok {
		return stop
	}
	n, err := d.e.transfer(ctx, d.st, a)
	d.res.Bytes += n
	if err == nil {
		d.worked = true
		if _, ok := a.(Resume); ok {
			return d.replan(ctx, d.st.OriginInScope, nil)
		}
		return stepNext
	}
	if cerr := ctx.Err(); cerr != nil {
		return d.end(StatusCritical, msgCancelled, cerr)
	}
	return d.remediate(ctx, a, err)
}

// checkBusy checks once per dataset, before the first send or destroy,
// that the target is not being received into. ok is false when the dataset
// ended (busy or cancelled).
func (d *dsRun) checkBusy(ctx context.Context) (step, bool) {
	if d.busyChecked {
		return stepNext, true
	}
	d.busyChecked = true
	busy, err := d.e.Dst.ReceiveBusy(ctx, []string{d.st.Target})
	switch {
	case ctx.Err() != nil:
		return d.end(StatusCritical, msgCancelled, ctx.Err()), false
	case err != nil:
		d.e.Log.Warn("cannot check for running zfs receive processes: " + err.Error())
	case busy[d.st.Target]:
		return d.end(StatusSoft, busyMessage(d.st.Target), nil), false
	}
	return stepNext, true
}

// forceDestroy destroys the target recursively, after confirmation, and
// re-plans; the sync snapshot already exists, so none is created. The
// datasets under the target are re-planned by Run before they run. It never
// destroys a pool root, a busy target, or once the re-plan budget is spent.
func (d *dsRun) forceDestroy(ctx context.Context) step {
	if d.st.TargetIsPoolRoot {
		return d.fail(ForceDestroyTarget{}, errors.New("refusing to destroy pool root dataset "+d.st.Target))
	}
	if d.rounds >= maxReplans {
		return d.giveUp(nil)
	}
	if stop, ok := d.checkBusy(ctx); !ok {
		return stop
	}
	if d.e.Confirm != nil && !d.e.Confirm("Destroy "+d.st.Target+" recursively (--force-delete)?") {
		return d.end(StatusCritical, msgDeclined, nil)
	}
	d.e.Log.Warn("Removing " + d.st.Target + " because no matching snapshots were found")
	if err := d.e.Dst.DestroyDataset(ctx, d.st.Target, true); err != nil {
		return d.fail(ForceDestroyTarget{}, err)
	}
	d.worked = true
	d.destroyed = d.st.Target
	return d.replan(ctx, d.st.OriginInScope, nil)
}

// post runs a non-send action. Only a bookmark failure is fatal.
func (d *dsRun) post(ctx context.Context, a Action) step {
	switch a := a.(type) {
	case Hold:
		ep, ds := d.side(a.Side)
		if err := ep.Hold(ctx, d.holdTag(), ds+"@"+a.Snap); err != nil {
			d.warn(a, err)
		}
	case Release:
		ep, ds := d.side(a.Side)
		if err := ep.Release(ctx, d.holdTag(), ds+"@"+a.Snap); err != nil {
			d.warn(a, err)
		}
	case CreateBookmark:
		if err := d.bookmark(ctx, a.Snap); err != nil {
			return d.fail(a, err)
		}
	case PruneSyncSnaps:
		ep, ds := d.side(a.Side)
		if err := ep.Destroy(ctx, ds, a.Snaps); err != nil {
			d.warn(a, err)
		}
	case DestroyTargetSnaps:
		d.destroyTargetSnaps(ctx, a)
	default:
		return d.fail(a, errors.New("unsupported action"))
	}
	return stepNext
}

func (d *dsRun) holdTag() string {
	return naming.HoldTag(d.e.Opts.Identifier, d.e.PlanOpts.Hostname)
}

// side returns the endpoint and dataset of side s.
func (d *dsRun) side(s Side) (endpoint.Endpoint, string) {
	if s == Target {
		return d.e.Dst, d.st.Target
	}
	return d.e.Src, d.st.Source
}

// bookmark creates source bookmark #snap, retrying once with the GUID
// suffixed name.
func (d *dsRun) bookmark(ctx context.Context, snap string) error {
	src := d.st.Source
	err := d.e.Src.Bookmark(ctx, src+"@"+snap, src+"#"+snap)
	if err == nil {
		return nil
	}
	guid := ""
	for _, s := range d.st.SourceSnaps {
		if s.Name == snap {
			guid = s.GUID
		}
	}
	alt := naming.BookmarkFallback(snap, guid)
	d.e.Log.Debug(fmt.Sprintf("creating bookmark %s#%s failed (%v), retrying as %s#%s", src, snap, err, src, alt))
	return d.e.Src.Bookmark(ctx, src+"@"+snap, src+"#"+alt)
}

// destroyTargetSnaps destroys the target snapshots missing on the source,
// oldest first, destroyBatch at a time. Failures are warnings. The state is
// deliberately not passed through stripSyncSnap: the new sync snapshot is
// on the source and must not count as missing there.
func (d *dsRun) destroyTargetSnaps(ctx context.Context, a DestroyTargetSnaps) {
	st, err := GatherOne(ctx, d.e.Src, d.e.Dst, d.e.Opts, d.st.Source, d.st.Target, d.e.SyncSnap, d.st.OriginInScope)
	if err != nil {
		d.warn(a, err)
		return
	}
	onSource := make(map[string]bool, len(st.SourceSnaps))
	for _, s := range st.SourceSnaps {
		onSource[s.Name] = true
	}
	var names []string
	for _, s := range st.TargetSnaps {
		if !onSource[s.Name] {
			names = append(names, s.Name)
		}
	}
	for start := 0; start < len(names); start += destroyBatch {
		batch := names[start:min(start+destroyBatch, len(names))]
		if err := d.e.Dst.Destroy(ctx, d.st.Target, batch); err != nil {
			d.warn(a, err)
		}
	}
}
