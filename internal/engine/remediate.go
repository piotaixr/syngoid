package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/piotaixr/syngoid/internal/zfs"
)

// remediate classifies the failure err of send action a and applies the
// remediation of spec §8.7: abort partial state and retry once
// (--no-resume), abort a resume whose source is gone and re-plan, destroy a
// conflicting snapshot (--force-delete) and re-plan, fall back from a clone
// send to full replication, or skip a vanished source. Anything else is
// critical.
func (d *dsRun) remediate(ctx context.Context, a Action, err error) step {
	source, target := errSides(err)
	_, isResume := a.(Resume)
	_, isClone := a.(SendClone)
	var exists *zfs.DestinationExistsError
	switch {
	case target && errors.Is(err, zfs.ErrPartialState):
		if !d.e.Opts.NoResume || d.retried {
			return d.fail(a, err)
		}
		d.retried = true
		if aerr := d.e.Dst.AbortResume(ctx, d.st.Target); aerr != nil {
			return d.fail(a, fmt.Errorf("%w (aborting the partial receive failed: %w)", err, aerr))
		}
		return stepRetry

	case source && isResume && errors.Is(err, zfs.ErrResumeSourceGone):
		if d.rounds >= maxReplans {
			return d.giveUp(err)
		}
		if aerr := d.e.Dst.AbortResume(ctx, d.st.Target); aerr != nil {
			return d.fail(a, fmt.Errorf("%w (aborting the partial receive failed: %w)", err, aerr))
		}
		return d.replan(ctx, d.st.OriginInScope, err)

	case target && errors.As(err, &exists):
		return d.destroyConflict(ctx, a, exists.Name, err)

	case isClone:
		d.e.Log.Warn(fmt.Sprintf("clone send of %s failed (%v); falling back to full replication", d.st.Source, err))
		return d.replan(ctx, false, err)

	case source && errors.Is(err, zfs.ErrNoDataset):
		return d.vanished()
	}
	return d.fail(a, err)
}

// destroyConflict destroys the target snapshot name that blocks a receive,
// when --force-delete allows it and Confirm agrees, then re-plans.
func (d *dsRun) destroyConflict(ctx context.Context, a Action, name string, err error) step {
	ds, snap, ok := strings.Cut(name, "@")
	if !d.e.Opts.ForceDelete || !ok || ds != d.st.Target {
		return d.fail(a, err)
	}
	if d.rounds >= maxReplans {
		return d.giveUp(err)
	}
	if d.e.Confirm != nil && !d.e.Confirm("Destroy conflicting "+name+" (--force-delete)?") {
		return d.end(StatusCritical, msgDeclined, err)
	}
	if derr := d.e.Dst.Destroy(ctx, ds, []string{snap}); derr != nil {
		return d.fail(a, fmt.Errorf("%w (destroying %s failed: %w)", err, name, derr))
	}
	d.worked = true
	return d.replan(ctx, d.st.OriginInScope, err)
}

// replan re-gathers the dataset and replaces its plan. cause is the error
// that triggered the round, nil after a successful resume or destroy. A
// re-plan with nothing left to do after work was done ends the dataset ok.
func (d *dsRun) replan(ctx context.Context, originInScope bool, cause error) step {
	if d.rounds >= maxReplans {
		return d.giveUp(cause)
	}
	d.rounds++
	st, err := GatherOne(ctx, d.e.Src, d.e.Dst, d.e.Opts, d.st.Source, d.st.Target, d.e.SyncSnap, originInScope)
	if errors.Is(err, zfs.ErrNoDataset) {
		return d.vanished()
	}
	if err != nil {
		return d.end(StatusCritical, fmt.Sprintf("%s: re-gather failed: %v", d.st.Source, err), err)
	}
	d.st = stripSyncSnap(st)
	d.plan = PlanDataset(d.st, d.e.PlanOpts)
	switch {
	case d.plan.Status == StatusOK:
		return stepRestart
	case d.plan.Status == StatusNothing && d.worked:
		return d.end(StatusOK, "", nil)
	}
	return d.end(d.plan.Status, d.plan.Message, nil)
}

// giveUp ends the dataset after too many re-plan rounds.
func (d *dsRun) giveUp(cause error) step {
	msg := fmt.Sprintf("giving up after %d re-plan rounds", maxReplans)
	if cause != nil {
		msg += ": " + cause.Error()
	}
	return d.end(StatusCritical, msg, cause)
}
