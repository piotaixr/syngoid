package app

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/piotaixr/syngoid/internal/cli"
	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/engine"
	"github.com/piotaixr/syngoid/internal/logx"
)

// runRestoreList implements restore --list: it opens only the source and
// prints the restore points of design §10.
func runRestoreList(ctx context.Context, env Env, parsed cli.Parsed) int {
	o := parsed.Options
	log := logx.New(logx.Config{Stdout: env.Stdout, Stderr: env.Stderr, Quiet: o.Quiet, Debug: o.Debug})
	if env.LoggerReady != nil {
		env.LoggerReady(log)
	}
	for _, w := range parsed.Warnings {
		log.Warn(w)
	}
	src, closeSrc, err := openSource(ctx, env, o, log)
	if err != nil {
		return criticalf(env, "%v", err)
	}
	defer closeSrc()
	datasets, snaps, err := engine.GatherSource(ctx, src, o)
	if err != nil {
		return criticalf(env, "%v", err)
	}
	pts := engine.RestorePoints(datasets, snaps)
	if len(pts) == 0 {
		_, _ = fmt.Fprintf(env.Stderr, "WARNING: no snapshots under %s\n", o.Source)
		return 1
	}
	loc := env.Location
	if loc == nil {
		loc = time.Local
	}
	engine.RenderRestorePoints(env.Stdout, pts, len(datasets), loc)
	return 0
}

// restoreNothingQualifies returns the critical message of restore design
// §5.4 when sel picks a snapshot for no dataset of states, or "".
func restoreNothingQualifies(o *cli.Options, sel engine.Selection, states []engine.DatasetState) string {
	src := o.Source.String()
	if len(states) == 0 {
		return "no dataset in scope under " + src
	}
	for _, s := range states {
		if _, ok := sel.Pick(s.SourceSnaps); ok {
			return ""
		}
	}
	var what string
	switch {
	case sel.Name != "":
		what = "a snapshot named " + sel.Name
	case sel.At != nil:
		what = "a snapshot at or before " + sel.At.Format(time.RFC3339)
	default:
		what = "any snapshot"
	}
	list := []string{}
	if o.Recursive {
		list = append(list, "-r")
	}
	if o.SkipParent {
		list = append(list, "--skip-parent")
	}
	for _, re := range o.ExcludeDatasets {
		list = append(list, "--exclude-datasets='"+re.String()+"'")
	}
	list = append(list, src)
	return fmt.Sprintf("no dataset under %s has %s; list the restore points with: syngoid restore --list %s", src, what, strings.Join(list, " "))
}

// restoreConflicts returns the conflict block of restore design §6.4 for
// the plans (one per state) that conflict with an existing target, or ""
// when none does. Each target is shown as Location.String() would show it;
// the pool-root message follows the block.
func restoreConflicts(o *cli.Options, states []engine.DatasetState, plans []engine.Plan) string {
	var targets, extra []string
	for i, p := range plans {
		if !p.Conflict {
			continue
		}
		loc := o.Target
		loc.Dataset = p.Target
		targets = append(targets, "  "+loc.String())
		if states[i].TargetIsPoolRoot {
			extra = append(extra, p.Message)
		}
	}
	if len(targets) == 0 {
		return ""
	}
	lines := append([]string{"restore needs an empty target; these datasets already exist:"}, targets...)
	lines = append(lines, "Destroy them first if they are to be replaced, or restore to another location.")
	return strings.Join(append(lines, extra...), "\n")
}

// addPreservedPropNotes adds to each OK plan a `set <key>=<value>` note per
// property --preserve-properties will carry to its target (restore design
// §9).
func addPreservedPropNotes(ctx context.Context, src endpoint.Endpoint, plans []engine.Plan, unprivLinux bool) error {
	for i := range plans {
		p := &plans[i]
		if p.Status != engine.StatusOK {
			continue
		}
		local, err := src.LocalProps(ctx, p.Source)
		if err != nil {
			return fmt.Errorf("local properties of %s: %w", p.Source, err)
		}
		for _, k := range engine.PreservedPropKeys(local, unprivLinux) {
			p.Notes = append(p.Notes, "set "+k+"="+local[k])
		}
	}
	return nil
}
