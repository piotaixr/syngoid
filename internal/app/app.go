// Package app wires the syngoid command line to its implementation.
package app

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/piotaixr/syngoid/internal/buildinfo"
	"github.com/piotaixr/syngoid/internal/cli"
	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/engine"
	"github.com/piotaixr/syngoid/internal/logx"
	"github.com/piotaixr/syngoid/internal/naming"
	"github.com/piotaixr/syngoid/internal/stream"
	"github.com/piotaixr/syngoid/internal/zfs"
)

// Env carries the process environment so Main can be tested without os.
type Env struct {
	Stdin       io.Reader
	Stdout      io.Writer
	Stderr      io.Writer
	StdinIsTTY  bool
	StderrIsTTY bool
	// TermSize reports the width and height of the stderr terminal; a height
	// of 0 means unknown. It may be nil, meaning 80 columns and unknown height.
	TermSize func() (width, height int)
	// Interrupts lets the user drain a run. It may be nil, meaning never.
	Interrupts engine.Interrupts
	Hostname   func() (string, error)
	Now        func() time.Time
	// NewLocal returns the endpoint of the local host, running zfs through
	// sudo when sudo is set.
	NewLocal func(sudo bool) endpoint.Endpoint
	// LoggerReady, when non-nil, is called with Main's logger before the
	// first NewLocal call so commands can be traced.
	LoggerReady func(*slog.Logger)
	// NewConnector returns the Connector for remote sides. It is called once
	// per run, and only when a side is remote. It may be nil, meaning remote
	// endpoints are not configured.
	NewConnector func(o *cli.Options, log *slog.Logger) (Connector, error)
	// PoolExists reports whether a local ZFS pool of that name exists. It
	// may be nil, meaning none does.
	PoolExists func(string) bool
	// Location is the time zone of displayed times; nil means time.Local.
	Location *time.Location
}

// Main runs syngoid with args (excluding the program name) and returns the
// process exit code.
func Main(ctx context.Context, args []string, env Env) int {
	parsed, err := cli.Parse(args, env.PoolExists)
	if errors.Is(err, cli.ErrHelp) && parsed.Command == cli.CmdFetch {
		_, _ = fmt.Fprintf(env.Stdout, "Usage: syngoid fetch (--platform OS/ARCH ... | --all) [--dir DIR]\n\nFlags:\n%s", cli.FetchUsage())
		return 0
	}
	if errors.Is(err, cli.ErrHelp) {
		_, _ = fmt.Fprintf(env.Stdout, "Usage: syngoid [sync] [flags] SOURCE TARGET\n"+
			"       syngoid restore [flags] SOURCE TARGET\n"+
			"       syngoid restore --list [flags] SOURCE\n"+
			"       syngoid permissions [flags] SOURCE TARGET\n"+
			"       syngoid fetch (--platform OS/ARCH ... | --all) [--dir DIR]\n"+
			"       syngoid version\n\nFlags:\n%s", cli.Usage())
		return 0
	}
	if err != nil {
		return criticalf(env, "%v", err)
	}
	switch parsed.Command {
	case cli.CmdVersion:
		return runVersion(env)
	case cli.CmdFetch:
		return runFetch(ctx, env, parsed.Fetch)
	case cli.CmdServe:
		return runServe(ctx, env)
	case cli.CmdRestore:
		if parsed.Options.Restore.List {
			return runRestoreList(ctx, env, parsed)
		}
	case cli.CmdSync, cli.CmdPermissions:
	}
	return runSync(ctx, env, parsed)
}

// criticalf prints a CRITICAL ERROR line on stderr and returns exit code 2.
func criticalf(env Env, format string, a ...any) int {
	_, _ = fmt.Fprintf(env.Stderr, "CRITICAL ERROR: "+format+"\n", a...)
	return 2
}

func runVersion(env Env) int {
	dirty := ""
	if buildinfo.Dirty == "true" {
		dirty = ", dirty"
	}
	_, _ = fmt.Fprintf(env.Stdout, "syngoid %s (commit %s%s, build %.12s) protocol %d\n",
		buildinfo.Version, buildinfo.Commit, dirty, buildinfo.BuildHash(), buildinfo.ProtocolVersion)
	return 0
}

// prompter asks yes/no questions on stdout, reading answers from one shared
// buffered reader.
type prompter struct {
	in  *bufio.Reader
	out io.Writer
}

// ask prints question with a [y/N] suffix and reports whether the answer was
// y or yes. A read error counts as no.
func (p *prompter) ask(question string) bool {
	_, _ = io.WriteString(p.out, question+" [y/N] ")
	line, _ := p.in.ReadString('\n')
	switch strings.ToLower(strings.TrimSpace(line)) {
	case "y", "yes":
		return true
	}
	return false
}

// runSync implements the sync, restore and permissions commands.
func runSync(ctx context.Context, env Env, parsed cli.Parsed) int {
	o := parsed.Options
	mode := stream.ResolveMode(o.Progress, env.StderrIsTTY)
	if o.Quiet {
		mode = stream.ProgressNone
	}
	stdout, stderr := env.Stdout, env.Stderr
	var term *stream.Terminal
	if mode == stream.ProgressBar {
		size := env.TermSize
		if size == nil {
			size = func() (int, int) { return 80, 0 }
		}
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		term = stream.NewTerminal(env.Stderr, size, ticker.C)
		defer term.Close()
		stdout, stderr = term.Writer(env.Stdout), term.Writer(env.Stderr)
	}
	log := logx.New(logx.Config{Stdout: stdout, Stderr: stderr, Quiet: o.Quiet, Debug: o.Debug})
	if env.LoggerReady != nil {
		env.LoggerReady(log)
	}
	for _, w := range parsed.Warnings {
		log.Warn(w)
	}
	if parsed.Command != cli.CmdPermissions && o.Confirm && !env.StdinIsTTY {
		return criticalf(env, "--confirm requires an interactive terminal")
	}
	host, err := env.Hostname()
	if err != nil {
		return criticalf(env, "cannot determine the hostname: %v", err)
	}
	syncSnap := naming.SyncSnapName(o.Identifier, host, env.Now())

	ends, err := openSides(ctx, env, o, log)
	if err != nil {
		return criticalf(env, "%v", err)
	}
	defer ends.close()
	src, dst := ends.src, ends.dst
	states, err := engine.Gather(ctx, src, dst, o, syncSnap)
	if err != nil {
		return criticalf(env, "%v", err)
	}
	if o.DumpSnaps {
		dumpSnaps(log, states)
	}

	var planner func(engine.DatasetState) engine.Plan
	var plans []engine.Plan
	if o.Restore != nil {
		sel := engine.SelectionFrom(o.Restore)
		if msg := restoreNothingQualifies(o, sel, states); msg != "" {
			return criticalf(env, "%s", msg)
		}
		planner = func(s engine.DatasetState) engine.Plan { return engine.PlanRestore(s, sel) }
		plans = planAll(states, planner)
		if msg := restoreConflicts(o, states, plans); msg != "" {
			return criticalf(env, "%s", msg)
		}
	}

	report, err := engine.Preflight(ctx, src, dst, o, states)
	if err != nil {
		return criticalf(env, "%v", err)
	}
	if parsed.Command == cli.CmdPermissions {
		report.Render(env.Stdout, true)
		if !report.OK() {
			return 2
		}
		return 0
	}
	if !report.OK() {
		report.Render(env.Stderr, false)
		return 2
	}

	po := engine.PlanOptionsFrom(o, host)
	if plans == nil {
		plans = planAll(states, func(s engine.DatasetState) engine.Plan { return engine.PlanDataset(s, po) })
	}
	dstInfo, err := dst.Info(ctx)
	if err != nil {
		return criticalf(env, "target host info: %v", err)
	}
	unprivLinux := dstInfo.OS == "linux" && dstInfo.UID != 0 && !o.TargetSudo
	if o.Restore != nil && o.PreserveProperties && (o.DryRun || o.Confirm) {
		if err := addPreservedPropNotes(ctx, src, plans, unprivLinux); err != nil {
			return criticalf(env, "%v", err)
		}
	}
	estimate := func(p engine.Plan, a engine.Action) (int64, bool) {
		for _, s := range states {
			if s.Source == p.Source && s.Target == p.Target {
				return engine.EstimateAction(ctx, src, s, a, o.SendOpts)
			}
		}
		return 0, false
	}
	if o.DryRun {
		engine.RenderPlan(env.Stdout, plans, syncSnap, estimate)
		return 0
	}

	var confirm func(string) bool
	if o.Confirm {
		p := &prompter{in: bufio.NewReader(env.Stdin), out: env.Stdout}
		engine.RenderPlan(env.Stdout, plans, syncSnap, estimate)
		if !p.ask("Proceed?") {
			log.Info("aborted by user")
			if ctx.Err() != nil {
				return 2
			}
			return 0
		}
		confirm = func(q string) bool {
			if term != nil {
				defer term.Suspend()()
			}
			return p.ask(q)
		}
	}

	codec := streamCodec(o)
	ex := &engine.Executor{
		Src: src, Dst: dst, Opts: o, PlanOpts: po, Planner: planner, Log: log,
		Progress:          stream.NewRenderer(mode, term, log, env.Now),
		Confirm:           confirm,
		SendCodec:         codec,
		RecvCodec:         codec,
		UnprivLinuxTarget: unprivLinux,
		SyncSnap:          syncSnap,
		Interrupts:        env.Interrupts,
	}
	start := env.Now()
	results := ex.Run(ctx, states, plans)
	if !o.Quiet {
		RenderSummary(env.Stdout, results, env.Now().Sub(start))
	}
	if ctx.Err() != nil || ex.Drained() {
		return 2
	}
	return engine.ExitCode(results)
}

// planAll plans every state with planner.
func planAll(states []engine.DatasetState, planner func(engine.DatasetState) engine.Plan) []engine.Plan {
	plans := make([]engine.Plan, len(states))
	for i, s := range states {
		plans[i] = planner(s)
	}
	return plans
}

// dumpSnaps logs the snapshot names of every dataset pair.
func dumpSnaps(log *slog.Logger, states []engine.DatasetState) {
	for _, s := range states {
		log.Info(fmt.Sprintf("snapshots of %s: %s", s.Source, snapNames(s.SourceSnaps)))
		log.Info(fmt.Sprintf("snapshots of %s: %s", s.Target, snapNames(s.TargetSnaps)))
	}
}

// snapNames joins the snapshot names, or returns "(none)".
func snapNames(snaps []zfs.Snapshot) string {
	if len(snaps) == 0 {
		return "(none)"
	}
	names := make([]string, len(snaps))
	for i, s := range snaps {
		names[i] = s.Name
	}
	return strings.Join(names, ", ")
}
