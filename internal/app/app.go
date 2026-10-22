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
	Hostname    func() (string, error)
	Now         func() time.Time
	// NewLocal returns the endpoint of the local host, running zfs through
	// sudo when sudo is set.
	NewLocal func(sudo bool) endpoint.Endpoint
	// LoggerReady, when non-nil, is called with Main's logger before the
	// first NewLocal call so commands can be traced.
	LoggerReady func(*slog.Logger)
	// PoolExists reports whether a local ZFS pool of that name exists. It
	// may be nil, meaning none does.
	PoolExists func(string) bool
}

// Main runs syngoid with args (excluding the program name) and returns the
// process exit code.
func Main(ctx context.Context, args []string, env Env) int {
	parsed, err := cli.Parse(args, env.PoolExists)
	if errors.Is(err, cli.ErrHelp) {
		_, _ = fmt.Fprintf(env.Stdout, "Usage: syngoid [sync] [flags] SOURCE TARGET\n"+
			"       syngoid permissions [flags] SOURCE TARGET\n"+
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
		return criticalf(env, "fetch is not supported yet (milestone 3)")
	case cli.CmdServe:
		return runServe(ctx, env)
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

// runSync implements the sync and permissions commands for local endpoints.
func runSync(ctx context.Context, env Env, parsed cli.Parsed) int {
	o := parsed.Options
	log := logx.New(logx.Config{Stdout: env.Stdout, Stderr: env.Stderr, Quiet: o.Quiet, Debug: o.Debug})
	if env.LoggerReady != nil {
		env.LoggerReady(log)
	}
	for _, w := range parsed.Warnings {
		log.Warn(w)
	}
	if o.Source.IsRemote() || o.Target.IsRemote() {
		return criticalf(env, "remote endpoints are not supported yet (milestone 2)")
	}
	if parsed.Command == cli.CmdSync && o.Confirm && !env.StdinIsTTY {
		return criticalf(env, "--confirm requires an interactive terminal")
	}
	host, err := env.Hostname()
	if err != nil {
		return criticalf(env, "cannot determine the hostname: %v", err)
	}
	syncSnap := naming.SyncSnapName(o.Identifier, host, env.Now())

	src, dst := env.NewLocal(o.SourceSudo), env.NewLocal(o.TargetSudo)
	states, err := engine.Gather(ctx, src, dst, o, syncSnap)
	if err != nil {
		return criticalf(env, "%v", err)
	}
	if o.DumpSnaps {
		dumpSnaps(log, states)
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
	plans := make([]engine.Plan, len(states))
	for i, s := range states {
		plans[i] = engine.PlanDataset(s, po)
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
		confirm = p.ask
	}

	dstInfo, err := dst.Info(ctx)
	if err != nil {
		return criticalf(env, "target host info: %v", err)
	}
	mode := o.Progress
	if o.Quiet {
		mode = stream.ProgressNone
	}
	ex := &engine.Executor{
		Src: src, Dst: dst, Opts: o, PlanOpts: po, Log: log,
		Progress:          stream.NewRenderer(mode, env.Stderr, env.StderrIsTTY, log),
		Confirm:           confirm,
		SendCodec:         stream.None,
		RecvCodec:         stream.None,
		UnprivLinuxTarget: dstInfo.OS == "linux" && dstInfo.UID != 0 && !o.TargetSudo,
		SyncSnap:          syncSnap,
	}
	start := env.Now()
	results := ex.Run(ctx, states, plans)
	if !o.Quiet {
		RenderSummary(env.Stdout, results, env.Now().Sub(start))
	}
	if ctx.Err() != nil {
		return 2
	}
	return engine.ExitCode(results)
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
