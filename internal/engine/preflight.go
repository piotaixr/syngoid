package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"

	"github.com/piotaixr/syngoid/internal/cli"
	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/perms"
	"github.com/piotaixr/syngoid/internal/zfs"
)

// DatasetMissing lists the permissions a user lacks on one dataset.
type DatasetMissing struct {
	// Dataset is the dataset being replicated. For a target that does not
	// exist yet the permissions are checked on its closest existing ancestor.
	Dataset string
	Missing []perms.Requirement
}

// SideReport is the preflight result for one end of the replication.
type SideReport struct {
	Side        Side
	Label, User string
	// Root means the user is uid 0 and nothing is checked; Sudo means zfs
	// runs through sudo and delegations are not checked.
	Root, Sudo bool
	// SudoErr is the failure of the non-interactive sudo check, if any.
	SudoErr string
	Missing []DatasetMissing
	// GrantDataset is where `zfs allow` must be run; Grant is the complete
	// required set for this side (empty for Root and Sudo sides).
	GrantDataset string
	Grant        []perms.Requirement
}

// PreflightReport is the preflight result for both sides; Sides is indexed by
// Source and Target.
type PreflightReport struct {
	Sides [2]SideReport
}

// OK reports whether no permission is missing and no sudo check failed.
func (r PreflightReport) OK() bool {
	for _, s := range r.Sides {
		if len(s.Missing) > 0 || s.SudoErr != "" {
			return false
		}
	}
	return true
}

// NeedsFrom derives the permission needs from the options. props are the
// properties --preserve-properties will pass (already filtered and sorted);
// every PROP of a --recvoptions `o PROP=value` is added after them.
func NeedsFrom(o *cli.Options, props []string, userProps bool) perms.Needs {
	n := perms.Needs{
		NoSyncSnap: o.NoSyncSnap, KeepSyncSnap: o.KeepSyncSnap, UseHold: o.UseHold,
		CreateBookmark: o.CreateBookmark, NoRollback: o.NoRollback,
		DeleteTargetSnapshots: o.DeleteTargetSnapshots, ForceDelete: o.ForceDelete,
		PreserveRecordsize: o.PreserveRecordsize,
		RawSend:            slices.ContainsFunc(o.SendOpts, func(op zfs.Opt) bool { return op.Flag == 'w' }),
		UserProps:          userProps,
	}
	n.Props = slices.Clone(props)
	for _, op := range o.RecvOpts {
		if op.Flag != 'o' {
			continue
		}
		name, _, _ := strings.Cut(op.Value, "=")
		if name != "" && !slices.Contains(n.Props, name) {
			n.Props = append(n.Props, name)
		}
	}
	return n
}

// Preflight checks that the effective user holds the permissions the options
// need on every dataset of both sides, without changing anything.
func Preflight(ctx context.Context, src, dst endpoint.Endpoint, o *cli.Options, states []DatasetState) (PreflightReport, error) {
	var rep PreflightReport
	srcInfo, err := src.Info(ctx)
	if err != nil {
		return rep, fmt.Errorf("source host info: %w", err)
	}
	dstInfo, err := dst.Info(ctx)
	if err != nil {
		return rep, fmt.Errorf("target host info: %w", err)
	}

	var props []string
	userProps := false
	if o.PreserveProperties && dstInfo.UID != 0 && !o.TargetSudo {
		unprivLinux := dstInfo.OS == "linux"
		if props, userProps, err = preservedProps(ctx, src, o, states, unprivLinux); err != nil {
			return rep, err
		}
	}
	needs := NeedsFrom(o, props, userProps)

	exists := existsCache(dst)
	sides := [2]struct {
		ep    endpoint.Endpoint
		info  endpoint.HostInfo
		sudo  bool
		root  string
		perms perms.Side
	}{
		{src, srcInfo, o.SourceSudo, o.Source.Dataset, perms.Source},
		{dst, dstInfo, o.TargetSudo, o.Target.Dataset, perms.Target},
	}
	for i, s := range sides {
		side := Side(i)
		sr := SideReport{Side: side, Label: s.ep.Label(), User: s.info.User, Root: s.info.UID == 0, Sudo: s.sudo && s.info.UID != 0}
		if sr.Label == "" {
			sr.Label = side.String()
		}
		rep.Sides[i] = sr
		if sr.Root {
			continue
		}
		grantDS := s.root
		if side == Target {
			if grantDS, err = closestExisting(ctx, exists, s.root); err != nil {
				return rep, err
			}
			if grantDS == "" {
				grantDS = s.root
			}
		}
		if sr.Sudo {
			if _, err := s.ep.GetProps(ctx, grantDS, "name"); err != nil {
				rep.Sides[i].SudoErr = errText(err)
			}
			continue
		}
		req := perms.Required(s.perms, needs)
		rep.Sides[i].GrantDataset, rep.Sides[i].Grant = grantDS, req
		checks, err := datasetsToCheck(ctx, side, states, exists)
		if err != nil {
			return rep, err
		}
		for _, c := range checks {
			raw, err := s.ep.Allow(ctx, c.check)
			if err != nil {
				return rep, fmt.Errorf("%s: zfs allow %s: %w", sr.Label, c.check, err)
			}
			d, err := perms.ParseAllow(raw)
			if err != nil {
				return rep, fmt.Errorf("%s: %s: %w", sr.Label, c.check, err)
			}
			have := perms.Effective(d, c.check, s.info.User, s.info.Groups)
			if miss := perms.Missing(have, req); len(miss) > 0 {
				rep.Sides[i].Missing = append(rep.Sides[i].Missing, DatasetMissing{Dataset: c.shown, Missing: miss})
			}
		}
	}
	return rep, nil
}

// errText is the stderr of a failed zfs command, else the error text.
func errText(err error) string {
	var ce *zfs.CommandError
	if errors.As(err, &ce) {
		if s := strings.TrimSpace(ce.Stderr); s != "" {
			return s
		}
	}
	return err.Error()
}

// preservedProps returns the sorted property names --preserve-properties
// will pass to the target, and whether any is a user property.
func preservedProps(ctx context.Context, src endpoint.Endpoint, o *cli.Options, states []DatasetState, unprivLinux bool) ([]string, bool, error) {
	set := map[string]bool{}
	userProps := false
	for _, st := range states {
		local, err := src.LocalProps(ctx, st.Source)
		if err != nil {
			return nil, false, fmt.Errorf("local properties of %s: %w", st.Source, err)
		}
		for k := range local {
			switch {
			case strings.Contains(k, ":"):
				userProps = true
			case slices.Contains(PreservePropsDenylist, k), unprivLinux && (k == "mountpoint" || k == "canmount"):
			default:
				set[k] = true
			}
		}
	}
	props := make([]string, 0, len(set))
	for k := range set {
		props = append(props, k)
	}
	sort.Strings(props)
	return props, userProps, nil
}

// existsCache returns a memoizing Exists over ep.
func existsCache(ep endpoint.Endpoint) func(context.Context, string) (bool, error) {
	memo := map[string]bool{}
	return func(ctx context.Context, ds string) (bool, error) {
		if v, ok := memo[ds]; ok {
			return v, nil
		}
		v, err := ep.Exists(ctx, ds)
		if err != nil {
			return false, fmt.Errorf("exists %s: %w", ds, err)
		}
		memo[ds] = v
		return v, nil
	}
}

// closestExisting returns ds when it exists, else its nearest existing
// ancestor, or "" when no ancestor exists.
func closestExisting(ctx context.Context, exists func(context.Context, string) (bool, error), ds string) (string, error) {
	for ; ds != ""; ds = parentDataset(ds) {
		ok, err := exists(ctx, ds)
		if err != nil {
			return "", err
		}
		if ok {
			return ds, nil
		}
	}
	return "", nil
}

func parentDataset(ds string) string {
	i := strings.LastIndexByte(ds, '/')
	if i < 0 {
		return ""
	}
	return ds[:i]
}

type checkTarget struct{ shown, check string }

// datasetsToCheck lists, without repeats, the datasets whose delegations must
// be read for side: each source dataset; each existing target dataset, and
// for absent ones the closest existing ancestor (shown under the first absent
// dataset that needs it).
func datasetsToCheck(ctx context.Context, side Side, states []DatasetState, exists func(context.Context, string) (bool, error)) ([]checkTarget, error) {
	var out []checkTarget
	seen := map[string]bool{}
	add := func(shown, check string) {
		if check != "" && !seen[check] {
			seen[check] = true
			out = append(out, checkTarget{shown, check})
		}
	}
	for _, st := range states {
		if side == Source {
			add(st.Source, st.Source)
			continue
		}
		if st.TargetExists {
			add(st.Target, st.Target)
			continue
		}
		anc, err := closestExisting(ctx, exists, parentDataset(st.Target))
		if err != nil {
			return nil, err
		}
		add(st.Target, anc)
	}
	return out, nil
}

// companions maps a permission to the permissions that must accompany it
// (spec §6.2).
var companions = map[string][]string{
	"snapshot":       {"mount"},
	"destroy":        {"mount"},
	"rollback":       {"mount"},
	"receive":        {"create", "mount"},
	"receive:append": {"create", "mount"},
	"create":         {"mount"},
}

// withCompanions returns the union of the missing permissions over datasets
// followed by their companions, without repeats.
func (s SideReport) withCompanions() []perms.Requirement {
	var out []perms.Requirement
	seen := map[string]bool{}
	add := func(r perms.Requirement) {
		if k := r.AnyOf[0]; !seen[k] {
			seen[k] = true
			out = append(out, r)
		}
	}
	for _, dm := range s.Missing {
		for _, r := range dm.Missing {
			add(r)
		}
	}
	for _, r := range slices.Clone(out) {
		for _, name := range r.AnyOf {
			for _, c := range companions[name] {
				add(perms.Requirement{AnyOf: []string{c}})
			}
		}
	}
	return out
}

// Render writes the failure report with ready-to-paste grant commands; it
// writes nothing for a clean report unless full is set, in which case it
// also lists the complete minimal grants of every delegation-checked side.
func (r PreflightReport) Render(w io.Writer, full bool) {
	var b strings.Builder
	defer func() { _, _ = io.WriteString(w, b.String()) }()
	if !r.OK() {
		fmt.Fprintln(&b, "CRITICAL ERROR: missing ZFS permissions")
		for _, s := range r.Sides {
			for _, dm := range s.Missing {
				names := make([]string, len(dm.Missing))
				for i, m := range dm.Missing {
					names[i] = m.String()
				}
				fmt.Fprintf(&b, "  %s %s: %s\n", s.Side, dm.Dataset, strings.Join(names, ", "))
			}
		}
		for _, s := range r.Sides {
			if s.SudoErr != "" {
				fmt.Fprintf(&b, "  [%s]  sudo -n zfs failed: %s\n", s.Label, s.SudoErr)
			}
		}
		granted := false
		for _, s := range r.Sides {
			if len(s.Missing) == 0 {
				continue
			}
			if !granted {
				fmt.Fprintln(&b, "Grant them (as root) with:")
				granted = true
			}
			fmt.Fprintf(&b, "  [%s]  %s\n", s.Label, perms.GrantCommand(s.User, s.GrantDataset, s.withCompanions()))
		}
		if granted {
			fmt.Fprintln(&b, "Alternatively, run with --sudo (requires a sudoers rule for zfs).")
		}
	}
	if !full {
		return
	}
	header := false
	for _, s := range r.Sides {
		if s.Root || s.Sudo {
			continue
		}
		if !header {
			fmt.Fprintln(&b, "Full minimal grants:")
			header = true
		}
		fmt.Fprintf(&b, "  [%s]  %s\n", s.Label, perms.GrantCommand(s.User, s.GrantDataset, s.Grant))
	}
}
