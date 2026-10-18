package engine

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"github.com/piotaixr/syngoid/internal/cli"
	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/zfs"
)

// TargetName maps source dataset ds to its target name. It reports false
// when ds is neither sourceRoot nor a descendant of it.
func TargetName(sourceRoot, targetRoot, ds string) (string, bool) {
	if ds == sourceRoot {
		return targetRoot, true
	}
	if strings.HasPrefix(ds, sourceRoot+"/") {
		return targetRoot + ds[len(sourceRoot):], true
	}
	return "", false
}

// snapFilter reports whether a snapshot name passes the include/exclude
// patterns: exclude wins, and when include patterns exist one must match.
func snapFilter(o *cli.Options, name string) bool {
	for _, re := range o.ExcludeSnaps {
		if re.MatchString(name) {
			return false
		}
	}
	if len(o.IncludeSnaps) == 0 {
		return true
	}
	return matchesAny(o.IncludeSnaps, name)
}

func matchesAny(res []*regexp.Regexp, s string) bool {
	for _, re := range res {
		if re.MatchString(s) {
			return true
		}
	}
	return false
}

func filterSnaps(o *cli.Options, in []zfs.Snapshot) []zfs.Snapshot {
	var out []zfs.Snapshot
	for _, s := range in {
		if snapFilter(o, s.Name) {
			out = append(out, s)
		}
	}
	SortSnapshots(out)
	return out
}

// sourceData is the batched listing of the source side.
type sourceData struct {
	tree  []zfs.DatasetInfo
	snaps map[string][]zfs.Snapshot
	bms   map[string][]zfs.Bookmark
}

func listSource(ctx context.Context, src endpoint.Endpoint, root string, recursive bool) (sourceData, error) {
	var d sourceData
	var err error
	if d.tree, err = src.ListTree(ctx, root, recursive); err != nil {
		return d, err
	}
	if d.snaps, err = src.ListSnapshots(ctx, root, recursive); err != nil {
		return d, err
	}
	if d.bms, err = src.ListBookmarks(ctx, root, recursive); err != nil {
		return d, err
	}
	return d, nil
}

// targetData is the batched listing of the target side; both maps are empty
// when the target root does not exist.
type targetData struct {
	tree  map[string]zfs.DatasetInfo
	snaps map[string][]zfs.Snapshot
}

func listTarget(ctx context.Context, dst endpoint.Endpoint, root string, recursive bool) (targetData, error) {
	t := targetData{tree: map[string]zfs.DatasetInfo{}}
	tree, err := dst.ListTree(ctx, root, recursive)
	if errors.Is(err, zfs.ErrNoDataset) {
		return t, nil
	}
	if err != nil {
		return t, err
	}
	for _, d := range tree {
		t.tree[d.Name] = d
	}
	if t.snaps, err = dst.ListSnapshots(ctx, root, recursive); err != nil {
		if errors.Is(err, zfs.ErrNoDataset) {
			return targetData{tree: map[string]zfs.DatasetInfo{}}, nil
		}
		return t, err
	}
	return t, nil
}

// buildState assembles the DatasetState of one pair from the batched
// listings. Callers supply originInScope for the source origin.
func buildState(o *cli.Options, src zfs.DatasetInfo, tgtName string, sd sourceData, td targetData, syncSnap string, originInScope bool) DatasetState {
	st := DatasetState{
		Source:           src.Name,
		Target:           tgtName,
		SourceSnaps:      filterSnaps(o, sd.snaps[src.Name]),
		TargetSnaps:      filterSnaps(o, td.snaps[tgtName]),
		SourceBookmarks:  sd.bms[src.Name],
		SyncProp:         src.SyncProp,
		SourceType:       src.Type,
		TargetIsPoolRoot: !strings.Contains(tgtName, "/"),
	}
	if t, ok := td.tree[tgtName]; ok {
		st.TargetExists = true
		st.TargetUsed = t.Used
		st.ResumeToken = t.ResumeToken
	}
	if !o.NoCloneHandling {
		st.Origin = src.Origin
		st.OriginInScope = originInScope
	}
	if !o.NoSyncSnap && snapFilter(o, syncSnap) {
		st.NewSyncSnap = syncSnap
	}
	return st
}

// Gather builds the state of every source dataset under o.Source.Dataset
// that maps below o.Target.Dataset, in source listing order, with one
// listing call per kind per side. A missing source root is an error; a
// missing target root means no target datasets exist.
func Gather(ctx context.Context, src, dst endpoint.Endpoint, o *cli.Options, syncSnap string) ([]DatasetState, error) {
	srcRoot, dstRoot := o.Source.Dataset, o.Target.Dataset
	sd, err := listSource(ctx, src, srcRoot, o.Recursive)
	if err != nil {
		return nil, err
	}
	td, err := listTarget(ctx, dst, dstRoot, o.Recursive)
	if err != nil {
		return nil, err
	}

	type kept struct {
		info zfs.DatasetInfo
		tgt  string
	}
	var ds []kept
	inScope := map[string]bool{}
	for _, d := range sd.tree {
		if o.Recursive && o.SkipParent && d.Name == srcRoot {
			continue
		}
		if matchesAny(o.ExcludeDatasets, d.Name) {
			continue
		}
		tgt, ok := TargetName(srcRoot, dstRoot, d.Name)
		if !ok {
			continue
		}
		ds = append(ds, kept{d, tgt})
		inScope[d.Name] = true
	}

	states := make([]DatasetState, 0, len(ds))
	for _, k := range ds {
		originDS, _, _ := strings.Cut(k.info.Origin, "@")
		states = append(states, buildState(o, k.info, k.tgt, sd, td, syncSnap, k.info.Origin != "" && inScope[originDS]))
	}
	return states, nil
}

// GatherOne builds the state of exactly sourceDS and targetDS without
// recursion.
func GatherOne(ctx context.Context, src, dst endpoint.Endpoint, o *cli.Options, sourceDS, targetDS, syncSnap string, originInScope bool) (DatasetState, error) {
	sd, err := listSource(ctx, src, sourceDS, false)
	if err != nil {
		return DatasetState{}, err
	}
	td, err := listTarget(ctx, dst, targetDS, false)
	if err != nil {
		return DatasetState{}, err
	}
	for _, d := range sd.tree {
		if d.Name == sourceDS {
			return buildState(o, d, targetDS, sd, td, syncSnap, originInScope), nil
		}
	}
	return DatasetState{}, &zfs.CommandError{
		Argv:   []string{"zfs", "list", sourceDS},
		Stderr: "cannot open '" + sourceDS + "': dataset does not exist",
		Kind:   zfs.ErrNoDataset,
	}
}
