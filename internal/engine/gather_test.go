package engine

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/piotaixr/syngoid/internal/cli"
	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/endpoint/fake"
	"github.com/piotaixr/syngoid/internal/zfs"
)

// countingEndpoint counts the list calls made through it.
type countingEndpoint struct {
	endpoint.Endpoint
	tree, snaps, bms int
	recursive        []bool
}

func (c *countingEndpoint) ListTree(ctx context.Context, root string, r bool) ([]zfs.DatasetInfo, error) {
	c.tree++
	c.recursive = append(c.recursive, r)
	return c.Endpoint.ListTree(ctx, root, r)
}

func (c *countingEndpoint) ListSnapshots(ctx context.Context, root string, r bool) (map[string][]zfs.Snapshot, error) {
	c.snaps++
	c.recursive = append(c.recursive, r)
	return c.Endpoint.ListSnapshots(ctx, root, r)
}

func (c *countingEndpoint) ListBookmarks(ctx context.Context, root string, r bool) (map[string][]zfs.Bookmark, error) {
	c.bms++
	c.recursive = append(c.recursive, r)
	return c.Endpoint.ListBookmarks(ctx, root, r)
}

type gatherEnv struct {
	sw, tw   *fake.World
	src, dst *countingEndpoint
}

func newGatherEnv() *gatherEnv {
	sw, tw := fake.NewWorld(), fake.NewWorld()
	sw.Create("tank/data", "")
	sw.Create("tank/data/vm", "")
	sw.Create("tank/data/vm/disk", "volume")
	return &gatherEnv{
		sw: sw, tw: tw,
		src: &countingEndpoint{Endpoint: fake.New(sw, endpoint.HostInfo{})},
		dst: &countingEndpoint{Endpoint: fake.New(tw, endpoint.HostInfo{})},
	}
}

func (g *gatherEnv) opts() *cli.Options {
	return &cli.Options{
		Source:    cli.Location{Dataset: "tank/data"},
		Target:    cli.Location{Dataset: "backup/data"},
		Recursive: true,
	}
}

func (g *gatherEnv) gather(t *testing.T, o *cli.Options) []DatasetState {
	t.Helper()
	st, err := Gather(context.Background(), g.src, g.dst, o, "syncoid_nas_T")
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func sources(st []DatasetState) []string {
	var out []string
	for _, s := range st {
		out = append(out, s.Source)
	}
	return out
}

func eqStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestGatherRecursive(t *testing.T) {
	g := newGatherEnv()
	g.sw.Snap("tank/data", "a")
	g.sw.MarkBookmark("tank/data", "a", "bm")
	st := g.gather(t, g.opts())
	if len(st) != 3 {
		t.Fatalf("states = %d, want 3", len(st))
	}
	wantT := []string{"backup/data", "backup/data/vm", "backup/data/vm/disk"}
	for i, s := range st {
		if s.Target != wantT[i] || s.TargetExists {
			t.Errorf("state %d = %s -> %s exists=%v", i, s.Source, s.Target, s.TargetExists)
		}
	}
	if st[2].SourceType != "volume" {
		t.Errorf("SourceType = %q", st[2].SourceType)
	}
	if len(st[0].SourceSnaps) != 1 || len(st[0].SourceBookmarks) != 1 {
		t.Errorf("snaps/bookmarks not gathered: %+v", st[0])
	}
	if g.src.tree != 1 || g.src.snaps != 1 || g.src.bms != 1 {
		t.Errorf("source list calls tree=%d snaps=%d bms=%d, want 1 each", g.src.tree, g.src.snaps, g.src.bms)
	}
	for _, r := range g.src.recursive {
		if !r {
			t.Error("source list call was not recursive")
		}
	}
	if g.dst.tree != 1 || g.dst.snaps > 1 || g.dst.bms != 0 {
		t.Errorf("target list calls tree=%d snaps=%d bms=%d", g.dst.tree, g.dst.snaps, g.dst.bms)
	}
}

func TestGatherTargetExists(t *testing.T) {
	g := newGatherEnv()
	g.tw.Create("backup/data", "")
	g.tw.Dataset("backup/data").Used = 1234
	g.tw.Dataset("backup/data").ResumeToken = "tok"
	g.tw.Snap("backup/data", "a")
	g.sw.SetProp("tank/data", "syncoid:sync", "false")
	st := g.gather(t, g.opts())
	if !st[0].TargetExists || st[0].TargetUsed != 1234 || st[0].ResumeToken != "tok" || len(st[0].TargetSnaps) != 1 {
		t.Errorf("target fields wrong: %+v", st[0])
	}
	if st[0].SyncProp != "false" {
		t.Errorf("SyncProp = %q", st[0].SyncProp)
	}
	if st[1].TargetExists {
		t.Error("child target should not exist")
	}
}

func TestGatherSiblingPrefixNotChild(t *testing.T) {
	if _, ok := TargetName("tank/data", "backup/data", "tank/data2"); ok {
		t.Error("tank/data2 must not map")
	}
	if got, ok := TargetName("tank/data", "backup/data", "tank/data/x"); !ok || got != "backup/data/x" {
		t.Errorf("got %q %v", got, ok)
	}
	if got, ok := TargetName("tank/data", "backup/data", "tank/data"); !ok || got != "backup/data" {
		t.Errorf("root got %q %v", got, ok)
	}
}

func TestGatherExcludeAndSkipParent(t *testing.T) {
	g := newGatherEnv()
	o := g.opts()
	o.ExcludeDatasets = []*regexp.Regexp{regexp.MustCompile(`vm$`)}
	if got, want := sources(g.gather(t, o)), []string{"tank/data", "tank/data/vm/disk"}; !eqStrings(got, want) {
		t.Errorf("exclude: got %v, want %v", got, want)
	}
	o = g.opts()
	o.SkipParent = true
	if got, want := sources(g.gather(t, o)), []string{"tank/data/vm", "tank/data/vm/disk"}; !eqStrings(got, want) {
		t.Errorf("skip-parent: got %v, want %v", got, want)
	}
	o.Recursive = false
	if got := sources(g.gather(t, o)); len(got) != 1 || got[0] != "tank/data" {
		t.Errorf("skip-parent non-recursive: got %v", got)
	}
}

func snapNames(s []zfs.Snapshot) []string {
	var out []string
	for _, x := range s {
		out = append(out, x.Name)
	}
	return out
}

func TestGatherSnapshotFilters(t *testing.T) {
	g := newGatherEnv()
	g.tw.Create("backup/data", "")
	for _, n := range []string{"auto1", "manual", "auto_hourly", "auto2"} {
		g.sw.Snap("tank/data", n)
		g.tw.Snap("backup/data", n)
	}
	o := g.opts()
	o.IncludeSnaps = []*regexp.Regexp{regexp.MustCompile(`^auto`)}
	st := g.gather(t, o)[0]
	want := []string{"auto1", "auto_hourly", "auto2"}
	if !eqStrings(snapNames(st.SourceSnaps), want) || !eqStrings(snapNames(st.TargetSnaps), want) {
		t.Errorf("include: src %v tgt %v, want %v", snapNames(st.SourceSnaps), snapNames(st.TargetSnaps), want)
	}
	o.ExcludeSnaps = []*regexp.Regexp{regexp.MustCompile(`hourly`)}
	st = g.gather(t, o)[0]
	want = []string{"auto1", "auto2"}
	if !eqStrings(snapNames(st.SourceSnaps), want) || !eqStrings(snapNames(st.TargetSnaps), want) {
		t.Errorf("exclude wins: src %v tgt %v, want %v", snapNames(st.SourceSnaps), snapNames(st.TargetSnaps), want)
	}
}

func TestGatherSyncSnapFiltered(t *testing.T) {
	g := newGatherEnv()
	for _, s := range g.gather(t, g.opts()) {
		if s.NewSyncSnap != "syncoid_nas_T" {
			t.Errorf("%s NewSyncSnap = %q", s.Source, s.NewSyncSnap)
		}
	}
	o := g.opts()
	o.ExcludeSnaps = []*regexp.Regexp{regexp.MustCompile(`^syncoid_`)}
	for _, s := range g.gather(t, o) {
		if s.NewSyncSnap != "" {
			t.Errorf("exclude: %s NewSyncSnap = %q", s.Source, s.NewSyncSnap)
		}
	}
	o = g.opts()
	o.NoSyncSnap = true
	for _, s := range g.gather(t, o) {
		if s.NewSyncSnap != "" {
			t.Errorf("no-sync-snap: %s NewSyncSnap = %q", s.Source, s.NewSyncSnap)
		}
	}
}

func TestGatherOriginInScope(t *testing.T) {
	g := newGatherEnv()
	g.sw.Create("tank/data/c", "")
	g.sw.Create("tank/data/far", "")
	g.sw.Snap("tank/data/vm", "s0")
	g.sw.Dataset("tank/data/c").Origin = "tank/data/vm@s0"
	g.sw.Dataset("tank/data/far").Origin = "tank/other@s0"
	byName := func(st []DatasetState) map[string]DatasetState {
		m := map[string]DatasetState{}
		for _, s := range st {
			m[s.Source] = s
		}
		return m
	}
	m := byName(g.gather(t, g.opts()))
	if c := m["tank/data/c"]; !c.OriginInScope || c.Origin != "tank/data/vm@s0" {
		t.Errorf("clone: %+v", c)
	}
	if f := m["tank/data/far"]; f.OriginInScope || f.Origin != "tank/other@s0" {
		t.Errorf("far clone: %+v", f)
	}
	o := g.opts()
	o.NoCloneHandling = true
	m = byName(g.gather(t, o))
	if c := m["tank/data/c"]; c.OriginInScope || c.Origin != "" {
		t.Errorf("no-clone-handling: %+v", c)
	}
}

func TestGatherSourceMissing(t *testing.T) {
	g := newGatherEnv()
	o := g.opts()
	o.Source.Dataset = "tank/absent"
	_, err := Gather(context.Background(), g.src, g.dst, o, "s")
	if !errors.Is(err, zfs.ErrNoDataset) {
		t.Fatalf("err = %v, want ErrNoDataset", err)
	}
}

func TestGatherPoolRoot(t *testing.T) {
	g := newGatherEnv()
	g.sw.Create("tank", "")
	o := g.opts()
	o.Source.Dataset = "tank"
	o.Target.Dataset = "backup"
	o.Recursive = false
	st := g.gather(t, o)
	if len(st) != 1 || !st[0].TargetIsPoolRoot {
		t.Errorf("states = %+v", st)
	}
	if g.gather(t, g.opts())[0].TargetIsPoolRoot {
		t.Error("backup/data is not a pool root")
	}
}

func TestGatherOne(t *testing.T) {
	g := newGatherEnv()
	g.sw.Snap("tank/data/vm", "a")
	g.sw.Snap("tank/data/vm/disk", "x")
	g.sw.Dataset("tank/data/vm").Origin = "tank/data@o"
	o := g.opts()
	st, err := GatherOne(context.Background(), g.src, g.dst, o, "tank/data/vm", "backup/data/vm", "syncoid_nas_T", true)
	if err != nil {
		t.Fatal(err)
	}
	if st.Source != "tank/data/vm" || st.Target != "backup/data/vm" || st.TargetExists || len(st.SourceSnaps) != 1 {
		t.Errorf("state = %+v", st)
	}
	if !st.OriginInScope || st.Origin != "tank/data@o" || st.NewSyncSnap != "syncoid_nas_T" {
		t.Errorf("origin/sync = %+v", st)
	}
	for _, r := range g.src.recursive {
		if r {
			t.Error("GatherOne must not recurse")
		}
	}
	o.NoCloneHandling = true
	st, err = GatherOne(context.Background(), g.src, g.dst, o, "tank/data/vm", "backup/data/vm", "s", true)
	if err != nil {
		t.Fatal(err)
	}
	if st.Origin != "" || st.OriginInScope {
		t.Errorf("no-clone-handling: %+v", st)
	}
	if _, err := GatherOne(context.Background(), g.src, g.dst, o, "tank/none", "backup/none", "s", false); !errors.Is(err, zfs.ErrNoDataset) {
		t.Errorf("missing source err = %v", err)
	}
}
