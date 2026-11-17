package engine

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/piotaixr/syngoid/internal/cli"
	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/endpoint/fake"
	"github.com/piotaixr/syngoid/internal/zfs"
)

var rpDatasets = []string{"b/d", "b/d/a", "b/d/b", "b/d/c", "b/d/e"}

// rpSnaps builds the fixture of the restore points tests; creations are
// seconds*1000, as the zfs package reports them.
func rpSnaps() map[string][]zfs.Snapshot {
	snaps := map[string][]zfs.Snapshot{}
	for i, ds := range rpDatasets {
		snaps[ds] = append(snaps[ds], zfs.Snapshot{Name: "daily1", Creation: uint64(100+i) * 1000})
		if ds != "b/d/e" {
			snaps[ds] = append(snaps[ds], zfs.Snapshot{Name: "daily2", Creation: uint64(200+i) * 1000})
		}
	}
	snaps["b/d/a"] = append(snaps["b/d/a"], zfs.Snapshot{Name: "solo", Creation: 150_000})
	return snaps
}

func TestRestorePoints(t *testing.T) {
	want := []RestorePoint{
		{Name: "daily2", Created: 203, Have: 4, Missing: []string{"b/d/e"}},
		{Name: "solo", Created: 150, Have: 1, Missing: []string{"b/d", "b/d/b", "b/d/c", "b/d/e"}},
		{Name: "daily1", Created: 104, Have: 5},
	}
	if got := RestorePoints(rpDatasets, rpSnaps()); !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v\nwant %+v", got, want)
	}
	t.Run("same time orders by name", func(t *testing.T) {
		snaps := map[string][]zfs.Snapshot{"x": {{Name: "b", Creation: 5000}, {Name: "a", Creation: 5999}}}
		got := RestorePoints([]string{"x"}, snaps)
		if len(got) != 2 || got[0].Name != "a" || got[1].Name != "b" {
			t.Errorf("got %+v", got)
		}
	})
	t.Run("none", func(t *testing.T) {
		if got := RestorePoints(rpDatasets, nil); len(got) != 0 {
			t.Errorf("got %+v", got)
		}
	})
}

func TestRenderRestorePoints(t *testing.T) {
	var b bytes.Buffer
	RenderRestorePoints(&b, RestorePoints(rpDatasets, rpSnaps()), 5, time.UTC)
	want := "CREATED            SNAPSHOT   DATASETS\n" +
		"1970-01-01 00:03   daily2     4/5        missing: b/d/e\n" +
		"1970-01-01 00:02   solo       1/5        missing: 4 datasets\n" +
		"1970-01-01 00:01   daily1     5/5\n"
	if b.String() != want {
		t.Errorf("got:\n%s\nwant:\n%s", b.String(), want)
	}
}

func TestGatherSource(t *testing.T) {
	w := fake.NewWorld()
	w.Create("tank/data", "")
	w.Create("tank/data/vm", "")
	w.Create("tank/data/vm/disk", "volume")
	w.Snap("tank/data/vm", "one")
	w.Snap("tank/data/vm", "two")
	ep := fake.New(w, endpoint.HostInfo{})
	o := &cli.Options{Source: cli.Location{Dataset: "tank/data"}, Recursive: true}
	gather := func(o *cli.Options) ([]string, map[string][]zfs.Snapshot) {
		t.Helper()
		ds, snaps, err := GatherSource(context.Background(), ep, o)
		if err != nil {
			t.Fatal(err)
		}
		return ds, snaps
	}
	ds, snaps := gather(o)
	if want := []string{"tank/data", "tank/data/vm", "tank/data/vm/disk"}; !reflect.DeepEqual(ds, want) {
		t.Errorf("recursive: got %v, want %v", ds, want)
	}
	if got := snaps["tank/data/vm"]; len(got) != 2 || got[0].Name != "one" || got[1].Name != "two" {
		t.Errorf("snapshots not oldest to newest: %+v", got)
	}
	o.SkipParent = true
	if ds, _ := gather(o); !reflect.DeepEqual(ds, []string{"tank/data/vm", "tank/data/vm/disk"}) {
		t.Errorf("skip-parent: got %v", ds)
	}
	o.SkipParent = false
	o.ExcludeDatasets = []*regexp.Regexp{regexp.MustCompile(`vm$`)}
	if ds, _ := gather(o); !reflect.DeepEqual(ds, []string{"tank/data", "tank/data/vm/disk"}) {
		t.Errorf("exclude: got %v", ds)
	}
	o.Source.Dataset = "tank/nope"
	if _, _, err := GatherSource(context.Background(), ep, o); !errors.Is(err, zfs.ErrNoDataset) {
		t.Errorf("missing source: err = %v, want ErrNoDataset", err)
	}
}
