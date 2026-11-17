package engine

import (
	"testing"
	"time"

	"github.com/piotaixr/syngoid/internal/cli"
	"github.com/piotaixr/syngoid/internal/zfs"
)

func TestSelectionPick(t *testing.T) {
	at := func(sec int64) *time.Time { t := time.Unix(sec, 0); return &t }
	cases := []struct {
		name string
		sel  Selection
		want string
		ok   bool
	}{
		{"newest", Selection{}, "c", true},
		{"at between", Selection{At: at(2500)}, "b", true},
		{"at exactly (inclusive)", Selection{At: at(2000)}, "b", true},
		{"at after all", Selection{At: at(9000)}, "c", true},
		{"at before all", Selection{At: at(999)}, "", false},
		{"name present", Selection{Name: "a"}, "a", true},
		{"name absent", Selection{Name: "zz"}, "", false},
	}
	for _, txgs := range []bool{true, false} {
		snaps := []zfs.Snapshot{
			{Name: "a", GUID: "1", CreateTxg: 10, Creation: 1000},
			{Name: "b", GUID: "2", CreateTxg: 20, Creation: 2000},
			{Name: "c", GUID: "3", CreateTxg: 30, Creation: 3000},
		}
		if !txgs {
			for i := range snaps {
				snaps[i].CreateTxg = 0
			}
		}
		for _, c := range cases {
			got, ok := c.sel.Pick(snaps)
			if ok != c.ok || got.Name != c.want {
				t.Errorf("%s (txgs=%v): got %q,%v want %q,%v", c.name, txgs, got.Name, ok, c.want, c.ok)
			}
			if _, ok := c.sel.Pick(nil); ok {
				t.Errorf("%s: Pick(nil) should be false", c.name)
			}
		}
	}
}

func TestSelectionFrom(t *testing.T) {
	if got := SelectionFrom(nil); got.At != nil || got.Name != "" {
		t.Errorf("nil: %+v", got)
	}
	at := time.Unix(5, 0)
	got := SelectionFrom(&cli.RestoreOptions{At: &at, Snapshot: "x"})
	if got.At != &at || got.Name != "x" {
		t.Errorf("got %+v", got)
	}
}

func TestSelectionMissing(t *testing.T) {
	snaps := []zfs.Snapshot{{Name: "a", Creation: 1}}
	at := time.Date(2026, 11, 10, 3, 0, 0, 0, time.UTC)
	cases := []struct {
		sel   Selection
		snaps []zfs.Snapshot
		want  string
	}{
		{Selection{Name: "daily"}, snaps, "no snapshot daily on backup/data"},
		{Selection{At: &at}, snaps, "no snapshot of backup/data at or before 2026-11-10T03:00:00Z"},
		{Selection{Name: "daily"}, nil, "no snapshots on backup/data"},
		{Selection{At: &at}, nil, "no snapshots on backup/data"},
		{Selection{}, nil, "no snapshots on backup/data"},
	}
	for _, c := range cases {
		if got := c.sel.Missing("backup/data", c.snaps); got != c.want {
			t.Errorf("got %q want %q", got, c.want)
		}
	}
}
