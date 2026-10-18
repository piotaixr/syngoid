package engine

import (
	"testing"

	"github.com/piotaixr/syngoid/internal/zfs"
)

func names(s []zfs.Snapshot) []string {
	out := make([]string, len(s))
	for i, x := range s {
		out[i] = x.Name
	}
	return out
}

func equalStrings(a, b []string) bool {
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

func TestSortSnapshotsTxgThenCreation(t *testing.T) {
	t.Run("txg wins when both non-zero", func(t *testing.T) {
		s := []zfs.Snapshot{
			{Name: "b", CreateTxg: 20, Creation: 1000},
			{Name: "a", CreateTxg: 10, Creation: 9000},
			{Name: "c", CreateTxg: 30, Creation: 5000},
		}
		SortSnapshots(s)
		if got, want := names(s), []string{"a", "b", "c"}; !equalStrings(got, want) {
			t.Fatalf("order = %v, want %v", got, want)
		}
	})
	t.Run("creation with tie-break decides when txg unknown", func(t *testing.T) {
		s := []zfs.Snapshot{
			{Name: "c", Creation: 2000},
			{Name: "b", Creation: 1001},
			{Name: "a", Creation: 1000},
		}
		SortSnapshots(s)
		if got, want := names(s), []string{"a", "b", "c"}; !equalStrings(got, want) {
			t.Fatalf("order = %v, want %v", got, want)
		}
	})
	t.Run("one txg zero falls back to creation", func(t *testing.T) {
		s := []zfs.Snapshot{
			{Name: "b", CreateTxg: 5, Creation: 3000},
			{Name: "a", CreateTxg: 0, Creation: 1000},
		}
		SortSnapshots(s)
		if got, want := names(s), []string{"a", "b"}; !equalStrings(got, want) {
			t.Fatalf("order = %v, want %v", got, want)
		}
	})
	t.Run("ties keep input order", func(t *testing.T) {
		s := []zfs.Snapshot{
			{Name: "x", CreateTxg: 7},
			{Name: "y", CreateTxg: 7},
			{Name: "z", CreateTxg: 7},
		}
		SortSnapshots(s)
		if got, want := names(s), []string{"x", "y", "z"}; !equalStrings(got, want) {
			t.Fatalf("order = %v, want %v", got, want)
		}
	})
}

func TestMatchingNewestNameAndGUID(t *testing.T) {
	src := []zfs.Snapshot{
		{Name: "s1", GUID: "1", CreateTxg: 1},
		{Name: "s2", GUID: "2", CreateTxg: 2},
		{Name: "s3", GUID: "33", CreateTxg: 3},
		{Name: "s4", GUID: "4", CreateTxg: 4},
	}
	tgt := []zfs.Snapshot{
		{Name: "s1", GUID: "1", CreateTxg: 1},
		{Name: "s2", GUID: "2", CreateTxg: 2},
		{Name: "s3", GUID: "999", CreateTxg: 3}, // same name, different guid
	}
	got, ok := Matching(src, tgt)
	if !ok || got != "s2" {
		t.Fatalf("Matching = %q, %v; want s2, true", got, ok)
	}
	if got, ok := Matching(src, nil); ok {
		t.Fatalf("Matching with empty target = %q, true; want false", got)
	}
	if got, ok := Matching(src, []zfs.Snapshot{{Name: "s1", GUID: "other"}}); ok {
		t.Fatalf("Matching with only guid mismatch = %q, true; want false", got)
	}
}

func TestBookmarkFallbackUsesTargetOrder(t *testing.T) {
	// Target txgs order t1 < t2; the bookmarks are listed in the opposite
	// order, so only the target's ordering picks t2.
	tgt := []zfs.Snapshot{
		{Name: "t1", GUID: "10", CreateTxg: 100},
		{Name: "t2", GUID: "20", CreateTxg: 200},
		{Name: "t3", GUID: "30", CreateTxg: 300},
	}
	bms := []zfs.Bookmark{
		{Name: "b20", GUID: "20", CreateTxg: 5},
		{Name: "b10", GUID: "10", CreateTxg: 50},
	}
	bm, name, ok := BookmarkFallback(tgt, bms)
	if !ok || name != "t2" || bm.Name != "b20" {
		t.Fatalf("BookmarkFallback = %+v, %q, %v; want b20, t2, true", bm, name, ok)
	}
	if _, _, ok := BookmarkFallback(tgt, []zfs.Bookmark{{Name: "x", GUID: "99"}}); ok {
		t.Fatal("BookmarkFallback matched without a shared guid")
	}
	if _, _, ok := BookmarkFallback(nil, bms); ok {
		t.Fatal("BookmarkFallback matched with empty target")
	}
}

func TestNextAfterBookmark(t *testing.T) {
	t.Run("txg", func(t *testing.T) {
		src := []zfs.Snapshot{
			{Name: "s1", CreateTxg: 10, Creation: 1000},
			{Name: "s2", CreateTxg: 20, Creation: 2000},
			{Name: "s3", CreateTxg: 30, Creation: 3000},
		}
		got, ok := NextAfterBookmark(src, zfs.Bookmark{CreateTxg: 20, Creation: 9999})
		if !ok || got != "s2" {
			t.Fatalf("got %q, %v; want s2, true", got, ok)
		}
		if got, ok := NextAfterBookmark(src, zfs.Bookmark{CreateTxg: 31}); ok {
			t.Fatalf("got %q, true; want false", got)
		}
	})
	t.Run("creation when a txg is zero", func(t *testing.T) {
		src := []zfs.Snapshot{
			{Name: "s1", CreateTxg: 10, Creation: 1000},
			{Name: "s2", CreateTxg: 0, Creation: 2000},
			{Name: "s3", CreateTxg: 30, Creation: 3000},
		}
		got, ok := NextAfterBookmark(src, zfs.Bookmark{CreateTxg: 0, Creation: 1500})
		if !ok || got != "s2" {
			t.Fatalf("got %q, %v; want s2, true", got, ok)
		}
	})
}

func TestExitCode(t *testing.T) {
	r := func(s ...Status) []Result {
		out := make([]Result, len(s))
		for i, st := range s {
			out[i] = Result{Status: st}
		}
		return out
	}
	cases := []struct {
		name string
		in   []Result
		want int
	}{
		{"empty", nil, 0},
		{"ok nothing skipped", r(StatusOK, StatusNothing, StatusSkipped), 0},
		{"soft", r(StatusOK, StatusSoft, StatusNothing), 1},
		{"critical", r(StatusSoft, StatusCritical, StatusOK), 2},
	}
	for _, c := range cases {
		if got := ExitCode(c.in); got != c.want {
			t.Errorf("%s: ExitCode = %d, want %d", c.name, got, c.want)
		}
	}
}

func TestStatusOrderAndStrings(t *testing.T) {
	ordered := []Status{StatusOK, StatusNothing, StatusSkipped, StatusSoft, StatusCritical}
	labels := []string{"ok", "nothing to do", "skipped", "failed (soft)", "failed"}
	for i, s := range ordered {
		if i > 0 && ordered[i-1] >= s {
			t.Errorf("status %d not greater than %d", s, ordered[i-1])
		}
		if s.String() != labels[i] {
			t.Errorf("Status(%d).String() = %q, want %q", s, s.String(), labels[i])
		}
	}
	if Source.String() != "source" || Target.String() != "target" {
		t.Errorf("Side strings = %q, %q", Source, Target)
	}
}
