package engine

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

const goldenSync = "syncoid_nas_2026-10-18:14:03:07-GMT02:00"

func renderBasic() string {
	plans := []Plan{
		{Source: "tank/data", Target: "backup/data", Status: StatusOK, Actions: []Action{
			SendIncremental{From: "a", To: goldenSync, Intermediates: true},
			PruneSyncSnaps{Side: Source, Snaps: []string{"old"}},
		}},
		{
			Source: "tank/data/vm", Target: "backup/data/vm", Status: StatusSkipped,
			Message: "Skipping dataset (syncoid:sync=false): tank/data/vm...",
		},
		{
			Source: "tank/data/db", Target: "backup/data/db", Status: StatusCritical,
			Message: noMatchMessage(DatasetState{Source: "tank/data/db", Target: "backup/data/db"}),
		},
	}
	est := func(_ Plan, a Action) (int64, bool) {
		if _, ok := a.(SendIncremental); ok {
			return 1288490189, true
		}
		return 0, false
	}
	var b bytes.Buffer
	RenderPlan(&b, plans, goldenSync, est)
	return b.String()
}

func TestRenderPlanGolden(t *testing.T) {
	want, err := os.ReadFile("testdata/plan_basic.golden")
	if err != nil {
		t.Fatal(err)
	}
	if got := renderBasic(); got != string(want) {
		t.Errorf("rendered plan differs from golden:\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

func TestRenderPlanStatusesAndTotals(t *testing.T) {
	plans := []Plan{
		{Source: "s1", Target: "t1", Status: StatusOK, Actions: []Action{
			SendFull{Snap: "a"}, SendIncremental{From: "a", To: "b"},
		}},
		{Source: "s2", Target: "t2", Status: StatusNothing, Message: "up to date"},
		{Source: "s3", Target: "t3", Status: StatusSoft, Message: "careful"},
	}
	est := func(_ Plan, a Action) (int64, bool) {
		if _, ok := a.(SendFull); ok {
			return 2048, false
		}
		return 0, false
	}
	var b bytes.Buffer
	RenderPlan(&b, plans, "sync", est)
	want := strings.Join([]string{
		"s1 → t1",
		"  send full @a  2.0 KiB",
		"  send incremental @a → @b",
		"s2 → t2",
		"  nothing to do: up to date",
		"s3 → t3",
		"  WARNING: careful",
		"Total estimated transfer: 2.0 KiB",
		"",
	}, "\n")
	if got := b.String(); got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}
