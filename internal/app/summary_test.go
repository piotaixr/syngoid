package app

import (
	"bytes"
	"testing"
	"time"

	"github.com/piotaixr/syngoid/internal/engine"
)

func TestRenderSummary(t *testing.T) {
	results := []engine.Result{
		{Source: "tank/data", Target: "backup/data", Status: engine.StatusOK, Bytes: 1288490189, Duration: 22 * time.Second},
		{
			Source: "tank/data/vm", Target: "backup/data/vm", Status: engine.StatusSkipped,
			Message: "Skipping dataset (syncoid:sync=false): tank/data/vm...\nsecond line",
		},
	}
	var buf bytes.Buffer
	RenderSummary(&buf, results, 25*time.Second)
	want := `Summary:
  tank/data → backup/data: ok, 1.2 GiB in 0:00:22
  tank/data/vm → backup/data/vm: skipped (Skipping dataset (syncoid:sync=false): tank/data/vm...)
  2 dataset(s): 1 ok, 0 nothing to do, 1 skipped, 0 failed; 1.2 GiB transferred in 0:00:25
`
	if buf.String() != want {
		t.Errorf("RenderSummary =\n%s\nwant\n%s", buf.String(), want)
	}
}

func TestRenderSummaryFailures(t *testing.T) {
	results := []engine.Result{
		{Source: "a", Target: "b", Status: engine.StatusNothing, Message: "up to date"},
		{Source: "a/c", Target: "b/c", Status: engine.StatusSoft, Message: "busy"},
		{Source: "a/d", Target: "b/d", Status: engine.StatusCritical, Message: "boom"},
	}
	var buf bytes.Buffer
	RenderSummary(&buf, results, 0)
	want := `Summary:
  a → b: nothing to do (up to date)
  a/c → b/c: failed (soft) (busy)
  a/d → b/d: failed (boom)
  3 dataset(s): 0 ok, 1 nothing to do, 0 skipped, 2 failed; 0 B transferred in 0:00:00
`
	if buf.String() != want {
		t.Errorf("RenderSummary =\n%s\nwant\n%s", buf.String(), want)
	}
}
