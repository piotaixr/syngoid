// Package engine holds the replication state types and the pure snapshot
// ordering and matching rules shared by the planner and executor.
package engine

import (
	"time"

	"github.com/piotaixr/syngoid/internal/zfs"
)

// Side identifies one end of a replication.
type Side int

// Replication sides.
const (
	Source Side = iota
	Target
)

// String returns "source" or "target".
func (s Side) String() string {
	if s == Target {
		return "target"
	}
	return "source"
}

// Status is the outcome of one dataset replication. Values are ordered by
// severity so the worst outcome is the numerically largest.
type Status int

// Outcome statuses, least to most severe.
const (
	StatusOK Status = iota
	StatusNothing
	StatusSkipped
	StatusSoft
	StatusCritical
)

// String returns the human-readable status label.
func (s Status) String() string {
	switch s {
	case StatusOK:
		return "ok"
	case StatusNothing:
		return "nothing to do"
	case StatusSkipped:
		return "skipped"
	case StatusSoft:
		return "failed (soft)"
	case StatusCritical:
		return "failed"
	}
	return "unknown"
}

// Result reports the outcome for one source/target dataset pair.
type Result struct {
	Source, Target string
	Status         Status
	Message        string
	Err            error
	Bytes          int64
	Duration       time.Duration
}

// DatasetState is the gathered state of one source/target pair. Snapshot
// slices are sorted oldest to newest.
type DatasetState struct {
	Source, Target   string
	SourceSnaps      []zfs.Snapshot
	TargetSnaps      []zfs.Snapshot
	SourceBookmarks  []zfs.Bookmark
	TargetExists     bool
	TargetUsed       int64
	ResumeToken      string
	SyncProp         string
	Origin           string
	OriginInScope    bool
	NewSyncSnap      string
	TargetIsPoolRoot bool
	SourceType       string
}

// ExitCode maps results to the process exit code: 0 when everything is ok,
// nothing to do or skipped, 1 for a soft failure, 2 for a critical failure.
func ExitCode(results []Result) int {
	code := 0
	for _, r := range results {
		switch r.Status {
		case StatusCritical:
			return 2
		case StatusSoft:
			code = 1
		case StatusOK, StatusNothing, StatusSkipped:
		}
	}
	return code
}
