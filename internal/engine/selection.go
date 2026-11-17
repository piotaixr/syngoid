package engine

import (
	"time"

	"github.com/piotaixr/syngoid/internal/cli"
	"github.com/piotaixr/syngoid/internal/zfs"
)

// Selection says which snapshot of each dataset a restore takes: the one
// named Name, else the newest created at or before At, else the newest.
type Selection struct {
	At   *time.Time
	Name string
}

// SelectionFrom builds the Selection for the restore options r (nil selects
// the newest snapshot).
func SelectionFrom(r *cli.RestoreOptions) Selection {
	if r == nil {
		return Selection{}
	}
	return Selection{At: r.At, Name: r.Snapshot}
}

// Pick returns the selected snapshot of snaps (sorted oldest to newest).
func (sel Selection) Pick(snaps []zfs.Snapshot) (zfs.Snapshot, bool) {
	switch {
	case sel.Name != "":
		for _, s := range snaps {
			if s.Name == sel.Name {
				return s, true
			}
		}
	case sel.At != nil:
		for i := len(snaps) - 1; i >= 0; i-- {
			if int64(snaps[i].Creation) <= sel.At.Unix() {
				return snaps[i], true
			}
		}
	case len(snaps) > 0:
		return snaps[len(snaps)-1], true
	}
	return zfs.Snapshot{}, false
}

// Missing is the soft-failure message for a dataset where Pick found nothing.
func (sel Selection) Missing(source string, snaps []zfs.Snapshot) string {
	switch {
	case len(snaps) == 0:
		return "no snapshots on " + source
	case sel.Name != "":
		return "no snapshot " + sel.Name + " on " + source
	case sel.At != nil:
		return "no snapshot of " + source + " at or before " + sel.At.Format(time.RFC3339)
	}
	return "no snapshots on " + source
}
