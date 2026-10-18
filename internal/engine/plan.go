package engine

import (
	"strings"

	"github.com/piotaixr/syngoid/internal/cli"
	"github.com/piotaixr/syngoid/internal/naming"
	"github.com/piotaixr/syngoid/internal/zfs"
)

// smallTargetUsed is the target size under which the "no matching snapshot"
// error hints that the target was created by hand.
const smallTargetUsed = 64 * 1024 * 1024

// PlanOptions are the options that influence planning.
type PlanOptions struct {
	NoStream, NoSyncSnap, KeepSyncSnap, CreateBookmark, UseHold bool
	DeleteTargetSnapshots, ForceDelete, NoResume, FiltersActive bool
	Hostname, Identifier                                        string
}

// PlanOptionsFrom extracts the planning options from o for the local
// hostname.
func PlanOptionsFrom(o *cli.Options, hostname string) PlanOptions {
	return PlanOptions{
		NoStream:              o.NoStream,
		NoSyncSnap:            o.NoSyncSnap,
		KeepSyncSnap:          o.KeepSyncSnap,
		CreateBookmark:        o.CreateBookmark,
		UseHold:               o.UseHold,
		DeleteTargetSnapshots: o.DeleteTargetSnapshots,
		ForceDelete:           o.ForceDelete,
		NoResume:              o.NoResume,
		FiltersActive:         len(o.IncludeSnaps)+len(o.ExcludeSnaps) > 0,
		Hostname:              hostname,
		Identifier:            o.Identifier,
	}
}

// Plan is the planned replication of one dataset pair. Status is StatusOK
// when Actions is non-empty; otherwise it is the terminal status explained
// by Message.
type Plan struct {
	Source, Target string
	Actions        []Action
	Status         Status
	Message        string
}

// PlanDataset plans the replication of one dataset pair, mirroring
// syncoid's per-dataset logic. s.SourceSnaps and s.TargetSnaps are sorted
// oldest to newest; s.NewSyncSnap, when set, is the not-yet-created sync
// snapshot and is treated as the newest source snapshot.
func PlanDataset(s DatasetState, o PlanOptions) Plan {
	p := Plan{Source: s.Source, Target: s.Target}
	stop := func(st Status, msg string) Plan {
		p.Status, p.Message = st, msg
		return p
	}

	if msg, ok := syncAllowed(s.SyncProp, o.Hostname, s.Source); !ok {
		return stop(StatusSkipped, msg)
	}

	if s.ResumeToken != "" && !o.NoResume {
		p.Actions = []Action{Resume{Token: s.ResumeToken}}
		return p
	}

	src := make([]string, 0, len(s.SourceSnaps)+1)
	for _, sn := range s.SourceSnaps {
		src = append(src, sn.Name)
	}
	if s.NewSyncSnap != "" {
		src = append(src, s.NewSyncSnap)
	}
	if len(src) == 0 {
		return stop(StatusSoft, "no snapshots exist on source "+s.Source+", and you asked for --no-sync-snap.")
	}
	newest := src[len(src)-1]

	inc := func(from, to string) []Action {
		if o.NoStream || !o.FiltersActive {
			return []Action{SendIncremental{From: from, To: to, Intermediates: !o.NoStream}}
		}
		chain := chainBetween(src, from, to)
		if len(chain) < 2 {
			return []Action{SendIncremental{From: from, To: to}}
		}
		out := make([]Action, 0, len(chain)-1)
		for i := 1; i < len(chain); i++ {
			out = append(out, SendIncremental{From: chain[i-1], To: chain[i]})
		}
		return out
	}

	var (
		sends []Action
		match string
	)
	if !s.TargetExists {
		base := src[0]
		if o.NoStream {
			base = newest
		}
		if s.Origin != "" && s.OriginInScope {
			sends = append(sends, SendClone{Origin: s.Origin, To: base})
		} else {
			sends = append(sends, SendFull{Snap: base})
		}
		if !o.NoStream && base != newest {
			sends = append(sends, inc(base, newest)...)
		}
	} else if m, ok := Matching(s.SourceSnaps, s.TargetSnaps); ok {
		if m == newest {
			return stop(StatusNothing, "no snapshots on source newer than "+m+" on target. Nothing to do, not syncing.")
		}
		match = m
		sends = inc(match, newest)
	} else {
		bm, tsnap, ok := BookmarkFallback(s.TargetSnaps, s.SourceBookmarks)
		if !ok {
			if o.ForceDelete && !s.TargetIsPoolRoot {
				p.Actions = []Action{ForceDestroyTarget{}}
				return p
			}
			return stop(StatusCritical, noMatchMessage(s))
		}
		match = tsnap
		sends = fromBookmark(s.SourceSnaps, bm, newest, o.NoStream, inc)
	}

	p.Actions = append(sends, postActions(s, o, match, newest)...)
	return p
}

// syncAllowed applies the syncoid:sync property rule and returns the skip
// message when the dataset must not be synced.
func syncAllowed(prop, host, source string) (string, bool) {
	switch prop {
	case "", "-", "true":
		return "", true
	case "false":
		return "Skipping dataset (syncoid:sync=false): " + source + "...", false
	}
	for _, h := range strings.Split(prop, ",") {
		if strings.TrimSpace(h) == host {
			return "", true
		}
	}
	return "Skipping dataset (syncoid:sync doesn't include " + host + "): " + source + "...", false
}

// chainBetween returns the names in list from from to to, inclusive, or nil
// when from or to is missing or out of order.
func chainBetween(list []string, from, to string) []string {
	start, end := -1, -1
	for i, n := range list {
		if n == from && start < 0 {
			start = i
		}
		if n == to {
			end = i
		}
	}
	if start < 0 || end < start {
		return nil
	}
	return list[start : end+1]
}

// fromBookmark plans the sends from source bookmark bm to newest.
func fromBookmark(src []zfs.Snapshot, bm zfs.Bookmark, newest string, noStream bool, inc func(from, to string) []Action) []Action {
	if noStream {
		return []Action{SendFromBookmark{Bookmark: bm.Name, To: newest}}
	}
	next, ok := NextAfterBookmark(src, bm)
	if !ok || next == newest {
		return []Action{SendFromBookmark{Bookmark: bm.Name, To: newest}}
	}
	return append([]Action{SendFromBookmark{Bookmark: bm.Name, To: next}}, inc(next, newest)...)
}

// noMatchMessage is the critical error for an existing target that shares
// no snapshot with the source.
func noMatchMessage(s DatasetState) string {
	msg := "Target " + s.Target + " exists but has no snapshots matching with " + s.Source + "!\n" +
		"Replication to target would require destroying existing\n" +
		"target. Cowardly refusing to destroy your existing target."
	if s.TargetUsed < smallTargetUsed {
		msg += "\n\nNOTE: Target " + s.Target + " dataset is < 64MB used - did you mistakenly run\n" +
			"`zfs create " + s.Target + "` on the target? ZFS initial\n" +
			"replication must be to a NON EXISTENT DATASET, which will\n" +
			"then be CREATED BY the initial replication process."
	}
	return msg
}

// postActions returns the actions that follow the sends: holds, bookmark
// creation, sync snapshot pruning and target snapshot deletion. match is
// the previous common snapshot, empty for a first sync.
func postActions(s DatasetState, o PlanOptions, match, newest string) []Action {
	var out []Action
	if o.UseHold {
		for _, side := range []Side{Source, Target} {
			out = append(out, Hold{Side: side, Snap: newest})
			if match != "" {
				out = append(out, Release{Side: side, Snap: match})
			}
		}
	}
	if o.NoSyncSnap && o.CreateBookmark {
		out = append(out, CreateBookmark{Snap: newest})
	}
	if s.NewSyncSnap != "" && !o.KeepSyncSnap {
		pat := naming.SyncSnapPattern(o.Identifier, o.Hostname)
		for _, sd := range []struct {
			side  Side
			snaps []zfs.Snapshot
		}{{Source, s.SourceSnaps}, {Target, s.TargetSnaps}} {
			var names []string
			for _, sn := range sd.snaps {
				if sn.Name != s.NewSyncSnap && pat.MatchString(sn.Name) {
					names = append(names, sn.Name)
				}
			}
			if len(names) > 0 {
				out = append(out, PruneSyncSnaps{Side: sd.side, Snaps: names})
			}
		}
	}
	if o.DeleteTargetSnapshots {
		out = append(out, DestroyTargetSnaps{})
	}
	return out
}
