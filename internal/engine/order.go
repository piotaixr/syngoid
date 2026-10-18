package engine

import (
	"sort"

	"github.com/piotaixr/syngoid/internal/zfs"
)

// useTxg reports whether createtxg values are comparable: both known.
func useTxg(a, b uint64) bool { return a != 0 && b != 0 }

// SortSnapshots orders s oldest to newest: by CreateTxg when both snapshots
// have one, otherwise by Creation. Ties keep input order.
func SortSnapshots(s []zfs.Snapshot) {
	sort.SliceStable(s, func(i, j int) bool {
		a, b := s[i], s[j]
		if useTxg(a.CreateTxg, b.CreateTxg) {
			return a.CreateTxg < b.CreateTxg
		}
		return a.Creation < b.Creation
	})
}

// Matching returns the name of the newest source snapshot that also exists
// on the target with the same GUID. src must be sorted oldest to newest.
func Matching(src, tgt []zfs.Snapshot) (string, bool) {
	byName := make(map[string]string, len(tgt))
	for _, t := range tgt {
		byName[t.Name] = t.GUID
	}
	for i := len(src) - 1; i >= 0; i-- {
		if guid, ok := byName[src[i].Name]; ok && guid == src[i].GUID {
			return src[i].Name, true
		}
	}
	return "", false
}

// BookmarkFallback returns the newest target snapshot (by the target's own
// order; tgt must be sorted oldest to newest) whose GUID equals the GUID of
// any source bookmark, together with that bookmark.
func BookmarkFallback(tgt []zfs.Snapshot, bms []zfs.Bookmark) (bm zfs.Bookmark, targetSnap string, ok bool) {
	byGUID := make(map[string]zfs.Bookmark, len(bms))
	for _, b := range bms {
		if _, dup := byGUID[b.GUID]; !dup {
			byGUID[b.GUID] = b
		}
	}
	for i := len(tgt) - 1; i >= 0; i-- {
		if b, found := byGUID[tgt[i].GUID]; found {
			return b, tgt[i].Name, true
		}
	}
	return zfs.Bookmark{}, "", false
}

// NextAfterBookmark returns the oldest source snapshot created at or after
// bm. src must be sorted oldest to newest.
func NextAfterBookmark(src []zfs.Snapshot, bm zfs.Bookmark) (string, bool) {
	for _, s := range src {
		if useTxg(s.CreateTxg, bm.CreateTxg) {
			if s.CreateTxg >= bm.CreateTxg {
				return s.Name, true
			}
		} else if s.Creation >= bm.Creation {
			return s.Name, true
		}
	}
	return "", false
}
