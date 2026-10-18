package zfs

// DatasetInfo describes one filesystem or volume as listed by ListTree.
// Origin, ResumeToken and SyncProp are empty when ZFS reports "-".
type DatasetInfo struct {
	Name        string
	Type        string
	Origin      string
	Used        int64
	ResumeToken string
	SyncProp    string
}

// Snapshot describes one snapshot. Name is the part after "@". GUID is the
// decimal string reported by ZFS. CreateTxg is 0 when unknown. Creation is
// the creation time in seconds times 1000 plus a per-listing tie-break
// counter (0..999) for snapshots sharing the same creation time.
type Snapshot struct {
	Name      string
	GUID      string
	CreateTxg uint64
	Creation  uint64
}

// Bookmark describes one bookmark. Name is the part after "#". Fields have
// the same meaning as in Snapshot.
type Bookmark struct {
	Name      string
	GUID      string
	CreateTxg uint64
	Creation  uint64
}
