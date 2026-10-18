// Package fake is an in-memory model of ZFS hosts for tests. A World is one
// host; an Endpoint exposes a World through endpoint.Endpoint. Sending from
// one endpoint into another's Receive replicates between two Worlds with
// ZFS's send/receive semantics, failing with realistic zfs stderr.
package fake

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/zfs"
)

const (
	filesystem = "filesystem"
	// mib is the size attributed to each snapshot in estimates and written@.
	mib = 1 << 20
	// guidBase offsets generated GUIDs so they look like real ones.
	guidBase = 4_000_000_000_000_000_000
	// firstTxg and firstSecs seed the per-World txg and clock counters.
	firstTxg  = 100
	firstSecs = 1_700_000_000
)

var (
	worldSeq atomic.Int64
	// guidSeq is shared by all Worlds so a GUID never names two different
	// snapshots, the way real GUIDs are globally unique.
	guidSeq atomic.Uint64
)

// Dataset is one filesystem or volume of a World. Fields are exported so
// tests can set up and inspect state directly; do that only while no
// Endpoint call is running.
type Dataset struct {
	Name, Type, Origin string
	Snaps              []zfs.Snapshot
	Bookmarks          []zfs.Bookmark
	// Holds maps a snapshot short name to the hold tags on it.
	Holds       map[string][]string
	ResumeToken string
	Used        int64
	// Props holds the local properties, syncoid:sync included.
	Props map[string]string

	// partial marks a dataset created by an interrupted full receive.
	partial bool
}

// World is the ZFS state of one host. It is safe for concurrent use.
type World struct {
	mu     sync.Mutex
	id     int64
	ds     map[string]*Dataset
	txg    uint64
	secs   uint64
	tokens int
}

// NewWorld returns an empty World.
func NewWorld() *World {
	return &World{id: worldSeq.Add(1), ds: map[string]*Dataset{}, txg: firstTxg, secs: firstSecs}
}

// Create adds an empty dataset of the given type ("" means filesystem). It
// does nothing when the dataset exists. Parents are not created implicitly.
func (w *World) Create(name, typ string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.createLocked(name, typ, "")
}

func (w *World) createLocked(name, typ, origin string) *Dataset {
	if d, ok := w.ds[name]; ok {
		return d
	}
	if typ == "" {
		typ = filesystem
	}
	d := &Dataset{Name: name, Type: typ, Origin: origin, Holds: map[string][]string{}, Props: map[string]string{}}
	w.ds[name] = d
	return d
}

// Snap creates snapshot name of ds with a unique GUID and increasing txg and
// creation time. It panics when ds does not exist.
func (w *World) Snap(ds, name string) zfs.Snapshot {
	w.mu.Lock()
	defer w.mu.Unlock()
	d := w.ds[ds]
	if d == nil {
		panic("fake: Snap: no such dataset " + ds)
	}
	return w.snapLocked(d, name)
}

func (w *World) snapLocked(d *Dataset, name string) zfs.Snapshot {
	w.txg++
	w.secs++
	s := zfs.Snapshot{
		Name:      name,
		GUID:      strconv.FormatUint(guidBase+guidSeq.Add(1), 10),
		CreateTxg: w.txg,
		Creation:  w.secs * 1000,
	}
	d.Snaps = append(d.Snaps, s)
	return s
}

// MarkBookmark creates bookmark bm of snapshot snap of ds. It panics when
// either does not exist.
func (w *World) MarkBookmark(ds, snap, bm string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	d := w.ds[ds]
	if d == nil {
		panic("fake: MarkBookmark: no such dataset " + ds)
	}
	i := snapIndex(d.Snaps, snap)
	if i < 0 {
		panic("fake: MarkBookmark: no such snapshot " + ds + "@" + snap)
	}
	d.Bookmarks = append(d.Bookmarks, zfs.Bookmark(d.Snaps[i]))
	d.Bookmarks[len(d.Bookmarks)-1].Name = bm
}

// SetProp sets a local property of ds. It panics when ds does not exist.
func (w *World) SetProp(ds, key, value string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	d := w.ds[ds]
	if d == nil {
		panic("fake: SetProp: no such dataset " + ds)
	}
	if d.Props == nil {
		d.Props = map[string]string{}
	}
	d.Props[key] = value
}

// Dataset returns the live dataset name, or nil when it does not exist.
func (w *World) Dataset(name string) *Dataset {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.ds[name]
}

func snapIndex(snaps []zfs.Snapshot, name string) int {
	for i, s := range snaps {
		if s.Name == name {
			return i
		}
	}
	return -1
}

func parentOf(ds string) string {
	i := strings.LastIndexByte(ds, '/')
	if i < 0 {
		return ""
	}
	return ds[:i]
}

// cmdErr builds the error zfs argv would fail with.
func cmdErr(argv []string, stderr string) *zfs.CommandError {
	return &zfs.CommandError{Argv: append([]string{"zfs"}, argv...), ExitCode: 1, Stderr: stderr, Kind: zfs.Classify(stderr)}
}

func noDataset(argv []string, name string) *zfs.CommandError {
	return cmdErr(argv, "cannot open '"+name+"': dataset does not exist")
}

// Faults injects failures into an Endpoint. Set it before using the Endpoint.
type Faults struct {
	// Receive and Send are called first with the spec; a non-nil result is
	// the operation's result. A failed Receive with -s in its args leaves a
	// resume token on the target.
	Receive func(s zfs.RecvSpec) error
	Send    func(s zfs.SendSpec) error
	// Busy is the answer of ReceiveBusy per dataset.
	Busy map[string]bool
	// Allow is the output of Allow per dataset.
	Allow map[string]string
	// BlockSend makes Send block until its context is done.
	BlockSend bool
}

// Endpoint is an endpoint.Endpoint over a World.
type Endpoint struct {
	// LabelValue is returned by Label.
	LabelValue string
	// Faults is read on every call; set it before concurrent use.
	Faults Faults

	world *World
	info  endpoint.HostInfo

	mu    sync.Mutex
	calls []string
}

var _ endpoint.Endpoint = (*Endpoint)(nil)

// New returns an Endpoint over w reporting info from Info.
func New(w *World, info endpoint.HostInfo) *Endpoint {
	return &Endpoint{world: w, info: info}
}

// Calls returns a copy of the call log: one "<Method> <main argument>" entry
// per mutating or transferring call (Snapshot, Destroy, DestroyDataset, Hold,
// Release, Bookmark, AbortResume, Send, Receive), failed ones included. Query
// methods (ListTree, GetProps, Exists, SendEstimate, ...) are not logged, so
// tests can assert the exact sequence of changes.
func (e *Endpoint) Calls() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.calls...)
}

func (e *Endpoint) record(entry string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls = append(e.calls, entry)
}

func (e *Endpoint) faults() Faults {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.Faults
}

// Label returns LabelValue.
func (e *Endpoint) Label() string { return e.LabelValue }

// Info returns the HostInfo given to New.
func (e *Endpoint) Info(context.Context) (endpoint.HostInfo, error) { return e.info, nil }

// Allow returns Faults.Allow[ds].
func (e *Endpoint) Allow(_ context.Context, ds string) (string, error) {
	return e.faults().Allow[ds], nil
}

// ReceiveBusy reports Faults.Busy for each dataset.
func (e *Endpoint) ReceiveBusy(_ context.Context, datasets []string) (map[string]bool, error) {
	busy := e.faults().Busy
	out := make(map[string]bool, len(datasets))
	for _, ds := range datasets {
		out[ds] = busy[ds]
	}
	return out, nil
}

// within reports whether name is root, or a descendant of it when recursive.
func within(root, name string, recursive bool) bool {
	return name == root || (recursive && strings.HasPrefix(name, root+"/"))
}

// treeLocked returns the datasets within root sorted by name, parents first.
func (w *World) treeLocked(root string, recursive bool) []*Dataset {
	var out []*Dataset
	for name, d := range w.ds {
		if within(root, name, recursive) {
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// ListTree lists the datasets under root, root first.
func (e *Endpoint) ListTree(_ context.Context, root string, recursive bool) ([]zfs.DatasetInfo, error) {
	w := e.world
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.ds[root] == nil {
		return nil, noDataset([]string{"list", "-Hp", root}, root)
	}
	var out []zfs.DatasetInfo
	for _, d := range w.treeLocked(root, recursive) {
		out = append(out, zfs.DatasetInfo{
			Name: d.Name, Type: d.Type, Origin: d.Origin, Used: d.Used,
			ResumeToken: d.ResumeToken, SyncProp: d.Props["syncoid:sync"],
		})
	}
	return out, nil
}

// ListSnapshots lists snapshots keyed by dataset, omitting datasets without any.
func (e *Endpoint) ListSnapshots(_ context.Context, root string, recursive bool) (map[string][]zfs.Snapshot, error) {
	w := e.world
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.ds[root] == nil {
		return nil, noDataset([]string{"get", "-Hp", "-t", "snapshot", root}, root)
	}
	out := map[string][]zfs.Snapshot{}
	for _, d := range w.treeLocked(root, recursive) {
		if len(d.Snaps) > 0 {
			out[d.Name] = append([]zfs.Snapshot(nil), d.Snaps...)
		}
	}
	return out, nil
}

// ListBookmarks lists bookmarks keyed by dataset, omitting datasets without any.
func (e *Endpoint) ListBookmarks(_ context.Context, root string, recursive bool) (map[string][]zfs.Bookmark, error) {
	w := e.world
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.ds[root] == nil {
		return nil, noDataset([]string{"get", "-Hp", "-t", "bookmark", root}, root)
	}
	out := map[string][]zfs.Bookmark{}
	for _, d := range w.treeLocked(root, recursive) {
		if len(d.Bookmarks) > 0 {
			out[d.Name] = append([]zfs.Bookmark(nil), d.Bookmarks...)
		}
	}
	return out, nil
}

// GetProps returns the named properties; unset ones are "-".
func (e *Endpoint) GetProps(_ context.Context, ds string, props ...string) (map[string]string, error) {
	w := e.world
	w.mu.Lock()
	defer w.mu.Unlock()
	argv := []string{"get", "-Hp", "-o", "property,value", strings.Join(props, ","), ds}
	d := w.ds[ds]
	if d == nil {
		return nil, noDataset(argv, ds)
	}
	out := make(map[string]string, len(props))
	for _, p := range props {
		switch {
		case p == "name":
			out[p] = d.Name
		case p == "type":
			out[p] = d.Type
		case p == "used" || p == "referenced":
			out[p] = strconv.FormatInt(d.Used, 10)
		case p == "recordsize" && d.Props[p] == "":
			out[p] = "131072"
		case p == "receive_resume_token":
			out[p] = dash(d.ResumeToken)
		case strings.HasPrefix(p, "written@"):
			snap := strings.TrimPrefix(p, "written@")
			i := snapIndex(d.Snaps, snap)
			if i < 0 {
				return nil, noDataset(argv, ds+"@"+snap)
			}
			out[p] = strconv.Itoa((len(d.Snaps) - 1 - i) * mib)
		default:
			out[p] = dash(d.Props[p])
		}
	}
	return out, nil
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// LocalProps returns a copy of the local properties of ds.
func (e *Endpoint) LocalProps(_ context.Context, ds string) (map[string]string, error) {
	w := e.world
	w.mu.Lock()
	defer w.mu.Unlock()
	d := w.ds[ds]
	if d == nil {
		return nil, noDataset([]string{"get", "-Hp", "-s", "local", "-o", "property,value", "all", ds}, ds)
	}
	out := make(map[string]string, len(d.Props))
	for k, v := range d.Props {
		out[k] = v
	}
	return out, nil
}

// Exists reports whether ds exists.
func (e *Endpoint) Exists(_ context.Context, ds string) (bool, error) {
	w := e.world
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.ds[ds] != nil, nil
}

// Snapshot creates all the named "ds@snap" snapshots, or none when any
// dataset is missing or any snapshot already exists.
func (e *Endpoint) Snapshot(_ context.Context, names []string) error {
	if len(names) == 0 {
		return nil
	}
	e.record("Snapshot " + strings.Join(names, ","))
	argv := append([]string{"snapshot"}, names...)
	w := e.world
	w.mu.Lock()
	defer w.mu.Unlock()
	type item struct {
		d    *Dataset
		snap string
	}
	var items []item
	seen := map[string]bool{}
	for _, n := range names {
		ds, snap, ok := strings.Cut(n, "@")
		if !ok {
			return cmdErr(argv, "cannot create snapshot '"+n+"': missing '@' delimiter in snapshot name")
		}
		d := w.ds[ds]
		if d == nil {
			return noDataset(argv, ds)
		}
		if seen[n] || snapIndex(d.Snaps, snap) >= 0 {
			return cmdErr(argv, "cannot create snapshot '"+n+"': dataset already exists")
		}
		seen[n] = true
		items = append(items, item{d, snap})
	}
	for _, it := range items {
		w.snapLocked(it.d, it.snap)
	}
	return nil
}

// Destroy destroys snaps (short names) of ds, all or none.
func (e *Endpoint) Destroy(_ context.Context, ds string, snaps []string) error {
	if len(snaps) == 0 {
		return nil
	}
	e.record("Destroy " + ds + "@" + strings.Join(snaps, ","))
	argv := []string{"destroy", ds + "@" + strings.Join(snaps, ",")}
	w := e.world
	w.mu.Lock()
	defer w.mu.Unlock()
	d := w.ds[ds]
	if d == nil {
		return noDataset(argv, ds)
	}
	drop := map[string]bool{}
	for _, s := range snaps {
		if snapIndex(d.Snaps, s) < 0 {
			return noDataset(argv, ds+"@"+s)
		}
		if len(d.Holds[s]) > 0 {
			return cmdErr(argv, "cannot destroy snapshot "+ds+"@"+s+": dataset is busy")
		}
		drop[s] = true
	}
	d.dropSnaps(func(s zfs.Snapshot) bool { return drop[s.Name] })
	return nil
}

// dropSnaps removes the snapshots (and their holds) for which drop is true.
func (d *Dataset) dropSnaps(drop func(zfs.Snapshot) bool) {
	var keep []zfs.Snapshot
	for _, s := range d.Snaps {
		if drop(s) {
			delete(d.Holds, s.Name)
			continue
		}
		keep = append(keep, s)
	}
	d.Snaps = keep
}

// DestroyDataset destroys ds, and its descendants and snapshots when recursive.
func (e *Endpoint) DestroyDataset(_ context.Context, ds string, recursive bool) error {
	argv := []string{"destroy", ds}
	if recursive {
		e.record("DestroyDataset -r " + ds)
		argv = []string{"destroy", "-r", ds}
	} else {
		e.record("DestroyDataset " + ds)
	}
	w := e.world
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.ds[ds] == nil {
		return noDataset(argv, ds)
	}
	tree := w.treeLocked(ds, true)
	if !recursive {
		var deps []string
		for _, d := range tree {
			if d.Name != ds {
				deps = append(deps, d.Name)
			}
			for _, s := range d.Snaps {
				deps = append(deps, d.Name+"@"+s.Name)
			}
		}
		if len(deps) > 0 {
			return cmdErr(argv, "cannot destroy '"+ds+"': filesystem has children\nuse '-r' to destroy the following datasets:\n"+strings.Join(deps, "\n"))
		}
	}
	for _, d := range tree {
		for _, s := range d.Snaps {
			if len(d.Holds[s.Name]) > 0 {
				return cmdErr(argv, "cannot destroy snapshot "+d.Name+"@"+s.Name+": dataset is busy")
			}
		}
	}
	for _, d := range tree {
		if d.ResumeToken != "" {
			unregisterResume(w.id, d.ResumeToken)
		}
		delete(w.ds, d.Name)
	}
	return nil
}

// Hold places hold tag on the full snapshot name.
func (e *Endpoint) Hold(_ context.Context, tag, snapshot string) error {
	e.record("Hold " + tag + " " + snapshot)
	argv := []string{"hold", tag, snapshot}
	w := e.world
	w.mu.Lock()
	defer w.mu.Unlock()
	d, snap, err := w.snapLookupLocked(argv, snapshot)
	if err != nil {
		return err
	}
	for _, t := range d.Holds[snap] {
		if t == tag {
			return cmdErr(argv, "cannot hold snapshot '"+snapshot+"': tag already exists on this dataset")
		}
	}
	d.Holds[snap] = append(d.Holds[snap], tag)
	return nil
}

// Release removes hold tag from the full snapshot name.
func (e *Endpoint) Release(_ context.Context, tag, snapshot string) error {
	e.record("Release " + tag + " " + snapshot)
	argv := []string{"release", tag, snapshot}
	w := e.world
	w.mu.Lock()
	defer w.mu.Unlock()
	d, snap, err := w.snapLookupLocked(argv, snapshot)
	if err != nil {
		return err
	}
	tags := d.Holds[snap]
	for i, t := range tags {
		if t == tag {
			if len(tags) == 1 {
				delete(d.Holds, snap)
			} else {
				d.Holds[snap] = append(tags[:i:i], tags[i+1:]...)
			}
			return nil
		}
	}
	return cmdErr(argv, "cannot release hold from snapshot '"+snapshot+"': no such tag on this dataset")
}

// snapLookupLocked resolves a full snapshot name to its dataset and short name.
func (w *World) snapLookupLocked(argv []string, full string) (*Dataset, string, error) {
	ds, snap, _ := strings.Cut(full, "@")
	d := w.ds[ds]
	if d == nil {
		return nil, "", noDataset(argv, ds)
	}
	if snapIndex(d.Snaps, snap) < 0 {
		return nil, "", noDataset(argv, full)
	}
	if d.Holds == nil {
		d.Holds = map[string][]string{}
	}
	return d, snap, nil
}

// Bookmark creates bookmark (ds#name) from snapshot (ds@name).
func (e *Endpoint) Bookmark(_ context.Context, snapshot, bookmark string) error {
	e.record("Bookmark " + snapshot + " " + bookmark)
	argv := []string{"bookmark", snapshot, bookmark}
	w := e.world
	w.mu.Lock()
	defer w.mu.Unlock()
	d, snap, err := w.snapLookupLocked(argv, snapshot)
	if err != nil {
		return err
	}
	bmDS, bm, _ := strings.Cut(bookmark, "#")
	if bmDS != d.Name {
		return cmdErr(argv, "cannot create bookmark '"+bookmark+"': source is not an earlier snapshot from the same fs")
	}
	if bookmarkIndex(d.Bookmarks, bm) >= 0 {
		return cmdErr(argv, "cannot create bookmark '"+bookmark+"': bookmark exists")
	}
	b := zfs.Bookmark(d.Snaps[snapIndex(d.Snaps, snap)])
	b.Name = bm
	d.Bookmarks = append(d.Bookmarks, b)
	return nil
}

func bookmarkIndex(bms []zfs.Bookmark, name string) int {
	for i, b := range bms {
		if b.Name == name {
			return i
		}
	}
	return -1
}

// AbortResume discards the resume state of ds, removing the dataset when it
// only exists because of the interrupted receive.
func (e *Endpoint) AbortResume(_ context.Context, ds string) error {
	e.record("AbortResume " + ds)
	argv := []string{"receive", "-A", ds}
	w := e.world
	w.mu.Lock()
	defer w.mu.Unlock()
	d := w.ds[ds]
	if d == nil {
		return noDataset(argv, ds)
	}
	if d.ResumeToken == "" {
		return cmdErr(argv, "cannot abort receive: '"+ds+"' does not have any resumable receive state to abort")
	}
	unregisterResume(w.id, d.ResumeToken)
	if d.partial {
		delete(w.ds, ds)
		return nil
	}
	d.ResumeToken = ""
	return nil
}

// SendEstimate returns 1 MiB per snapshot the stream carries (at least 4096),
// or 0 when the stream cannot be built.
func (e *Endpoint) SendEstimate(_ context.Context, s zfs.SendSpec) (int64, error) {
	h, err := e.world.buildHeader(s)
	if err != nil {
		return 0, nil
	}
	return max(int64(len(h.Snaps))*mib, 4096), nil
}
