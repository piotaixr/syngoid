package fake

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/stream"
	"github.com/piotaixr/syngoid/internal/zfs"
)

// header is the whole fake stream: one JSON line followed by EOF.
type header struct {
	Dataset string `json:"dataset"`
	Type    string `json:"type"`
	// From names the incremental source (ds@snap or ds#bookmark) on the
	// sending side; FromGUID is its GUID. Both are empty for full streams.
	From     string `json:"from,omitempty"`
	FromGUID string `json:"from_guid,omitempty"`
	// Origin is the full origin snapshot name of a clone stream, OriginGUID
	// its GUID.
	Origin     string         `json:"origin,omitempty"`
	OriginGUID string         `json:"origin_guid,omitempty"`
	Snaps      []zfs.Snapshot `json:"snaps"`
	// ResumeOf is the resume token this stream continues.
	ResumeOf string `json:"resume_of,omitempty"`
	// SrcWorld identifies the sending World.
	SrcWorld int64 `json:"src_world"`
}

func (h header) isClone() bool       { return h.Origin != "" }
func (h header) isIncremental() bool { return h.FromGUID != "" }

// resumeKey identifies the partial state of one target World.
type resumeKey struct {
	world int64
	token string
}

// resumable maps an interrupted receive's token to the stream header, so the
// sending endpoint can re-emit the stream for a resume send.
var resumable = struct {
	sync.Mutex
	m map[resumeKey]header
}{m: map[resumeKey]header{}}

func registerResume(world int64, token string, h header) {
	h.ResumeOf = ""
	resumable.Lock()
	defer resumable.Unlock()
	resumable.m[resumeKey{world, token}] = h
}

func unregisterResume(world int64, token string) {
	resumable.Lock()
	defer resumable.Unlock()
	delete(resumable.m, resumeKey{world, token})
}

// lookupResume finds the header registered under token that was sent by
// World src (and, when ds is set, for dataset ds).
func lookupResume(src int64, token, ds string) (header, bool) {
	resumable.Lock()
	defer resumable.Unlock()
	for k, h := range resumable.m {
		if k.token == token && h.SrcWorld == src && (ds == "" || h.Dataset == ds) {
			return h, true
		}
	}
	return header{}, false
}

// buildHeader assembles the stream a send of s produces from w.
func (w *World) buildHeader(s zfs.SendSpec) (header, error) {
	if s.ResumeToken != "" {
		return w.resumeHeader(s)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	argv := zfs.SendArgs(s)
	d := w.ds[s.Dataset]
	if d == nil {
		return header{}, noDataset(argv, s.Dataset)
	}
	to := snapIndex(d.Snaps, s.To)
	if to < 0 {
		return header{}, noDataset(argv, s.Dataset+"@"+s.To)
	}
	h := header{Dataset: d.Name, Type: d.Type, SrcWorld: w.id, Snaps: []zfs.Snapshot{d.Snaps[to]}}
	notEarlier := cmdErr(argv, "cannot send '"+s.Dataset+"@"+s.To+"': not an earlier snapshot from the same fs")
	switch {
	case s.FromSnap != "":
		from := snapIndex(d.Snaps, s.FromSnap)
		if from < 0 {
			return header{}, noDataset(argv, s.Dataset+"@"+s.FromSnap)
		}
		if from >= to {
			return header{}, notEarlier
		}
		h.From, h.FromGUID = s.Dataset+"@"+s.FromSnap, d.Snaps[from].GUID
		if s.Intermediates {
			h.Snaps = append([]zfs.Snapshot(nil), d.Snaps[from+1:to+1]...)
		}
	case s.FromBookmark != "":
		b := bookmarkIndex(d.Bookmarks, s.FromBookmark)
		if b < 0 {
			return header{}, cmdErr(argv, "cannot open '"+s.Dataset+"#"+s.FromBookmark+"': bookmark does not exist")
		}
		if d.Bookmarks[b].CreateTxg >= d.Snaps[to].CreateTxg {
			return header{}, notEarlier
		}
		h.From, h.FromGUID = s.Dataset+"#"+s.FromBookmark, d.Bookmarks[b].GUID
	case s.FromOrigin != "":
		od, osnap, _ := strings.Cut(s.FromOrigin, "@")
		var o *Dataset
		if o = w.ds[od]; o == nil || snapIndex(o.Snaps, osnap) < 0 {
			return header{}, noDataset(argv, s.FromOrigin)
		}
		h.Origin, h.OriginGUID = s.FromOrigin, o.Snaps[snapIndex(o.Snaps, osnap)].GUID
	}
	return h, nil
}

// resumeHeader re-emits the stream of an interrupted receive, provided the
// snapshots it was built from still exist.
func (w *World) resumeHeader(s zfs.SendSpec) (header, error) {
	h, ok := lookupResume(w.id, s.ResumeToken, s.Dataset)
	argv := zfs.SendArgs(s)
	if !ok {
		return header{}, cmdErr(argv, "cannot resume send: invalid resume token '"+s.ResumeToken+"'")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	d := w.ds[h.Dataset]
	if d == nil {
		return header{}, noDataset(argv, h.Dataset)
	}
	gone := func(name string) error {
		return cmdErr(argv, "cannot resume send: '"+name+"' used in the initial send no longer exists")
	}
	if h.From != "" {
		found := false
		for _, sn := range d.Snaps {
			found = found || (h.From == d.Name+"@"+sn.Name && sn.GUID == h.FromGUID)
		}
		for _, b := range d.Bookmarks {
			found = found || (h.From == d.Name+"#"+b.Name && b.GUID == h.FromGUID)
		}
		if !found {
			return header{}, gone(h.From)
		}
	}
	for _, sn := range h.Snaps {
		if i := snapIndex(d.Snaps, sn.Name); i < 0 || d.Snaps[i].GUID != sn.GUID {
			return header{}, gone(d.Name + "@" + sn.Name)
		}
	}
	h.ResumeOf = s.ResumeToken
	return h, nil
}

// Send writes the fake stream of s from the World, compressed with o.Codec.
func (e *Endpoint) Send(ctx context.Context, s zfs.SendSpec, o endpoint.SendOpts, w io.Writer) error {
	if s.ResumeToken != "" {
		e.record("Send " + s.ResumeToken)
	} else {
		e.record("Send " + s.Dataset + "@" + s.To)
	}
	f := e.faults()
	if f.Send != nil {
		if err := f.Send(s); err != nil {
			return err
		}
	}
	if f.BlockSend {
		<-ctx.Done()
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	h, err := e.world.buildHeader(s)
	if err != nil {
		return err
	}
	line, err := json.Marshal(h)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	enc, err := stream.NewEncoder(o.Codec, w)
	if err != nil {
		return err
	}
	if _, err := enc.Write(line); err != nil {
		_ = enc.Close()
		return err
	}
	if o.Progress != nil {
		o.Progress(int64(len(line)))
	}
	return enc.Close()
}

// readStream decodes the fake stream from r and drains it.
func readStream(codec stream.Codec, r io.Reader) (header, error) {
	dec, err := stream.NewDecoder(codec, r)
	if err != nil {
		return header{}, fmt.Errorf("fake: read stream: %w", err)
	}
	defer func() { _ = dec.Close() }()
	var h header
	if err := json.NewDecoder(dec).Decode(&h); err != nil {
		return header{}, fmt.Errorf("fake: read stream header: %w", err)
	}
	// Hide dec's WriteTo: lz4's reports io.EOF as a failure.
	if _, err := io.Copy(io.Discard, struct{ io.Reader }{dec}); err != nil {
		return header{}, fmt.Errorf("fake: read stream: %w", err)
	}
	return h, nil
}

// hasFlag reports whether args contain the single-letter flag, alone or
// clustered (-sF).
func hasFlag(args []string, flag byte) bool {
	for _, a := range args {
		if len(a) < 2 || a[0] != '-' || a[1] == '-' {
			continue
		}
		if strings.IndexByte(a[1:], flag) >= 0 && !strings.ContainsAny(a[1:], "=0123456789") {
			return true
		}
	}
	return false
}

// recvPlan is a validated receive, ready to apply.
type recvPlan struct {
	target   *Dataset // nil when the dataset must be created
	origin   string   // origin of a created clone
	keep     int      // snapshots of target kept before appending; -1 keeps all
	newSnaps []zfs.Snapshot
	resume   bool
}

// planReceive checks whether h can be received into ds the way zfs would.
func (w *World) planReceive(argv []string, ds string, h header, force bool) (recvPlan, error) {
	t := w.ds[ds]
	p := recvPlan{target: t, keep: -1}
	if parent := parentOf(ds); parent != "" && w.ds[parent] == nil {
		return p, noDataset(argv, parent)
	}
	switch {
	case t != nil && t.ResumeToken != "":
		if h.ResumeOf != t.ResumeToken {
			return p, cmdErr(argv, "cannot receive incremental stream: destination "+ds+" contains partially-complete state from \"zfs receive -s\".")
		}
		p.resume = true
	case h.ResumeOf != "" && t == nil:
		return p, noDataset(argv, ds)
	case h.ResumeOf != "":
		return p, cmdErr(argv, "cannot receive resume stream: destination "+ds+" has no matching partially-complete state")
	}
	existsErr := cmdErr(argv, "cannot receive new filesystem stream: destination '"+ds+"' exists\nmust specify -F to overwrite it")
	var kept []zfs.Snapshot
	switch {
	case h.isIncremental():
		if t == nil {
			return p, noDataset(argv, ds)
		}
		kept = t.Snaps
		i := snapIndex2(kept, h.FromGUID)
		switch {
		case i >= 0 && i == len(kept)-1:
		case i >= 0 && force:
			p.keep = i + 1
			kept = kept[:i+1]
		default:
			return p, cmdErr(argv, "cannot receive incremental stream: most recent snapshot of "+ds+" does not match incremental source")
		}
	case h.isClone():
		if t != nil && !p.resume {
			return p, existsErr
		}
		if t == nil {
			origin, ok := w.findSnapshotByGUID(h.OriginGUID)
			if !ok {
				return p, cmdErr(argv, "cannot receive new filesystem stream: clone origin does not exist")
			}
			p.origin = origin
		}
	default:
		if t != nil && !p.resume {
			return p, existsErr
		}
	}
	for _, s := range h.Snaps {
		if i := snapIndex(kept, s.Name); i >= 0 {
			if kept[i].GUID != s.GUID {
				return p, cmdErr(argv, "cannot restore to "+ds+"@"+s.Name+": destination already exists")
			}
			continue
		}
		p.newSnaps = append(p.newSnaps, s)
	}
	return p, nil
}

// snapIndex2 returns the index of the snapshot with the given GUID, or -1.
func snapIndex2(snaps []zfs.Snapshot, guid string) int {
	for i, s := range snaps {
		if s.GUID == guid {
			return i
		}
	}
	return -1
}

// findSnapshotByGUID returns the full name of the snapshot with the GUID.
func (w *World) findSnapshotByGUID(guid string) (string, bool) {
	names := make([]string, 0, len(w.ds))
	for name := range w.ds {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		d := w.ds[name]
		if i := snapIndex2(d.Snaps, guid); i >= 0 {
			return d.Name + "@" + d.Snaps[i].Name, true
		}
	}
	return "", false
}

// Receive applies the fake stream read from r to the World.
func (e *Endpoint) Receive(ctx context.Context, s zfs.RecvSpec, codec stream.Codec, r io.Reader) error {
	e.record("Receive " + s.Dataset)
	h, err := readStream(codec, r)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	argv := append(append([]string{"receive"}, s.Args...), s.Dataset)
	w := e.world
	// The fault runs outside the World lock so it may use the World.
	ferr := error(nil)
	if f := e.faults(); f.Receive != nil {
		ferr = f.Receive(s)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if ferr != nil {
		if hasFlag(s.Args, 's') {
			if p, perr := w.planReceive(argv, s.Dataset, h, false); perr == nil {
				w.leaveResumeState(s.Dataset, h, p)
			}
		}
		return ferr
	}
	p, err := w.planReceive(argv, s.Dataset, h, hasFlag(s.Args, 'F'))
	if err != nil {
		return err
	}
	w.apply(s.Dataset, h, p)
	return nil
}

// leaveResumeState records an interrupted receive: the target keeps a resume
// token (and exists, if the stream was a full one) and nothing else changes.
func (w *World) leaveResumeState(ds string, h header, p recvPlan) {
	t := p.target
	if t == nil {
		t = w.createLocked(ds, h.Type, p.origin)
		t.partial = true
	} else if t.ResumeToken != "" {
		unregisterResume(w.id, t.ResumeToken)
	}
	w.tokens++
	t.ResumeToken = "tok-" + strconv.Itoa(w.tokens)
	registerResume(w.id, t.ResumeToken, h)
}

// apply commits a validated receive.
func (w *World) apply(ds string, h header, p recvPlan) {
	t := p.target
	if t == nil {
		t = w.createLocked(ds, h.Type, p.origin)
	}
	if p.keep >= 0 {
		kept := t.Snaps[:p.keep]
		t.dropSnaps(func(s zfs.Snapshot) bool { return snapIndex(kept, s.Name) < 0 })
	}
	t.Snaps = append(t.Snaps, p.newSnaps...)
	if p.resume {
		unregisterResume(w.id, t.ResumeToken)
		t.ResumeToken = ""
		t.partial = false
	}
}
