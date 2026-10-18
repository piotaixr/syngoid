package zfs

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// maxTieBreak caps the per-listing creation tie-break counter.
const maxTieBreak = 999

// ListTree lists filesystems and volumes under root, root first when it
// exists. With recursive false only root itself is listed.
func (c *Client) ListTree(ctx context.Context, root string, recursive bool) ([]DatasetInfo, error) {
	args := []string{"list", "-Hp", "-o", "name,type,origin,used,receive_resume_token,syncoid:sync", "-t", "filesystem,volume"}
	if recursive {
		args = append(args, "-r")
	}
	out, err := c.Output(ctx, append(args, root)...)
	if err != nil {
		return nil, err
	}
	return parseTree(out)
}

// ListSnapshots lists snapshots of root (and its descendants when
// recursive), keyed by dataset name, in listing order. When the typed query
// fails for a reason other than a missing dataset it retries once with a
// plain "all" listing for ZFS versions lacking "-t".
func (c *Client) ListSnapshots(ctx context.Context, root string, recursive bool) (map[string][]Snapshot, error) {
	out, err := c.Output(ctx, getArgs("snapshot", recursive, root)...)
	if err != nil {
		if errors.Is(err, ErrNoDataset) || ctx.Err() != nil {
			return nil, err
		}
		out, err = c.Output(ctx, fallbackArgs(recursive, root)...)
		if err != nil {
			return nil, err
		}
		return parseSnapshots(out, "snapshot")
	}
	return parseSnapshots(out, "")
}

// ListBookmarks lists bookmarks of root (and its descendants when
// recursive), keyed by dataset name. A ZFS without bookmark support yields
// an empty map.
func (c *Client) ListBookmarks(ctx context.Context, root string, recursive bool) (map[string][]Bookmark, error) {
	out, err := c.Output(ctx, getArgs("bookmark", recursive, root)...)
	if err != nil {
		if errors.Is(err, ErrBookmarksUnsupported) {
			return map[string][]Bookmark{}, nil
		}
		return nil, err
	}
	snaps, err := parseGuidProps(out, '#', "")
	if err != nil {
		return nil, err
	}
	res := make(map[string][]Bookmark, len(snaps))
	for ds, list := range snaps {
		bms := make([]Bookmark, len(list))
		for i, s := range list {
			bms[i] = Bookmark(s)
		}
		res[ds] = bms
	}
	return res, nil
}

// GetProps returns the named properties of ds as reported by zfs get
// (unset values are "-").
func (c *Client) GetProps(ctx context.Context, ds string, props ...string) (map[string]string, error) {
	out, err := c.Output(ctx, "get", "-Hp", "-o", "property,value", strings.Join(props, ","), ds)
	if err != nil {
		return nil, err
	}
	return parsePropValues(out)
}

// LocalProps returns the properties of ds whose source is local.
func (c *Client) LocalProps(ctx context.Context, ds string) (map[string]string, error) {
	out, err := c.Output(ctx, "get", "-Hp", "-s", "local", "-o", "property,value", "all", ds)
	if err != nil {
		return nil, err
	}
	return parsePropValues(out)
}

// Exists reports whether dataset ds exists. Any error other than a missing
// dataset is returned.
func (c *Client) Exists(ctx context.Context, ds string) (bool, error) {
	if _, err := c.Output(ctx, "list", "-H", "-o", "name", ds); err != nil {
		if errors.Is(err, ErrNoDataset) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func depthArgs(recursive bool) []string {
	if recursive {
		return []string{"-r"}
	}
	return []string{"-d", "1"}
}

func getArgs(kind string, recursive bool, root string) []string {
	args := []string{"get", "-Hp", "-t", kind, "-o", "name,property,value", "guid,createtxg,creation"}
	args = append(args, depthArgs(recursive)...)
	return append(args, root)
}

func fallbackArgs(recursive bool, root string) []string {
	args := append([]string{"get", "-Hp"}, depthArgs(recursive)...)
	return append(args, "-o", "name,property,value", "all", root)
}

func parseTree(out string) ([]DatasetInfo, error) {
	var res []DatasetInfo
	for _, line := range splitLines(out) {
		f := strings.Split(line, "\t")
		if len(f) != 6 {
			return nil, fmt.Errorf("zfs list: expected 6 fields, got %d in line %q", len(f), line)
		}
		used, err := strconv.ParseInt(f[3], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("zfs list: bad used value in line %q: %w", line, err)
		}
		res = append(res, DatasetInfo{
			Name:        f[0],
			Type:        f[1],
			Origin:      dashEmpty(f[2]),
			Used:        used,
			ResumeToken: dashEmpty(f[4]),
			SyncProp:    dashEmpty(f[5]),
		})
	}
	return res, nil
}

func parseSnapshots(out, wantType string) (map[string][]Snapshot, error) {
	return parseGuidProps(out, '@', wantType)
}

// parseGuidProps groups "name<TAB>property<TAB>value" lines by the item
// name (dataset + sep + item) and builds items from their guid, createtxg
// and creation properties. With wantType empty every line must contain sep.
// Otherwise (a full "all" listing) lines without sep are skipped and only
// items whose "type" property equals wantType are kept.
func parseGuidProps(out string, sep byte, wantType string) (map[string][]Snapshot, error) {
	type item struct {
		ds, name string
		props    map[string]string
	}
	var order []*item
	byName := map[string]*item{}
	for _, line := range splitLines(out) {
		f := strings.SplitN(line, "\t", 3)
		if len(f) != 3 {
			return nil, fmt.Errorf("zfs get: expected 3 fields, got %d in line %q", len(f), line)
		}
		i := strings.IndexByte(f[0], sep)
		if i < 0 {
			if wantType != "" {
				continue
			}
			return nil, fmt.Errorf("zfs get: no %q in name of line %q", string(sep), line)
		}
		it := byName[f[0]]
		if it == nil {
			it = &item{ds: f[0][:i], name: f[0][i+1:], props: map[string]string{}}
			byName[f[0]] = it
			order = append(order, it)
		}
		it.props[f[1]] = f[2]
	}

	res := map[string][]Snapshot{}
	// Tie-break counters are per listing, keyed by the raw creation value.
	seen := map[uint64]int{}
	for _, it := range order {
		if wantType != "" && it.props["type"] != wantType {
			continue
		}
		full := it.ds + string(sep) + it.name
		guid, ok := it.props["guid"]
		if !ok {
			return nil, fmt.Errorf("zfs get: no guid for %q", full)
		}
		creation, err := strconv.ParseUint(it.props["creation"], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("zfs get: bad creation for %q: %w", full, err)
		}
		var txg uint64
		if v := it.props["createtxg"]; v != "" && v != "-" {
			if txg, err = strconv.ParseUint(v, 10, 64); err != nil {
				return nil, fmt.Errorf("zfs get: bad createtxg for %q: %w", full, err)
			}
		}
		n := min(seen[creation], maxTieBreak)
		seen[creation]++
		res[it.ds] = append(res[it.ds], Snapshot{
			Name:      it.name,
			GUID:      guid,
			CreateTxg: txg,
			Creation:  creation*1000 + uint64(n),
		})
	}
	return res, nil
}

func parsePropValues(out string) (map[string]string, error) {
	res := map[string]string{}
	for _, line := range splitLines(out) {
		f := strings.SplitN(line, "\t", 2)
		if len(f) != 2 {
			return nil, fmt.Errorf("zfs get: expected 2 fields, got %d in line %q", len(f), line)
		}
		res[f[0]] = f[1]
	}
	return res, nil
}

func splitLines(out string) []string {
	var lines []string
	for _, l := range strings.Split(out, "\n") {
		if l != "" {
			lines = append(lines, l)
		}
	}
	return lines
}

func dashEmpty(s string) string {
	if s == "-" {
		return ""
	}
	return s
}
