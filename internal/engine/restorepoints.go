package engine

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/piotaixr/syngoid/internal/zfs"
)

// RestorePoint is one snapshot name shared by Have datasets.
type RestorePoint struct {
	Name    string
	Created uint64   // latest creation among the snapshots of that name, seconds
	Have    int      // datasets that have a snapshot of that name
	Missing []string // datasets in scope without it, in listing order
}

// RestorePoints groups snaps by name: newest Created first, then by Name.
func RestorePoints(datasets []string, snaps map[string][]zfs.Snapshot) []RestorePoint {
	type group struct {
		created uint64
		has     map[string]bool
	}
	groups := map[string]*group{}
	for _, ds := range datasets {
		for _, s := range snaps[ds] {
			g := groups[s.Name]
			if g == nil {
				g = &group{has: map[string]bool{}}
				groups[s.Name] = g
			}
			g.has[ds] = true
			// Creation is seconds*1000 plus a tie-break counter.
			g.created = max(g.created, s.Creation/1000)
		}
	}
	pts := make([]RestorePoint, 0, len(groups))
	for name, g := range groups {
		p := RestorePoint{Name: name, Created: g.created, Have: len(g.has)}
		for _, ds := range datasets {
			if !g.has[ds] {
				p.Missing = append(p.Missing, ds)
			}
		}
		pts = append(pts, p)
	}
	sort.Slice(pts, func(i, j int) bool {
		if pts[i].Created != pts[j].Created {
			return pts[i].Created > pts[j].Created
		}
		return pts[i].Name < pts[j].Name
	})
	return pts
}

// RenderRestorePoints writes the table of restore design §10; total is the
// number of datasets in scope. Times are shown in loc.
func RenderRestorePoints(w io.Writer, pts []RestorePoint, total int, loc *time.Location) {
	var b strings.Builder
	tw := tabwriter.NewWriter(&b, 0, 0, 3, ' ', 0)
	_, _ = fmt.Fprint(tw, "CREATED\tSNAPSHOT\tDATASETS\t\n")
	for _, p := range pts {
		missing := ""
		switch n := len(p.Missing); {
		case n == 0:
		case n <= 3:
			missing = "missing: " + strings.Join(p.Missing, ", ")
		default:
			missing = fmt.Sprintf("missing: %d datasets", n)
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%d/%d\t%s\t\n",
			time.Unix(int64(p.Created), 0).In(loc).Format("2006-01-02 15:04"), p.Name, p.Have, total, missing)
	}
	_ = tw.Flush()
	for _, line := range strings.Split(strings.TrimSuffix(b.String(), "\n"), "\n") {
		_, _ = io.WriteString(w, strings.TrimRight(line, " ")+"\n")
	}
}
