// Package perms parses `zfs allow <dataset>` output and resolves the
// permissions a user effectively holds on a dataset.
package perms

import (
	"fmt"
	"regexp"
	"strings"
)

// scope says where a grant applies relative to the section's dataset.
type scope int

const (
	scopeLocal scope = iota
	scopeDescendent
	scopeLocalDescendent
)

func (s scope) onSelf() bool        { return s == scopeLocal || s == scopeLocalDescendent }
func (s scope) onDescendents() bool { return s == scopeDescendent || s == scopeLocalDescendent }

type grant struct {
	scope scope
	kind  string // "user", "group" or "everyone"
	name  string // empty for "everyone"
	perms []string
}

type section struct {
	dataset string
	sets    map[string][]string
	grants  []grant
}

// Delegations is the parsed form of `zfs allow` output.
type Delegations struct {
	sections []section
}

var headerRe = regexp.MustCompile(`^---- Permissions on (.+?)(?: -+)?\s*$`)

const (
	subNone = iota // before any subsection header
	subSets
	subGrants
	subIgnored // create-time or unknown subsection
)

// ParseAllow parses the output of `zfs allow <dataset>`. It returns an error
// only for structurally invalid lines.
func ParseAllow(out string) (Delegations, error) {
	var d Delegations
	var cur *section
	sub, sc := subNone, scopeLocal
	for i, line := range strings.Split(out, "\n") {
		line = strings.TrimRight(line, " \r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		if m := headerRe.FindStringSubmatch(line); m != nil {
			d.sections = append(d.sections, section{dataset: m[1], sets: map[string][]string{}})
			cur = &d.sections[len(d.sections)-1]
			sub = subNone
			continue
		}
		if line[0] != '\t' {
			if strings.HasPrefix(line, "---- ") || !strings.HasSuffix(line, ":") {
				return Delegations{}, fmt.Errorf("zfs allow: line %d: unrecognized line: %q", i+1, line)
			}
			switch strings.TrimSuffix(line, ":") {
			case "Permission sets":
				sub = subSets
			case "Local permissions":
				sub, sc = subGrants, scopeLocal
			case "Descendent permissions":
				sub, sc = subGrants, scopeDescendent
			case "Local+Descendent permissions":
				sub, sc = subGrants, scopeLocalDescendent
			default:
				sub = subIgnored
			}
			continue
		}
		if cur == nil || sub == subNone {
			return Delegations{}, fmt.Errorf("zfs allow: line %d: entry outside a permissions subsection: %q", i+1, line)
		}
		if sub == subIgnored {
			continue
		}
		f := strings.Fields(line)
		if sub == subSets {
			if len(f) != 2 || !strings.HasPrefix(f[0], "@") {
				return Delegations{}, fmt.Errorf("zfs allow: line %d: malformed permission set entry: %q", i+1, line)
			}
			cur.sets[f[0]] = splitPerms(f[1])
			continue
		}
		g := grant{scope: sc, kind: f[0]}
		switch {
		case (f[0] == "user" || f[0] == "group") && len(f) == 3:
			g.name, g.perms = f[1], splitPerms(f[2])
		case f[0] == "everyone" && len(f) == 2:
			g.perms = splitPerms(f[1])
		default:
			return Delegations{}, fmt.Errorf("zfs allow: line %d: unrecognized entry: %q", i+1, line)
		}
		cur.grants = append(cur.grants, g)
	}
	return d, nil
}

func splitPerms(s string) []string {
	var out []string
	for p := range strings.SplitSeq(s, ",") {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Effective returns the set of permissions user (a member of groups) holds on
// dataset. A section for the dataset itself contributes Local and
// Local+Descendent grants; a section for a proper ancestor contributes
// Descendent and Local+Descendent grants.
func Effective(d Delegations, dataset, user string, groups []string) map[string]bool {
	out := map[string]bool{}
	for i := range d.sections {
		s := &d.sections[i]
		self := s.dataset == dataset
		if !self && !strings.HasPrefix(dataset, s.dataset+"/") {
			continue
		}
		for _, g := range s.grants {
			if self && !g.scope.onSelf() || !self && !g.scope.onDescendents() {
				continue
			}
			if !applies(g, user, groups) {
				continue
			}
			d.expand(i, g.perms, out, map[setRef]bool{})
		}
	}
	return out
}

func applies(g grant, user string, groups []string) bool {
	switch g.kind {
	case "everyone":
		return true
	case "user":
		return g.name == user
	case "group":
		for _, name := range groups {
			if name == g.name {
				return true
			}
		}
	}
	return false
}

type setRef struct {
	section int
	name    string
}

// expand adds perms to out, replacing @set references by their members.
// Sets resolve from section si, else the nearest ancestor section defining
// them; unknown sets expand to nothing and cycles are cut.
func (d Delegations) expand(si int, perms []string, out map[string]bool, seen map[setRef]bool) {
	for _, p := range perms {
		if !strings.HasPrefix(p, "@") {
			out[p] = true
			continue
		}
		def := d.findSet(si, p)
		if def < 0 {
			continue
		}
		ref := setRef{def, p}
		if seen[ref] {
			continue
		}
		seen[ref] = true
		d.expand(def, d.sections[def].sets[p], out, seen)
	}
}

// findSet returns the index of the section defining name, starting at section
// si and then the nearest ancestor section, or -1.
func (d Delegations) findSet(si int, name string) int {
	if _, ok := d.sections[si].sets[name]; ok {
		return si
	}
	best, bestLen := -1, -1
	ds := d.sections[si].dataset
	for j, s := range d.sections {
		if _, ok := s.sets[name]; ok && strings.HasPrefix(ds, s.dataset+"/") && len(s.dataset) > bestLen {
			best, bestLen = j, len(s.dataset)
		}
	}
	return best
}
