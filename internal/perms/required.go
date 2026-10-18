package perms

import "strings"

// Requirement is a permission that is satisfied by holding any one of the
// listed alternatives.
type Requirement struct {
	AnyOf []string
}

// String joins the alternatives with " or ".
func (r Requirement) String() string {
	return strings.Join(r.AnyOf, " or ")
}

// Side selects which end of a replication a requirement set is computed for.
type Side int

// Replication sides.
const (
	Source Side = iota
	Target
)

// Needs is the subset of options that determines the required permissions.
type Needs struct {
	NoSyncSnap, KeepSyncSnap, UseHold, CreateBookmark, NoRollback bool
	NoStream                                                      bool
	DeleteTargetSnapshots, ForceDelete, PreserveRecordsize        bool
	RawSend                                                       bool
	// Props are the properties that will be set on the target, in order.
	Props []string
	// UserProps is true if any user property will be set on the target.
	UserProps bool
}

// Required computes the permissions needed on one side, de-duplicated by first
// appearance.
func Required(side Side, n Needs) []Requirement {
	var reqs []Requirement
	add := func(names ...string) {
		reqs = append(reqs, Requirement{AnyOf: names})
	}
	syncDestroy := !n.NoSyncSnap && !n.KeepSyncSnap

	if side == Source {
		if n.RawSend {
			add("send", "send:raw", "send:encrypted")
		} else {
			add("send")
		}
		if !n.NoSyncSnap {
			add("snapshot")
			add("mount")
		}
		if syncDestroy {
			add("destroy")
			add("mount")
		}
		if !n.NoStream {
			// zfs send -I places temporary holds on the snapshots it sends;
			// the kernel releases them, so release is not needed.
			add("hold")
		}
		if n.UseHold {
			add("hold")
			add("release")
		}
		if n.CreateBookmark {
			add("bookmark")
		}
		return dedupe(reqs)
	}

	add("create")
	add("mount")
	if n.NoRollback {
		add("receive", "receive:append")
	} else {
		add("receive")
	}
	if syncDestroy || n.DeleteTargetSnapshots || n.ForceDelete {
		add("destroy")
		add("mount")
	}
	if n.UseHold {
		add("hold")
		add("release")
	}
	if n.PreserveRecordsize {
		add("recordsize")
	}
	for _, p := range n.Props {
		add(p)
	}
	if n.UserProps {
		add("userprop")
	}
	return dedupe(reqs)
}

func dedupe(reqs []Requirement) []Requirement {
	out := make([]Requirement, 0, len(reqs))
	seen := make(map[string]bool, len(reqs))
	for _, r := range reqs {
		key := strings.Join(r.AnyOf, "\x00")
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, r)
	}
	return out
}

// Missing returns, in order, the requirements for which have holds none of the
// alternatives.
func Missing(have map[string]bool, req []Requirement) []Requirement {
	var out []Requirement
	for _, r := range req {
		ok := false
		for _, name := range r.AnyOf {
			if have[name] {
				ok = true
				break
			}
		}
		if !ok {
			out = append(out, r)
		}
	}
	return out
}

// GrantCommand returns a ready-to-paste `zfs allow` command granting the first
// alternative of each requirement, or "" if req is empty.
func GrantCommand(user, dataset string, req []Requirement) string {
	if len(req) == 0 {
		return ""
	}
	names := make([]string, len(req))
	for i, r := range req {
		names[i] = r.AnyOf[0]
	}
	return "zfs allow -u " + user + " " + strings.Join(names, ",") + " " + dataset
}
