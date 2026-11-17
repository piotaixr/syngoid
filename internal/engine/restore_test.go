package engine

import (
	"testing"

	"github.com/piotaixr/syngoid/internal/zfs"
)

func restoreCases() []struct {
	name     string
	s        DatasetState
	sel      Selection
	status   Status
	action   string
	msg      string
	conflict bool
} {
	one := zfs.Snapshot{Name: "one", GUID: "11", CreateTxg: 1, Creation: 100}
	two := zfs.Snapshot{Name: "two", GUID: "22", CreateTxg: 2, Creation: 200}
	src := []zfs.Snapshot{one, two}
	base := DatasetState{Source: "backup/d", Target: "tank/d", SourceSnaps: src}
	with := func(f func(*DatasetState)) DatasetState { s := base; f(&s); return s }
	const exists = "target tank/d already exists"
	return []struct {
		name     string
		s        DatasetState
		sel      Selection
		status   Status
		action   string
		msg      string
		conflict bool
	}{
		{"no target", base, Selection{}, StatusOK, "send full @two", "", false},
		{"no target, by name", base, Selection{Name: "one"}, StatusOK, "send full @one", "", false},
		{"no snapshots", with(func(s *DatasetState) { s.SourceSnaps = nil }), Selection{}, StatusSoft, "", "no snapshots on backup/d", false},
		{"name absent", base, Selection{Name: "zz"}, StatusSoft, "", "no snapshot zz on backup/d", false},
		{"no pick wins over existing target", with(func(s *DatasetState) { s.TargetExists = true }), Selection{Name: "zz"}, StatusSoft, "", "no snapshot zz on backup/d", false},
		{"finished", with(func(s *DatasetState) { s.TargetExists = true; s.TargetSnaps = []zfs.Snapshot{two} }), Selection{}, StatusNothing, "", "already restored at @two", false},
		{"interrupted", with(func(s *DatasetState) { s.TargetExists = true; s.ResumeToken = "tok"; s.ResumeGUID = "22" }), Selection{}, StatusOK, "resume interrupted receive", "", false},
		{"token for another snapshot", with(func(s *DatasetState) { s.TargetExists = true; s.ResumeToken = "tok"; s.ResumeGUID = "11" }), Selection{}, StatusCritical, "", exists, true},
		{"token with unknown guid", with(func(s *DatasetState) { s.TargetExists = true; s.ResumeToken = "tok" }), Selection{}, StatusCritical, "", exists, true},
		{"token and snapshots", with(func(s *DatasetState) {
			s.TargetExists = true
			s.ResumeToken = "tok"
			s.ResumeGUID = "22"
			s.TargetSnaps = []zfs.Snapshot{one}
		}), Selection{}, StatusCritical, "", exists, true},
		{"other snapshot", with(func(s *DatasetState) { s.TargetExists = true; s.TargetSnaps = []zfs.Snapshot{one} }), Selection{}, StatusCritical, "", exists, true},
		{"same name other guid", with(func(s *DatasetState) {
			s.TargetExists = true
			s.TargetSnaps = []zfs.Snapshot{{Name: "two", GUID: "99"}}
		}), Selection{}, StatusCritical, "", exists, true},
		{"extra snapshot", with(func(s *DatasetState) {
			s.TargetExists = true
			s.TargetSnaps = []zfs.Snapshot{two, {Name: "local", GUID: "5"}}
		}), Selection{}, StatusCritical, "", exists, true},
		{"empty existing target", with(func(s *DatasetState) { s.TargetExists = true }), Selection{}, StatusCritical, "", exists, true},
		{"pool root", with(func(s *DatasetState) { s.Target = "tank"; s.TargetExists = true; s.TargetIsPoolRoot = true }), Selection{}, StatusCritical, "", "target tank is a pool root and always exists; restore its children with --skip-parent", true},
		{"sync property is ignored", with(func(s *DatasetState) { s.SyncProp = "false" }), Selection{}, StatusOK, "send full @two", "", false},
		{"clone is a full send", with(func(s *DatasetState) { s.Origin = "backup/o@x"; s.OriginInScope = true }), Selection{}, StatusOK, "send full @two", "", false},
	}
}

func TestPlanRestore(t *testing.T) {
	for _, c := range restoreCases() {
		t.Run(c.name, func(t *testing.T) {
			p := PlanRestore(c.s, c.sel)
			if p.Source != c.s.Source || p.Target != c.s.Target {
				t.Errorf("pair = %s → %s", p.Source, p.Target)
			}
			if p.Status != c.status {
				t.Errorf("status = %v, want %v", p.Status, c.status)
			}
			if p.Message != c.msg {
				t.Errorf("message = %q, want %q", p.Message, c.msg)
			}
			if p.Conflict != c.conflict {
				t.Errorf("conflict = %v, want %v", p.Conflict, c.conflict)
			}
			if c.action == "" {
				if len(p.Actions) != 0 {
					t.Errorf("actions = %v, want none", p.Actions)
				}
				return
			}
			if len(p.Actions) != 1 || p.Actions[0].Describe() != c.action {
				t.Errorf("actions = %v, want [%s]", p.Actions, c.action)
			}
		})
	}
}

func TestPlanRestoreOnlySafeActions(t *testing.T) {
	for _, c := range restoreCases() {
		for _, a := range PlanRestore(c.s, c.sel).Actions {
			switch a.(type) {
			case SendFull, Resume:
			default:
				t.Errorf("%s: forbidden action %T", c.name, a)
			}
		}
	}
}
