package engine

import "github.com/piotaixr/syngoid/internal/zfs"

// PlanRestore plans the restore of one dataset pair (restore design §6.2).
// It only ever plans a full send or a resume: a restore never destroys,
// rolls back, holds or sends incrementally.
func PlanRestore(s DatasetState, sel Selection) Plan {
	p := Plan{Source: s.Source, Target: s.Target}
	stop := func(st Status, msg string) Plan {
		p.Status, p.Message = st, msg
		return p
	}
	conflict := func(msg string) Plan {
		p.Conflict = true
		return stop(StatusCritical, msg)
	}

	chosen, ok := sel.Pick(s.SourceSnaps)
	if !ok {
		return stop(StatusSoft, sel.Missing(s.Source, s.SourceSnaps))
	}
	if !s.TargetExists {
		p.Actions = []Action{SendFull{Snap: chosen.Name}}
		return p
	}
	if s.TargetIsPoolRoot {
		return conflict("target " + s.Target + " is a pool root and always exists; restore its children with --skip-parent")
	}
	if restoreFinished(s.TargetSnaps, s.ResumeToken, chosen) {
		return stop(StatusNothing, "already restored at @"+chosen.Name)
	}
	if s.ResumeToken != "" && len(s.TargetSnaps) == 0 && s.ResumeGUID != "" && s.ResumeGUID == chosen.GUID {
		p.Actions = []Action{Resume{Token: s.ResumeToken}}
		return p
	}
	return conflict("target " + s.Target + " already exists")
}

// restoreFinished reports whether the target holds exactly the chosen
// snapshot and no interrupted receive (restore design §6.3, case 1).
func restoreFinished(snaps []zfs.Snapshot, token string, chosen zfs.Snapshot) bool {
	return token == "" && len(snaps) == 1 &&
		snaps[0].Name == chosen.Name && snaps[0].GUID == chosen.GUID
}
