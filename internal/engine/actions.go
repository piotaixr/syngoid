package engine

import "fmt"

// Action is one step of a dataset replication plan.
type Action interface {
	// Describe returns a one-line human-readable description.
	Describe() string
	// Destructive reports whether the action destroys user data.
	Destructive() bool
}

// SendFull sends a full stream of Snap.
type SendFull struct{ Snap string }

// SendIncremental sends From → To, with -I when Intermediates is set and -i
// otherwise.
type SendIncremental struct {
	From, To      string
	Intermediates bool
}

// SendFromBookmark sends incrementally from a source bookmark to To.
type SendFromBookmark struct{ Bookmark, To string }

// SendClone sends To incrementally from the clone origin snapshot Origin.
type SendClone struct{ Origin, To string }

// Resume resumes an interrupted receive from its resume token.
type Resume struct{ Token string }

// Hold places the hold tag on Snap on Side.
type Hold struct {
	Side Side
	Snap string
}

// Release removes the hold tag from Snap on Side.
type Release struct {
	Side Side
	Snap string
}

// CreateBookmark creates a source bookmark for Snap.
type CreateBookmark struct{ Snap string }

// PruneSyncSnaps destroys this host's old sync snapshots on Side.
type PruneSyncSnaps struct {
	Side  Side
	Snaps []string
}

// DestroyTargetSnaps destroys target snapshots missing on the source.
type DestroyTargetSnaps struct{}

// ForceDestroyTarget recursively destroys the target dataset.
type ForceDestroyTarget struct{}

// Describe implements Action.
func (a SendFull) Describe() string { return "send full @" + a.Snap }

// Describe implements Action.
func (a SendIncremental) Describe() string {
	d := "send incremental @" + a.From + " → @" + a.To
	if a.Intermediates {
		d += " (with intermediates)"
	}
	return d
}

// Describe implements Action.
func (a SendFromBookmark) Describe() string {
	return "send incremental #" + a.Bookmark + " → @" + a.To
}

// Describe implements Action.
func (a SendClone) Describe() string { return "send clone of " + a.Origin + " → @" + a.To }

// Describe implements Action.
func (Resume) Describe() string { return "resume interrupted receive" }

// Describe implements Action.
func (a Hold) Describe() string { return "hold @" + a.Snap + " on " + a.Side.String() }

// Describe implements Action.
func (a Release) Describe() string { return "release @" + a.Snap + " on " + a.Side.String() }

// Describe implements Action.
func (a CreateBookmark) Describe() string { return "create bookmark #" + a.Snap }

// Describe implements Action.
func (a PruneSyncSnaps) Describe() string {
	return fmt.Sprintf("prune %d sync snapshot(s) on %s", len(a.Snaps), a.Side)
}

// Describe implements Action.
func (DestroyTargetSnaps) Describe() string { return "destroy target snapshots missing on source" }

// Describe implements Action.
func (ForceDestroyTarget) Describe() string { return "DESTROY target recursively (--force-delete)" }

// Destructive implements Action.
func (SendFull) Destructive() bool { return false }

// Destructive implements Action.
func (SendIncremental) Destructive() bool { return false }

// Destructive implements Action.
func (SendFromBookmark) Destructive() bool { return false }

// Destructive implements Action.
func (SendClone) Destructive() bool { return false }

// Destructive implements Action.
func (Resume) Destructive() bool { return false }

// Destructive implements Action.
func (Hold) Destructive() bool { return false }

// Destructive implements Action.
func (Release) Destructive() bool { return false }

// Destructive implements Action.
func (CreateBookmark) Destructive() bool { return false }

// Destructive implements Action.
func (PruneSyncSnaps) Destructive() bool { return false }

// Destructive implements Action.
func (DestroyTargetSnaps) Destructive() bool { return false }

// Destructive implements Action.
func (ForceDestroyTarget) Destructive() bool { return true }
