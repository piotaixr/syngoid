package engine

import (
	"fmt"
	"io"
	"strings"

	"github.com/piotaixr/syngoid/internal/stream"
)

// RenderPlan writes the human-readable plan for --dry-run and --confirm.
// est supplies the size estimate of each send action; it may be nil.
func RenderPlan(w io.Writer, plans []Plan, syncSnap string, est func(Plan, Action) (int64, bool)) {
	var b strings.Builder
	creating := 0
	for _, p := range plans {
		if p.Status != StatusOK {
			continue
		}
		for _, a := range p.Actions {
			if dest, ok := sendDest(a); ok && dest != "" && dest == syncSnap {
				creating++
				break
			}
		}
	}
	if creating > 0 {
		fmt.Fprintf(&b, "create sync snapshot %s on %d dataset(s)\n", syncSnap, creating)
	}

	var total int64
	anyApprox := false
	for _, p := range plans {
		fmt.Fprintf(&b, "%s → %s\n", p.Source, p.Target)
		switch p.Status {
		case StatusOK:
			for _, a := range p.Actions {
				b.WriteString("  " + a.Describe())
				if _, ok := sendDest(a); ok && est != nil {
					n, approx := est(p, a)
					anyApprox = anyApprox || approx
					total += n
					if n != 0 {
						b.WriteString("  ")
						if approx {
							b.WriteString("~")
						}
						b.WriteString(stream.HumanBytes(n))
					}
				}
				b.WriteString("\n")
			}
		case StatusSkipped:
			writeMessage(&b, "skip: ", p.Message)
		case StatusNothing:
			writeMessage(&b, "nothing to do: ", p.Message)
		case StatusSoft:
			writeMessage(&b, "WARNING: ", p.Message)
		case StatusCritical:
			writeMessage(&b, "CRITICAL: ", p.Message)
		}
	}

	b.WriteString("Total estimated transfer: ")
	if anyApprox {
		b.WriteString("~")
	}
	b.WriteString(stream.HumanBytes(total) + "\n")
	_, _ = io.WriteString(w, b.String())
}

// writeMessage writes label and msg indented by two spaces; continuation
// lines are indented by four and blank lines stay empty.
func writeMessage(b *strings.Builder, label, msg string) {
	for i, line := range strings.Split(msg, "\n") {
		switch {
		case i == 0:
			b.WriteString("  " + label + line)
		case line == "":
		default:
			b.WriteString("    " + line)
		}
		b.WriteString("\n")
	}
}
