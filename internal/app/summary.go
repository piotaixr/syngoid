package app

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/piotaixr/syngoid/internal/engine"
	"github.com/piotaixr/syngoid/internal/stream"
)

// RenderSummary writes the end-of-run summary: one line per dataset pair and
// a totals line.
func RenderSummary(w io.Writer, results []engine.Result, elapsed time.Duration) {
	var b strings.Builder
	b.WriteString("Summary:\n")
	var ok, nothing, skipped, failed int
	var total int64
	for _, r := range results {
		fmt.Fprintf(&b, "  %s → %s: %s", r.Source, r.Target, r.Status)
		switch r.Status {
		case engine.StatusOK:
			ok++
			fmt.Fprintf(&b, ", %s in %s", stream.HumanBytes(r.Bytes), stream.HumanDuration(r.Duration))
			total += r.Bytes
		case engine.StatusNothing:
			nothing++
			if r.Bytes > 0 {
				fmt.Fprintf(&b, ", %s in %s", stream.HumanBytes(r.Bytes), stream.HumanDuration(r.Duration))
				total += r.Bytes
			}
		case engine.StatusSkipped:
			skipped++
		case engine.StatusSoft, engine.StatusCritical:
			failed++
		}
		if r.Status != engine.StatusOK {
			first, _, _ := strings.Cut(r.Message, "\n")
			if first != "" {
				fmt.Fprintf(&b, " (%s)", first)
			}
		}
		b.WriteByte('\n')
	}
	fmt.Fprintf(&b, "  %d dataset(s): %d ok, %d nothing to do, %d skipped, %d failed; %s transferred in %s\n",
		len(results), ok, nothing, skipped, failed, stream.HumanBytes(total), stream.HumanDuration(elapsed))
	_, _ = io.WriteString(w, b.String())
}
