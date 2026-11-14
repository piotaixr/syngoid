package stream

import (
	"fmt"
	"math"
	"sync"
	"time"
	"unicode/utf8"
)

// maxETASeconds caps the displayed ETA at 99999:59:59 and keeps the Duration
// conversion from overflowing when the rate is tiny.
const maxETASeconds = 99999*3600 + 3599

// spinnerFrames are the braille frames of the waiting and aborting rows; one
// frame lasts spinnerStep.
var spinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

const spinnerStep = 200 * time.Millisecond

// barRenderer shows one row per running transfer in a Terminal live region.
type barRenderer struct {
	term *Terminal
	now  func() time.Time

	mu         sync.Mutex // never held while calling into term
	running    []*barTransfer
	aborting   bool
	abortStart time.Time
}

func newBarRenderer(term *Terminal, now func() time.Time) *barRenderer {
	b := &barRenderer{term: term, now: now}
	term.SetRegion(b.rows)
	return b
}

func (b *barRenderer) Start(label string, estimate int64) Transfer {
	t := &barTransfer{r: b, label: label, estimate: estimate, ph: newPhases(b.now)}
	b.mu.Lock()
	b.running = append(b.running, t)
	b.mu.Unlock()
	b.term.Redraw()
	return t
}

func (b *barRenderer) Aborting() {
	b.mu.Lock()
	if !b.aborting {
		b.aborting, b.abortStart = true, b.now()
	}
	b.mu.Unlock()
	b.term.Redraw()
}

func (b *barRenderer) Close() { b.term.SetRegion(nil) }

// rows returns the region rows, one per running transfer in start order. The
// terminal cuts them to its width, so width is unused.
func (b *barRenderer) rows(int) []string {
	now := b.now()
	b.mu.Lock()
	defer b.mu.Unlock()
	pad := 0
	for _, t := range b.running {
		pad = max(pad, utf8.RuneCountInString(t.label))
	}
	out := make([]string, len(b.running))
	for i, t := range b.running {
		label := fmt.Sprintf("%-*s", pad, t.label)
		switch {
		case b.aborting:
			since := now.Sub(b.abortStart)
			out[i] = fmt.Sprintf("%s  %s aborting…  %s", label, spinner(since), HumanDuration(since))
		case !t.ph.sent.IsZero():
			since := now.Sub(t.ph.sent)
			out[i] = fmt.Sprintf("%s  %s waiting for zfs receive  %s sent  %s",
				label, spinner(since), HumanBytes(t.done), HumanDuration(since))
		default:
			out[i] = streamLine(label, t.estimate, t.done, now.Sub(t.ph.start))
		}
	}
	return out
}

// spinner returns the frame for a row that has been waiting for since.
func spinner(since time.Duration) string {
	return spinnerFrames[int(since/spinnerStep)%len(spinnerFrames)]
}

type barTransfer struct {
	r        *barRenderer
	label    string
	estimate int64
	ph       phases
	done     int64 // guarded by r.mu
}

func (t *barTransfer) Update(rawBytes int64) {
	t.r.mu.Lock()
	t.done = rawBytes
	t.r.mu.Unlock()
}

func (t *barTransfer) Sent() {
	t.r.mu.Lock()
	t.ph.markSent()
	t.r.mu.Unlock()
	t.r.term.Redraw()
}

func (t *barTransfer) Finish(rawBytes int64, err error) {
	t.r.mu.Lock()
	elapsed, tail := t.ph.split()
	for i, row := range t.r.running {
		if row == t {
			t.r.running = append(t.r.running[:i], t.r.running[i+1:]...)
			break
		}
	}
	t.r.mu.Unlock()

	line := streamLine(t.label, t.estimate, rawBytes, elapsed)
	if err == nil && tail >= minTail {
		line += "  zfs receive +" + HumanDuration(tail)
	}
	t.r.term.Print(line)
}

// streamLine formats the streaming row: percent, bytes, rate and ETA, or just
// bytes and rate when the estimate is unknown. label is printed as given.
func streamLine(label string, estimate, done int64, elapsed time.Duration) string {
	r := rate(done, elapsed)
	rateStr := HumanBytes(int64(r)) + "/s"
	if estimate <= 0 {
		return fmt.Sprintf("%s  %s  %s", label, HumanBytes(done), rateStr)
	}
	pct := int(float64(done) * 100 / float64(estimate))
	if pct > 100 {
		pct = 100
	}
	var eta time.Duration
	if remaining := estimate - done; r > 0 && remaining > 0 {
		eta = time.Duration(math.Min(math.Round(float64(remaining)/r), maxETASeconds)) * time.Second
	}
	return fmt.Sprintf("%s  %3d%%  %s/%s  %s  ETA %s",
		label, pct, HumanBytes(done), HumanBytes(estimate), rateStr, HumanDuration(eta))
}
