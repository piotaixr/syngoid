package stream

import (
	"fmt"
	"io"
	"log/slog"
	"math"
	"sync"
	"time"
)

// ProgressMode selects how transfer progress is shown.
type ProgressMode string

// Supported progress modes.
const (
	ProgressAuto  ProgressMode = "auto"
	ProgressBar   ProgressMode = "bar"
	ProgressPlain ProgressMode = "plain"
	ProgressNone  ProgressMode = "none"
)

// barThrottle is the minimum interval between two bar redraws.
const barThrottle = 200 * time.Millisecond

// maxETASeconds caps the displayed ETA at 99999:59:59 and keeps the Duration
// conversion from overflowing when the rate is tiny.
const maxETASeconds = 99999*3600 + 3599

// minTail is the shortest receive tail worth reporting.
const minTail = time.Second

// Renderer reports the progress of transfers. It replaces syncoid's pv.
type Renderer interface {
	// Start begins a transfer labelled label and returns its handle.
	// estimate is the expected total in bytes, or 0 when unknown.
	Start(label string, estimate int64) Transfer
	// Aborting tells the renderer that running transfers are being aborted.
	Aborting()
	// Close stops the renderer and releases what it drew.
	Close()
}

// Transfer reports the progress of one transfer. Its stream phase runs from
// Start to Sent; the receive tail runs from Sent to Finish.
type Transfer interface {
	// Update reports the raw bytes transferred so far.
	Update(rawBytes int64)
	// Sent records that the whole stream has been handed to the receiver.
	Sent()
	// Finish ends the transfer. err is the transfer outcome; failures
	// themselves are reported by the caller.
	Finish(rawBytes int64, err error)
}

// NewRenderer returns the Renderer for mode. ProgressAuto selects the bar when
// isTTY is true and plain logging otherwise. The bar draws on w; plain logs
// through log.
func NewRenderer(mode ProgressMode, w io.Writer, isTTY bool, log *slog.Logger) Renderer {
	if mode == ProgressAuto {
		mode = ProgressPlain
		if isTTY {
			mode = ProgressBar
		}
	}
	switch mode {
	case ProgressBar:
		return &barRenderer{w: w, now: time.Now}
	case ProgressPlain:
		return &plainRenderer{log: log, now: time.Now}
	default:
		return noneRenderer{}
	}
}

type noneRenderer struct{}

func (noneRenderer) Start(string, int64) Transfer { return noneTransfer{} }
func (noneRenderer) Aborting()                    {}
func (noneRenderer) Close()                       {}

type noneTransfer struct{}

func (noneTransfer) Update(int64)        {}
func (noneTransfer) Sent()               {}
func (noneTransfer) Finish(int64, error) {}

// phases times the two phases of one transfer.
type phases struct {
	now   func() time.Time
	start time.Time
	sent  time.Time // zero until Sent
}

func newPhases(now func() time.Time) phases { return phases{now: now, start: now()} }

func (p *phases) markSent() { p.sent = p.now() }

// split returns the stream duration at the current time and the receive tail.
// The tail is zero unless Sent was called.
func (p *phases) split() (stream, tail time.Duration) {
	now := p.now()
	if p.sent.IsZero() {
		return now.Sub(p.start), 0
	}
	return p.sent.Sub(p.start), now.Sub(p.sent)
}

// plainRenderer logs one summary line when a transfer succeeds.
type plainRenderer struct {
	log *slog.Logger
	now func() time.Time
}

func (p *plainRenderer) Start(label string, _ int64) Transfer {
	return &plainTransfer{r: p, label: label, ph: newPhases(p.now)}
}
func (*plainRenderer) Aborting() {}
func (*plainRenderer) Close()    {}

type plainTransfer struct {
	r     *plainRenderer
	label string
	ph    phases
}

func (*plainTransfer) Update(int64) {}
func (t *plainTransfer) Sent()      { t.ph.markSent() }

func (t *plainTransfer) Finish(rawBytes int64, err error) {
	if err != nil {
		return
	}
	elapsed, tail := t.ph.split()
	msg := fmt.Sprintf("%s: transferred %s in %s (%s/s)", t.label,
		HumanBytes(rawBytes), HumanDuration(elapsed), HumanBytes(int64(rate(rawBytes, elapsed))))
	if tail >= minTail {
		msg += fmt.Sprintf("; zfs receive finished %s later", HumanDuration(tail))
	}
	t.r.log.Info(msg)
}

// barRenderer redraws a single terminal line.
type barRenderer struct {
	w   io.Writer
	now func() time.Time

	mu sync.Mutex
}

func (b *barRenderer) Start(label string, estimate int64) Transfer {
	return &barTransfer{r: b, label: label, estimate: estimate, ph: newPhases(b.now)}
}
func (*barRenderer) Aborting() {}
func (*barRenderer) Close()    {}

type barTransfer struct {
	r        *barRenderer
	label    string
	estimate int64
	ph       phases

	lastDraw time.Time
	hasDrawn bool
}

func (t *barTransfer) Update(rawBytes int64) {
	t.r.mu.Lock()
	defer t.r.mu.Unlock()
	now := t.r.now()
	if t.hasDrawn && now.Sub(t.lastDraw) < barThrottle {
		return
	}
	t.lastDraw, t.hasDrawn = now, true
	t.draw(rawBytes, now.Sub(t.ph.start), "", "")
}

func (t *barTransfer) Sent() {
	t.r.mu.Lock()
	defer t.r.mu.Unlock()
	t.ph.markSent()
}

func (t *barTransfer) Finish(rawBytes int64, _ error) {
	t.r.mu.Lock()
	defer t.r.mu.Unlock()
	elapsed, tail := t.ph.split()
	suffix := ""
	if tail >= minTail {
		suffix = "  zfs receive +" + HumanDuration(tail)
	}
	t.draw(rawBytes, elapsed, suffix, "\n")
}

// draw writes one bar line with suffix inside it and end after it.
func (t *barTransfer) draw(done int64, elapsed time.Duration, suffix, end string) {
	r := rate(done, elapsed)
	rateStr := HumanBytes(int64(r)) + "/s"
	var line string
	if t.estimate > 0 {
		pct := int(float64(done) * 100 / float64(t.estimate))
		if pct > 100 {
			pct = 100
		}
		var eta time.Duration
		if remaining := t.estimate - done; r > 0 && remaining > 0 {
			eta = time.Duration(math.Min(math.Round(float64(remaining)/r), maxETASeconds)) * time.Second
		}
		line = fmt.Sprintf("\r%s  %3d%%  %s/%s  %s  ETA %s%s\x1b[K",
			t.label, pct, HumanBytes(done), HumanBytes(t.estimate), rateStr, HumanDuration(eta), suffix)
	} else {
		line = fmt.Sprintf("\r%s  %s  %s%s\x1b[K", t.label, HumanBytes(done), rateStr, suffix)
	}
	// A failed terminal write must not abort the transfer being shown.
	_, _ = io.WriteString(t.r.w, line+end)
}

// rate returns bytes per second, or 0 when elapsed is not positive.
func rate(bytes int64, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}
	return float64(bytes) / elapsed.Seconds()
}

// HumanBytes formats n using binary units, with one decimal above 1023 bytes.
// The unit is chosen after rounding, so 1048575 prints "1.0 MiB".
func HumanBytes(n int64) string {
	if n < 1024 {
		return fmt.Sprintf("%d B", n)
	}
	units := []string{"KiB", "MiB", "GiB", "TiB"}
	v := float64(n) / 1024
	i := 0
	for math.Round(v*10)/10 >= 1024 && i < len(units)-1 {
		v /= 1024
		i++
	}
	return fmt.Sprintf("%.1f %s", v, units[i])
}

// HumanDuration formats d as H:MM:SS, truncated to whole seconds.
func HumanDuration(d time.Duration) string {
	s := int64(d / time.Second)
	return fmt.Sprintf("%d:%02d:%02d", s/3600, s%3600/60, s%60)
}
