package stream

import (
	"fmt"
	"log/slog"
	"math"
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

// ResolveMode turns ProgressAuto into the bar on a TTY and plain logging
// otherwise. Other modes are returned unchanged.
func ResolveMode(m ProgressMode, isTTY bool) ProgressMode {
	if m != ProgressAuto {
		return m
	}
	if isTTY {
		return ProgressBar
	}
	return ProgressPlain
}

// NewRenderer returns the Renderer for mode. The bar draws its live region on
// term, which it requires; plain logs through log. ProgressAuto is treated as
// plain: callers resolve it with ResolveMode first. now is the clock.
func NewRenderer(mode ProgressMode, term *Terminal, log *slog.Logger, now func() time.Time) Renderer {
	switch mode {
	case ProgressBar:
		return newBarRenderer(term, now)
	case ProgressPlain, ProgressAuto:
		return &plainRenderer{log: log, now: now}
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
