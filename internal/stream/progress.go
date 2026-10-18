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

// Renderer reports the progress of one transfer. It replaces syncoid's pv.
type Renderer interface {
	// Start begins a transfer labelled label. estimate is the expected total
	// in bytes, or 0 when unknown.
	Start(label string, estimate int64)
	// Update reports the raw bytes transferred so far.
	Update(rawBytes int64)
	// Finish ends the transfer after elapsed. err is the transfer outcome;
	// failures themselves are reported by the caller.
	Finish(rawBytes int64, elapsed time.Duration, err error)
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
		return &plainRenderer{log: log}
	default:
		return noneRenderer{}
	}
}

type noneRenderer struct{}

func (noneRenderer) Start(string, int64)                {}
func (noneRenderer) Update(int64)                       {}
func (noneRenderer) Finish(int64, time.Duration, error) {}

// plainRenderer logs one summary line when the transfer succeeds.
type plainRenderer struct{ log *slog.Logger }

func (*plainRenderer) Start(string, int64) {}
func (*plainRenderer) Update(int64)        {}

func (p *plainRenderer) Finish(rawBytes int64, elapsed time.Duration, err error) {
	if err != nil {
		return
	}
	p.log.Info(fmt.Sprintf("transferred %s in %s (%s/s)",
		HumanBytes(rawBytes), HumanDuration(elapsed), HumanBytes(int64(rate(rawBytes, elapsed)))))
}

// barRenderer redraws a single terminal line.
type barRenderer struct {
	w   io.Writer
	now func() time.Time

	mu       sync.Mutex
	label    string
	estimate int64
	start    time.Time
	lastDraw time.Time
	hasDrawn bool
}

func (b *barRenderer) Start(label string, estimate int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.label, b.estimate = label, estimate
	b.start = b.now()
	b.hasDrawn = false
}

func (b *barRenderer) Update(rawBytes int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	if b.hasDrawn && now.Sub(b.lastDraw) < barThrottle {
		return
	}
	b.lastDraw, b.hasDrawn = now, true
	b.draw(rawBytes, now.Sub(b.start), "")
}

func (b *barRenderer) Finish(rawBytes int64, elapsed time.Duration, _ error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.draw(rawBytes, elapsed, "\n")
}

// draw writes one bar line followed by tail.
func (b *barRenderer) draw(done int64, elapsed time.Duration, tail string) {
	r := rate(done, elapsed)
	rateStr := HumanBytes(int64(r)) + "/s"
	var line string
	if b.estimate > 0 {
		pct := int(float64(done) * 100 / float64(b.estimate))
		if pct > 100 {
			pct = 100
		}
		var eta time.Duration
		if remaining := b.estimate - done; r > 0 && remaining > 0 {
			eta = time.Duration(math.Min(math.Round(float64(remaining)/r), maxETASeconds)) * time.Second
		}
		line = fmt.Sprintf("\r%s  %3d%%  %s/%s  %s  ETA %s\x1b[K",
			b.label, pct, HumanBytes(done), HumanBytes(b.estimate), rateStr, HumanDuration(eta))
	} else {
		line = fmt.Sprintf("\r%s  %s  %s\x1b[K", b.label, HumanBytes(done), rateStr)
	}
	// A failed terminal write must not abort the transfer being shown.
	_, _ = io.WriteString(b.w, line+tail)
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
