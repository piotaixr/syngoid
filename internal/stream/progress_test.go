package stream

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

const gib = int64(1) << 30

func TestHumanBytes(t *testing.T) {
	tests := map[int64]string{
		0:          "0 B",
		512:        "512 B",
		1023:       "1023 B",
		1024:       "1.0 KiB",
		1048575:    "1.0 MiB",
		1288490189: "1.2 GiB",
		5 << 40:    "5.0 TiB",
	}
	for in, want := range tests {
		if got := HumanBytes(in); got != want {
			t.Errorf("HumanBytes(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestHumanDuration(t *testing.T) {
	tests := map[time.Duration]string{
		13 * time.Second:   "0:00:13",
		3725 * time.Second: "1:02:05",
		0:                  "0:00:00",
	}
	for in, want := range tests {
		if got := HumanDuration(in); got != want {
			t.Errorf("HumanDuration(%v) = %q, want %q", in, got, want)
		}
	}
}

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestBar(w *bytes.Buffer, c *fakeClock) *barRenderer {
	r := NewRenderer(ProgressBar, w, true, nil).(*barRenderer)
	r.now = c.now
	return r
}

func lastSegment(s string) string {
	return s[strings.LastIndex(s, "\r"):]
}

func TestBarRender(t *testing.T) {
	var buf bytes.Buffer
	clk := &fakeClock{t: time.Unix(1000, 0)}
	r := newTestBar(&buf, clk)

	r.Start("tank/a", 2*gib)
	clk.advance(10 * time.Second)
	r.Update(gib)

	seg := lastSegment(buf.String())
	for _, want := range []string{"\r", "tank/a", " 50%", "1.0 GiB/2.0 GiB", "102.4 MiB/s", "ETA 0:00:10", "\x1b[K"} {
		if !strings.Contains(seg, want) {
			t.Errorf("segment %q missing %q", seg, want)
		}
	}
	if !strings.HasPrefix(seg, "\r") {
		t.Errorf("segment %q does not start with CR", seg)
	}

	r.Finish(2*gib, 20*time.Second, nil)
	out := buf.String()
	if !strings.HasSuffix(out, "\n") {
		t.Errorf("output does not end with newline: %q", out)
	}
	if final := lastSegment(out); !strings.Contains(final, "100%") || !strings.Contains(final, "2.0 GiB/2.0 GiB") {
		t.Errorf("final line %q", final)
	}
}

func TestBarRenderUnknownEstimate(t *testing.T) {
	var buf bytes.Buffer
	clk := &fakeClock{t: time.Unix(1000, 0)}
	r := newTestBar(&buf, clk)

	r.Start("tank/a", 0)
	clk.advance(10 * time.Second)
	r.Update(gib)

	seg := lastSegment(buf.String())
	if strings.Contains(seg, "%") || strings.Contains(seg, "ETA") {
		t.Errorf("segment %q has percent or ETA", seg)
	}
	if want := "\rtank/a  1.0 GiB  102.4 MiB/s\x1b[K"; seg != want {
		t.Errorf("segment = %q, want %q", seg, want)
	}
}

func TestBarRenderThrottle(t *testing.T) {
	var buf bytes.Buffer
	clk := &fakeClock{t: time.Unix(1000, 0)}
	r := newTestBar(&buf, clk)

	r.Start("x", gib)
	clk.advance(time.Second)
	r.Update(1)
	r.Update(2)
	clk.advance(100 * time.Millisecond)
	r.Update(3)
	if n := strings.Count(buf.String(), "\r"); n != 1 {
		t.Fatalf("writes before 200ms elapsed = %d, want 1", n)
	}
	clk.advance(100 * time.Millisecond)
	r.Update(4)
	if n := strings.Count(buf.String(), "\r"); n != 2 {
		t.Fatalf("writes after 200ms = %d, want 2", n)
	}
	r.Finish(4, time.Second, nil)
	if n := strings.Count(buf.String(), "\r"); n != 3 {
		t.Fatalf("Finish must write regardless of throttle, writes = %d", n)
	}
}

func TestPlainRender(t *testing.T) {
	var out bytes.Buffer
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		if a.Key == slog.TimeKey {
			return slog.Attr{}
		}
		return a
	}}))
	r := NewRenderer(ProgressPlain, &out, false, log)

	r.Start("tank/a", 2*gib)
	r.Update(gib)
	if out.Len() != 0 || logs.Len() != 0 {
		t.Fatalf("Start/Update wrote output=%q logs=%q", out.String(), logs.String())
	}
	r.Finish(2*gib, 20*time.Second, nil)
	if out.Len() != 0 {
		t.Errorf("plain wrote to w: %q", out.String())
	}
	if want := `level=INFO msg="transferred 2.0 GiB in 0:00:20 (102.4 MiB/s)"`; strings.TrimSpace(logs.String()) != want {
		t.Errorf("log = %q, want %q", logs.String(), want)
	}
}

func TestPlainRenderErrorLogsNothing(t *testing.T) {
	var logs bytes.Buffer
	r := NewRenderer(ProgressPlain, nil, false, slog.New(slog.NewTextHandler(&logs, nil)))
	r.Finish(gib, time.Second, errTest)
	if logs.Len() != 0 {
		t.Errorf("logged on error: %q", logs.String())
	}
}

var errTest = bytes.ErrTooLarge

func TestNoneRender(t *testing.T) {
	var out bytes.Buffer
	r := NewRenderer(ProgressNone, &out, true, nil)
	r.Start("x", gib)
	r.Update(gib)
	r.Finish(gib, time.Second, nil)
	if out.Len() != 0 {
		t.Errorf("none wrote %q", out.String())
	}
}

func TestAutoMode(t *testing.T) {
	var out bytes.Buffer
	log := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	if _, ok := NewRenderer(ProgressAuto, &out, true, log).(*barRenderer); !ok {
		t.Error("auto on a TTY should be bar")
	}
	if _, ok := NewRenderer(ProgressAuto, &out, false, log).(*plainRenderer); !ok {
		t.Error("auto off a TTY should be plain")
	}
}

func TestBarETAClamped(t *testing.T) {
	var buf bytes.Buffer
	clk := &fakeClock{t: time.Unix(1000, 0)}
	r := newTestBar(&buf, clk)
	r.Start("x", 1<<62)
	clk.advance(time.Hour)
	r.Update(1) // tiny rate, enormous remainder
	if seg := lastSegment(buf.String()); !strings.Contains(seg, "ETA 99999:59:59") {
		t.Errorf("segment %q: ETA not clamped", seg)
	}
	buf.Reset()
	r.Finish(1<<62+5, time.Second, nil) // done beyond estimate
	if seg := lastSegment(buf.String()); !strings.Contains(seg, "ETA 0:00:00") {
		t.Errorf("segment %q: ETA should be 0", seg)
	}
}
