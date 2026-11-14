package stream

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

const gib = int64(1) << 30

var errTest = bytes.ErrTooLarge

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

func newTestPlain(c *fakeClock) (*plainRenderer, *bytes.Buffer) {
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		if a.Key == slog.TimeKey {
			return slog.Attr{}
		}
		return a
	}}))
	return NewRenderer(ProgressPlain, nil, log, c.now).(*plainRenderer), &logs
}

func TestPlainRender(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	r, logs := newTestPlain(clk)

	h := r.Start("tank/a", 2*gib)
	h.Update(gib)
	if logs.Len() != 0 {
		t.Fatalf("Start/Update logged %q", logs.String())
	}
	clk.advance(20 * time.Second)
	h.Finish(2*gib, nil)
	if want := `level=INFO msg="tank/a: transferred 2.0 GiB in 0:00:20 (102.4 MiB/s)"`; strings.TrimSpace(logs.String()) != want {
		t.Errorf("log = %q, want %q", logs.String(), want)
	}
}

func TestPlainRenderTail(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	r, logs := newTestPlain(clk)

	h := r.Start("tank/a", 0)
	clk.advance(2 * time.Second)
	h.Update(570266)
	h.Sent()
	clk.advance(15 * time.Second)
	h.Finish(570266, nil)
	if want := `level=INFO msg="tank/a: transferred 556.9 KiB in 0:00:02 (278.5 KiB/s); zfs receive finished 0:00:15 later"`; strings.TrimSpace(logs.String()) != want {
		t.Errorf("log = %q, want %q", logs.String(), want)
	}
}

func TestPlainRenderShortTail(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	r, logs := newTestPlain(clk)

	h := r.Start("tank/a", 0)
	clk.advance(2 * time.Second)
	h.Sent()
	clk.advance(500 * time.Millisecond)
	h.Finish(570266, nil)
	if want := `level=INFO msg="tank/a: transferred 556.9 KiB in 0:00:02 (278.5 KiB/s)"`; strings.TrimSpace(logs.String()) != want {
		t.Errorf("log = %q, want %q", logs.String(), want)
	}
}

func TestPlainRenderNoSent(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	r, logs := newTestPlain(clk)

	h := r.Start("tank/a", 0)
	clk.advance(17 * time.Second)
	h.Finish(17*1024, nil)
	if want := `level=INFO msg="tank/a: transferred 17.0 KiB in 0:00:17 (1.0 KiB/s)"`; strings.TrimSpace(logs.String()) != want {
		t.Errorf("log = %q, want %q", logs.String(), want)
	}
}

func TestPlainRenderErrorLogsNothing(t *testing.T) {
	var logs bytes.Buffer
	r := NewRenderer(ProgressPlain, nil, slog.New(slog.NewTextHandler(&logs, nil)), time.Now)
	r.Start("tank/a", 0).Finish(gib, errTest)
	if logs.Len() != 0 {
		t.Errorf("logged on error: %q", logs.String())
	}
}

func TestNoneRender(t *testing.T) {
	var out bytes.Buffer
	term := NewTerminal(&out, func() (int, int) { return 80, 0 }, nil)
	r := NewRenderer(ProgressNone, term, nil, time.Now)
	h := r.Start("x", gib)
	h.Update(gib)
	h.Sent()
	h.Finish(gib, nil)
	r.Aborting()
	r.Close()
	if out.Len() != 0 {
		t.Errorf("none wrote %q", out.String())
	}
}

func TestResolveMode(t *testing.T) {
	tests := []struct {
		mode  ProgressMode
		isTTY bool
		want  ProgressMode
	}{
		{ProgressAuto, true, ProgressBar},
		{ProgressAuto, false, ProgressPlain},
		{ProgressBar, false, ProgressBar},
		{ProgressPlain, true, ProgressPlain},
		{ProgressNone, true, ProgressNone},
	}
	for _, tt := range tests {
		if got := ResolveMode(tt.mode, tt.isTTY); got != tt.want {
			t.Errorf("ResolveMode(%q, %v) = %q, want %q", tt.mode, tt.isTTY, got, tt.want)
		}
	}
}

func TestNewRendererAutoIsPlain(t *testing.T) {
	log := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	if _, ok := NewRenderer(ProgressAuto, nil, log, time.Now).(*plainRenderer); !ok {
		t.Error("auto should be treated as plain")
	}
}
