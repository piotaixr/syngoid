package stream

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func newTestBar(clk *fakeClock) (*barRenderer, *Terminal, *bytes.Buffer) {
	var buf bytes.Buffer
	term := NewTerminal(&buf, func() int { return 80 }, nil)
	r := NewRenderer(ProgressBar, term, nil, clk.now).(*barRenderer)
	return r, term, &buf
}

func TestBarRows(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	r, _, _ := newTestBar(clk)

	a := r.Start("tank/a", 595251)
	b := r.Start("tank/blog", 0)
	b.Update(37786)
	b.Sent()
	clk.advance(14 * time.Second)
	a.Update(365466)

	want := []string{
		"tank/a      61%  356.9 KiB/581.3 KiB  25.5 KiB/s  ETA 0:00:09",
		"tank/blog  ⠋ waiting for zfs receive  36.9 KiB sent  0:00:14",
	}
	got := r.rows(80)
	if len(got) != len(want) {
		t.Fatalf("rows = %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestBarSpinnerFrames(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	r, _, _ := newTestBar(clk)
	h := r.Start("x", 0)
	h.Sent()

	for _, tt := range []struct {
		after time.Duration
		frame string
	}{{0, "⠋"}, {200 * time.Millisecond, "⠙"}, {2 * time.Second, "⠋"}} {
		clk.t = time.Unix(1000, 0).Add(tt.after)
		want := "x  " + tt.frame + " waiting for zfs receive  0 B sent  " + HumanDuration(tt.after)
		if got := r.rows(80)[0]; got != want {
			t.Errorf("after %v: row = %q, want %q", tt.after, got, want)
		}
	}
}

func TestBarUnknownEstimate(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	r, _, _ := newTestBar(clk)
	h := r.Start("tank/a", 0)
	clk.advance(10 * time.Second)
	h.Update(gib)

	if got, want := r.rows(80)[0], "tank/a  1.0 GiB  102.4 MiB/s"; got != want {
		t.Errorf("row = %q, want %q", got, want)
	}
}

func TestBarAborting(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	r, _, _ := newTestBar(clk)
	r.Start("tank/a", gib)
	h := r.Start("tank/blog", 0)
	h.Sent()
	clk.advance(5 * time.Second)

	r.Aborting()
	want := []string{"tank/a     ⠋ aborting…  0:00:00", "tank/blog  ⠋ aborting…  0:00:00"}
	for i, got := range r.rows(80) {
		if got != want[i] {
			t.Errorf("row %d = %q, want %q", i, got, want[i])
		}
	}
	clk.advance(3 * time.Second) // 15 frames in: ⠴
	if got, want := r.rows(80)[0], "tank/a     ⠴ aborting…  0:00:03"; got != want {
		t.Errorf("later row = %q, want %q", got, want)
	}
}

func TestBarFinishPrintsFinalLine(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	r, _, buf := newTestBar(clk)

	h := r.Start("tank/a", 36864)
	clk.advance(time.Second)
	h.Update(37786)
	h.Sent()
	clk.advance(15 * time.Second)
	h.Finish(37786, nil)

	line := "tank/a  100%  36.9 KiB/36.0 KiB  36.9 KiB/s  ETA 0:00:00  zfs receive +0:00:15\n"
	if out := buf.String(); !strings.HasSuffix(out, erase(1)+line) {
		t.Errorf("output %q does not end with the erased row and %q", out, line)
	}
	if strings.Contains(buf.String(), "\x1b[K") {
		t.Errorf("output %q contains an erase-line sequence", buf.String())
	}
	if rows := r.rows(80); len(rows) != 0 {
		t.Errorf("rows after Finish = %q, want none", rows)
	}
}

func TestBarFinishErrorHasNoTail(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	r, _, buf := newTestBar(clk)

	h := r.Start("tank/a", 0)
	clk.advance(time.Second)
	h.Sent()
	clk.advance(15 * time.Second)
	h.Finish(1024, errTest)

	line := "tank/a  1.0 KiB  1.0 KiB/s\n"
	if out := buf.String(); !strings.HasSuffix(out, erase(1)+line) {
		t.Errorf("output %q does not end with %q", out, line)
	}
}

func TestBarFinishShortTailOmitted(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	r, _, buf := newTestBar(clk)

	h := r.Start("tank/a", 0)
	clk.advance(2 * time.Second)
	h.Sent()
	clk.advance(500 * time.Millisecond)
	h.Finish(2048, nil)

	line := "tank/a  2.0 KiB  1.0 KiB/s\n"
	if out := buf.String(); !strings.HasSuffix(out, erase(1)+line) {
		t.Errorf("output %q does not end with %q", out, line)
	}
}

func TestBarFinishWithoutSentHasNoTail(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	r, _, buf := newTestBar(clk)

	h := r.Start("tank/a", 0)
	clk.advance(17 * time.Second)
	h.Finish(17*1024, nil)

	line := "tank/a  17.0 KiB  1.0 KiB/s\n"
	if out := buf.String(); !strings.HasSuffix(out, erase(1)+line) {
		t.Errorf("output %q does not end with %q", out, line)
	}
}

func TestBarJobsOne(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	r, _, _ := newTestBar(clk)

	if rows := r.rows(80); len(rows) != 0 {
		t.Fatalf("rows before Start = %q", rows)
	}
	h := r.Start("tank/a", gib)
	if rows := r.rows(80); len(rows) != 1 {
		t.Fatalf("rows with one transfer = %q, want 1", rows)
	}
	h.Finish(gib, nil)
	if rows := r.rows(80); len(rows) != 0 {
		t.Errorf("rows after Finish = %q", rows)
	}
}

func TestBarCloseRemovesRegion(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	r, term, buf := newTestBar(clk)
	r.Start("tank/a", gib)
	buf.Reset()

	r.Close()
	if got := buf.String(); got != erase(1) {
		t.Fatalf("Close wrote %q, want %q", got, erase(1))
	}
	buf.Reset()
	term.Redraw()
	if buf.Len() != 0 {
		t.Errorf("Redraw after Close drew %q", buf.String())
	}
}

func TestBarETAClamped(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	r, _, buf := newTestBar(clk)
	h := r.Start("x", 1<<62)
	clk.advance(time.Hour)
	h.Update(1) // tiny rate, enormous remainder
	if row := r.rows(80)[0]; !strings.Contains(row, "ETA 99999:59:59") {
		t.Errorf("row %q: ETA not clamped", row)
	}
	h.Finish(1<<62+5, nil) // done beyond estimate
	if out := buf.String(); !strings.Contains(out, "ETA 0:00:00") {
		t.Errorf("output %q: ETA should be 0", out)
	}
}

func TestBarConcurrent(t *testing.T) {
	var buf syncBuffer
	term := NewTerminal(&buf, func() int { return 80 }, nil)
	r := NewRenderer(ProgressBar, term, nil, time.Now)
	done := make(chan struct{})
	for range 8 {
		go func() {
			defer func() { done <- struct{}{} }()
			for range 50 {
				h := r.Start("x", 100)
				h.Update(10)
				h.Sent()
				term.Redraw()
				h.Finish(100, nil)
			}
		}()
	}
	for range 8 {
		<-done
	}
	r.Aborting()
	r.Close()
}
