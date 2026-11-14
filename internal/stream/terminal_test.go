package stream

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

func erase(n int) string { return fmt.Sprintf("\r\x1b[%dA\x1b[J", n) }

func fixedRegion(rows ...string) func(int) []string {
	return func(int) []string { return rows }
}

func newTestTerminal(width int) (*Terminal, *bytes.Buffer) {
	var w bytes.Buffer
	return NewTerminal(&w, func() (int, int) { return width, 0 }, nil), &w
}

func TestTerminalRegionRedraw(t *testing.T) {
	term, w := newTestTerminal(80)
	term.SetRegion(fixedRegion("one", "two"))
	w.Reset()

	term.Redraw()
	if got, want := w.String(), "one\ntwo\n"; got != want {
		t.Fatalf("first redraw = %q, want %q", got, want)
	}
	w.Reset()
	term.Redraw()
	if got, want := w.String(), erase(2)+"one\ntwo\n"; got != want {
		t.Fatalf("second redraw = %q, want %q", got, want)
	}
}

func TestTerminalWriterPrintsAbove(t *testing.T) {
	term, w := newTestTerminal(80)
	term.SetRegion(fixedRegion("row"))
	term.Redraw()
	w.Reset()

	if _, err := term.Writer(w).Write([]byte("INFO: x\n")); err != nil {
		t.Fatal(err)
	}
	if got, want := w.String(), erase(1)+"INFO: x\n"+"row\n"; got != want {
		t.Errorf("output = %q, want %q", got, want)
	}
}

func TestTerminalWriterBuffersPartialLines(t *testing.T) {
	term, _ := newTestTerminal(80)
	var out bytes.Buffer
	wr := term.Writer(&out)

	if n, _ := wr.Write([]byte("INFO: a")); n != 7 {
		t.Errorf("Write returned %d, want 7", n)
	}
	if out.Len() != 0 {
		t.Fatalf("partial line written: %q", out.String())
	}
	_, _ = wr.Write([]byte("b\nINFO: c\n"))
	if got, want := out.String(), "INFO: ab\nINFO: c\n"; got != want {
		t.Errorf("out = %q, want %q", got, want)
	}
}

func TestTerminalWriterKeepsEscapesOffOtherStream(t *testing.T) {
	term, w := newTestTerminal(80)
	term.SetRegion(fixedRegion("row"))
	term.Redraw()
	w.Reset()
	out := &bytes.Buffer{}

	_, _ = term.Writer(out).Write([]byte("INFO: x\n"))
	if got := out.String(); got != "INFO: x\n" {
		t.Errorf("out = %q, want the plain line only", got)
	}
	if got, want := w.String(), erase(1)+"row\n"; got != want {
		t.Errorf("terminal = %q, want %q", got, want)
	}
}

func TestTerminalTruncatesRunes(t *testing.T) {
	term, w := newTestTerminal(12)
	term.SetRegion(fixedRegion("pool/srv/blog ⠙ waiting…"))
	w.Reset()
	term.Redraw()

	row := strings.TrimSuffix(w.String(), "\n")
	if !utf8.ValidString(row) {
		t.Fatalf("row %q is not valid UTF-8", row)
	}
	if n := utf8.RuneCountInString(row); n != 11 {
		t.Errorf("row %q has %d runes, want 11", row, n)
	}
}

func TestTerminalSuspend(t *testing.T) {
	term, w := newTestTerminal(80)
	term.SetRegion(fixedRegion("row"))
	term.Redraw()
	w.Reset()

	resume := term.Suspend()
	if got := w.String(); got != erase(1) {
		t.Fatalf("suspend wrote %q, want %q", got, erase(1))
	}
	w.Reset()
	term.Redraw()
	if w.Len() != 0 {
		t.Fatalf("Redraw while suspended wrote %q", w.String())
	}
	resume()
	if got := w.String(); got != "row\n" {
		t.Errorf("resume wrote %q, want %q", got, "row\n")
	}
}

func TestTerminalPrint(t *testing.T) {
	term, w := newTestTerminal(80)
	term.SetRegion(fixedRegion("row"))
	term.Redraw()
	w.Reset()

	term.Print("done")
	if got, want := w.String(), erase(1)+"done\nrow\n"; got != want {
		t.Errorf("output = %q, want %q", got, want)
	}
}

func TestTerminalConcurrentWriters(t *testing.T) {
	var w bytes.Buffer
	term := NewTerminal(&w, func() (int, int) { return 80, 0 }, nil)
	term.SetRegion(fixedRegion("row"))
	var outA, outB bytes.Buffer
	wa, wb := term.Writer(&outA), term.Writer(&outB)

	var wg sync.WaitGroup
	for g := range 8 {
		wr, tag := wa, "a"
		if g%2 == 1 {
			wr, tag = wb, "b"
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 100 {
				_, _ = fmt.Fprintf(wr, "INFO: %s-%d-%d\n", tag, g, i)
				term.Redraw()
			}
		}()
	}
	wg.Wait()

	for name, out := range map[string]*bytes.Buffer{"a": &outA, "b": &outB} {
		lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
		if len(lines) != 400 {
			t.Errorf("%s: %d lines, want 400", name, len(lines))
		}
		for _, l := range lines {
			if !strings.HasPrefix(l, "INFO: "+name+"-") || strings.Count(l, "INFO: ") != 1 {
				t.Fatalf("%s: mangled line %q", name, l)
			}
		}
	}
}

func TestTerminalClose(t *testing.T) {
	var w bytes.Buffer
	tick := make(chan time.Time, 1)
	term := NewTerminal(&w, func() (int, int) { return 80, 0 }, tick)
	term.SetRegion(fixedRegion("row"))
	term.Redraw()
	w.Reset()

	term.Close()
	if got := w.String(); got != erase(1) {
		t.Fatalf("Close wrote %q, want %q", got, erase(1))
	}
	w.Reset()
	tick <- time.Now()
	time.Sleep(20 * time.Millisecond)
	if w.Len() != 0 {
		t.Errorf("tick after Close drew %q", w.String())
	}
}

func TestTerminalTickRedraws(t *testing.T) {
	var w syncBuffer
	tick := make(chan time.Time)
	term := NewTerminal(&w, func() (int, int) { return 80, 0 }, tick)
	defer term.Close()
	term.SetRegion(fixedRegion("row"))
	w.Reset()

	tick <- time.Now()
	tick <- time.Now() // the goroutine finished the first redraw to receive this
	if !strings.Contains(w.String(), "row\n") {
		t.Errorf("tick did not redraw: %q", w.String())
	}
}

// syncBuffer is a bytes.Buffer safe for a writer goroutine and a reader.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func (s *syncBuffer) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.b.Reset()
}

func TestTerminalSuspendHoldsLines(t *testing.T) {
	term, w := newTestTerminal(80)
	term.SetRegion(fixedRegion("row"))
	term.Redraw()
	var out bytes.Buffer
	wr := term.Writer(&out)
	resume := term.Suspend()
	w.Reset()

	_, _ = wr.Write([]byte("INFO: a\n"))
	term.Print("done")
	_, _ = wr.Write([]byte("INFO: b\nINFO: par"))
	if w.Len() != 0 || out.Len() != 0 {
		t.Fatalf("output while suspended: terminal %q, out %q", w.String(), out.String())
	}

	w.Reset()
	resume()
	if got, want := out.String(), "INFO: a\nINFO: b\n"; got != want {
		t.Errorf("out = %q, want %q", got, want)
	}
	if got, want := w.String(), "done\nrow\n"; got != want {
		t.Errorf("terminal = %q, want %q", got, want)
	}
	_, _ = wr.Write([]byte("tial\n"))
	if got, want := out.String(), "INFO: a\nINFO: b\nINFO: partial\n"; got != want {
		t.Errorf("out after resume = %q, want %q", got, want)
	}
}

func TestTerminalSuspendFlushesInOrder(t *testing.T) {
	var log orderLog
	term := NewTerminal(log.dst("term"), func() (int, int) { return 80, 0 }, nil)
	a, b := term.Writer(log.dst("a")), term.Writer(log.dst("b"))
	resume := term.Suspend()
	_, _ = a.Write([]byte("1\n"))
	term.Print("2")
	_, _ = b.Write([]byte("3\n"))
	_, _ = a.Write([]byte("4\n"))
	if len(log.entries) != 0 {
		t.Fatalf("output while suspended: %q", log.entries)
	}
	resume()
	want := []string{"a:1\n", "term:2\n", "b:3\n", "a:4\n"}
	if fmt.Sprint(log.entries) != fmt.Sprint(want) {
		t.Errorf("flush = %q, want %q", log.entries, want)
	}
}

func TestTerminalCloseWhileSuspendedFlushes(t *testing.T) {
	term, w := newTestTerminal(80)
	var out bytes.Buffer
	wr := term.Writer(&out)
	resume := term.Suspend()
	_, _ = wr.Write([]byte("INFO: a\n"))
	term.Print("done")
	term.Close()
	if got, want := out.String(), "INFO: a\n"; got != want {
		t.Errorf("out = %q, want %q", got, want)
	}
	if got, want := w.String(), "done\n"; got != want {
		t.Errorf("terminal = %q, want %q", got, want)
	}
	resume()
	if got, want := w.String(), "done\n"; got != want {
		t.Errorf("terminal after resume = %q, want %q", got, want)
	}
}

func TestTerminalCapsRegionAtHeight(t *testing.T) {
	var w bytes.Buffer
	term := NewTerminal(&w, func() (int, int) { return 20, 4 }, nil)
	term.SetRegion(fixedRegion("r1", "r2", "r3", "r4", "r5"))
	w.Reset()
	term.Redraw()
	if got, want := w.String(), "r1\nr2\n… and 3 more\n"; got != want {
		t.Fatalf("draw = %q, want %q", got, want)
	}
	w.Reset()
	term.Redraw()
	if got, want := w.String(), erase(3)+"r1\nr2\n… and 3 more\n"; got != want {
		t.Errorf("redraw = %q, want %q", got, want)
	}

	// The overflow row is truncated like any other.
	w.Reset()
	term2 := NewTerminal(&w, func() (int, int) { return 6, 4 }, nil)
	term2.SetRegion(fixedRegion("r1", "r2", "r3", "r4", "r5"))
	w.Reset()
	term2.Redraw()
	if got, want := w.String(), "r1\nr2\n… and\n"; got != want {
		t.Errorf("narrow draw = %q, want %q", got, want)
	}
}

func TestTerminalNoCapWhenHeightUnknownOrEnough(t *testing.T) {
	for _, h := range []int{0, -1, 6} {
		var w bytes.Buffer
		term := NewTerminal(&w, func() (int, int) { return 80, h }, nil)
		term.SetRegion(fixedRegion("r1", "r2", "r3", "r4", "r5"))
		w.Reset()
		term.Redraw()
		if got, want := w.String(), "r1\nr2\nr3\nr4\nr5\n"; got != want {
			t.Errorf("height %d: draw = %q, want %q", h, got, want)
		}
	}
}

// orderLog records writes to several destinations in one sequence.
type orderLog struct{ entries []string }

type orderDst struct {
	log  *orderLog
	name string
}

func (l *orderLog) dst(name string) io.Writer { return orderDst{l, name} }

func (d orderDst) Write(p []byte) (int, error) {
	d.log.entries = append(d.log.entries, d.name+":"+string(p))
	return len(p), nil
}
