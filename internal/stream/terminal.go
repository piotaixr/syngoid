package stream

import (
	"fmt"
	"io"
	"sync"
	"time"
)

// Terminal manages a live region at the bottom of a terminal stream. Log
// lines written through Writer or Print appear above the region, which is
// erased before and redrawn after each of them. It is safe for concurrent use.
type Terminal struct {
	w     io.Writer
	width func() int

	mu        sync.Mutex // guards everything below and every write to w or a wrapped destination
	region    func(width int) []string
	drawn     int // region rows currently on screen
	suspended bool
	closed    bool

	done chan struct{}
	wg   sync.WaitGroup
}

// NewTerminal returns a Terminal drawing on w. width reports the terminal width
// in columns. When tick is non-nil, the region is redrawn on every tick until
// Close.
func NewTerminal(w io.Writer, width func() int, tick <-chan time.Time) *Terminal {
	t := &Terminal{w: w, width: width, done: make(chan struct{})}
	if tick != nil {
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			for {
				select {
				case <-tick:
					t.Redraw()
				case <-t.done:
					return
				}
			}
		}()
	}
	return t
}

// SetRegion installs the function that renders the region rows, or removes the
// region when render is nil. Any drawn region is erased; the new one appears at
// the next Redraw.
func (t *Terminal) SetRegion(render func(width int) []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.eraseLocked()
	t.region = render
}

// Redraw erases the region and draws it again.
func (t *Terminal) Redraw() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.eraseLocked()
	t.drawLocked()
}

// Print writes one permanent line above the region. line has no trailing
// newline.
func (t *Terminal) Print(line string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.eraseLocked()
	_, _ = io.WriteString(t.w, line+"\n")
	t.drawLocked()
}

// Suspend erases the region and stops drawing it until the returned function
// is called, so that something else can use the terminal.
func (t *Terminal) Suspend() (resume func()) {
	t.mu.Lock()
	t.eraseLocked()
	t.suspended = true
	t.mu.Unlock()
	return func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.suspended = false
		t.drawLocked()
	}
}

// Close stops the tick goroutine and erases the region. The Terminal still
// passes lines through Writer and Print afterwards, without a region.
func (t *Terminal) Close() {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.closed = true
	close(t.done)
	t.eraseLocked()
	t.region = nil
	t.mu.Unlock()
	t.wg.Wait()
}

// Writer returns a writer that sends complete lines to dst above the region.
// A trailing partial line is buffered until its newline arrives. Escape
// sequences go to the terminal stream only, never to dst.
func (t *Terminal) Writer(dst io.Writer) io.Writer {
	return &termWriter{t: t, dst: dst}
}

type termWriter struct {
	t   *Terminal
	dst io.Writer
	buf []byte // partial line, guarded by t.mu
}

func (tw *termWriter) Write(p []byte) (int, error) {
	t := tw.t
	t.mu.Lock()
	defer t.mu.Unlock()
	tw.buf = append(tw.buf, p...)
	end := -1
	for i := len(tw.buf) - 1; i >= 0; i-- {
		if tw.buf[i] == '\n' {
			end = i + 1
			break
		}
	}
	if end < 0 {
		return len(p), nil
	}
	lines := tw.buf[:end]
	t.eraseLocked()
	_, err := tw.dst.Write(lines)
	t.drawLocked()
	tw.buf = append([]byte(nil), tw.buf[end:]...)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

// eraseLocked removes the drawn region rows. t.mu must be held.
func (t *Terminal) eraseLocked() {
	if t.drawn == 0 {
		return
	}
	_, _ = fmt.Fprintf(t.w, "\r\x1b[%dA\x1b[J", t.drawn)
	t.drawn = 0
}

// drawLocked draws the region rows, each cut to one column less than the
// terminal width so that none wraps. t.mu must be held.
func (t *Terminal) drawLocked() {
	if t.region == nil || t.suspended {
		return
	}
	limit := max(t.width()-1, 1)
	rows := t.region(limit + 1)
	for _, row := range rows {
		_, _ = io.WriteString(t.w, truncateRunes(row, limit)+"\n")
	}
	t.drawn = len(rows)
}

// truncateRunes cuts s to at most n runes.
func truncateRunes(s string, n int) string {
	for i := range s {
		if n == 0 {
			return s[:i]
		}
		n--
	}
	return s
}
