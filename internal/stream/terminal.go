package stream

import (
	"fmt"
	"io"
	"sync"
	"time"
)

// Terminal manages a live region at the bottom of a terminal stream. Log
// lines written through Writer or Print appear above the region, which is
// erased before and redrawn after each of them. While suspended, those lines
// are held and written, in order, on resume. It is safe for concurrent use.
//
// A window that shrinks between a draw and the next erase can leave stray
// region rows on screen once; that is accepted.
type Terminal struct {
	w    io.Writer
	size func() (width, height int)

	mu        sync.Mutex // guards everything below and every write to w or a wrapped destination
	region    func(width int) []string
	drawn     int // region rows currently on screen
	suspended bool
	held      []heldLines // lines written while suspended, in order
	closed    bool

	done chan struct{}
	wg   sync.WaitGroup
}

// heldLines are complete lines waiting for the end of a suspension; a nil dst
// means the terminal stream itself.
type heldLines struct {
	dst  io.Writer
	data []byte
}

// NewTerminal returns a Terminal drawing on w. size reports the terminal width
// and height; a height of 0 or less means unknown. When tick is non-nil, the
// region is redrawn on every tick until Close.
func NewTerminal(w io.Writer, size func() (width, height int), tick <-chan time.Time) *Terminal {
	t := &Terminal{w: w, size: size, done: make(chan struct{})}
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
	if t.suspended {
		t.held = append(t.held, heldLines{data: []byte(line + "\n")})
		return
	}
	t.eraseLocked()
	_, _ = io.WriteString(t.w, line+"\n")
	t.drawLocked()
}

// Suspend erases the region and stops drawing it until the returned function
// is called, so that something else (a --confirm question) can use the
// terminal. Lines from Writer and Print are held meanwhile; resume writes them
// in order, then redraws the region.
func (t *Terminal) Suspend() (resume func()) {
	t.mu.Lock()
	t.eraseLocked()
	t.suspended = true
	t.mu.Unlock()
	return func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		if !t.suspended {
			return
		}
		t.suspended = false
		t.flushLocked()
		t.drawLocked()
	}
}

// Close stops the tick goroutine, erases the region and writes any held
// lines, ending a suspension. The Terminal still passes lines through Writer
// and Print afterwards, without a region. A buffered partial line is not
// flushed: the logger writes whole lines.
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
	t.suspended = false
	t.flushLocked()
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
	var err error
	if t.suspended {
		t.held = append(t.held, heldLines{dst: tw.dst, data: append([]byte(nil), lines...)})
	} else {
		t.eraseLocked()
		_, err = tw.dst.Write(lines)
		t.drawLocked()
	}
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

// flushLocked writes the held lines to their destinations. No region may be
// drawn; t.mu must be held.
func (t *Terminal) flushLocked() {
	for _, h := range t.held {
		dst := h.dst
		if dst == nil {
			dst = t.w
		}
		_, _ = dst.Write(h.data)
	}
	t.held = nil
}

// drawLocked draws the region rows, each cut to one column less than the
// terminal width so that none wraps. When the terminal height is known and the
// rows do not fit above the cursor line, the first height-2 rows are drawn
// followed by "… and K more", since rows scrolled off the screen could not be
// erased. t.mu must be held.
func (t *Terminal) drawLocked() {
	if t.region == nil || t.suspended {
		return
	}
	width, height := t.size()
	limit := max(width-1, 1)
	rows := t.region(limit + 1)
	if height > 0 && len(rows) > height-1 {
		keep := max(height-2, 0)
		rows = append(rows[:keep:keep], fmt.Sprintf("… and %d more", len(rows)-keep))
	}
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
