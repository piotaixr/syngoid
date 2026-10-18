// Package logx provides syncoid-style console logging on top of log/slog.
package logx

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
)

// LevelCritical is the slog level used for syncoid's "CRITICAL ERROR" messages.
const LevelCritical = slog.LevelError

// Attribute keys understood by the console handler and shared by other handlers.
const (
	KeyHost    = "host"
	KeyDataset = "dataset"
	KeyAction  = "action"
)

// Config configures the console handler.
type Config struct {
	// Stdout receives Debug and Info messages; Stderr receives Warn and Critical ones.
	Stdout, Stderr io.Writer
	// Quiet suppresses everything below Critical.
	Quiet bool
	// Debug enables Debug messages.
	Debug bool
}

// New returns a logger that writes syncoid-style lines to the console and fans
// every record out to each extra handler that enables its level.
func New(cfg Config, extra ...slog.Handler) *slog.Logger {
	var h slog.Handler = newConsole(cfg)
	if len(extra) > 0 {
		h = &fanOut{handlers: append([]slog.Handler{h}, extra...)}
	}
	return slog.New(h)
}

// Critical logs msg at LevelCritical.
func Critical(l *slog.Logger, msg string, args ...any) {
	l.Log(context.Background(), LevelCritical, msg, args...)
}

type console struct {
	mu     *sync.Mutex // shared by handlers derived via WithAttrs
	stdout io.Writer
	stderr io.Writer
	min    slog.Level
	host   string
}

func newConsole(cfg Config) *console {
	minLevel := slog.LevelInfo
	switch {
	case cfg.Quiet:
		minLevel = LevelCritical
	case cfg.Debug:
		minLevel = slog.LevelDebug
	}
	return &console{mu: &sync.Mutex{}, stdout: cfg.Stdout, stderr: cfg.Stderr, min: minLevel}
}

func (c *console) Enabled(_ context.Context, l slog.Level) bool { return l >= c.min }

func (c *console) Handle(_ context.Context, r slog.Record) error {
	host := c.host
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == KeyHost {
			host = a.Value.Resolve().String()
		}
		return true
	})

	var prefix string
	w := c.stdout
	switch {
	case r.Level >= LevelCritical:
		prefix, w = "CRITICAL ERROR: ", c.stderr
	case r.Level >= slog.LevelWarn:
		prefix, w = "WARNING: ", c.stderr
	case r.Level >= slog.LevelInfo:
		prefix = "INFO: "
	default:
		prefix = "DEBUG: "
	}

	var b strings.Builder
	b.WriteString(prefix)
	if host != "" {
		b.WriteString("[" + host + "] ")
	}
	b.WriteString(strings.TrimRight(r.Message, "\n"))
	b.WriteByte('\n')

	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := io.WriteString(w, b.String())
	return err
}

func (c *console) WithAttrs(attrs []slog.Attr) slog.Handler {
	nc := *c
	for _, a := range attrs {
		if a.Key == KeyHost {
			nc.host = a.Value.Resolve().String()
		}
	}
	return &nc
}

// WithGroup returns c unchanged: console output never prints attributes other than the host.
func (c *console) WithGroup(string) slog.Handler { return c }

type fanOut struct {
	handlers []slog.Handler
}

func (f *fanOut) Enabled(ctx context.Context, l slog.Level) bool {
	for _, h := range f.handlers {
		if h.Enabled(ctx, l) {
			return true
		}
	}
	return false
}

func (f *fanOut) Handle(ctx context.Context, r slog.Record) error {
	var errs []error
	for _, h := range f.handlers {
		if h.Enabled(ctx, r.Level) {
			errs = append(errs, h.Handle(ctx, r.Clone()))
		}
	}
	return errors.Join(errs...)
}

func (f *fanOut) WithAttrs(attrs []slog.Attr) slog.Handler {
	hs := make([]slog.Handler, len(f.handlers))
	for i, h := range f.handlers {
		hs[i] = h.WithAttrs(attrs)
	}
	return &fanOut{handlers: hs}
}

func (f *fanOut) WithGroup(name string) slog.Handler {
	hs := make([]slog.Handler, len(f.handlers))
	for i, h := range f.handlers {
		hs[i] = h.WithGroup(name)
	}
	return &fanOut{handlers: hs}
}
