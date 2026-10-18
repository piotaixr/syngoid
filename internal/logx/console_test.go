package logx_test

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/piotaixr/syngoid/internal/logx"
)

func newBufLogger(cfg logx.Config, extra ...slog.Handler) (l *slog.Logger, stdout, stderr *bytes.Buffer) {
	stdout, stderr = &bytes.Buffer{}, &bytes.Buffer{}
	cfg.Stdout, cfg.Stderr = stdout, stderr
	return logx.New(cfg, extra...), stdout, stderr
}

func TestConsolePrefixes(t *testing.T) {
	l, stdout, stderr := newBufLogger(logx.Config{})
	l.Debug("d")
	l.Info("hello")
	l.Warn("w")
	logx.Critical(l, "c")
	if got, want := stdout.String(), "INFO: hello\n"; got != want {
		t.Errorf("stdout = %q, want %q", got, want)
	}
	if got, want := stderr.String(), "WARNING: w\nCRITICAL ERROR: c\n"; got != want {
		t.Errorf("stderr = %q, want %q", got, want)
	}

	l, stdout, stderr = newBufLogger(logx.Config{Debug: true})
	l.Debug("d")
	if got, want := stdout.String(), "DEBUG: d\n"; got != want {
		t.Errorf("debug stdout = %q, want %q", got, want)
	}
	if stderr.Len() != 0 {
		t.Errorf("debug stderr = %q, want empty", stderr.String())
	}
}

func TestConsoleQuiet(t *testing.T) {
	l, stdout, stderr := newBufLogger(logx.Config{Quiet: true})
	l.Info("hello")
	l.Warn("w")
	logx.Critical(l, "c")
	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want empty", stdout.String())
	}
	if got, want := stderr.String(), "CRITICAL ERROR: c\n"; got != want {
		t.Errorf("stderr = %q, want %q", got, want)
	}
}

func TestConsoleQuietDebug(t *testing.T) {
	l, stdout, stderr := newBufLogger(logx.Config{Quiet: true, Debug: true})
	l.Debug("d")
	l.Info("hello")
	l.Warn("w")
	logx.Critical(l, "c")
	if got, want := stdout.String(), "DEBUG: d\n"; got != want {
		t.Errorf("stdout = %q, want %q", got, want)
	}
	if got, want := stderr.String(), "CRITICAL ERROR: c\n"; got != want {
		t.Errorf("stderr = %q, want %q", got, want)
	}
}

func TestConsoleHostPrefix(t *testing.T) {
	l, stdout, _ := newBufLogger(logx.Config{Debug: true})
	l.Debug("zfs list", logx.KeyHost, "backup@nas", logx.KeyDataset, "tank/a", logx.KeyAction, "list")
	if got, want := stdout.String(), "DEBUG: [backup@nas] zfs list\n"; got != want {
		t.Errorf("stdout = %q, want %q", got, want)
	}

	// host carried by WithAttrs / With
	l, stdout, _ = newBufLogger(logx.Config{})
	l.With(logx.KeyHost, "h1").Info("hi")
	if got, want := stdout.String(), "INFO: [h1] hi\n"; got != want {
		t.Errorf("With stdout = %q, want %q", got, want)
	}
}

func TestConsoleMultiline(t *testing.T) {
	l, stdout, _ := newBufLogger(logx.Config{})
	l.Info("line1\nline2\n")
	l.Info("single\n\n")
	if got, want := stdout.String(), "INFO: line1\nline2\nINFO: single\n"; got != want {
		t.Errorf("stdout = %q, want %q", got, want)
	}
}

type recHandler struct {
	level   slog.Level
	records *[]slog.Record // shared with handlers derived via WithAttrs
	attrs   []slog.Attr
}

func newRecHandler(level slog.Level) *recHandler {
	return &recHandler{level: level, records: &[]slog.Record{}}
}

func (h *recHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= h.level }

func (h *recHandler) Handle(_ context.Context, r slog.Record) error {
	*h.records = append(*h.records, r)
	return nil
}

func (h *recHandler) WithAttrs(a []slog.Attr) slog.Handler {
	c := *h
	c.attrs = append(append([]slog.Attr(nil), h.attrs...), a...)
	return &c
}

func (h *recHandler) WithGroup(string) slog.Handler { return h }

func TestFanOut(t *testing.T) {
	extra := newRecHandler(slog.LevelWarn)
	l, stdout, stderr := newBufLogger(logx.Config{Quiet: true}, extra)
	l.Info("info")
	l.Warn("warn")
	logx.Critical(l, "crit")
	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want empty", stdout.String())
	}
	if got, want := stderr.String(), "CRITICAL ERROR: crit\n"; got != want {
		t.Errorf("stderr = %q, want %q", got, want)
	}
	// extra has its own Enabled check: it gets Warn and Critical even though console is quiet.
	if len(*extra.records) != 2 || (*extra.records)[0].Message != "warn" || (*extra.records)[1].Message != "crit" {
		t.Errorf("extra records = %v, want [warn crit]", *extra.records)
	}

	l, _, _ = newBufLogger(logx.Config{}, extra)
	*extra.records = nil
	l.Debug("dropped by everyone")
	if len(*extra.records) != 0 {
		t.Errorf("extra got a record nobody enabled: %v", *extra.records)
	}
}

func TestFanOutWithAttrs(t *testing.T) {
	extra := newRecHandler(slog.LevelDebug)
	l, stdout, _ := newBufLogger(logx.Config{}, extra)
	l.With(logx.KeyHost, "h2").Info("x")
	if got, want := stdout.String(), "INFO: [h2] x\n"; got != want {
		t.Errorf("stdout = %q, want %q", got, want)
	}
	if len(*extra.records) != 1 {
		t.Fatalf("extra records = %d, want 1", len(*extra.records))
	}
}
