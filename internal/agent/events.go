package agent

import (
	"context"
	"log/slog"

	"github.com/piotaixr/syngoid/internal/rpc"
)

// eventHandler is a slog.Handler that sends every record it enables to the
// initiator as a log Event on the control stream. Only the message travels:
// attributes are dropped, as the initiator adds its own host prefix.
type eventHandler struct {
	codec *rpc.Codec
	min   slog.Level
}

// newEventLogger returns a logger that emits log events at level min and up.
func newEventLogger(codec *rpc.Codec, min slog.Level) *slog.Logger {
	return slog.New(&eventHandler{codec: codec, min: min})
}

func (h *eventHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= h.min }

func (h *eventHandler) Handle(_ context.Context, r slog.Record) error {
	// A failed write means the session is gone; there is nobody to tell.
	return h.codec.Write(rpc.Message{Event: &rpc.Event{
		Kind:  rpc.EventLog,
		Level: levelName(r.Level),
		Msg:   r.Message,
	}})
}

func (h *eventHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *eventHandler) WithGroup(string) slog.Handler      { return h }

// levelName maps l to one of the four Event level names, rounding
// intermediate levels down.
func levelName(l slog.Level) string {
	switch {
	case l >= slog.LevelError:
		return "ERROR"
	case l >= slog.LevelWarn:
		return "WARN"
	case l >= slog.LevelInfo:
		return "INFO"
	}
	return "DEBUG"
}
