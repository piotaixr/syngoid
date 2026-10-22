// Package rpc defines the wire protocol between the syngoid initiator and
// its agent: JSON-lines control messages on one yamux stream (§7.1).
package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/piotaixr/syngoid/internal/stream"
	"github.com/piotaixr/syngoid/internal/zfs"
)

// Method names. The Endpoint operations come first; Cancel and Shutdown
// are protocol-level.
const (
	MethodHello          = "hello"
	MethodInfo           = "info"
	MethodListTree       = "list_tree"
	MethodListSnapshots  = "list_snapshots"
	MethodListBookmarks  = "list_bookmarks"
	MethodGetProps       = "get_props"
	MethodLocalProps     = "local_props"
	MethodExists         = "exists"
	MethodSnapshot       = "snapshot"
	MethodDestroy        = "destroy"
	MethodDestroyDataset = "destroy_dataset"
	MethodHold           = "hold"
	MethodRelease        = "release"
	MethodBookmark       = "bookmark"
	MethodAbortResume    = "abort_resume"
	MethodSendEstimate   = "send_estimate"
	MethodReceiveBusy    = "receive_busy"
	MethodSend           = "send"
	MethodReceive        = "receive"
	MethodAllow          = "allow"
	// MethodCancel cancels the agent-side context of request CancelParams.ID.
	MethodCancel = "cancel"
	// MethodShutdown asks the agent to finish and exit.
	MethodShutdown = "shutdown"
)

// Hello is the params of the first request (id 1). Hello and HelloReply are
// frozen: fields may be added, never renamed or removed.
type Hello struct {
	Protocol  int    `json:"protocol"`
	Version   string `json:"version"`
	BuildHash string `json:"build_hash"`
	Debug     bool   `json:"debug"`
	Sudo      bool   `json:"sudo"`
}

// HelloReply is the result of the hello request.
type HelloReply struct {
	Protocol   int      `json:"protocol"`
	Version    string   `json:"version"`
	BuildHash  string   `json:"build_hash"`
	ExeSHA256  string   `json:"exe_sha256"`
	OS         string   `json:"os"`
	Arch       string   `json:"arch"`
	User       string   `json:"user"`
	UID        int      `json:"uid"`
	Groups     []string `json:"groups"`
	ZFSVersion string   `json:"zfs_version"`
	Home       string   `json:"home"`
}

// Message is the single envelope of every control line. A message with
// Method is a request, one with Event is an event, anything else is a
// response (Result or Error) to the request with the same ID.
type Message struct {
	ID     uint64          `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *Error          `json:"error,omitempty"`
	Event  *Event          `json:"event,omitempty"`
}

// Event kinds.
const (
	EventLog      = "log"
	EventProgress = "progress"
)

// Event is an agent-initiated notification. Level is a slog level name
// ("DEBUG", "INFO", "WARN", "ERROR") and Msg the log line for Kind "log";
// Bytes is the transferred byte count for Kind "progress". RequestID ties
// the event to the request it belongs to, when there is one.
type Event struct {
	Kind      string `json:"kind"`
	RequestID uint64 `json:"request_id,omitempty"`
	Level     string `json:"level,omitempty"`
	Msg       string `json:"msg,omitempty"`
	Bytes     int64  `json:"bytes,omitempty"`
}

// Error codes.
const (
	CodeCommand       = "command"
	CodeInvalid       = "invalid"
	CodeCanceled      = "canceled"
	CodeUnknownMethod = "unknown_method"
	CodeInternal      = "internal"
)

// Error is the error member of a response. For CodeCommand, Stderr, Argv
// and ExitCode carry the failed command; for CodeInvalid, Argv holds the
// InvalidError's Field and Value.
type Error struct {
	Code     string   `json:"code"`
	Message  string   `json:"message"`
	Stderr   string   `json:"stderr,omitempty"`
	Argv     []string `json:"argv,omitempty"`
	ExitCode int      `json:"exit_code,omitempty"`
}

// InvalidError reports a request parameter that failed validation.
type InvalidError struct {
	Field, Value string
}

func (e *InvalidError) Error() string {
	return "invalid " + e.Field + ": " + strconv.Quote(e.Value)
}

// EncodeError maps err to its wire form.
func EncodeError(err error) *Error {
	var ce *zfs.CommandError
	var ie *InvalidError
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return &Error{Code: CodeCanceled, Message: err.Error()}
	case errors.As(err, &ce):
		return &Error{Code: CodeCommand, Message: err.Error(), Stderr: ce.Stderr, Argv: ce.Argv, ExitCode: ce.ExitCode}
	case errors.As(err, &ie):
		return &Error{Code: CodeInvalid, Message: err.Error(), Argv: []string{ie.Field, ie.Value}}
	}
	return &Error{Code: CodeInternal, Message: err.Error()}
}

// Err rebuilds the Go error: a *zfs.CommandError (with Kind re-derived by
// zfs.Classify, so errors.Is sees the zfs sentinels), an *InvalidError,
// context.Canceled, or a plain error carrying Message.
func (e *Error) Err() error {
	switch e.Code {
	case CodeCommand:
		return &zfs.CommandError{Argv: e.Argv, ExitCode: e.ExitCode, Stderr: e.Stderr, Kind: zfs.Classify(e.Stderr)}
	case CodeInvalid:
		if len(e.Argv) == 2 {
			return &InvalidError{Field: e.Argv[0], Value: e.Argv[1]}
		}
	case CodeCanceled:
		return context.Canceled
	}
	return errors.New(e.Message)
}

// DatasetParams is the params of list_tree, list_snapshots, list_bookmarks,
// exists, local_props, allow, abort_resume and destroy_dataset.
type DatasetParams struct {
	Dataset   string `json:"dataset"`
	Recursive bool   `json:"recursive"`
}

// GetPropsParams is the params of get_props.
type GetPropsParams struct {
	Dataset string   `json:"dataset"`
	Props   []string `json:"props"`
}

// SnapshotParams is the params of snapshot; Names are full snapshot names.
type SnapshotParams struct {
	Names []string `json:"names"`
}

// DestroyParams is the params of destroy.
type DestroyParams struct {
	Dataset string   `json:"dataset"`
	Snaps   []string `json:"snaps"`
}

// HoldParams is the params of hold and release.
type HoldParams struct {
	Tag      string `json:"tag"`
	Snapshot string `json:"snapshot"`
}

// BookmarkParams is the params of bookmark.
type BookmarkParams struct {
	Snapshot string `json:"snapshot"`
	Bookmark string `json:"bookmark"`
}

// SendEstimateParams is the params of send_estimate.
type SendEstimateParams struct {
	Spec zfs.SendSpec `json:"spec"`
}

// ReceiveBusyParams is the params of receive_busy.
type ReceiveBusyParams struct {
	Datasets []string `json:"datasets"`
}

// SendParams is the params of send; the stream travels on a data stream.
type SendParams struct {
	Spec  zfs.SendSpec `json:"spec"`
	Codec stream.Codec `json:"codec"`
}

// ReceiveParams is the params of receive; the stream travels on a data
// stream.
type ReceiveParams struct {
	Spec  zfs.RecvSpec `json:"spec"`
	Codec stream.Codec `json:"codec"`
}

// CancelParams is the params of cancel.
type CancelParams struct {
	ID uint64 `json:"id"`
}
