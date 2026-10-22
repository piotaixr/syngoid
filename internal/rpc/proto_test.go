package rpc

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/piotaixr/syngoid/internal/zfs"
)

func TestErrorMapping(t *testing.T) {
	t.Run("command", func(t *testing.T) {
		in := &zfs.CommandError{
			Argv:     []string{"zfs", "receive", "tank/b"},
			ExitCode: 1,
			Stderr:   "cannot receive incremental stream: destination tank/b contains partially-complete state from \"zfs receive -s\".\n",
		}
		e := EncodeError(in)
		if e.Code != CodeCommand {
			t.Fatalf("Code = %q, want %q", e.Code, CodeCommand)
		}
		var got *zfs.CommandError
		if !errors.As(e.Err(), &got) {
			t.Fatalf("Err() = %T, want *zfs.CommandError", e.Err())
		}
		if !reflect.DeepEqual(got.Argv, in.Argv) || got.ExitCode != in.ExitCode || got.Stderr != in.Stderr {
			t.Errorf("Err() = %+v, want Argv/ExitCode/Stderr of %+v", got, in)
		}
		if !errors.Is(e.Err(), zfs.ErrPartialState) {
			t.Error("Err() does not match zfs.ErrPartialState")
		}
	})
	t.Run("wrapped command", func(t *testing.T) {
		in := fmt.Errorf("receiving: %w", &zfs.CommandError{Argv: []string{"zfs", "list"}, ExitCode: 2, Stderr: "x"})
		if e := EncodeError(in); e.Code != CodeCommand {
			t.Errorf("Code = %q, want %q", e.Code, CodeCommand)
		}
	})
	for _, in := range []error{context.Canceled, context.DeadlineExceeded, fmt.Errorf("x: %w", context.Canceled)} {
		e := EncodeError(in)
		if e.Code != CodeCanceled {
			t.Errorf("EncodeError(%v).Code = %q, want %q", in, e.Code, CodeCanceled)
		}
		if !errors.Is(e.Err(), context.Canceled) {
			t.Errorf("EncodeError(%v).Err() = %v, want context.Canceled", in, e.Err())
		}
	}
	t.Run("invalid", func(t *testing.T) {
		e := EncodeError(&InvalidError{Field: "dataset", Value: "-x"})
		if e.Code != CodeInvalid {
			t.Fatalf("Code = %q, want %q", e.Code, CodeInvalid)
		}
		var got *InvalidError
		if !errors.As(e.Err(), &got) || got.Field != "dataset" || got.Value != "-x" {
			t.Fatalf("Err() = %#v, want InvalidError{dataset, -x}", e.Err())
		}
		if want := `invalid dataset: "-x"`; got.Error() != want {
			t.Errorf("Error() = %q, want %q", got.Error(), want)
		}
	})
	t.Run("internal", func(t *testing.T) {
		e := EncodeError(errors.New("boom"))
		if e.Code != CodeInternal || e.Err().Error() != "boom" {
			t.Errorf("got code %q err %q, want internal/boom", e.Code, e.Err())
		}
	})
	t.Run("unknown method", func(t *testing.T) {
		e := &Error{Code: CodeUnknownMethod, Message: "nope"}
		if e.Err() == nil || e.Err().Error() != "nope" {
			t.Errorf("Err() = %v, want an error saying nope", e.Err())
		}
	})
}
