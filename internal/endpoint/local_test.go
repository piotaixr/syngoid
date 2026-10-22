package endpoint_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"

	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/stream"
	"github.com/piotaixr/syngoid/internal/zfs"
	"github.com/piotaixr/syngoid/internal/zfs/zfstest"
)

var _ endpoint.Endpoint = (*endpoint.Local)(nil)

func newLocal(fe *zfstest.FakeExec) *endpoint.Local {
	return endpoint.NewLocal(&zfs.Client{Exec: fe}, fe)
}

func TestLocalInfo(t *testing.T) {
	fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{
		"id -un":      {Stdout: "backup\n"},
		"id -u":       {Stdout: "1001\n"},
		"id -Gn":      {Stdout: "backup zfsadm\n"},
		"zfs version": {Stdout: "zfs-2.3.4-1\nzfs-kmod-2.3.4-1\n"},
	}}
	l := newLocal(fe)
	if l.Label() != "" {
		t.Errorf("Label = %q, want empty", l.Label())
	}
	got, err := l.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := endpoint.HostInfo{OS: runtime.GOOS, Arch: runtime.GOARCH, User: "backup", UID: 1001, Groups: []string{"backup", "zfsadm"}, ZFSVersion: "zfs-2.3.4-1"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Info = %+v, want %+v", got, want)
	}

	delete(fe.Responses, "zfs version")
	fe.Responses["zfs version"] = zfstest.Response{Stderr: "boom\n", Exit: 1}
	got, err = l.Info(context.Background())
	if err != nil {
		t.Fatalf("failing zfs version must not error: %v", err)
	}
	if got.ZFSVersion != "" {
		t.Errorf("ZFSVersion = %q, want empty", got.ZFSVersion)
	}
}

func TestLocalInfoSudoOnlyForZFS(t *testing.T) {
	fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{
		"id -un":              {Stdout: "backup\n"},
		"id -u":               {Stdout: "1001\n"},
		"id -Gn":              {Stdout: "backup\n"},
		"sudo -n zfs version": {Stdout: "zfs-2.3.4-1\n"},
	}}
	l := endpoint.NewLocal(&zfs.Client{Exec: fe, Sudo: true}, fe)
	got, err := l.Info(context.Background())
	if err != nil || got.ZFSVersion != "zfs-2.3.4-1" {
		t.Fatalf("Info = %+v, err %v", got, err)
	}
}

func TestLocalInfoIDError(t *testing.T) {
	fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{
		"id -un": {Stderr: "nope\n", Exit: 1},
	}}
	if _, err := newLocal(fe).Info(context.Background()); err == nil {
		t.Fatal("want error when id fails")
	}
}

func TestLocalAllow(t *testing.T) {
	fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{
		"zfs allow tank/a": {Stdout: "---- Permissions on tank/a ----\n"},
	}}
	out, err := newLocal(fe).Allow(context.Background(), "tank/a")
	if err != nil || out != "---- Permissions on tank/a ----\n" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

func TestLocalReceiveBusy(t *testing.T) {
	fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{
		"ps -wwAo args=": {Stdout: "/sbin/init\nzfs receive -s -F tank/b\nzfs recv tank/c/d\nsshd: foo\n"},
	}}
	got, err := newLocal(fe).ReceiveBusy(context.Background(), []string{"tank/b", "tank/c", "tank/c/d"})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"tank/b": true, "tank/c": false, "tank/c/d": true}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ReceiveBusy = %v, want %v", got, want)
	}
	n := 0
	for _, c := range fe.Calls {
		if c[0] == "ps" {
			n++
		}
	}
	if n != 1 {
		t.Errorf("ps called %d times, want 1", n)
	}
}

func TestLocalReceiveBusyLongLine(t *testing.T) {
	// FreeBSD ps without a tty cuts lines at 79 columns unless -ww: the
	// unlimited-width form comes first, so a long receive line is seen whole.
	ds := "tank/" + strings.Repeat("long-component/", 15) + "leaf"
	fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{
		"ps -wwAo args=": {Stdout: "zfs receive -s -F " + ds + "\n"},
		"ps -Ao args=":   {Stdout: ("zfs receive -s -F " + ds)[:79] + "\n"},
	}}
	got, err := newLocal(fe).ReceiveBusy(context.Background(), []string{ds})
	if err != nil {
		t.Fatal(err)
	}
	if !got[ds] {
		t.Errorf("long receive line missed: %v", got)
	}
	if len(fe.Calls) != 1 || strings.Join(fe.Calls[0], " ") != "ps -wwAo args=" {
		t.Errorf("calls = %q, want only ps -wwAo args=", fe.Calls)
	}
}

func TestLocalReceiveBusyError(t *testing.T) {
	fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{
		"ps -wwAo args=": {Stderr: "x\n", Exit: 1},
		"ps -Ao args=":   {Stderr: "x\n", Exit: 1},
		"ps -eo args":    {Stderr: "x\n", Exit: 1},
	}}
	if _, err := newLocal(fe).ReceiveBusy(context.Background(), []string{"tank/b"}); err == nil {
		t.Fatal("want error")
	}
}

func TestLocalReceiveBusyPsFallbackNoWide(t *testing.T) {
	// illumos ps has no -w: the second form, still without a header.
	fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{
		"ps -wwAo args=": {Stderr: "ps: illegal option -- w\n", Exit: 2},
		"ps -Ao args=":   {Stdout: "zfs receive -s tank/b\n"},
	}}
	got, err := newLocal(fe).ReceiveBusy(context.Background(), []string{"tank/b", "tank/c"})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"tank/b": true, "tank/c": false}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ReceiveBusy = %v, want %v", got, want)
	}
	if len(fe.Calls) != 2 {
		t.Errorf("calls = %q, want two", fe.Calls)
	}
}

func TestLocalReceiveBusyPsFallback(t *testing.T) {
	fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{
		"ps -wwAo args=": {Stderr: "ps: illegal option -- w\n", Exit: 1},
		"ps -Ao args=":   {Stderr: "ps: illegal option -- o\n", Exit: 1},
		"ps -eo args":    {Stdout: "ARGS\nzfs receive -s tank/b\n"},
	}}
	got, err := newLocal(fe).ReceiveBusy(context.Background(), []string{"tank/b", "tank/c"})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"tank/b": true, "tank/c": false}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ReceiveBusy = %v, want %v", got, want)
	}
}

func TestLocalReceiveBusyPsFallbackAllFail(t *testing.T) {
	fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{
		"ps -wwAo args=": {Stderr: "first form failed\n", Exit: 1},
		"ps -Ao args=":   {Stderr: "second form failed\n", Exit: 1},
		"ps -eo args":    {Stderr: "third form failed\n", Exit: 1},
	}}
	_, err := newLocal(fe).ReceiveBusy(context.Background(), []string{"tank/b"})
	if err == nil {
		t.Fatal("want error")
	}
	for _, want := range []string{"first form failed", "second form failed", "third form failed"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lacks %q", err, want)
		}
	}
}

func TestLocalReceiveBusyPsFallbackDropsHeader(t *testing.T) {
	// The header is the first line whatever it says: a dataset named like it
	// must not be reported busy.
	fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{
		"ps -wwAo args=": {Exit: 1},
		"ps -Ao args=":   {Exit: 1},
		"ps -eo args":    {Stdout: "zfs receive tank/h\n/sbin/init\n"},
	}}
	got, err := newLocal(fe).ReceiveBusy(context.Background(), []string{"tank/h"})
	if err != nil {
		t.Fatal(err)
	}
	if got["tank/h"] {
		t.Errorf("header line was scanned: %v", got)
	}
}

func TestLocalReceiveBusyCancelStopsFallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{
		"ps -wwAo args=": {Block: true},
	}}
	if _, err := newLocal(fe).ReceiveBusy(ctx, []string{"tank/b"}); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if len(fe.Calls) != 1 {
		t.Errorf("calls after cancel = %q, want one", fe.Calls)
	}
}

// TestReceiveBusyCaptured replays the ps output captured by
// scripts/ci/capture-fixtures.sh (testdata/<os>/ps_*.txt) while a process
// showed "/bin/sh .../b/zfs receive -s <218-character target>". A capture
// with a .stderr file failed with exit status 1 (illumos ps has no -w).
// Each row starts at one ps form; the forms before it fail.
func TestReceiveBusyCaptured(t *testing.T) {
	target := "syngoidfx/busy"
	for c := 1; c <= 6; c++ {
		target += fmt.Sprintf("/component-%d-of-a-long-target-name", c)
	}
	forms := []struct{ argv, file string }{
		{"ps -wwAo args=", "ps_ww.txt"},
		{"ps -Ao args=", "ps_A.txt"},
		{"ps -eo args", "ps_e.txt"},
	}
	// want[i] is the verdict when the first form tried is forms[i].
	tests := []struct {
		os   string
		want [3]bool
	}{
		{"linux", [3]bool{true, true, true}},
		// FreeBSD 15.1 ps -A printed the whole line to a non-terminal even
		// without -ww; ps -e there means "show the environment", not "every
		// process", so the receive is not listed at all.
		{"freebsd", [3]bool{true, true, false}},
		// Known limitation (spec §14 item 4): -ww fails, and -A and -e keep
		// only the first 80 bytes of the arguments, which cuts the target
		// to "syngoidfx/busy/component-1-of-a-lo".
		{"illumos", [3]bool{false, false, false}},
	}
	for _, tt := range tests {
		t.Run(tt.os, func(t *testing.T) {
			for start := range forms {
				resp := map[string]zfstest.Response{}
				for i, f := range forms {
					resp[f.argv] = capturedPs(t, tt.os, f.file)
					if i < start {
						resp[f.argv] = zfstest.Response{Stderr: "forced failure\n", Exit: 1}
					}
				}
				fe := &zfstest.FakeExec{Responses: resp}
				got, err := newLocal(fe).ReceiveBusy(context.Background(), []string{target, "syngoidfx/busy"})
				if err != nil {
					t.Fatalf("from %s: %v", forms[start].argv, err)
				}
				want := map[string]bool{target: tt.want[start], "syngoidfx/busy": false}
				if !reflect.DeepEqual(got, want) {
					t.Errorf("from %s: ReceiveBusy = %v, want %v", forms[start].argv, got, want)
				}
			}
		})
	}
}

// capturedPs returns the captured result of one ps form: its stdout, or exit
// status 1 with its stderr when the capture failed.
func capturedPs(t *testing.T, osName, file string) zfstest.Response {
	t.Helper()
	dir := filepath.Join("testdata", osName)
	if stderr, err := os.ReadFile(filepath.Join(dir, file+".stderr")); err == nil {
		return zfstest.Response{Stderr: string(stderr), Exit: 1}
	}
	stdout, err := os.ReadFile(filepath.Join(dir, file))
	if err != nil {
		t.Fatal(err)
	}
	return zfstest.Response{Stdout: string(stdout)}
}

func payload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

func TestLocalSendCodecAndProgress(t *testing.T) {
	data := payload(1 << 20)
	spec := zfs.SendSpec{Dataset: "tank/a", To: "s1"}
	key := "zfs send tank/a@s1"
	fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{key: {StdoutBytes: data}}}
	var out bytes.Buffer
	var last int64
	err := newLocal(fe).Send(context.Background(), spec, endpoint.SendOpts{
		Codec:    stream.Zstd,
		Progress: func(n int64) { last = n },
	}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if last != 1<<20 {
		t.Errorf("final progress = %d, want %d", last, 1<<20)
	}
	if bytes.Equal(out.Bytes(), data) {
		t.Fatal("output was not compressed")
	}
	dec, err := zstd.NewReader(&out)
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	got, err := io.ReadAll(dec)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("decoded mismatch: err=%v len=%d", err, len(got))
	}
}

func TestLocalSendNoneNoProgress(t *testing.T) {
	data := []byte("hello stream")
	fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{"zfs send tank/a@s1": {StdoutBytes: data}}}
	var out bytes.Buffer
	if err := newLocal(fe).Send(context.Background(), zfs.SendSpec{Dataset: "tank/a", To: "s1"}, endpoint.SendOpts{Codec: stream.None}, &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Errorf("out = %q", out.Bytes())
	}
}

func TestLocalSendError(t *testing.T) {
	fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{"zfs send tank/a@s1": {Stderr: "boom\n", Exit: 1}}}
	err := newLocal(fe).Send(context.Background(), zfs.SendSpec{Dataset: "tank/a", To: "s1"}, endpoint.SendOpts{Codec: stream.None}, io.Discard)
	var ce *zfs.CommandError
	if !errors.As(err, &ce) {
		t.Fatalf("err = %v, want *CommandError", err)
	}
}

func TestLocalReceiveDecodes(t *testing.T) {
	data := payload(300 << 10)
	var enc bytes.Buffer
	w, err := stream.NewEncoder(stream.Zstd, &enc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	key := "zfs receive -F tank/b"
	fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{key: {}}}
	err = newLocal(fe).Receive(context.Background(), zfs.RecvSpec{Dataset: "tank/b", Args: []string{"-F"}}, stream.Zstd, &enc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fe.Stdin[key], data) {
		t.Errorf("stdin len=%d, want decoded %d bytes", len(fe.Stdin[key]), len(data))
	}
}

func TestLocalReceiveDecodeError(t *testing.T) {
	key := "zfs receive tank/b"
	fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{key: {}}}
	err := newLocal(fe).Receive(context.Background(), zfs.RecvSpec{Dataset: "tank/b", Args: []string{}}, stream.Gzip, bytes.NewReader([]byte("definitely not gzip")))
	if !errors.Is(err, gzip.ErrHeader) {
		t.Fatalf("err = %v, want gzip.ErrHeader", err)
	}
}

func TestLocalReceiveError(t *testing.T) {
	key := "zfs receive tank/b"
	fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{key: {Stderr: "bad\n", Exit: 1}}}
	err := newLocal(fe).Receive(context.Background(), zfs.RecvSpec{Dataset: "tank/b", Args: []string{}}, stream.None, bytes.NewReader([]byte("x")))
	var ce *zfs.CommandError
	if !errors.As(err, &ce) {
		t.Fatalf("err = %v, want *CommandError", err)
	}
}

// endless is an io.Reader producing zeros forever.
type endless struct{}

func (endless) Read(p []byte) (int, error) { return len(p), nil }

func TestLocalReceiveFailsWithoutDrainingStdin(t *testing.T) {
	key := "zfs receive tank/b"
	for range 50 {
		fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{
			key: {Stderr: "cannot receive: bad stream\n", Exit: 1, NoDrainStdin: true},
		}}
		err := newLocal(fe).Receive(context.Background(), zfs.RecvSpec{Dataset: "tank/b"}, stream.None, endless{})
		var ce *zfs.CommandError
		if !errors.As(err, &ce) || ce.ExitCode != 1 {
			t.Fatalf("err = %v, want the *CommandError from zfs receive", err)
		}
	}
}

func TestLocalReceiveSucceedsWithoutDrainingStdin(t *testing.T) {
	key := "zfs receive tank/b"
	for range 50 {
		fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{key: {NoDrainStdin: true}}}
		if err := newLocal(fe).Receive(context.Background(), zfs.RecvSpec{Dataset: "tank/b"}, stream.None, endless{}); err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
	}
}

// blockedWriter records writes and then blocks forever (until release).
type blockedWriter struct {
	mu      sync.Mutex
	writes  int
	release chan struct{}
}

func (b *blockedWriter) Write(p []byte) (int, error) {
	b.mu.Lock()
	b.writes++
	b.mu.Unlock()
	<-b.release
	return len(p), nil
}

func TestLocalSendFailureWritesNothingAndDoesNotBlock(t *testing.T) {
	fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{
		"zfs send tank/a@s1": {StdoutBytes: payload(4096), Stderr: "cannot send\n", Exit: 1},
	}}
	w := &blockedWriter{release: make(chan struct{})}
	defer close(w.release)
	done := make(chan error, 1)
	go func() {
		done <- newLocal(fe).Send(context.Background(), zfs.SendSpec{Dataset: "tank/a", To: "s1"}, endpoint.SendOpts{Codec: stream.Zstd}, w)
	}()
	select {
	case err := <-done:
		var ce *zfs.CommandError
		if !errors.As(err, &ce) {
			t.Fatalf("err = %v, want *CommandError", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Send blocked on a stalled consumer after zfs send failed")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.writes != 0 {
		t.Errorf("w received %d writes after failure, want 0", w.writes)
	}
}

// argvExec runs argv through zfs.OSExec in place of every command.
type argvExec []string

func (a argvExec) Run(ctx context.Context, c zfs.Cmd) error {
	c.Argv = a
	return zfs.OSExec{}.Run(ctx, c)
}

func TestLocalReceiveFailsPromptlyWhileInputBlocks(t *testing.T) {
	ex := argvExec{"sh", "-c", "echo 'cannot receive: failed' >&2; exit 1"}
	l := endpoint.NewLocal(&zfs.Client{Exec: ex}, ex)
	in, feed := io.Pipe() // never written: the sender is stalled
	defer func() { _ = feed.Close() }()
	done := make(chan error, 1)
	go func() {
		done <- l.Receive(context.Background(), zfs.RecvSpec{Dataset: "tank/b"}, stream.None, in)
	}()
	select {
	case err := <-done:
		var ce *zfs.CommandError
		if !errors.As(err, &ce) || ce.ExitCode != 1 {
			t.Fatalf("err = %v, want the *CommandError from zfs receive", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Receive waited for the blocked input after zfs receive failed")
	}
}
