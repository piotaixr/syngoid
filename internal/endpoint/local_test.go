package endpoint_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"runtime"
	"testing"

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
		"ps -Ao args=": {Stdout: "/sbin/init\nzfs receive -s -F tank/b\nzfs recv tank/c/d\nsshd: foo\n"},
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

func TestLocalReceiveBusyError(t *testing.T) {
	fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{
		"ps -Ao args=": {Stderr: "x\n", Exit: 1},
	}}
	if _, err := newLocal(fe).ReceiveBusy(context.Background(), []string{"tank/b"}); err == nil {
		t.Fatal("want error")
	}
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
	if err == nil {
		t.Fatal("want decode error")
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
