package rpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/piotaixr/syngoid/internal/zfs"
)

func mustRaw(t testing.TB, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestMessageRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	c := NewCodec(&buf)

	hello := Message{ID: 1, Method: MethodHello, Params: mustRaw(t, Hello{Protocol: 1, Version: "dev", BuildHash: "h", Debug: true})}
	if err := c.Write(hello); err != nil {
		t.Fatal(err)
	}
	const wantLine = `{"id":1,"method":"hello","params":{"protocol":1,"version":"dev","build_hash":"h","debug":true,"sudo":false}}` + "\n"
	if got := buf.String(); got != wantLine {
		t.Fatalf("hello line =\n%q\nwant\n%q", got, wantLine)
	}
	buf.Reset()

	msgs := []Message{
		hello,
		{ID: 2, Result: mustRaw(t, HelloReply{Protocol: 1, Version: "dev", BuildHash: "h", OS: "linux", Arch: "amd64", User: "u", UID: 1000, Groups: []string{"wheel"}, ZFSVersion: "2.2.0", Home: "/home/u"})},
		{ID: 3, Error: &Error{Code: CodeCommand, Message: "m", Stderr: "e\n", Argv: []string{"zfs", "list"}, ExitCode: 1}},
		{Event: &Event{Kind: "log", RequestID: 4, Level: "DEBUG", Msg: "zfs list"}},
		{Event: &Event{Kind: "progress", RequestID: 5, Bytes: 1 << 40}},
	}
	for _, m := range msgs {
		if err := c.Write(m); err != nil {
			t.Fatal(err)
		}
	}
	for i, want := range msgs {
		got, err := c.Read()
		if err != nil {
			t.Fatalf("message %d: %v", i, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("message %d = %+v, want %+v", i, got, want)
		}
	}
	if _, err := c.Read(); !errors.Is(err, io.EOF) {
		t.Errorf("Read at end = %v, want io.EOF", err)
	}
}

func TestParamsRoundTrip(t *testing.T) {
	in := SendParams{
		Spec:  zfs.SendSpec{Dataset: "tank/a", To: "s1", FromSnap: "s0", Intermediates: true, Options: []zfs.Opt{{Flag: 'p'}, {Flag: 'o', Value: "x=y"}}},
		Codec: "zstd",
	}
	var out SendParams
	if err := json.Unmarshal(mustRaw(t, in), &out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Errorf("round trip = %+v, want %+v", out, in)
	}
}

func TestCodecLargeMessage(t *testing.T) {
	snaps := make([]zfs.Snapshot, 300_000)
	for i := range snaps {
		snaps[i] = zfs.Snapshot{Name: fmt.Sprintf("syncoid_host_2026-10-21:12:00:%06d-GMT02:00", i), GUID: "1234567890123456789", CreateTxg: uint64(i), Creation: uint64(i)}
	}
	res := mustRaw(t, map[string][]zfs.Snapshot{"tank/big": snaps})
	if len(res) < 32<<20 {
		t.Fatalf("result is %d bytes, want at least 32 MiB", len(res))
	}
	var buf bytes.Buffer
	c := NewCodec(&buf)
	if err := c.Write(Message{ID: 7, Result: res}); err != nil {
		t.Fatal(err)
	}
	got, err := c.Read()
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != 7 || !bytes.Equal(got.Result, res) {
		t.Error("large result did not round-trip")
	}
}

func FuzzCodecRead(f *testing.F) {
	f.Add([]byte(`{"id":1,"method":"hello","params":{"protocol":1}}` + "\n"))
	f.Add([]byte(`{"event":{"kind":"log","msg":"x"}}` + "\n{"))
	f.Add([]byte("\x00\xff{{{\n\n[]"))
	f.Add([]byte(strings.Repeat("[", 100000)))
	f.Fuzz(func(t *testing.T, data []byte) {
		c := NewCodec(bytes.NewBuffer(data))
		for range 1000 {
			if _, err := c.Read(); err != nil {
				return
			}
		}
	})
}
