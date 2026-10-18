package zfs_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/piotaixr/syngoid/internal/zfs"
	"github.com/piotaixr/syngoid/internal/zfs/zfstest"
)

func mustOpts(t *testing.T, s string) []zfs.Opt {
	t.Helper()
	o, err := zfs.ParseOpts(s)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func TestSendArgs(t *testing.T) {
	tests := []struct {
		name string
		spec zfs.SendSpec
		opts string
		want []string
	}{
		{"full", zfs.SendSpec{Dataset: "tank/a", To: "s1"}, "Lc", []string{"send", "-L", "-c", "tank/a@s1"}},
		{"incremental", zfs.SendSpec{Dataset: "tank/a", FromSnap: "s1", To: "s2", Intermediates: true}, "", []string{"send", "-I", "tank/a@s1", "tank/a@s2"}},
		{"incremental no intermediates", zfs.SendSpec{Dataset: "tank/a", FromSnap: "s1", To: "s2"}, "", []string{"send", "-i", "tank/a@s1", "tank/a@s2"}},
		{"bookmark", zfs.SendSpec{Dataset: "tank/a", FromBookmark: "b1", To: "s2"}, "LR", []string{"send", "-L", "-i", "tank/a#b1", "tank/a@s2"}},
		{"bookmark ignores intermediates", zfs.SendSpec{Dataset: "tank/a", FromBookmark: "b1", To: "s2", Intermediates: true}, "", []string{"send", "-i", "tank/a#b1", "tank/a@s2"}},
		{"clone", zfs.SendSpec{Dataset: "tank/a", FromOrigin: "tank/o@s0", To: "s1"}, "", []string{"send", "-i", "tank/o@s0", "tank/a@s1"}},
		{"resume", zfs.SendSpec{ResumeToken: "tok"}, "Pv", []string{"send", "-P", "-v", "-t", "tok"}},
		{"resume drops stream-only", zfs.SendSpec{ResumeToken: "tok"}, "LPc", []string{"send", "-P", "-t", "tok"}},
		{"stream keeps R", zfs.SendSpec{Dataset: "tank/a", To: "s1"}, "R", []string{"send", "-R", "tank/a@s1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.spec.Options = mustOpts(t, tt.opts)
			if got := zfs.SendArgs(tt.spec); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SendArgs = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSendArgsNameWithSpace(t *testing.T) {
	got := zfs.SendArgs(zfs.SendSpec{Dataset: "tank/my data", To: "s1"})
	want := []string{"send", "tank/my data@s1"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestEstimateArgs(t *testing.T) {
	tests := []struct {
		name string
		spec zfs.SendSpec
		opts string
		want []string
	}{
		{"incremental", zfs.SendSpec{Dataset: "tank/a", FromSnap: "s1", To: "s2"}, "", []string{"send", "-nvP", "-i", "tank/a@s1", "tank/a@s2"}},
		{"incremental intermediates", zfs.SendSpec{Dataset: "tank/a", FromSnap: "s1", To: "s2", Intermediates: true}, "", []string{"send", "-nvP", "-I", "tank/a@s1", "tank/a@s2"}},
		{"resume", zfs.SendSpec{ResumeToken: "tok"}, "", []string{"send", "-nvP", "-t", "tok"}},
		{"resume keeps only V e", zfs.SendSpec{ResumeToken: "tok"}, "PVev", []string{"send", "-V", "-e", "-nvP", "-t", "tok"}},
		{"stream", zfs.SendSpec{Dataset: "tank/a", To: "s1"}, "LPvR", []string{"send", "-L", "-R", "-nvP", "tank/a@s1"}},
		{"bookmark intersection", zfs.SendSpec{Dataset: "tank/a", FromBookmark: "b1", To: "s2"}, "LRPhw", []string{"send", "-L", "-w", "-nvP", "-i", "tank/a#b1", "tank/a@s2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.spec.Options = mustOpts(t, tt.opts)
			if got := zfs.EstimateArgs(tt.spec); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EstimateArgs = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSendEstimate(t *testing.T) {
	inc := zfs.SendSpec{Dataset: "tank/a", FromSnap: "s1", To: "s2"}
	incKey := "zfs send -nvP -i tank/a@s1 tank/a@s2"
	full := zfs.SendSpec{Dataset: "tank/a", To: "s1"}
	fullKey := "zfs send -nvP tank/a@s1"
	res := zfs.SendSpec{ResumeToken: "tok"}
	resKey := "zfs send -nvP -t tok"

	tests := []struct {
		name string
		spec zfs.SendSpec
		key  string
		resp zfstest.Response
		want int64
	}{
		{"minimum", full, fullKey, zfstest.Response{Stdout: "full\ttank/a@s1\t1000\nsize\t1000\n"}, 4096},
		{"large", inc, incKey, zfstest.Response{Stdout: "incremental\ts1\ttank/a@s2\t123456\nsize\t123456\n"}, 123456},
		{"resume last field", res, resKey, zfstest.Response{Stdout: "resume token contents:\nnvlist version: 0\nincremental\ttank/a@s1\ttank/a@s2\t555555\n"}, 555555},
		{"garbage", inc, incKey, zfstest.Response{Stdout: "hello world\n"}, 0},
		{"empty", inc, incKey, zfstest.Response{}, 0},
		{"failure", inc, incKey, zfstest.Response{Exit: 1, Stderr: "boom"}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := newClient(map[string]zfstest.Response{tt.key: tt.resp})
			got, err := c.SendEstimate(context.Background(), tt.spec)
			if err != nil || got != tt.want {
				t.Errorf("SendEstimate = %d, %v; want %d, nil", got, err, tt.want)
			}
		})
	}
}

// stdioExec is an Exec that records the Cmd it was handed.
type stdioExec struct {
	cmd zfs.Cmd
	err error
	got string
}

func (s *stdioExec) Run(_ context.Context, c zfs.Cmd) error {
	s.cmd = c
	if c.Stdout != nil {
		_, _ = c.Stdout.Write([]byte("STREAM"))
	}
	if c.Stdin != nil {
		var b bytes.Buffer
		_, _ = b.ReadFrom(c.Stdin)
		s.got = b.String()
	}
	return s.err
}

func TestSendWritesToWriter(t *testing.T) {
	se := &stdioExec{}
	c := &zfs.Client{Exec: se, Sudo: true}
	var w bytes.Buffer
	err := c.Send(context.Background(), zfs.SendSpec{Dataset: "tank/a", To: "s1", Options: mustOpts(t, "c")}, &w)
	if err != nil {
		t.Fatal(err)
	}
	if want := "sudo -n zfs send -c tank/a@s1"; strings.Join(se.cmd.Argv, " ") != want {
		t.Errorf("argv = %v, want %s", se.cmd.Argv, want)
	}
	if w.String() != "STREAM" {
		t.Errorf("writer got %q", w.String())
	}
}

func TestReceiveReadsFromReader(t *testing.T) {
	se := &stdioExec{}
	c := &zfs.Client{Exec: se}
	err := c.Receive(context.Background(), zfs.RecvSpec{Dataset: "pool/b", Args: []string{"-s", "-F"}}, strings.NewReader("DATA"))
	if err != nil {
		t.Fatal(err)
	}
	if want := "zfs receive -s -F pool/b"; strings.Join(se.cmd.Argv, " ") != want {
		t.Errorf("argv = %v, want %s", se.cmd.Argv, want)
	}
	if se.got != "DATA" {
		t.Errorf("stdin got %q", se.got)
	}
}

func TestSendReceiveReturnExecErrorUnchanged(t *testing.T) {
	ce := &zfs.CommandError{ExitCode: 1, Stderr: "x", Kind: zfs.ErrNoDataset}
	se := &stdioExec{err: ce}
	c := &zfs.Client{Exec: se}
	var w bytes.Buffer
	for name, err := range map[string]error{
		"send":    c.Send(context.Background(), zfs.SendSpec{Dataset: "a", To: "s"}, &w),
		"receive": c.Receive(context.Background(), zfs.RecvSpec{Dataset: "b"}, strings.NewReader("")),
	} {
		var got *zfs.CommandError
		if !errors.As(err, &got) || got != ce || !errors.Is(err, zfs.ErrNoDataset) {
			t.Errorf("%s: error %v not passed through unchanged", name, err)
		}
	}
}
