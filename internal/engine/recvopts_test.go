package engine

import (
	"reflect"
	"testing"

	"github.com/piotaixr/syngoid/internal/cli"
	"github.com/piotaixr/syngoid/internal/zfs"
)

func mustOpts(t *testing.T, s string) []zfs.Opt {
	t.Helper()
	o, err := zfs.ParseOpts(s)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func TestRecvArgs(t *testing.T) {
	tests := []struct {
		name    string
		opts    cli.Options
		unpriv  bool
		recvopt string
		want    []string
	}{
		{"defaults", cli.Options{}, false, "", []string{"-s", "-F"}},
		{"no resume", cli.Options{NoResume: true}, false, "", []string{"-F"}},
		{"no rollback", cli.Options{NoRollback: true}, false, "", []string{"-s"}},
		{"unpriv linux", cli.Options{}, true, "", []string{"-s", "-F", "-u"}},
		{
			"recvoptions filtered first",
			cli.Options{},
			false, "ux recordsize o compression=lz4",
			[]string{"-u", "-x", "recordsize", "-o", "compression=lz4", "-s", "-F"},
		},
		{"recvoptions drop disallowed", cli.Options{}, false, "FsA", []string{"-s", "-F"}},
		{
			"-u not duplicated",
			cli.Options{},
			true, "ux recordsize o compression=lz4",
			[]string{"-u", "-x", "recordsize", "-o", "compression=lz4", "-s", "-F"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := tt.opts
			o.RecvOpts = mustOpts(t, tt.recvopt)
			got := RecvArgs(&o, false, "filesystem", nil, "", tt.unpriv)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRecvArgsPreserveProperties(t *testing.T) {
	o := &cli.Options{PreserveProperties: true}
	props := map[string]string{"compression": "lz4", "atime": "off", "guid": "1", "mountpoint": "/x", "canmount": "noauto"}

	got := RecvArgs(o, false, "filesystem", props, "", false)
	want := []string{"-s", "-F", "-o", "atime=off", "-o", "canmount=noauto", "-o", "compression=lz4", "-o", "mountpoint=/x"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	got = RecvArgs(o, false, "filesystem", props, "", true)
	want = []string{"-s", "-F", "-u", "-o", "atime=off", "-o", "compression=lz4"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("unpriv: got %v, want %v", got, want)
	}
}

func TestPreservePropsDenylist(t *testing.T) {
	if len(PreservePropsDenylist) != 35 {
		t.Errorf("denylist has %d entries, spec lists 35", len(PreservePropsDenylist))
	}
}

func TestRecvArgsPreserveRecordsize(t *testing.T) {
	o := &cli.Options{PreserveRecordsize: true, NoResume: true, NoRollback: true}
	tests := []struct {
		name       string
		o          *cli.Options
		full       bool
		sourceType string
		want       []string
	}{
		{"full filesystem", o, true, "filesystem", []string{"-o", "recordsize=131072"}},
		{"incremental", o, false, "filesystem", nil},
		{"volume", o, true, "volume", nil},
		{"ignored with preserve-properties", &cli.Options{PreserveRecordsize: true, PreserveProperties: true, NoResume: true, NoRollback: true}, true, "filesystem", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RecvArgs(tt.o, tt.full, tt.sourceType, nil, "131072", false)
			if len(got) != len(tt.want) || (len(got) > 0 && !reflect.DeepEqual(got, tt.want)) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}
