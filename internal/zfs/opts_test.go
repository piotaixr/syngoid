package zfs_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/piotaixr/syngoid/internal/zfs"
)

func TestParseOpts(t *testing.T) {
	tests := []struct {
		in      string
		allowed string
		want    []string
	}{
		{"ux recordsize o compression=lz4", zfs.RecvAllowed, []string{"-u", "-x", "recordsize", "-o", "compression=lz4"}},
		{"Lc e", zfs.SendAllowedStream, []string{"-L", "-c", "-e"}},
	}
	for _, tt := range tests {
		opts, err := zfs.ParseOpts(tt.in)
		if err != nil {
			t.Fatalf("ParseOpts(%q): %v", tt.in, err)
		}
		if got := zfs.FilterOpts(opts, tt.allowed); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("FilterOpts(ParseOpts(%q)) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestParseOptsEmpty(t *testing.T) {
	opts, err := zfs.ParseOpts("")
	if err != nil || opts != nil {
		t.Fatalf("ParseOpts(\"\") = %v, %v; want nil, nil", opts, err)
	}
}

func TestParseOptsErrors(t *testing.T) {
	tests := []struct{ in, want string }{
		{"xo foo", "must be the last"},
		{"o", "option -o requires a value"},
		{"u x", "option -x requires a value"},
		{"L1", "invalid option"},
	}
	for _, tt := range tests {
		_, err := zfs.ParseOpts(tt.in)
		if err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("ParseOpts(%q) error = %v, want containing %q", tt.in, err, tt.want)
		}
	}
}

func TestFilterOptsDropsDisallowed(t *testing.T) {
	opts, err := zfs.ParseOpts("Lw R")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"-L", "-w"}
	if got := zfs.FilterOpts(opts, zfs.SendAllowedBookmark); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
