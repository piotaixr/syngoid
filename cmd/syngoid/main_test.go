package main

import (
	"bytes"
	"os"
	"testing"

	"github.com/piotaixr/syngoid/internal/zfs/zfstest"
)

func TestIsTTYFalseForNonTerminals(t *testing.T) {
	null, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = null.Close() }()
	if isTTY(null) {
		t.Error("isTTY(/dev/null) = true, want false")
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	defer func() { _ = w.Close() }()
	if isTTY(r) || isTTY(w) {
		t.Error("isTTY(pipe) = true, want false")
	}
}

func TestPoolExists(t *testing.T) {
	fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{
		"zfs list -H -o name -d 0": {Stdout: "tank\nbackup\n"},
	}}
	var stderr bytes.Buffer
	p := &pools{exec: fe, stderr: &stderr}
	if !p.exists("tank") || p.exists("nas") {
		t.Error("exists(tank) must be true and exists(nas) false")
	}
	if stderr.Len() != 0 {
		t.Errorf("stderr = %q, want empty", stderr.String())
	}
}

func TestPoolExistsWarnsOnceWhenZFSFails(t *testing.T) {
	fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{
		"zfs list -H -o name -d 0": {Stderr: "zfs: command not found\n", Exit: 127},
	}}
	var stderr bytes.Buffer
	p := &pools{exec: fe, stderr: &stderr}
	if p.exists("tank") || p.exists("nas") {
		t.Error("exists must be false when zfs fails")
	}
	if got, want := stderr.String(), "WARNING: Unable to enumerate pools (is zfs available?)\n"; got != want {
		t.Errorf("stderr = %q, want %q", got, want)
	}
}
