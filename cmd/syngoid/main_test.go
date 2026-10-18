package main

import (
	"os"
	"testing"
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
