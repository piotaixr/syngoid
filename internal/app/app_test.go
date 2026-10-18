package app

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func testEnv() (Env, *bytes.Buffer, *bytes.Buffer) {
	var stdout, stderr bytes.Buffer
	return Env{
		Stdin:    strings.NewReader(""),
		Stdout:   &stdout,
		Stderr:   &stderr,
		Hostname: func() (string, error) { return "testhost", nil },
		Now:      time.Now,
	}, &stdout, &stderr
}

func TestMainVersion(t *testing.T) {
	env, stdout, _ := testEnv()
	if got := Main(context.Background(), []string{"version"}, env); got != 0 {
		t.Fatalf("exit code = %d, want 0", got)
	}
	out := stdout.String()
	for _, want := range []string{"syngoid dev", "protocol 1"} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout %q does not contain %q", out, want)
		}
	}
}

func TestMainUnknownFlagExit2(t *testing.T) {
	env, _, stderr := testEnv()
	if got := Main(context.Background(), []string{"--nope"}, env); got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
	if stderr.Len() == 0 {
		t.Error("stderr is empty, want an error message")
	}
}

func TestMainUnknownCommandExit2(t *testing.T) {
	env, _, stderr := testEnv()
	if got := Main(context.Background(), []string{"bogus"}, env); got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
	if stderr.Len() == 0 {
		t.Error("stderr is empty, want an error message")
	}
}
