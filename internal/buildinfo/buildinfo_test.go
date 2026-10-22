package buildinfo

import (
	"regexp"
	"testing"
)

func TestBuildHashPrefersStamp(t *testing.T) {
	old := Hash
	t.Cleanup(func() { Hash = old })

	Hash = "abc"
	if got := BuildHash(); got != "abc" {
		t.Errorf("BuildHash() = %q, want the stamped value", got)
	}

	Hash = ""
	want, err := ExeSHA256()
	if err != nil {
		t.Fatalf("ExeSHA256: %v", err)
	}
	if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(want) {
		t.Fatalf("ExeSHA256() = %q, want 64 hex characters", want)
	}
	if got := BuildHash(); got != want {
		t.Errorf("BuildHash() = %q, want the executable hash %q", got, want)
	}
}
