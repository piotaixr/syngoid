package trusttest_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strings"
	"testing"

	"github.com/piotaixr/syngoid/internal/trust"
	"github.com/piotaixr/syngoid/internal/trust/trusttest"
)

// No test here may call t.Parallel: trusttest.New swaps a package-global key list.

func TestSignVerifies(t *testing.T) {
	s := trusttest.New(t)
	files := s.Release("1.2.3", map[string][]byte{"a": []byte("hello")})
	if err := trust.Verify(files["SHA256SUMS"], files["SHA256SUMS.minisig"], "1.2.3"); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if string(files["a"]) != "hello" {
		t.Fatal("asset not preserved")
	}
	sums, err := trust.ParseSums(files["SHA256SUMS"])
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256([]byte("hello"))
	if len(sums) != 1 || sums["a"] != hex.EncodeToString(h[:]) {
		t.Fatalf("sums = %v", sums)
	}
	if s.PublicKey().Key == nil {
		t.Fatal("no public key")
	}
}

func TestSignMatchesCLIShape(t *testing.T) {
	s := trusttest.New(t)
	ours, err := trust.ParseSignature(s.Sign([]byte("x"), "syngoid 1.2.3"))
	if err != nil {
		t.Fatal(err)
	}
	golden, err := os.ReadFile("../testdata/sums.txt.minisig")
	if err != nil {
		t.Fatal(err)
	}
	theirs, err := trust.ParseSignature(golden)
	if err != nil {
		t.Fatal(err)
	}
	if ours.Algorithm != "ED" || theirs.Algorithm != "ED" {
		t.Fatalf("algorithms %q %q", ours.Algorithm, theirs.Algorithm)
	}
	raw := string(s.Sign([]byte("x"), "c"))
	if !strings.HasPrefix(raw, "untrusted comment: ") || !strings.HasPrefix(string(golden), "untrusted comment: ") {
		t.Fatal("untrusted comment prefix")
	}
	if !strings.HasSuffix(raw, "\n") || strings.Count(raw, "\n") != 4 {
		t.Fatalf("layout: %q", raw)
	}
}

func TestKeysRestored(t *testing.T) {
	var files map[string][]byte
	t.Run("inner", func(t *testing.T) {
		files = trusttest.New(t).Release("1.2.3", map[string][]byte{"a": []byte("x")})
		if err := trust.Verify(files["SHA256SUMS"], files["SHA256SUMS.minisig"], "1.2.3"); err != nil {
			t.Fatal(err)
		}
	})
	err := trust.Verify(files["SHA256SUMS"], files["SHA256SUMS.minisig"], "1.2.3")
	if err == nil || !strings.Contains(err.Error(), "unknown signing key") {
		t.Fatalf("err = %v", err)
	}
}
