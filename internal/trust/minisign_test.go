package trust

import (
	"bytes"
	"encoding/base64"
	"os"
	"strings"
	"testing"
)

func readFile(t testing.TB, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func loadKey(t testing.TB, name string) PublicKey {
	t.Helper()
	k, err := ParsePublicKey(string(readFile(t, name)))
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func TestVerifyGolden(t *testing.T) {
	keys := []PublicKey{loadKey(t, "test.pub")}
	if err := VerifyWith(keys, readFile(t, "sums.txt"), readFile(t, "sums.txt.minisig"), "1.2.3"); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyRejects(t *testing.T) {
	test := loadKey(t, "test.pub")
	other := loadKey(t, "other.pub")
	sums := readFile(t, "sums.txt")
	sig := readFile(t, "sums.txt.minisig")

	tampered := bytes.Clone(sums)
	tampered[0] ^= 1

	edited := bytes.Replace(sig, []byte("trusted comment: syngoid 1.2.3"), []byte("trusted comment: syngoid 1.2.4"), 1)

	parsed, err := ParseSignature(sig)
	if err != nil {
		t.Fatal(err)
	}
	raw := append([]byte("Ed"), parsed.KeyID[:]...)
	raw = append(raw, parsed.Sig...)
	lines := strings.Split(string(sig), "\n")
	lines[1] = base64.StdEncoding.EncodeToString(raw)
	legacy := []byte(strings.Join(lines, "\n"))

	tests := []struct {
		name string
		keys []PublicKey
		sums []byte
		sig  []byte
		ver  string
		want string
	}{
		{"tampered sums", []PublicKey{test}, tampered, sig, "1.2.3", "invalid signature"},
		{"unknown key", []PublicKey{other}, sums, sig, "1.2.3", "unknown signing key " + test.IDString()},
		{"no keys", nil, sums, sig, "1.2.3", "unknown signing key " + test.IDString()},
		{"wrong version", []PublicKey{test}, sums, readFile(t, "sums-other-version.txt.minisig"), "1.2.3", `signed for "syngoid 1.2.2", want "syngoid 1.2.3"`},
		{"edited trusted comment", []PublicKey{test}, sums, edited, "1.2.3", "invalid trusted comment signature"},
		{"legacy algorithm", []PublicKey{test}, sums, legacy, "1.2.3", `unsupported signature algorithm "Ed"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := VerifyWith(tc.keys, tc.sums, tc.sig, tc.ver)
			if err == nil || err.Error() != tc.want {
				t.Fatalf("got %v, want %q", err, tc.want)
			}
		})
	}
}

func TestVerifyFailsClosedWithoutEmbeddedKeys(t *testing.T) {
	ReplaceKeysForTesting(t)
	err := Verify(readFile(t, "sums.txt"), readFile(t, "sums.txt.minisig"), "1.2.3")
	if err == nil || !strings.HasPrefix(err.Error(), "unknown signing key ") {
		t.Fatalf("got %v", err)
	}
}

func TestReplaceKeysForTesting(t *testing.T) {
	k := loadKey(t, "test.pub")
	t.Run("inner", func(t *testing.T) {
		ReplaceKeysForTesting(t, k)
		if err := Verify(readFile(t, "sums.txt"), readFile(t, "sums.txt.minisig"), "1.2.3"); err != nil {
			t.Fatal(err)
		}
	})
	if err := Verify(readFile(t, "sums.txt"), readFile(t, "sums.txt.minisig"), "1.2.3"); err == nil {
		t.Fatal("keys were not restored")
	}
}

func TestParseSignatureMalformed(t *testing.T) {
	good := strings.Split(strings.TrimRight(string(readFile(t, "sums.txt.minisig")), "\n"), "\n")
	with := func(i int, s string) string {
		l := append([]string(nil), good...)
		l[i] = s
		return strings.Join(l, "\n")
	}
	tests := map[string]string{
		"three lines":       strings.Join(good[:3], "\n"),
		"bad base64":        with(1, "!!!not base64!!!"),
		"short signature":   with(1, base64.StdEncoding.EncodeToString([]byte("ED12345678"))),
		"no trusted prefix": with(2, "comment: syngoid 1.2.3"),
		"short global sig":  with(3, base64.StdEncoding.EncodeToString([]byte("short"))),
		"no untrusted line": with(0, "something else"),
		"empty":             "",
	}
	for name, in := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseSignature([]byte(in)); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestParseSignatureTolerance(t *testing.T) {
	sig := readFile(t, "sums.txt.minisig")
	want, err := ParseSignature(sig)
	if err != nil {
		t.Fatal(err)
	}
	crlf := strings.ReplaceAll(string(sig), "\n", "\r\n")
	for name, in := range map[string]string{
		"crlf":             crlf,
		"no final newline": strings.TrimRight(string(sig), "\n"),
	} {
		got, err := ParseSignature([]byte(in))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got.TrustedComment != want.TrustedComment || !bytes.Equal(got.GlobalSig, want.GlobalSig) {
			t.Fatalf("%s: parsed differently", name)
		}
	}
	if want.Algorithm != "ED" || want.TrustedComment != "syngoid 1.2.3" {
		t.Fatalf("unexpected parse: %+v", want)
	}
}

func TestParsePublicKey(t *testing.T) {
	file := string(readFile(t, "test.pub"))
	lines := strings.Split(strings.TrimSpace(file), "\n")
	whole, err := ParsePublicKey(file)
	if err != nil {
		t.Fatal(err)
	}
	bare, err := ParsePublicKey(lines[1])
	if err != nil {
		t.Fatal(err)
	}
	if whole.ID != bare.ID || !bytes.Equal(whole.Key, bare.Key) {
		t.Fatal("whole file and bare line differ")
	}
	if want := strings.TrimPrefix(lines[0], "untrusted comment: minisign public key "); whole.IDString() != want {
		t.Fatalf("IDString %s, want %s", whole.IDString(), want)
	}
	for _, bad := range []string{"", "!!!", base64.StdEncoding.EncodeToString([]byte("Ed1234567")), "a\nb\nc"} {
		if _, err := ParsePublicKey(bad); err == nil {
			t.Fatalf("expected error for %q", bad)
		}
	}
}

func TestEmbeddedKeys(t *testing.T) {
	keys := trustedKeys()
	if len(keys) != 2 {
		t.Fatalf("got %d embedded keys, want 2", len(keys))
	}
	if keys[0].ID == keys[1].ID {
		t.Fatal("embedded keys share an ID")
	}
}

func TestEmbeddedKeyIDs(t *testing.T) {
	keys := trustedKeys()
	if len(keys) != 2 {
		t.Fatalf("got %d embedded keys, want 2", len(keys))
	}
	want := []string{"1D1D0F872FE988EE", "7AA1ABAB97755335"}
	for i, k := range keys {
		if got := k.IDString(); got != want[i] {
			t.Fatalf("embedded key %d: ID %s, want %s", i, got, want[i])
		}
	}
}
