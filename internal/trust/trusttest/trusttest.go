// Package trusttest signs fake releases in the minisign format so tests can
// exercise download and verification code without the maintainer's key.
package trusttest

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"sort"
	"testing"

	"golang.org/x/crypto/blake2b"

	"github.com/piotaixr/syngoid/internal/trust"
)

// Signer signs with a throwaway Ed25519 key.
type Signer struct {
	tb   testing.TB
	pub  trust.PublicKey
	priv ed25519.PrivateKey
}

// New generates a fresh key with a random ID and installs it as the only
// trusted key until the test ends. It goes through
// trust.ReplaceKeysForTesting, which swaps a package-global list, so tests
// that call New must not use t.Parallel.
func New(tb testing.TB) *Signer {
	tb.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		tb.Fatal(err)
	}
	s := &Signer{tb: tb, priv: priv, pub: trust.PublicKey{Key: pub}}
	if _, err := rand.Read(s.pub.ID[:]); err != nil {
		tb.Fatal(err)
	}
	trust.ReplaceKeysForTesting(tb, s.pub)
	return s
}

// PublicKey returns the signer's public key.
func (s *Signer) PublicKey() trust.PublicKey { return s.pub }

// Sign returns a minisign "ED" signature file for sums with the given trusted
// comment, laid out like the CLI output.
func (s *Signer) Sign(sums []byte, trustedComment string) []byte {
	digest := blake2b.Sum512(sums)
	sig := ed25519.Sign(s.priv, digest[:])
	raw := append([]byte("ED"), s.pub.ID[:]...)
	raw = append(raw, sig...)
	global := ed25519.Sign(s.priv, append(append([]byte{}, sig...), trustedComment...))
	enc := base64.StdEncoding.EncodeToString
	return []byte("untrusted comment: signature from trusttest key\n" +
		enc(raw) + "\n" +
		"trusted comment: " + trustedComment + "\n" +
		enc(global) + "\n")
}

// Release returns assets plus SHA256SUMS (lines sorted by name) and
// SHA256SUMS.minisig signed with the trusted comment "syngoid <version>".
// The input map is not modified.
func (s *Signer) Release(version string, assets map[string][]byte) map[string][]byte {
	names := make([]string, 0, len(assets))
	files := make(map[string][]byte, len(assets)+2)
	for name, data := range assets {
		names = append(names, name)
		files[name] = data
	}
	sort.Strings(names)
	var sums []byte
	for _, name := range names {
		h := sha256.Sum256(assets[name])
		sums = append(sums, hex.EncodeToString(h[:])+"  "+name+"\n"...)
	}
	files["SHA256SUMS"] = sums
	files["SHA256SUMS.minisig"] = s.Sign(sums, "syngoid "+version)
	return files
}
