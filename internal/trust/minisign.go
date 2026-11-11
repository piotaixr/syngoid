// Package trust verifies minisign signatures over release checksums against
// the public keys embedded in the binary. It does cryptography only: no
// network, no file system.
package trust

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/blake2b"
)

// PublicKey is a minisign public key.
type PublicKey struct {
	ID  [8]byte
	Key ed25519.PublicKey
}

// IDString returns the key ID in minisign's display form: the little-endian
// uint64 of ID in uppercase hex.
func (k PublicKey) IDString() string { return idString(k.ID) }

func idString(id [8]byte) string {
	return fmt.Sprintf("%016X", binary.LittleEndian.Uint64(id[:]))
}

// ParsePublicKey parses a minisign public key: a whole .pub file (untrusted
// comment line, then the base64 line) or the bare base64 line.
func ParsePublicKey(text string) (PublicKey, error) {
	lines := splitLines(text)
	var b64 string
	switch {
	case len(lines) == 1:
		b64 = lines[0]
	case len(lines) == 2 && strings.HasPrefix(lines[0], "untrusted comment:"):
		b64 = lines[1]
	default:
		return PublicKey{}, errors.New("public key: expected a base64 line, optionally after an untrusted comment")
	}
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return PublicKey{}, fmt.Errorf("public key: %w", err)
	}
	if len(raw) != 2+8+ed25519.PublicKeySize || string(raw[:2]) != "Ed" {
		return PublicKey{}, errors.New("public key: not an Ed25519 minisign key")
	}
	var k PublicKey
	copy(k.ID[:], raw[2:10])
	k.Key = ed25519.PublicKey(raw[10:])
	return k, nil
}

// Signature is a parsed minisign signature file.
type Signature struct {
	Algorithm      string // "ED" (prehashed) or "Ed" (legacy)
	KeyID          [8]byte
	Sig            []byte
	TrustedComment string
	GlobalSig      []byte
}

const trustedPrefix = "trusted comment: "

// ParseSignature parses a minisign signature file: the untrusted comment, the
// base64 signature, the trusted comment and the base64 global signature. It
// accepts CRLF line ends and a missing final newline.
func ParseSignature(b []byte) (Signature, error) {
	lines := splitLines(string(b))
	if len(lines) != 4 {
		return Signature{}, fmt.Errorf("signature: expected 4 lines, got %d", len(lines))
	}
	if !strings.HasPrefix(lines[0], "untrusted comment:") {
		return Signature{}, errors.New("signature: missing untrusted comment")
	}
	raw, err := base64.StdEncoding.DecodeString(lines[1])
	if err != nil {
		return Signature{}, fmt.Errorf("signature: %w", err)
	}
	if len(raw) != 2+8+ed25519.SignatureSize {
		return Signature{}, errors.New("signature: wrong length")
	}
	comment, ok := strings.CutPrefix(lines[2], trustedPrefix)
	if !ok {
		return Signature{}, errors.New("signature: missing trusted comment")
	}
	global, err := base64.StdEncoding.DecodeString(lines[3])
	if err != nil {
		return Signature{}, fmt.Errorf("signature: global signature: %w", err)
	}
	if len(global) != ed25519.SignatureSize {
		return Signature{}, errors.New("signature: global signature wrong length")
	}
	s := Signature{
		Algorithm:      string(raw[:2]),
		Sig:            raw[10:],
		TrustedComment: comment,
		GlobalSig:      global,
	}
	copy(s.KeyID[:], raw[2:10])
	return s, nil
}

// splitLines splits text into lines, dropping CRLF carriage returns and
// trailing blank lines.
func splitLines(text string) []string {
	lines := strings.Split(text, "\n")
	for i := range lines {
		lines[i] = strings.TrimSuffix(lines[i], "\r")
	}
	for len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}

// Verify checks sig over sums against the embedded keys and requires the
// trusted comment to name wantVersion.
func Verify(sums, sig []byte, wantVersion string) error {
	return VerifyWith(trustedKeys(), sums, sig, wantVersion)
}

// VerifyWith is Verify against an explicit key set. It checks, in order: the
// key ID is known, the algorithm is prehashed Ed25519 ("ED"), the signature
// over blake2b-512(sums), the global signature over signature and trusted
// comment, and finally that the trusted comment is "syngoid <wantVersion>".
func VerifyWith(keys []PublicKey, sums, sig []byte, wantVersion string) error {
	s, err := ParseSignature(sig)
	if err != nil {
		return err
	}
	var key *PublicKey
	for i := range keys {
		if keys[i].ID == s.KeyID {
			key = &keys[i]
			break
		}
	}
	if key == nil {
		return fmt.Errorf("unknown signing key %s", idString(s.KeyID))
	}
	if s.Algorithm != "ED" {
		return fmt.Errorf("unsupported signature algorithm %q", s.Algorithm)
	}
	if len(key.Key) != ed25519.PublicKeySize {
		return fmt.Errorf("invalid public key length %d for key %s", len(key.Key), idString(key.ID))
	}
	digest := blake2b.Sum512(sums)
	if !ed25519.Verify(key.Key, digest[:], s.Sig) {
		return errors.New("invalid signature")
	}
	global := append(append([]byte(nil), s.Sig...), s.TrustedComment...)
	if !ed25519.Verify(key.Key, global, s.GlobalSig) {
		return errors.New("invalid trusted comment signature")
	}
	if want := "syngoid " + wantVersion; s.TrustedComment != want {
		return fmt.Errorf("signed for %q, want %q", s.TrustedComment, want)
	}
	return nil
}
