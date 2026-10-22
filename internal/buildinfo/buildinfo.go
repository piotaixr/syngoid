// Package buildinfo carries version information stamped in at link time.
package buildinfo

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"sync"
)

// ProtocolVersion is the version of the syngoid wire protocol.
const ProtocolVersion = 1

// These variables are set with -ldflags "-X ...".
var (
	// Version is the release version, or "dev" for local builds.
	Version = "dev"
	// Commit is the VCS revision the binary was built from.
	Commit = "unknown"
	// Dirty is "true" when the build tree had uncommitted changes.
	Dirty = ""
	// Hash identifies the build for the agent handshake (§5.2). Release
	// builds stamp it so every platform's binary of one release agrees;
	// when empty, BuildHash falls back to the hash of the executable.
	Hash = ""
)

// IsDev reports whether this is a development or dirty build.
func IsDev() bool {
	return Version == "dev" || Dirty == "true"
}

// exeSHA256 hashes the running executable once; the file is read in full, so
// the result is cached for the life of the process.
var exeSHA256 = sync.OnceValues(func() (string, error) {
	path, err := os.Executable()
	if err != nil {
		return "", err
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
})

// ExeSHA256 returns the hex SHA-256 of the running executable.
func ExeSHA256() (string, error) { return exeSHA256() }

// BuildHash returns Hash when it was stamped in, otherwise ExeSHA256, or
// "unknown" when the executable cannot be read.
func BuildHash() string {
	if Hash != "" {
		return Hash
	}
	sum, err := ExeSHA256()
	if err != nil {
		return "unknown"
	}
	return sum
}
