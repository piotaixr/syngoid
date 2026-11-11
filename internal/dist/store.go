//go:build unix

// Package dist keeps verified syngoid agent binaries for other platforms in a
// store directory (the user cache, or any --bin-store) and downloads them from
// a release (design §4.2). Nothing is installed unless the signed SHA256SUMS
// vouches for it.
package dist

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/piotaixr/syngoid/internal/platform"
	"github.com/piotaixr/syngoid/internal/trust"
)

const (
	sumsFile = "SHA256SUMS"
	sigFile  = "SHA256SUMS.minisig"

	maxSums  = 1 << 20   // SHA256SUMS and its signature
	maxAgent = 256 << 20 // one agent binary

	netTimeout = 30 * time.Second
)

// AssetName is the release asset holding the raw agent for p.
func AssetName(version string, p platform.Platform) string {
	return "syngoid_" + version + "_" + p.OS + "_" + p.Arch
}

// UserCacheDir returns the initiator's cache for version: under
// $XDG_CACHE_HOME when that is an absolute path, else under $HOME/.cache.
func UserCacheDir(version string) (string, error) {
	base := os.Getenv("XDG_CACHE_HOME")
	if !filepath.IsAbs(base) {
		home := os.Getenv("HOME")
		if home == "" {
			return "", errors.New("cannot locate the user cache: HOME is not set")
		}
		base = filepath.Join(home, ".cache")
	}
	return filepath.Join(base, "syngoid", "bin", version), nil
}

var placeholder = regexp.MustCompile(`\{[^{}]*\}`)

// ExpandURL fills the --download-url template for one release file and checks
// that the result is an http(s) URL.
func ExpandURL(tmpl, version, file string, p platform.Platform) (string, error) {
	if !strings.Contains(tmpl, "{file}") {
		return "", fmt.Errorf("--download-url %q: must contain {file}", tmpl)
	}
	out := strings.NewReplacer("{version}", version, "{os}", p.OS, "{arch}", p.Arch, "{file}", file).Replace(tmpl)
	if m := placeholder.FindString(strings.NewReplacer("{version}", "", "{os}", "", "{arch}", "", "{file}", "").Replace(tmpl)); m != "" {
		return "", fmt.Errorf("--download-url %q: unknown placeholder %s", tmpl, m)
	}
	if u, err := url.Parse(out); err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return "", fmt.Errorf("--download-url %q: scheme must be http or https", tmpl)
	}
	return out, nil
}

// Store is a store directory for one release version: SHA256SUMS,
// SHA256SUMS.minisig and <os>-<arch>/syngoid. It is safe for concurrent use;
// two Stores (or processes) on one Dir are safe because every write is a
// temp file plus rename of verified content.
type Store struct {
	Dir         string
	Version     string
	URLTemplate string       // --download-url template
	Client      *http.Client // nil: proxy from the environment, 30 s dial and header timeouts
	Log         *slog.Logger // nil: discard

	mu      sync.Mutex // serialises Download
	sums    map[string]string
	sumsRaw []byte // verified SHA256SUMS and signature, kept after the first download
	sigRaw  []byte
}

// NewUserCache returns a Store on the initiator's user cache for version.
func NewUserCache(version, urlTemplate string, log *slog.Logger) (*Store, error) {
	dir, err := UserCacheDir(version)
	if err != nil {
		return nil, err
	}
	return &Store{Dir: dir, Version: version, URLTemplate: urlTemplate, Log: log}, nil
}

func (s *Store) log() *slog.Logger {
	if s.Log == nil {
		return slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return s.Log
}

func (s *Store) client() *http.Client {
	if s.Client != nil {
		return s.Client
	}
	return &http.Client{Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: netTimeout}).DialContext,
		TLSHandshakeTimeout:   netTimeout,
		ResponseHeaderTimeout: netTimeout,
		ForceAttemptHTTP2:     true,
	}}
}

// Lookup returns the stored binary for p when the store's checksums verify
// for Version and the binary matches them. A corrupt binary or invalid sums
// are removed with a warning; absent files are a plain miss.
func (s *Store) Lookup(p platform.Platform) (string, bool) {
	sumsPath := filepath.Join(s.Dir, sumsFile)
	sums, err := os.ReadFile(sumsPath)
	if err != nil {
		return "", false
	}
	sig, err := os.ReadFile(filepath.Join(s.Dir, sigFile))
	if err != nil {
		return "", false
	}
	want, err := parseVerified(sums, sig, s.Version)
	if err != nil {
		s.log().Warn(fmt.Sprintf("%s does not verify (%v), removing it", sumsPath, err))
		_ = os.Remove(sumsPath)
		_ = os.Remove(sumsPath + ".minisig")
		return "", false
	}
	digest, ok := want[AssetName(s.Version, p)]
	if !ok {
		return "", false
	}
	path := filepath.Join(s.Dir, p.Dir(), "syngoid")
	got, err := hashFile(path)
	if err != nil {
		return "", false
	}
	if got != digest {
		s.log().Warn(fmt.Sprintf("cached agent for %s does not match SHA256SUMS, removing it", p))
		_ = os.Remove(path)
		return "", false
	}
	return path, true
}

func parseVerified(sums, sig []byte, version string) (map[string]string, error) {
	if err := trust.Verify(sums, sig, version); err != nil {
		return nil, err
	}
	return trust.ParseSums(sums)
}

func hashFile(path string) (string, error) {
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
}
