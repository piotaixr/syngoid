//go:build unix

package dist

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/piotaixr/syngoid/internal/platform"
)

const tempPattern = ".syngoid-dl-*"

// Download fetches the agent for p, checks it against the signed SHA256SUMS
// and installs sums, signature and binary into Dir, the binary last. The
// verified sums are kept, so later downloads for other platforms do not fetch
// them again. Nothing is installed unless every check passed.
func (s *Store) Download(ctx context.Context, p platform.Platform) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Another call through this Store may have installed p while we waited.
	if path, ok := s.Lookup(p); ok {
		return path, nil
	}

	asset := AssetName(s.Version, p)
	urlOf := func(file string) (string, error) { return ExpandURL(s.URLTemplate, s.Version, file, p) }
	sumsURL, err := urlOf(sumsFile)
	if err != nil {
		return "", err
	}
	sigURL, err := urlOf(sigFile)
	if err != nil {
		return "", err
	}
	binURL, err := urlOf(asset)
	if err != nil {
		return "", err
	}

	if s.sums == nil {
		if err := s.fetchSums(ctx, sumsURL, sigURL); err != nil {
			return "", err
		}
	}
	want, ok := s.sums[asset]
	if !ok {
		return "", fmt.Errorf("%s has no entry for %s", sumsFile, asset)
	}

	binDir := filepath.Join(s.Dir, p.Dir())
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(binDir, tempPattern)
	if err != nil {
		return "", err
	}
	defer func() { _ = os.Remove(tmp.Name()) }() // a no-op once renamed
	s.log().Info(fmt.Sprintf("downloading syngoid agent for %s from %s", p, binURL))
	h := sha256.New()
	err = s.get(ctx, binURL, maxAgent, io.MultiWriter(tmp, h))
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return "", err
	}
	if hex.EncodeToString(h.Sum(nil)) != want {
		return "", fmt.Errorf("%s: SHA-256 does not match %s", asset, sumsFile)
	}
	if err := os.Chmod(tmp.Name(), 0o755); err != nil {
		return "", err
	}

	if err := installFile(s.Dir, sumsFile, s.sumsRaw); err != nil {
		return "", err
	}
	if err := installFile(s.Dir, sigFile, s.sigRaw); err != nil {
		return "", err
	}
	dst := filepath.Join(binDir, "syngoid")
	if err := os.Rename(tmp.Name(), dst); err != nil {
		return "", err
	}
	return dst, nil
}

// fetchSums downloads and verifies the sums and signature and memoises them.
func (s *Store) fetchSums(ctx context.Context, sumsURL, sigURL string) error {
	sums, err := s.getBytes(ctx, sumsURL)
	if err != nil {
		return err
	}
	sig, err := s.getBytes(ctx, sigURL)
	if err != nil {
		return err
	}
	parsed, err := parseVerified(sums, sig, s.Version)
	if err != nil {
		return fmt.Errorf("%s from %s: %w", sumsFile, sumsURL, err)
	}
	s.sums, s.sumsRaw, s.sigRaw = parsed, sums, sig
	return nil
}

// installFile atomically writes data as dir/name through a temp file.
func installFile(dir, name string, data []byte) error {
	tmp, err := os.CreateTemp(dir, tempPattern)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	_, err = tmp.Write(data)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Chmod(tmp.Name(), 0o644)
	}
	if err != nil {
		return err
	}
	return os.Rename(tmp.Name(), filepath.Join(dir, name))
}

func (s *Store) getBytes(ctx context.Context, url string) ([]byte, error) {
	var b bytes.Buffer
	if err := s.get(ctx, url, maxSums, &b); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// get streams a 2xx response body into dst, failing when it exceeds limit
// bytes.
func (s *Store) get(ctx context.Context, url string, limit int64, dst io.Writer) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := s.client().Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	n, err := io.Copy(dst, io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return fmt.Errorf("GET %s: %w", url, err)
	}
	if n > limit {
		return fmt.Errorf("%s: larger than %d bytes", url, limit)
	}
	return nil
}
