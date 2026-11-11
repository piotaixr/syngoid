//go:build unix

package bootstrap

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/piotaixr/syngoid/internal/buildinfo"
	"github.com/piotaixr/syngoid/internal/dist"
)

// packagedStore is the store distribution packages install (§11.5).
const packagedStore = "/usr/lib/syngoid/bin"

// AgentDirName names the remote agent directory: the version, plus the
// first 8 hex digits of the build hash for development builds.
func AgentDirName() string {
	if !buildinfo.IsDev() {
		return buildinfo.Version
	}
	h := buildinfo.BuildHash()
	return buildinfo.Version + "-" + h[:min(len(h), 8)]
}

// DefaultRemoteBinDir is the remote bin dir used without --remote-bin-dir.
func DefaultRemoteBinDir() string { return "~/.cache/syngoid/" + AgentDirName() }

// Resolver finds the local agent binary to push for a platform (§5.3).
type Resolver struct {
	// BinStores are the --bin-store dirs, searched in order.
	BinStores []string
	// PackagedStore is searched last; "" means /usr/lib/syngoid/bin.
	PackagedStore string
	// Executable returns the running binary; nil means os.Executable.
	Executable func() (string, error)
	// Cache is the verified user cache, looked up and downloaded into for
	// release builds; nil means neither.
	Cache *dist.Store
	// CacheErr says why Cache is nil for a release build.
	CacheErr error
	// NoDownload skips the download step; a cache hit still wins.
	NoDownload bool
}

// Resolve returns the running binary when p is Self, else the first regular
// file DIR/<os>-<arch>/syngoid over BinStores, then PackagedStore, then (release
// builds only) a verified user cache hit, then a download into the cache.
func (r Resolver) Resolve(ctx context.Context, p Platform) (string, error) {
	if p == Self() {
		if r.Executable != nil {
			return r.Executable()
		}
		return os.Executable()
	}
	sub := p.Dir()
	pkg := r.PackagedStore
	if pkg == "" {
		pkg = packagedStore
	}
	for _, dir := range append(append([]string{}, r.BinStores...), pkg) {
		path := filepath.Join(dir, sub, "syngoid")
		if fi, err := os.Stat(path); err == nil && fi.Mode().IsRegular() {
			return path, nil
		}
	}
	tail := fmt.Sprintf("put one at DIR/%s/syngoid and pass --bin-store=DIR, or install %s/%s/syngoid", sub, packagedStore, sub)
	if buildinfo.IsDev() {
		return "", fmt.Errorf("no syngoid agent binary for %s: %s", p, tail)
	}
	fetch := fmt.Sprintf("run \"syngoid fetch --platform %s\", or %s", p, tail)
	if r.Cache == nil {
		if r.CacheErr != nil {
			return "", fmt.Errorf("no syngoid agent binary for %s: %w; %s", p, r.CacheErr, tail)
		}
		return "", fmt.Errorf("no syngoid agent binary for %s: %s", p, tail)
	}
	if path, ok := r.Cache.Lookup(p); ok {
		return path, nil
	}
	if r.NoDownload {
		return "", fmt.Errorf("no syngoid agent binary for %s (--no-download): %s", p, fetch)
	}
	path, err := r.Cache.Download(ctx, p)
	if err != nil {
		return "", fmt.Errorf("no syngoid agent binary for %s: download failed: %w; run \"syngoid fetch --platform %s\" where downloads work, or %s", p, err, p, tail)
	}
	return path, nil
}
