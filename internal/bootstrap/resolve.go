//go:build unix

package bootstrap

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/piotaixr/syngoid/internal/buildinfo"
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
}

// Resolve returns the running binary when p is Self, else the first regular
// file DIR/<os>-<arch>/syngoid over BinStores, then PackagedStore.
func (r Resolver) Resolve(p Platform) (string, error) {
	if p == Self() {
		if r.Executable != nil {
			return r.Executable()
		}
		return os.Executable()
	}
	sub := p.OS + "-" + p.Arch
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
	return "", fmt.Errorf("no syngoid agent binary for %s: put one at DIR/%s/syngoid and pass --bin-store=DIR, or install %s/%s/syngoid",
		p, sub, packagedStore, sub)
}
