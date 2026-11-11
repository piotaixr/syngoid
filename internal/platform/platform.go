// Package platform names the os/arch pairs syngoid releases are built for.
package platform

import (
	"fmt"
	"runtime"
	"strings"
)

// Platform is an os/arch pair in Go's GOOS/GOARCH naming.
type Platform struct{ OS, Arch string }

// String returns "os/arch".
func (p Platform) String() string { return p.OS + "/" + p.Arch }

// Dir returns "os-arch", the store directory name.
func (p Platform) Dir() string { return p.OS + "-" + p.Arch }

var supported = []Platform{
	{"linux", "amd64"},
	{"linux", "arm64"},
	{"linux", "arm"},
	{"linux", "riscv64"},
	{"linux", "ppc64le"},
	{"freebsd", "amd64"},
	{"freebsd", "arm64"},
	{"illumos", "amd64"},
	{"darwin", "amd64"},
	{"darwin", "arm64"},
}

// Supported lists the release targets (§11.1). The result is a copy.
func Supported() []Platform { return append([]Platform(nil), supported...) }

// Self returns the platform this binary was built for.
func Self() Platform { return Platform{OS: runtime.GOOS, Arch: runtime.GOARCH} }

// Parse parses "os/arch", which must be a supported target.
func Parse(s string) (Platform, error) {
	for _, p := range supported {
		if p.String() == s {
			return p, nil
		}
	}
	names := make([]string, len(supported))
	for i, p := range supported {
		names[i] = p.String()
	}
	return Platform{}, fmt.Errorf("unsupported platform %q; supported: %s", s, strings.Join(names, ", "))
}
