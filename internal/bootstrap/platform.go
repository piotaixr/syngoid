//go:build unix

// Package bootstrap brings up syngoid agents on remote hosts: it probes the
// remote over ssh, pushes the agent binary when needed, starts
// `syngoid serve` and performs the handshake (§5).
package bootstrap

import (
	"fmt"
	"runtime"
	"strings"
)

// Platform is an os/arch pair in Go's GOOS/GOARCH naming.
type Platform struct{ OS, Arch string }

func (p Platform) String() string { return p.OS + "/" + p.Arch }

// supported lists the release targets (§11.1).
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

var unameOS = map[string]string{"Linux": "linux", "FreeBSD": "freebsd", "SunOS": "illumos", "Darwin": "darwin"}

var unameArch = map[string]string{
	"x86_64": "amd64", "amd64": "amd64",
	"aarch64": "arm64", "arm64": "arm64",
	"armv7l":  "arm",
	"riscv64": "riscv64",
	"ppc64le": "ppc64le",
}

// ParsePlatform maps the remote's `uname -s`, `uname -m` and, on illumos,
// `isainfo -k` to a supported Platform (§5.2).
func ParsePlatform(unameS, unameM, isainfo string) (Platform, error) {
	p := Platform{OS: unameOS[unameS], Arch: unameArch[unameM]}
	if p.OS == "" {
		p.OS = strings.ToLower(unameS)
	}
	if unameM == "i86pc" {
		// illumos reports the machine class; the kernel ISA tells 64-bit.
		p.Arch = unameArch[isainfo]
		if p.Arch == "" {
			p.Arch = strings.ToLower(isainfo)
		}
	}
	if p.Arch == "" {
		p.Arch = strings.ToLower(unameM)
	}
	for _, s := range supported {
		if p == s {
			return p, nil
		}
	}
	names := make([]string, len(supported))
	for i, s := range supported {
		names[i] = s.String()
	}
	return Platform{}, fmt.Errorf("unsupported remote platform %s (uname -s=%s, uname -m=%s); supported: %s",
		p, unameS, unameM, strings.Join(names, ", "))
}

// Self returns the platform this binary was built for.
func Self() Platform { return Platform{OS: runtime.GOOS, Arch: runtime.GOARCH} }
