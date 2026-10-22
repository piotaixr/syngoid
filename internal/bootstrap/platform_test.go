//go:build unix

package bootstrap

import (
	"runtime"
	"testing"
)

func TestParsePlatform(t *testing.T) {
	const supported = "supported: linux/amd64, linux/arm64, linux/arm, linux/riscv64, linux/ppc64le, " +
		"freebsd/amd64, freebsd/arm64, illumos/amd64, darwin/amd64, darwin/arm64"
	tests := []struct {
		s, m, isa string
		want      string // "" when an error is expected
		wantErr   string
	}{
		{s: "Linux", m: "x86_64", want: "linux/amd64"},
		{s: "Linux", m: "aarch64", want: "linux/arm64"},
		{s: "Linux", m: "armv7l", want: "linux/arm"},
		{s: "Linux", m: "riscv64", want: "linux/riscv64"},
		{s: "Linux", m: "ppc64le", want: "linux/ppc64le"},
		{s: "FreeBSD", m: "amd64", want: "freebsd/amd64"},
		{s: "FreeBSD", m: "arm64", want: "freebsd/arm64"},
		{s: "SunOS", m: "i86pc", isa: "amd64", want: "illumos/amd64"},
		{s: "SunOS", m: "i86pc", isa: "i386", wantErr: "unsupported remote platform illumos/i386 (uname -s=SunOS, uname -m=i86pc); " + supported},
		{s: "Darwin", m: "arm64", want: "darwin/arm64"},
		{s: "Darwin", m: "x86_64", want: "darwin/amd64"},
		{s: "Linux", m: "armv6l", wantErr: "unsupported remote platform linux/armv6l (uname -s=Linux, uname -m=armv6l); " + supported},
		{s: "OpenBSD", m: "amd64", wantErr: "unsupported remote platform openbsd/amd64 (uname -s=OpenBSD, uname -m=amd64); " + supported},
	}
	for _, tt := range tests {
		p, err := ParsePlatform(tt.s, tt.m, tt.isa)
		if tt.wantErr != "" {
			if err == nil || err.Error() != tt.wantErr {
				t.Errorf("ParsePlatform(%q, %q, %q) error = %v, want %q", tt.s, tt.m, tt.isa, err, tt.wantErr)
			}
			continue
		}
		if err != nil || p.String() != tt.want {
			t.Errorf("ParsePlatform(%q, %q, %q) = %v, %v; want %s", tt.s, tt.m, tt.isa, p, err, tt.want)
		}
	}
}

func TestSelf(t *testing.T) {
	if got, want := Self().String(), runtime.GOOS+"/"+runtime.GOARCH; got != want {
		t.Errorf("Self = %s, want %s", got, want)
	}
}
