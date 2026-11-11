package platform

import (
	"runtime"
	"testing"
)

func TestParse(t *testing.T) {
	p, err := Parse("linux/arm")
	if err != nil || p != (Platform{"linux", "arm"}) {
		t.Fatalf("Parse(linux/arm) = %v, %v", p, err)
	}
	const list = "linux/amd64, linux/arm64, linux/arm, linux/riscv64, linux/ppc64le, " +
		"freebsd/amd64, freebsd/arm64, illumos/amd64, darwin/amd64, darwin/arm64"
	for _, s := range []string{"linux/386", "linux"} {
		_, err := Parse(s)
		want := `unsupported platform "` + s + `"; supported: ` + list
		if err == nil || err.Error() != want {
			t.Errorf("Parse(%q) error = %v, want %q", s, err, want)
		}
	}
}

func TestSupportedCount(t *testing.T) {
	if n := len(Supported()); n != 10 {
		t.Fatalf("len(Supported()) = %d, want 10", n)
	}
}

func TestNames(t *testing.T) {
	p := Platform{"linux", "arm64"}
	if p.String() != "linux/arm64" || p.Dir() != "linux-arm64" {
		t.Errorf("String/Dir = %s, %s", p, p.Dir())
	}
	if got, want := Self().String(), runtime.GOOS+"/"+runtime.GOARCH; got != want {
		t.Errorf("Self = %s, want %s", got, want)
	}
}
