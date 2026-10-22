//go:build unix

package bootstrap

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/piotaixr/syngoid/internal/buildinfo"
)

// otherPlatform returns a supported platform that is not Self.
func otherPlatform() Platform {
	for _, p := range supported {
		if p != Self() {
			return p
		}
	}
	panic("no other platform")
}

func storeBinary(t *testing.T, store string, p Platform) string {
	t.Helper()
	path := filepath.Join(store, p.OS+"-"+p.Arch, "syngoid")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestResolveOrder(t *testing.T) {
	self := func() (string, error) { return "/self/syngoid", nil }
	if got, err := (Resolver{Executable: self}).Resolve(Self()); err != nil || got != "/self/syngoid" {
		t.Errorf("Resolve(Self) = %q, %v", got, err)
	}

	p := otherPlatform()
	empty, store1, store2, pkg := t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir()
	// A directory where the binary should be is not a hit.
	if err := os.MkdirAll(filepath.Join(empty, p.OS+"-"+p.Arch, "syngoid"), 0o755); err != nil {
		t.Fatal(err)
	}
	inStore2 := storeBinary(t, store2, p)
	r := Resolver{BinStores: []string{empty, store1, store2}, PackagedStore: pkg, Executable: self}
	if got, err := r.Resolve(p); err != nil || got != inStore2 {
		t.Errorf("store hit: Resolve = %q, %v; want %q", got, err, inStore2)
	}
	inStore1 := storeBinary(t, store1, p)
	if got, err := r.Resolve(p); err != nil || got != inStore1 {
		t.Errorf("first store wins: Resolve = %q, %v; want %q", got, err, inStore1)
	}
	inPkg := storeBinary(t, pkg, p)
	if got, err := r.Resolve(p); err != nil || got != inStore1 {
		t.Errorf("store before packaged: Resolve = %q, %v; want %q", got, err, inStore1)
	}
	r.BinStores = []string{empty}
	if got, err := r.Resolve(p); err != nil || got != inPkg {
		t.Errorf("packaged hit: Resolve = %q, %v; want %q", got, err, inPkg)
	}

	r = Resolver{BinStores: []string{empty}, PackagedStore: t.TempDir(), Executable: self}
	want := "no syngoid agent binary for " + p.String() + ": put one at DIR/" + p.OS + "-" + p.Arch +
		"/syngoid and pass --bin-store=DIR, or install /usr/lib/syngoid/bin/" + p.OS + "-" + p.Arch + "/syngoid"
	if _, err := r.Resolve(p); err == nil || err.Error() != want {
		t.Errorf("Resolve error = %v\nwant %s", err, want)
	}
}

func TestAgentDirName(t *testing.T) {
	saved, savedDirty := buildinfo.Version, buildinfo.Dirty
	t.Cleanup(func() { buildinfo.Version, buildinfo.Dirty = saved, savedDirty })

	buildinfo.Version, buildinfo.Dirty = "1.2.3", ""
	if got := AgentDirName(); got != "1.2.3" {
		t.Errorf("release AgentDirName = %q", got)
	}
	if got := DefaultRemoteBinDir(); got != "~/.cache/syngoid/1.2.3" {
		t.Errorf("release DefaultRemoteBinDir = %q", got)
	}
	buildinfo.Version = "dev"
	if got := AgentDirName(); !regexp.MustCompile(`^dev-[0-9a-f]{8}$`).MatchString(got) {
		t.Errorf("dev AgentDirName = %q", got)
	}
}
