//go:build releasecheck

package trust_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/piotaixr/syngoid/internal/dist"
	"github.com/piotaixr/syngoid/internal/platform"
	"github.com/piotaixr/syngoid/internal/trust"
)

// TestReleaseCheck verifies a GoReleaser dist directory with syngoid's own
// verifier: the SHA256SUMS signature against the public key named by
// SYNGOID_RELEASECHECK_PUBKEY, and every raw agent and archive hash. It is run
// by `mise run release-check` (scripts/release-check.sh). Files are found
// through artifacts.json because GoReleaser leaves each raw agent at
// dist/<build target>/syngoid and gives it its asset name only on upload.
// The raw agent for this host is also run, to check its version stamp.
func TestReleaseCheck(t *testing.T) {
	dir := os.Getenv("SYNGOID_RELEASECHECK_DIR")
	if dir == "" {
		dir = filepath.Join("..", "..", "dist")
	}
	pubPath := os.Getenv("SYNGOID_RELEASECHECK_PUBKEY")
	if pubPath == "" {
		t.Fatal("SYNGOID_RELEASECHECK_PUBKEY is not set")
	}

	pubText, err := os.ReadFile(pubPath)
	if err != nil {
		t.Fatal(err)
	}
	key, err := trust.ParsePublicKey(string(pubText))
	if err != nil {
		t.Fatal(err)
	}

	var meta struct {
		Version string `json:"version"`
	}
	metaJSON, err := os.ReadFile(filepath.Join(dir, "metadata.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(metaJSON, &meta); err != nil {
		t.Fatalf("metadata.json: %v", err)
	}
	if meta.Version == "" {
		t.Fatal("metadata.json: no version")
	}

	sums, err := os.ReadFile(filepath.Join(dir, "SHA256SUMS"))
	if err != nil {
		t.Fatal(err)
	}
	sig, err := os.ReadFile(filepath.Join(dir, "SHA256SUMS.minisig"))
	if err != nil {
		t.Fatal(err)
	}
	if err := trust.VerifyWith([]trust.PublicKey{key}, sums, sig, meta.Version); err != nil {
		t.Fatalf("VerifyWith: %v", err)
	}

	artifacts := readArtifacts(t, dir)
	paths := map[string]string{}
	for _, a := range artifacts {
		paths[a.Name] = a.Path
	}
	listed, err := trust.ParseSums(sums)
	if err != nil {
		t.Fatal(err)
	}
	targets := platform.Supported()
	if want := 2 * len(targets); len(listed) != want {
		t.Errorf("SHA256SUMS has %d entries, want %d", len(listed), want)
	}
	for _, p := range targets {
		raw := dist.AssetName(meta.Version, p)
		for _, name := range []string{raw, raw + ".tar.gz"} {
			want, ok := listed[name]
			if !ok {
				t.Errorf("SHA256SUMS does not list %s", name)
				continue
			}
			path, ok := paths[name]
			if !ok {
				t.Errorf("artifacts.json has no %s", name)
				continue
			}
			if got := fileSHA256(t, path); got != want {
				t.Errorf("%s: sha256 %s, SHA256SUMS says %s", name, got, want)
			}
		}
	}

	checkVersionStamp(t, artifacts, meta.Version)
}

// checkVersionStamp runs the raw agent built for this host with `version` and
// checks that it reports version, i.e. that the -X ldflags stamp took. It
// skips on a host no release target matches.
func checkVersionStamp(t *testing.T, artifacts []artifact, version string) {
	t.Helper()
	self := platform.Self() // runtime.GOOS and runtime.GOARCH
	name := dist.AssetName(version, self)
	var bin string
	for _, a := range artifacts {
		if a.GoOS == self.OS && a.GoArch == self.Arch && a.Name == name {
			bin = a.Path
		}
	}
	if bin == "" {
		t.Skipf("artifacts.json has no raw agent for %s", self)
	}
	out, err := exec.Command(bin, "version").Output()
	if err != nil {
		t.Fatalf("%s version: %v", name, err)
	}
	got := strings.TrimSpace(string(out))
	if f := strings.Fields(got); len(f) < 2 || f[1] != version || strings.Contains(got, "dev") {
		t.Errorf("%s version printed %q, want version %s and no \"dev\"", name, got, version)
	}
}

// artifact is one entry of GoReleaser's artifacts.json, with Path resolved
// under the dist directory.
type artifact struct {
	Name   string `json:"name"`
	Path   string `json:"path"`
	GoOS   string `json:"goos"`
	GoArch string `json:"goarch"`
}

// readArtifacts reads dir/artifacts.json. GoReleaser records paths relative
// to the project root, starting "dist/".
func readArtifacts(t *testing.T, dir string) []artifact {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "artifacts.json"))
	if err != nil {
		t.Fatal(err)
	}
	var artifacts []artifact
	if err := json.Unmarshal(b, &artifacts); err != nil {
		t.Fatalf("artifacts.json: %v", err)
	}
	for i, a := range artifacts {
		rel, ok := strings.CutPrefix(filepath.ToSlash(a.Path), "dist/")
		if !ok {
			t.Fatalf("artifacts.json: %s is not under dist/", a.Path)
		}
		artifacts[i].Path = filepath.Join(dir, filepath.FromSlash(rel))
	}
	return artifacts
}

func fileSHA256(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
