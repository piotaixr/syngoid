//go:build releasecheck

package trust_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
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

	paths := artifactPaths(t, dir)
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
}

// artifactPaths maps each artifact name in dir/artifacts.json to its file.
// GoReleaser records paths relative to the project root, starting "dist/".
func artifactPaths(t *testing.T, dir string) map[string]string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "artifacts.json"))
	if err != nil {
		t.Fatal(err)
	}
	var artifacts []struct {
		Name string `json:"name"`
		Path string `json:"path"`
	}
	if err := json.Unmarshal(b, &artifacts); err != nil {
		t.Fatalf("artifacts.json: %v", err)
	}
	paths := map[string]string{}
	for _, a := range artifacts {
		rel, ok := strings.CutPrefix(filepath.ToSlash(a.Path), "dist/")
		if !ok {
			t.Fatalf("artifacts.json: %s is not under dist/", a.Path)
		}
		paths[a.Name] = filepath.Join(dir, filepath.FromSlash(rel))
	}
	return paths
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
