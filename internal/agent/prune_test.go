package agent

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

// cache builds home/.cache/syngoid with a directory per name, the i-th
// modified i hours after the epoch, and returns home.
func cache(t *testing.T, names ...string) string {
	t.Helper()
	home := t.TempDir()
	base := filepath.Join(home, ".cache", "syngoid")
	for i, n := range names {
		d := filepath.Join(base, n)
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		mt := time.Unix(1_700_000_000, 0).Add(time.Duration(i) * time.Hour)
		if err := os.Chtimes(d, mt, mt); err != nil {
			t.Fatal(err)
		}
	}
	return home
}

func present(t *testing.T, home string) map[string]bool {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(home, ".cache", "syngoid"))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, e := range entries {
		got[e.Name()] = true
	}
	return got
}

func checkPresent(t *testing.T, home string, want ...string) {
	t.Helper()
	got := present(t, home)
	if len(got) != len(want) {
		t.Errorf("remaining = %v, want %v", got, want)
	}
	for _, w := range want {
		if !got[w] {
			t.Errorf("%s was removed (remaining %v)", w, got)
		}
	}
}

func TestPruneSiblings(t *testing.T) {
	// Oldest first; the current 1.2.0 sits between its siblings by mtime.
	home := cache(t, "1.0.0", "1.1.0", "1.2.0", "dev-0123abcd", "bin", "notes")
	// Make bin and notes the newest so only the pattern keeps them.
	base := filepath.Join(home, ".cache", "syngoid")
	late := time.Unix(1_800_000_000, 0)
	for _, n := range []string{"bin", "notes"} {
		if err := os.Chtimes(filepath.Join(base, n), late, late); err != nil {
			t.Fatal(err)
		}
	}
	PruneSiblings(filepath.Join(base, "1.2.0", "syngoid"), home, discard)
	checkPresent(t, home, "1.2.0", "dev-0123abcd", "bin", "notes")
}

func TestPruneSiblingsOutsideDefaultBase(t *testing.T) {
	home := cache(t, "1.0.0", "1.1.0", "1.2.0")
	PruneSiblings("/opt/x/1.2.0/syngoid", home, discard)
	checkPresent(t, home, "1.0.0", "1.1.0", "1.2.0")

	// Deeper than the cache directory, or a current dir that is not an
	// agent directory: also nothing.
	base := filepath.Join(home, ".cache", "syngoid")
	PruneSiblings(filepath.Join(base, "1.2.0", "sub", "syngoid"), home, discard)
	PruneSiblings(filepath.Join(base, "bin", "syngoid"), home, discard)
	PruneSiblings("", home, discard)
	PruneSiblings(filepath.Join(base, "1.2.0", "syngoid"), "", discard)
	checkPresent(t, home, "1.0.0", "1.1.0", "1.2.0")
}

func TestPruneSiblingsTieRemovesNothing(t *testing.T) {
	home := cache(t, "1.0.0", "1.1.0", "1.2.0")
	base := filepath.Join(home, ".cache", "syngoid")
	same := time.Unix(1_800_000_000, 0)
	for _, n := range []string{"1.0.0", "1.1.0"} {
		if err := os.Chtimes(filepath.Join(base, n), same, same); err != nil {
			t.Fatal(err)
		}
	}
	PruneSiblings(filepath.Join(base, "1.2.0", "syngoid"), home, discard)
	checkPresent(t, home, "1.0.0", "1.1.0", "1.2.0")
}

func TestPruneSiblingsLeavesFilesAndSymlinks(t *testing.T) {
	home := cache(t, "1.0.0", "1.1.0", "1.2.0")
	base := filepath.Join(home, ".cache", "syngoid")
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(base, "1.9.0")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "1.8.0"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	PruneSiblings(filepath.Join(base, "1.2.0", "syngoid"), home, discard)
	checkPresent(t, home, "1.1.0", "1.2.0", "1.8.0", "1.9.0")
	if _, err := os.Stat(outside); err != nil {
		t.Errorf("symlink target was touched: %v", err)
	}
}

func TestPruneSiblingsMissingCache(t *testing.T) {
	home := t.TempDir()
	PruneSiblings(filepath.Join(home, ".cache", "syngoid", "1.2.0", "syngoid"), home, discard)
}
