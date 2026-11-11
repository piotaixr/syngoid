//go:build unix

package dist

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/piotaixr/syngoid/internal/logx"
	"github.com/piotaixr/syngoid/internal/platform"
	"github.com/piotaixr/syngoid/internal/trust/trusttest"
)

func TestAssetName(t *testing.T) {
	got := AssetName("1.2.3", platform.Platform{OS: "linux", Arch: "arm64"})
	if got != "syngoid_1.2.3_linux_arm64" {
		t.Errorf("AssetName = %q", got)
	}
}

func TestUserCacheDir(t *testing.T) {
	tests := []struct {
		name, xdg, home, want, wantErr string
	}{
		{name: "absolute XDG", xdg: "/x/cache", home: "/h", want: "/x/cache/syngoid/bin/1.2.3"},
		{name: "relative XDG ignored", xdg: "rel", home: "/h", want: "/h/.cache/syngoid/bin/1.2.3"},
		{name: "XDG unset", home: "/h", want: "/h/.cache/syngoid/bin/1.2.3"},
		{name: "nothing", wantErr: "cannot locate the user cache: HOME is not set"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("XDG_CACHE_HOME", tt.xdg)
			t.Setenv("HOME", tt.home)
			got, err := UserCacheDir("1.2.3")
			if tt.wantErr != "" {
				if err == nil || err.Error() != tt.wantErr {
					t.Fatalf("error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("UserCacheDir = %q, %v; want %q", got, err, tt.want)
			}
		})
	}
}

func TestExpandURL(t *testing.T) {
	p := platform.Platform{OS: "linux", Arch: "arm64"}
	const def = "https://github.com/piotaixr/syngoid/releases/download/v{version}/{file}"
	got, err := ExpandURL(def, "1.2.3", "SHA256SUMS", p)
	if want := "https://github.com/piotaixr/syngoid/releases/download/v1.2.3/SHA256SUMS"; err != nil || got != want {
		t.Fatalf("ExpandURL = %q, %v; want %q", got, err, want)
	}
	got, err = ExpandURL("http://h/{os}/{arch}/{version}/{file}", "1.2.3", "f", p)
	if want := "http://h/linux/arm64/1.2.3/f"; err != nil || got != want {
		t.Fatalf("ExpandURL = %q, %v; want %q", got, err, want)
	}
	for _, tt := range []struct{ tmpl, want string }{
		{"https://h/x", `--download-url "https://h/x": must contain {file}`},
		{"https://h/{file}/{nope}", `--download-url "https://h/{file}/{nope}": unknown placeholder {nope}`},
		{"ftp://h/{file}", `--download-url "ftp://h/{file}": scheme must be http or https`},
	} {
		if _, err := ExpandURL(tt.tmpl, "1.2.3", "f", p); err == nil || err.Error() != tt.want {
			t.Errorf("ExpandURL(%q) error = %v, want %q", tt.tmpl, err, tt.want)
		}
	}
}

func TestLookup(t *testing.T) {
	fx := newFixture(t, "1.2.3")
	s, _, _ := fx.store(t)
	if path, ok := s.Lookup(testPlatform); ok {
		t.Fatalf("Lookup on an empty dir = %q, true", path)
	}
	want, err := s.Download(t.Context(), testPlatform)
	if err != nil {
		t.Fatal(err)
	}
	before := fx.total()
	fresh := &Store{Dir: s.Dir, Version: "1.2.3"} // no client: it must not need one
	got, ok := fresh.Lookup(testPlatform)
	if !ok || got != want {
		t.Fatalf("Lookup = %q, %v; want %q, true", got, ok, want)
	}
	if _, ok := fresh.Lookup(otherPlatform); ok {
		t.Error("Lookup of a platform never downloaded hit")
	}
	if fx.total() != before {
		t.Error("Lookup made an HTTP request")
	}
}

func TestLookupMissWithoutWarning(t *testing.T) {
	trusttest.New(t)
	var out, errb bytes.Buffer
	s := &Store{Dir: t.TempDir(), Version: "1.2.3", Log: logx.New(logx.Config{Stdout: &out, Stderr: &errb})}
	if _, ok := s.Lookup(testPlatform); ok {
		t.Fatal("hit on an empty dir")
	}
	if out.Len()+errb.Len() != 0 {
		t.Errorf("unexpected log output: %q %q", out.String(), errb.String())
	}
}

func TestLookupCorruptBinaryRemoved(t *testing.T) {
	fx := newFixture(t, "1.2.3")
	s, _, errb := fx.store(t)
	path, err := s.Download(t.Context(), testPlatform)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("tampered"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Lookup(testPlatform); ok {
		t.Fatal("corrupt binary hit")
	}
	want := "WARNING: cached agent for linux/arm64 does not match SHA256SUMS, removing it\n"
	if errb.String() != want {
		t.Errorf("stderr = %q, want %q", errb.String(), want)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("corrupt binary still present: %v", err)
	}
}

func TestLookupBadSumsRemoved(t *testing.T) {
	fx := newFixture(t, "1.2.3")
	s, _, errb := fx.store(t)
	path, err := s.Download(t.Context(), testPlatform)
	if err != nil {
		t.Fatal(err)
	}
	sums := filepath.Join(s.Dir, "SHA256SUMS")
	b, _ := os.ReadFile(sums)
	if err := os.WriteFile(sums, append(b, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Lookup(testPlatform); ok {
		t.Fatal("hit with tampered sums")
	}
	if want := "WARNING: " + sums + " does not verify ("; !strings.HasPrefix(errb.String(), want) {
		t.Errorf("stderr = %q, want prefix %q", errb.String(), want)
	}
	for _, f := range []string{sums, sums + ".minisig"} {
		if _, err := os.Stat(f); !os.IsNotExist(err) {
			t.Errorf("%s still present: %v", f, err)
		}
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("binary should be left alone: %v", err)
	}
}
