//go:build unix

package app

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/piotaixr/syngoid/internal/buildinfo"
	"github.com/piotaixr/syngoid/internal/dist"
	"github.com/piotaixr/syngoid/internal/platform"
	"github.com/piotaixr/syngoid/internal/trust/trusttest"
)

// fetchEnv stamps a release build, points the user cache at a temp dir and
// serves a signed 1.2.3 release of the given platforms.
type fetchEnv struct {
	env            Env
	stdout, stderr *bytes.Buffer
	cache          string
	srv            *httptest.Server

	mu   sync.Mutex
	hits map[string]int
}

func newFetchEnv(t *testing.T, platforms []platform.Platform) *fetchEnv {
	t.Helper()
	saved, savedDirty := buildinfo.Version, buildinfo.Dirty
	t.Cleanup(func() { buildinfo.Version, buildinfo.Dirty = saved, savedDirty })
	buildinfo.Version, buildinfo.Dirty = "1.2.3", ""

	fe := &fetchEnv{cache: t.TempDir(), hits: map[string]int{}}
	t.Setenv("XDG_CACHE_HOME", fe.cache)
	fe.env, fe.stdout, fe.stderr = testEnv()

	assets := map[string][]byte{}
	for _, p := range platforms {
		assets[dist.AssetName("1.2.3", p)] = []byte("agent for " + p.String())
	}
	files := trusttest.New(t).Release("1.2.3", assets)
	fe.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/")
		fe.mu.Lock()
		fe.hits[name]++
		fe.mu.Unlock()
		b, ok := files[name]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(b)
	}))
	t.Cleanup(fe.srv.Close)
	return fe
}

func (fe *fetchEnv) run(args ...string) int {
	args = append([]string{"fetch", "--download-url", fe.srv.URL + "/{file}"}, args...)
	return Main(context.Background(), args, fe.env)
}

func (fe *fetchEnv) agentHits() int {
	fe.mu.Lock()
	defer fe.mu.Unlock()
	n := 0
	for name, c := range fe.hits {
		if strings.HasPrefix(name, "syngoid_") {
			n += c
		}
	}
	return n
}

func TestFetchAllDir(t *testing.T) {
	fe := newFetchEnv(t, platform.Supported())
	dir := filepath.Join(t.TempDir(), "store")
	if got := fe.run("--all", "--dir", dir); got != 0 {
		t.Fatalf("exit code = %d, want 0; stderr %q", got, fe.stderr)
	}
	for _, p := range platform.Supported() {
		b, err := os.ReadFile(filepath.Join(dir, p.Dir(), "syngoid"))
		if err != nil || string(b) != "agent for "+p.String() {
			t.Errorf("%s: %q, %v", p, b, err)
		}
	}
	for _, f := range []string{"SHA256SUMS", "SHA256SUMS.minisig"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Error(err)
		}
		if fe.hits[f] != 1 {
			t.Errorf("%s requested %d times, want 1", f, fe.hits[f])
		}
	}
	if _, err := os.Stat(filepath.Join(fe.cache, "syngoid")); err == nil {
		t.Error("--dir must not touch the user cache")
	}
}

func TestFetchIntoUserCache(t *testing.T) {
	p := platform.Platform{OS: "linux", Arch: "arm64"}
	fe := newFetchEnv(t, []platform.Platform{p})
	if got := fe.run("--platform", "linux/arm64"); got != 0 {
		t.Fatalf("exit code = %d, want 0; stderr %q", got, fe.stderr)
	}
	path := filepath.Join(fe.cache, "syngoid", "bin", "1.2.3", "linux-arm64", "syngoid")
	if b, err := os.ReadFile(path); err != nil || string(b) != "agent for linux/arm64" {
		t.Errorf("%s: %q, %v", path, b, err)
	}
}

func TestFetchAlreadyPresent(t *testing.T) {
	p := platform.Platform{OS: "linux", Arch: "arm64"}
	fe := newFetchEnv(t, []platform.Platform{p})
	if got := fe.run("--platform", "linux/arm64"); got != 0 {
		t.Fatalf("first run exit code = %d; stderr %q", got, fe.stderr)
	}
	before := fe.agentHits()
	fe.stdout.Reset()
	if got := fe.run("--platform", "linux/arm64"); got != 0 {
		t.Fatalf("second run exit code = %d; stderr %q", got, fe.stderr)
	}
	if want := "INFO: linux/arm64 already present"; !strings.Contains(fe.stdout.String(), want) {
		t.Errorf("stdout %q does not contain %q", fe.stdout, want)
	}
	if got := fe.agentHits(); got != before {
		t.Errorf("agent requests %d -> %d, want none", before, got)
	}
}

func TestFetchPartialFailure(t *testing.T) {
	fe := newFetchEnv(t, []platform.Platform{{OS: "linux", Arch: "arm64"}})
	got := fe.run("--platform", "freebsd/amd64", "--platform", "linux/arm64")
	if got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
	var line string
	for _, l := range strings.Split(fe.stderr.String(), "\n") {
		if strings.HasPrefix(l, "CRITICAL ERROR: ") && strings.Contains(l, "freebsd/amd64") {
			line = l
		}
	}
	if line == "" {
		t.Errorf("stderr %q has no CRITICAL ERROR line naming freebsd/amd64", fe.stderr)
	}
	path := filepath.Join(fe.cache, "syngoid", "bin", "1.2.3", "linux-arm64", "syngoid")
	if _, err := os.Stat(path); err != nil {
		t.Errorf("linux/arm64 not installed: %v", err)
	}
}

func TestFetchDevBuild(t *testing.T) {
	fe := newFetchEnv(t, nil)
	buildinfo.Version = "dev"
	if got := fe.run("--all"); got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
	if want := "CRITICAL ERROR: fetch needs a release build\n"; fe.stderr.String() != want {
		t.Errorf("stderr %q, want %q", fe.stderr, want)
	}
	if len(fe.hits) != 0 {
		t.Errorf("requests made: %v", fe.hits)
	}
}

// TestFetchCancelled cancels the context while the first agent download is in
// flight: fetch must stop, not touch the remaining platforms, log exactly one
// "CRITICAL ERROR: fetch cancelled" line and exit 2.
func TestFetchCancelled(t *testing.T) {
	saved, savedDirty := buildinfo.Version, buildinfo.Dirty
	t.Cleanup(func() { buildinfo.Version, buildinfo.Dirty = saved, savedDirty })
	buildinfo.Version, buildinfo.Dirty = "1.2.3", ""
	t.Setenv("XDG_CACHE_HOME", t.TempDir())

	platforms := []platform.Platform{{OS: "linux", Arch: "arm64"}, {OS: "freebsd", Arch: "amd64"}}
	assets := map[string][]byte{}
	for _, p := range platforms {
		assets[dist.AssetName("1.2.3", p)] = []byte("agent for " + p.String())
	}
	files := trusttest.New(t).Release("1.2.3", assets)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	agentReqs := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/")
		if strings.HasPrefix(name, "syngoid_") {
			mu.Lock()
			agentReqs++
			mu.Unlock()
			cancel()
			<-r.Context().Done()
			return
		}
		b, ok := files[name]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(b)
	}))
	t.Cleanup(srv.Close)

	env, _, stderr := testEnv()
	got := Main(ctx, []string{"fetch", "--download-url", srv.URL + "/{file}", "--platform", "linux/arm64", "--platform", "freebsd/amd64"}, env)
	if got != 2 {
		t.Fatalf("exit code = %d, want 2; stderr %q", got, stderr)
	}
	if want := "CRITICAL ERROR: fetch cancelled\n"; stderr.String() != want {
		t.Errorf("stderr %q, want %q", stderr, want)
	}
	mu.Lock()
	defer mu.Unlock()
	if agentReqs != 1 {
		t.Errorf("agent requests = %d, want 1", agentReqs)
	}
}
