//go:build unix

package bootstrap

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/piotaixr/syngoid/internal/buildinfo"
	"github.com/piotaixr/syngoid/internal/dist"
	"github.com/piotaixr/syngoid/internal/logx"
	"github.com/piotaixr/syngoid/internal/platform"
	"github.com/piotaixr/syngoid/internal/trust/trusttest"
)

// otherPlatform returns a supported platform that is not Self.
func otherPlatform() Platform {
	for _, p := range platform.Supported() {
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
	if got, err := (Resolver{Executable: self}).Resolve(context.Background(), Self()); err != nil || got != "/self/syngoid" {
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
	if got, err := r.Resolve(context.Background(), p); err != nil || got != inStore2 {
		t.Errorf("store hit: Resolve = %q, %v; want %q", got, err, inStore2)
	}
	inStore1 := storeBinary(t, store1, p)
	if got, err := r.Resolve(context.Background(), p); err != nil || got != inStore1 {
		t.Errorf("first store wins: Resolve = %q, %v; want %q", got, err, inStore1)
	}
	inPkg := storeBinary(t, pkg, p)
	if got, err := r.Resolve(context.Background(), p); err != nil || got != inStore1 {
		t.Errorf("store before packaged: Resolve = %q, %v; want %q", got, err, inStore1)
	}
	r.BinStores = []string{empty}
	if got, err := r.Resolve(context.Background(), p); err != nil || got != inPkg {
		t.Errorf("packaged hit: Resolve = %q, %v; want %q", got, err, inPkg)
	}

	r = Resolver{BinStores: []string{empty}, PackagedStore: t.TempDir(), Executable: self}
	want := "no syngoid agent binary for " + p.String() + ": put one at DIR/" + p.OS + "-" + p.Arch +
		"/syngoid and pass --bin-store=DIR, or install /usr/lib/syngoid/bin/" + p.OS + "-" + p.Arch + "/syngoid"
	if _, err := r.Resolve(context.Background(), p); err == nil || err.Error() != want {
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

// releaseBuild stamps a release version for the test.
func releaseBuild(t *testing.T) {
	t.Helper()
	saved, savedDirty := buildinfo.Version, buildinfo.Dirty
	t.Cleanup(func() { buildinfo.Version, buildinfo.Dirty = saved, savedDirty })
	buildinfo.Version, buildinfo.Dirty = "1.2.3", ""
}

// releaseServer serves a signed 1.2.3 release of p and counts requests.
func releaseServer(t *testing.T, p Platform) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	files := trusttest.New(t).Release("1.2.3", map[string][]byte{
		dist.AssetName("1.2.3", p): []byte("agent for " + p.String()),
	})
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		b, ok := files[strings.TrimPrefix(r.URL.Path, "/")]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(b)
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func newCache(srv *httptest.Server) *dist.Store {
	return &dist.Store{Dir: filepath.Join(os.TempDir(), "unused"), Version: "1.2.3", URLTemplate: srv.URL + "/{file}"}
}

func tail(p Platform) string {
	d := p.OS + "-" + p.Arch
	return "put one at DIR/" + d + "/syngoid and pass --bin-store=DIR, or install /usr/lib/syngoid/bin/" + d + "/syngoid"
}

func TestResolveDownloads(t *testing.T) {
	releaseBuild(t)
	p := otherPlatform()
	srv, hits := releaseServer(t, p)
	cache := newCache(srv)
	cache.Dir = t.TempDir()
	r := Resolver{PackagedStore: t.TempDir(), Cache: cache}
	got, err := r.Resolve(context.Background(), p)
	if err != nil || got != filepath.Join(cache.Dir, p.OS+"-"+p.Arch, "syngoid") {
		t.Fatalf("Resolve = %q, %v", got, err)
	}
	n := hits.Load()
	if n == 0 {
		t.Fatal("no download happened")
	}
	if got2, err := r.Resolve(context.Background(), p); err != nil || got2 != got || hits.Load() != n {
		t.Errorf("second Resolve = %q, %v, %d new requests", got2, err, hits.Load()-n)
	}
}

func TestResolveCacheOrder(t *testing.T) {
	releaseBuild(t)
	p := otherPlatform()
	srv, hits := releaseServer(t, p)
	cache := newCache(srv)
	cache.Dir = t.TempDir()
	cached, err := cache.Download(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	hits.Store(0)
	r := Resolver{PackagedStore: t.TempDir(), Cache: cache}
	if got, err := r.Resolve(context.Background(), p); err != nil || got != cached || hits.Load() != 0 {
		t.Errorf("cache hit: %q, %v, %d requests", got, err, hits.Load())
	}
	store := t.TempDir()
	inStore := storeBinary(t, store, p)
	r.BinStores = []string{store}
	if got, err := r.Resolve(context.Background(), p); err != nil || got != inStore {
		t.Errorf("stores before cache: %q, %v; want %q", got, err, inStore)
	}
}

func TestResolveNoDownload(t *testing.T) {
	releaseBuild(t)
	p := otherPlatform()
	srv, hits := releaseServer(t, p)
	cache := newCache(srv)
	cache.Dir = t.TempDir()
	r := Resolver{PackagedStore: t.TempDir(), Cache: cache, NoDownload: true}
	want := "no syngoid agent binary for " + p.String() + " (--no-download): run \"syngoid fetch --platform " + p.String() + "\", or " + tail(p)
	if _, err := r.Resolve(context.Background(), p); err == nil || err.Error() != want {
		t.Errorf("error = %v\nwant %s", err, want)
	}
	if hits.Load() != 0 {
		t.Errorf("%d requests", hits.Load())
	}
	// A cache hit still wins.
	r.NoDownload = false
	cached, err := cache.Download(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	hits.Store(0)
	r.NoDownload = true
	if got, err := r.Resolve(context.Background(), p); err != nil || got != cached || hits.Load() != 0 {
		t.Errorf("cache hit: %q, %v, %d requests", got, err, hits.Load())
	}
}

func TestResolveDevSkipsCache(t *testing.T) {
	releaseBuild(t)
	p := otherPlatform()
	srv, hits := releaseServer(t, p)
	cache := newCache(srv)
	cache.Dir = t.TempDir()
	if _, err := cache.Download(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	hits.Store(0)
	buildinfo.Version = "dev"
	r := Resolver{PackagedStore: t.TempDir(), Cache: cache}
	want := "no syngoid agent binary for " + p.String() + ": " + tail(p)
	if _, err := r.Resolve(context.Background(), p); err == nil || err.Error() != want {
		t.Errorf("error = %v\nwant %s", err, want)
	}
	if hits.Load() != 0 {
		t.Errorf("%d requests", hits.Load())
	}
}

func TestResolveDownloadFailure(t *testing.T) {
	releaseBuild(t)
	p := otherPlatform()
	srv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(srv.Close)
	cache := newCache(srv)
	cache.Dir = t.TempDir()
	r := Resolver{PackagedStore: t.TempDir(), Cache: cache}
	_, err := r.Resolve(context.Background(), p)
	pre := "no syngoid agent binary for " + p.String() + ": download failed: "
	post := "; run \"syngoid fetch --platform " + p.String() + "\" where downloads work, or " + tail(p)
	if err == nil || !strings.HasPrefix(err.Error(), pre) || !strings.HasSuffix(err.Error(), post) || !strings.Contains(err.Error(), "GET ") {
		t.Errorf("error = %v", err)
	}
}

func TestResolveWithoutCache(t *testing.T) {
	releaseBuild(t)
	p := otherPlatform()
	r := Resolver{PackagedStore: t.TempDir(), CacheErr: errors.New("cannot locate the user cache: HOME is not set")}
	want := "no syngoid agent binary for " + p.String() + ": cannot locate the user cache: HOME is not set; " + tail(p)
	if _, err := r.Resolve(context.Background(), p); err == nil || err.Error() != want {
		t.Errorf("error = %v\nwant %s", err, want)
	}
}

func TestResolveRedownloadsCorruptCache(t *testing.T) {
	releaseBuild(t)
	p := otherPlatform()
	srv, hits := releaseServer(t, p)
	var errb bytes.Buffer
	cache := newCache(srv)
	cache.Dir = t.TempDir()
	cache.Log = logx.New(logx.Config{Stdout: io.Discard, Stderr: &errb})
	r := Resolver{PackagedStore: t.TempDir(), Cache: cache}
	got, err := r.Resolve(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(got, []byte("tampered"), 0o755); err != nil {
		t.Fatal(err)
	}
	before := hits.Load()
	got2, err := r.Resolve(context.Background(), p)
	if err != nil || got2 != got {
		t.Fatalf("second Resolve = %q, %v", got2, err)
	}
	if hits.Load() == before {
		t.Error("no second download")
	}
	if want := "WARNING: cached agent for " + p.String() + " does not match SHA256SUMS"; !strings.Contains(errb.String(), want) {
		t.Errorf("stderr %q does not contain %q", errb.String(), want)
	}
	if b, err := os.ReadFile(got2); err != nil || string(b) != "agent for "+p.String() {
		t.Errorf("content %q, %v", b, err)
	}
}
