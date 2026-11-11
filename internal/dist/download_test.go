//go:build unix

package dist

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/piotaixr/syngoid/internal/logx"
	"github.com/piotaixr/syngoid/internal/platform"
	"github.com/piotaixr/syngoid/internal/trust/trusttest"
)

var (
	testPlatform  = platform.Platform{OS: "linux", Arch: "arm64"}
	otherPlatform = platform.Platform{OS: "freebsd", Arch: "amd64"}
)

// fixture serves a signed release and counts requests per path.
type fixture struct {
	srv      *httptest.Server
	version  string
	files    map[string][]byte
	override map[string]http.HandlerFunc // set before the first request

	mu   sync.Mutex
	hits map[string]int
}

func binaryFor(p platform.Platform) []byte {
	return bytes.Repeat([]byte("agent for "+p.String()+"\n"), 100)
}

// newFixture signs a release of the given version covering both test
// platforms. The version the store asks for is set by the test.
func newFixture(t *testing.T, version string) *fixture {
	t.Helper()
	signer := trusttest.New(t)
	fx := &fixture{
		version:  version,
		hits:     map[string]int{},
		override: map[string]http.HandlerFunc{},
		files: signer.Release(version, map[string][]byte{
			AssetName(version, testPlatform):  binaryFor(testPlatform),
			AssetName(version, otherPlatform): binaryFor(otherPlatform),
		}),
	}
	fx.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fx.mu.Lock()
		fx.hits[r.URL.Path]++
		fx.mu.Unlock()
		if h, ok := fx.override[r.URL.Path]; ok {
			h(w, r)
			return
		}
		b, ok := fx.files[strings.TrimPrefix(r.URL.Path, "/")]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(b)
	}))
	t.Cleanup(fx.srv.Close)
	return fx
}

func (fx *fixture) hitCount(path string) int {
	fx.mu.Lock()
	defer fx.mu.Unlock()
	return fx.hits[path]
}

func (fx *fixture) total() int {
	fx.mu.Lock()
	defer fx.mu.Unlock()
	n := 0
	for _, c := range fx.hits {
		n += c
	}
	return n
}

// store returns a Store on a fresh dir for version 1.2.3 plus the captured
// stdout and stderr.
func (fx *fixture) store(t *testing.T) (*Store, *bytes.Buffer, *bytes.Buffer) {
	t.Helper()
	var out, errb bytes.Buffer
	return &Store{
		Dir:         t.TempDir(),
		Version:     "1.2.3",
		URLTemplate: fx.srv.URL + "/{file}",
		Log:         logx.New(logx.Config{Stdout: &out, Stderr: &errb}),
	}, &out, &errb
}

// treeFiles lists every file and directory under dir, relative.
func treeFiles(t *testing.T, dir string) []string {
	t.Helper()
	var names []string
	err := filepath.WalkDir(dir, func(p string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p != dir {
			rel, _ := filepath.Rel(dir, p)
			names = append(names, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return names
}

func noTemps(t *testing.T, dir string) {
	t.Helper()
	for _, n := range treeFiles(t, dir) {
		if strings.Contains(n, ".syngoid-dl-") {
			t.Errorf("temp file left behind: %s", n)
		}
	}
}

func noBinary(t *testing.T, s *Store) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(s.Dir, testPlatform.Dir(), "syngoid")); !os.IsNotExist(err) {
		t.Errorf("binary installed: %v", err)
	}
}

func TestDownloadInstalls(t *testing.T) {
	fx := newFixture(t, "1.2.3")
	s, out, _ := fx.store(t)
	path, err := s.Download(t.Context(), testPlatform)
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(s.Dir, "linux-arm64", "syngoid"); path != want {
		t.Fatalf("path = %q, want %q", path, want)
	}
	st, err := os.Stat(path)
	if err != nil || st.Mode().Perm() != 0o755 {
		t.Fatalf("stat = %v, %v; want mode 0755", st, err)
	}
	if got, _ := os.ReadFile(path); !bytes.Equal(got, binaryFor(testPlatform)) {
		t.Error("installed content differs")
	}
	for _, f := range []string{"SHA256SUMS", "SHA256SUMS.minisig"} {
		if got, _ := os.ReadFile(filepath.Join(s.Dir, f)); !bytes.Equal(got, fx.files[f]) {
			t.Errorf("%s not installed verbatim", f)
		}
	}
	noTemps(t, s.Dir)
	want := "INFO: downloading syngoid agent for linux/arm64 from " + fx.srv.URL + "/syngoid_1.2.3_linux_arm64\n"
	if out.String() != want {
		t.Errorf("stdout = %q, want %q", out.String(), want)
	}

	if _, err := s.Download(t.Context(), otherPlatform); err != nil {
		t.Fatal(err)
	}
	if n := fx.hitCount("/SHA256SUMS"); n != 1 {
		t.Errorf("SHA256SUMS fetched %d times, want 1", n)
	}
	if n := fx.hitCount("/SHA256SUMS.minisig"); n != 1 {
		t.Errorf("SHA256SUMS.minisig fetched %d times, want 1", n)
	}
	if _, ok := s.Lookup(otherPlatform); !ok {
		t.Error("second platform is not a Lookup hit")
	}
}

func TestDownloadExpandsPlatformInSumsURL(t *testing.T) {
	fx := newFixture(t, "1.2.3")
	s, _, _ := fx.store(t)
	s.URLTemplate = fx.srv.URL + "/{os}/{arch}/{file}"
	for name, b := range fx.files {
		fx.files["linux/arm64/"+name] = b
	}
	if _, err := s.Download(t.Context(), testPlatform); err != nil {
		t.Fatal(err)
	}
	if fx.hitCount("/linux/arm64/SHA256SUMS") != 1 {
		t.Error("sums URL did not expand {os}/{arch}")
	}
}

func TestDownloadRejectsOtherVersion(t *testing.T) {
	fx := newFixture(t, "1.2.2")
	s, _, _ := fx.store(t)
	_, err := s.Download(t.Context(), testPlatform)
	if err == nil || !strings.Contains(err.Error(), `signed for "syngoid 1.2.2", want "syngoid 1.2.3"`) {
		t.Fatalf("error = %v", err)
	}
	if !strings.HasPrefix(err.Error(), "SHA256SUMS from "+fx.srv.URL+"/SHA256SUMS: ") {
		t.Errorf("error lacks the source prefix: %v", err)
	}
	if names := treeFiles(t, s.Dir); len(names) != 0 {
		t.Errorf("Dir not empty: %v", names)
	}
}

func TestDownloadBadSignature(t *testing.T) {
	fx := newFixture(t, "1.2.3")
	bad := bytes.Clone(fx.files["SHA256SUMS"])
	bad[0] ^= 1
	fx.files["SHA256SUMS"] = bad
	s, _, _ := fx.store(t)
	if _, err := s.Download(t.Context(), testPlatform); err == nil {
		t.Fatal("tampered sums accepted")
	}
	if names := treeFiles(t, s.Dir); len(names) != 0 {
		t.Errorf("Dir not empty: %v", names)
	}
}

func TestDownloadHashMismatch(t *testing.T) {
	fx := newFixture(t, "1.2.3")
	name := AssetName("1.2.3", testPlatform)
	bad := bytes.Clone(fx.files[name])
	bad[0] ^= 1
	fx.files[name] = bad
	s, _, _ := fx.store(t)
	_, err := s.Download(t.Context(), testPlatform)
	if want := name + ": SHA-256 does not match SHA256SUMS"; err == nil || err.Error() != want {
		t.Fatalf("error = %v, want %q", err, want)
	}
	noBinary(t, s)
	noTemps(t, s.Dir)
	for _, f := range []string{"SHA256SUMS", "SHA256SUMS.minisig"} {
		if _, err := os.Stat(filepath.Join(s.Dir, f)); !os.IsNotExist(err) {
			t.Errorf("%s installed despite the failure", f)
		}
	}
}

func TestDownloadNoEntry(t *testing.T) {
	fx := newFixture(t, "1.2.3")
	s, _, _ := fx.store(t)
	_, err := s.Download(t.Context(), platform.Platform{OS: "linux", Arch: "riscv64"})
	if want := "SHA256SUMS has no entry for syngoid_1.2.3_linux_riscv64"; err == nil || err.Error() != want {
		t.Fatalf("error = %v, want %q", err, want)
	}
}

func TestDownloadHTTPErrors(t *testing.T) {
	for _, tt := range []struct {
		path   string
		status int
		text   string
	}{
		{"/SHA256SUMS", 404, "404 Not Found"},
		{"/SHA256SUMS.minisig", 500, "500 Internal Server Error"},
		{"/syngoid_1.2.3_linux_arm64", 404, "404 Not Found"},
		{"/syngoid_1.2.3_linux_arm64", 500, "500 Internal Server Error"},
	} {
		t.Run(fmt.Sprint(tt.path, tt.status), func(t *testing.T) {
			fx := newFixture(t, "1.2.3")
			fx.override[tt.path] = func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(tt.status) }
			s, _, _ := fx.store(t)
			_, err := s.Download(t.Context(), testPlatform)
			if want := "GET " + fx.srv.URL + tt.path + ": " + tt.text; err == nil || err.Error() != want {
				t.Fatalf("error = %v, want %q", err, want)
			}
			noBinary(t, s)
			noTemps(t, s.Dir)
		})
	}
}

func TestDownloadSizeCaps(t *testing.T) {
	for _, path := range []string{"/SHA256SUMS", "/SHA256SUMS.minisig"} {
		t.Run(path, func(t *testing.T) {
			fx := newFixture(t, "1.2.3")
			fx.override[path] = func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write(make([]byte, 1<<20+1))
			}
			s, _, _ := fx.store(t)
			_, err := s.Download(t.Context(), testPlatform)
			if want := fx.srv.URL + path + ": larger than 1048576 bytes"; err == nil || err.Error() != want {
				t.Fatalf("error = %v, want %q", err, want)
			}
		})
	}
}

func TestDownloadCancelledMidBodyInstallsNothing(t *testing.T) {
	fx := newFixture(t, "1.2.3")
	half := make(chan struct{})
	body := binaryFor(testPlatform)
	fx.override["/syngoid_1.2.3_linux_arm64"] = func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body[:len(body)/2])
		w.(http.Flusher).Flush()
		close(half)
		<-r.Context().Done()
	}
	s, _, _ := fx.store(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		<-half
		cancel()
	}()
	_, err := s.Download(ctx, testPlatform)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	noBinary(t, s)
	noTemps(t, s.Dir)
}

func TestDownloadConcurrentSameDir(t *testing.T) {
	fx := newFixture(t, "1.2.3")
	a, _, _ := fx.store(t)
	b := &Store{Dir: a.Dir, Version: a.Version, URLTemplate: a.URLTemplate}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := range 8 {
		s := a
		if i%2 == 1 {
			s = b
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Download(context.Background(), testPlatform)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
	path, ok := a.Lookup(testPlatform)
	if !ok {
		t.Fatal("installed binary does not verify")
	}
	got, _ := os.ReadFile(path)
	if sha256.Sum256(got) != sha256.Sum256(binaryFor(testPlatform)) {
		t.Error("content differs")
	}
	noTemps(t, a.Dir)
}

func TestDownloadFollowsRedirect(t *testing.T) {
	fx := newFixture(t, "1.2.3")
	redir := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, fx.srv.URL+r.URL.Path, http.StatusFound)
	}))
	defer redir.Close()
	s, _, _ := fx.store(t)
	s.URLTemplate = redir.URL + "/{file}"
	if _, err := s.Download(t.Context(), testPlatform); err != nil {
		t.Fatal(err)
	}
}

func TestDownloadScheme(t *testing.T) {
	fx := newFixture(t, "1.2.3")
	s, _, _ := fx.store(t)
	s.URLTemplate = "ftp://x/{file}"
	_, err := s.Download(t.Context(), testPlatform)
	if want := `--download-url "ftp://x/{file}": scheme must be http or https`; err == nil || err.Error() != want {
		t.Fatalf("error = %v, want %q", err, want)
	}
	if fx.total() != 0 {
		t.Error("a request was made")
	}
}

func TestDownloadConcurrentSameStoreOnce(t *testing.T) {
	fx := newFixture(t, "1.2.3")
	s, _, _ := fx.store(t)
	var wg sync.WaitGroup
	paths := make(chan string, 8)
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, err := s.Download(context.Background(), testPlatform)
			paths <- p
			errs <- err
		}()
	}
	wg.Wait()
	close(paths)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
	first := ""
	for p := range paths {
		if first == "" {
			first = p
		}
		if p == "" || p != first {
			t.Errorf("path %q, want %q", p, first)
		}
	}
	if n := fx.hitCount("/" + AssetName("1.2.3", testPlatform)); n != 1 {
		t.Errorf("agent requested %d times, want 1", n)
	}
}
