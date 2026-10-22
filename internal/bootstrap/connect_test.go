//go:build unix

package bootstrap

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/piotaixr/syngoid/internal/buildinfo"
	"github.com/piotaixr/syngoid/internal/logx"
	"github.com/piotaixr/syngoid/internal/rpc"
)

func matchingReply() rpc.HelloReply {
	return rpc.HelloReply{
		Protocol:  buildinfo.ProtocolVersion,
		Version:   buildinfo.Version,
		BuildHash: buildinfo.BuildHash(),
		ExeSHA256: "abc",
	}
}

func TestCheckHello(t *testing.T) {
	if !buildinfo.IsDev() {
		t.Fatal("tests expect a dev build")
	}
	if err := CheckHello(matchingReply(), ""); err != nil {
		t.Errorf("equal: %v", err)
	}
	if err := CheckHello(matchingReply(), "abc"); err != nil {
		t.Errorf("equal with pushed SHA: %v", err)
	}
	tests := map[string]struct {
		edit   func(*rpc.HelloReply)
		pushed string
	}{
		"protocol":   {edit: func(r *rpc.HelloReply) { r.Protocol = 2 }},
		"version":    {edit: func(r *rpc.HelloReply) { r.Version = "1.0.1" }},
		"build hash": {edit: func(r *rpc.HelloReply) { r.BuildHash = "other" }},
		"pushed SHA": {edit: func(*rpc.HelloReply) {}, pushed: "def"},
	}
	for name, tt := range tests {
		r := matchingReply()
		tt.edit(&r)
		err := CheckHello(r, tt.pushed)
		if !errors.Is(err, ErrMismatch) {
			t.Errorf("%s: CheckHello = %v, want ErrMismatch", name, err)
		}
		// Only a dev build-hash difference carries the stamping hint.
		const hint = "-X github.com/piotaixr/syngoid/internal/buildinfo.Hash="
		if hasHint := err != nil && strings.Contains(err.Error(), hint); hasHint != (name == "build hash") {
			t.Errorf("%s: hint present = %v: %v", name, hasHint, err)
		}
	}
}

func connect(t *testing.T, tr Transport, o Options) *rpc.Remote {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r, err := Connect(ctx, tr, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	info, err := r.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.User != testHostInfo.User || info.UID != testHostInfo.UID {
		t.Errorf("Info = %+v, want %+v", info, testHostInfo)
	}
	return r
}

func TestConnectPushesWhenMissing(t *testing.T) {
	tr := newShTransport(t)
	var logs lockedBuffer
	log := logx.New(logx.Config{Stdout: &logs, Stderr: &logs})
	connect(t, tr, Options{Log: log})
	if n := tr.pushes.Load(); n != 1 {
		t.Errorf("pushes = %d, want 1", n)
	}
	dir := filepath.Join(tr.home, ".cache", "syngoid", "dev-"+buildinfo.BuildHash()[:8])
	data, err := os.ReadFile(filepath.Join(dir, "syngoid"))
	if err != nil {
		t.Fatal(err)
	}
	want, err := buildinfo.ExeSHA256()
	if err != nil {
		t.Fatal(err)
	}
	if sum := sha256.Sum256(data); hex.EncodeToString(sum[:]) != want {
		t.Error("pushed binary is not the test binary")
	}
	wantLog := "INFO: [sh-test] pushing syngoid agent to sh-test:" + dir + "/syngoid\n"
	if !strings.Contains(logs.String(), wantLog) {
		t.Errorf("log = %q, want it to contain %q", logs.String(), wantLog)
	}
}

func TestConnectReusesExisting(t *testing.T) {
	tr := newShTransport(t)
	connect(t, tr, Options{})
	tr.pushes.Store(0)
	connect(t, tr, Options{})
	if n := tr.pushes.Load(); n != 0 {
		t.Errorf("pushes on reuse = %d, want 0", n)
	}
}

func TestConnectRepushOnMismatch(t *testing.T) {
	tr := newShTransport(t)
	dir := filepath.Join(tr.home, ".cache", "syngoid", AgentDirName())
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeScript(t, filepath.Join(dir, "syngoid"), "echo garbage; exit 0")
	connect(t, tr, Options{})
	if n := tr.pushes.Load(); n != 1 {
		t.Errorf("pushes = %d, want 1", n)
	}
}

func TestConnectFailsAfterSecondMismatch(t *testing.T) {
	tr := newShTransport(t)
	boom := filepath.Join(t.TempDir(), "boom")
	writeScript(t, boom, "echo boom >&2; exit 1")
	o := Options{Resolver: Resolver{Executable: func() (string, error) { return boom, nil }}}
	r, err := Connect(context.Background(), tr, o)
	if err == nil {
		_ = r.Close()
		t.Fatal("Connect succeeded")
	}
	msg := err.Error()
	if !strings.HasPrefix(msg, "sh-test: agent handshake failed: ") || !strings.HasSuffix(msg, "\nremote stderr:\nboom") {
		t.Errorf("error = %q", msg)
	}
	if n := tr.pushes.Load(); n != 2 {
		t.Errorf("pushes = %d, want 2", n)
	}
}

func TestConnectHandshakeTimeout(t *testing.T) {
	tr := newShTransport(t)
	tr.closeWait = 100 * time.Millisecond
	hang := filepath.Join(t.TempDir(), "hang")
	writeScript(t, hang, "exec sleep 30")
	o := Options{
		Resolver:         Resolver{Executable: func() (string, error) { return hang, nil }},
		HandshakeTimeout: 200 * time.Millisecond,
	}
	start := time.Now()
	_, err := Connect(context.Background(), tr, o)
	if err == nil || !strings.Contains(err.Error(), "agent handshake failed") {
		t.Fatalf("Connect error = %v", err)
	}
	// Two attempts, each bounded by the timeout plus the proc's close.
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("Connect took %v", d)
	}
}

func TestConnectRejectsRelativeHome(t *testing.T) {
	tr := newShTransport(t)
	tr.home = ""
	_, err := Connect(context.Background(), tr, Options{})
	if err == nil || !strings.Contains(err.Error(), "not absolute; pass an absolute --remote-bin-dir") {
		t.Errorf("Connect error = %v", err)
	}
	if n := tr.pushes.Load(); n != 0 {
		t.Errorf("pushes = %d", n)
	}
}

func TestConnectRejectsBadRemoteDir(t *testing.T) {
	tr := newShTransport(t)
	_, err := Connect(context.Background(), tr, Options{RemoteBinDir: "/a\nb", Log: slog.New(slog.DiscardHandler)})
	if err == nil || !strings.Contains(err.Error(), `"/a\nb"`) {
		t.Errorf("Connect error = %v", err)
	}
	if n := tr.pushes.Load(); n != 0 {
		t.Errorf("pushes = %d", n)
	}
}
