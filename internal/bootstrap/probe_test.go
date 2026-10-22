//go:build unix

package bootstrap

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func probe(t *testing.T, tr *shTransport, dir string) ProbeResult {
	t.Helper()
	out, err := tr.Run(context.Background(), ProbeCommand(dir), nil)
	if err != nil {
		t.Fatal(err)
	}
	pr, err := ParseProbe(string(out))
	if err != nil {
		t.Fatalf("ParseProbe(%q): %v", out, err)
	}
	return pr
}

func TestProbeScriptExpandsTilde(t *testing.T) {
	tr := newShTransport(t)
	pr := probe(t, tr, "~/.cache/syngoid/x")
	want := tr.home + "/.cache/syngoid/x"
	if pr.Dir != want || pr.Home != tr.home || pr.Executable {
		t.Fatalf("probe = %+v, want Dir %q, Home %q, not executable", pr, want, tr.home)
	}
	if pr.Platform != Self() {
		t.Errorf("Platform = %v, want %v", pr.Platform, Self())
	}
	if err := os.MkdirAll(want, 0o700); err != nil {
		t.Fatal(err)
	}
	writeScript(t, filepath.Join(want, "syngoid"), "exit 0")
	if pr := probe(t, tr, "~/.cache/syngoid/x"); !pr.Executable {
		t.Errorf("Executable = false after installing %s/syngoid", want)
	}
}

func TestPushCommandAtomic(t *testing.T) {
	tr := newShTransport(t)
	dir := filepath.Join(tr.home, "a", "fresh")
	data := make([]byte, 1<<20)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Run(context.Background(), PushCommand(dir, int64(len(data))), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "syngoid"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Error("pushed content differs")
	}
	for _, p := range []string{filepath.Join(dir, "syngoid"), dir} {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode().Perm() != 0o700 {
			t.Errorf("%s mode = %v, want 0700", p, fi.Mode().Perm())
		}
	}
	left, err := filepath.Glob(filepath.Join(dir, ".syngoid.tmp.*"))
	if err != nil || len(left) != 0 {
		t.Errorf("temporary files left: %v (%v)", left, err)
	}
}

func TestPushCommandTightensExistingDir(t *testing.T) {
	tr := newShTransport(t)
	dir := filepath.Join(tr.home, "loose")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o775); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Run(context.Background(), PushCommand(dir, 3), strings.NewReader("bin")); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o700 {
		t.Errorf("mode of the existing agent dir after a push = %v, want 0700", fi.Mode().Perm())
	}
}

func TestPushCommandShortInput(t *testing.T) {
	tr := newShTransport(t)
	dir := filepath.Join(tr.home, "d")
	for name, size := range map[string]int64{"short": 1 << 20, "long": 1} {
		_, err := tr.Run(context.Background(), PushCommand(dir, size), strings.NewReader("only a few bytes"))
		if err == nil {
			t.Errorf("%s: push of %d bytes from a 16-byte stdin succeeded", name, size)
		}
		left, _ := filepath.Glob(filepath.Join(dir, "*"))
		hidden, _ := filepath.Glob(filepath.Join(dir, ".syngoid.tmp.*"))
		if len(left)+len(hidden) != 0 {
			t.Errorf("%s: files left after a failed push: %v %v", name, left, hidden)
		}
	}
}

func TestPushCommandOddDir(t *testing.T) {
	tr := newShTransport(t)
	dir := filepath.Join(tr.home, "it's a dir")
	if pr := probe(t, tr, dir); pr.Dir != dir || pr.Executable {
		t.Fatalf("probe = %+v, want Dir %q, not executable", pr, dir)
	}
	script := "#!/bin/sh\necho \"ran $1\"\n"
	if _, err := tr.Run(context.Background(), PushCommand(dir, int64(len(script))), strings.NewReader(script)); err != nil {
		t.Fatal(err)
	}
	if pr := probe(t, tr, dir); !pr.Executable {
		t.Fatal("pushed binary not executable")
	}
	out, err := tr.Run(context.Background(), ServeCommand(dir), nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "ran serve\n" {
		t.Errorf("serve command output = %q", out)
	}
}

func TestParseProbe(t *testing.T) {
	ok := "Linux\nx86_64\n-\n/home/u\n/home/u/.cache/syngoid/1.0.0\nyes\n"
	pr, err := ParseProbe(ok)
	if err != nil {
		t.Fatal(err)
	}
	want := ProbeResult{Platform: Platform{"linux", "amd64"}, Home: "/home/u", Dir: "/home/u/.cache/syngoid/1.0.0", Executable: true}
	if pr != want {
		t.Errorf("ParseProbe = %+v, want %+v", pr, want)
	}
	illumos, err := ParseProbe("SunOS\ni86pc\namd64\n/export/home/u\n/export/home/u/d\nno\n")
	if err != nil || illumos.Platform != (Platform{"illumos", "amd64"}) || illumos.Executable {
		t.Errorf("illumos probe = %+v, %v", illumos, err)
	}
	for _, bad := range []string{
		"",
		"Linux\nx86_64\n-\n/home/u\n/home/u/d\n", // 5 lines
		"motd\nLinux\nx86_64\n-\n/home/u\n/home/u/d\nyes\n", // 7 lines
		"Linux\nx86_64\n-\n/home/u\n~u/d\nno\n",             // relative dir
		"Linux\nx86_64\n-\n/home/u\n/home/u/d\nmaybe\n",     // bad flag
		"Linux\nmips\n-\n/home/u\n/home/u/d\nno\n",          // platform
		"Linux\nx86_64\n-\n/home/u\n/home/u/dé\nno\n",       // non-ASCII dir
		"Linux\nx86_64\n-\n/home/u\n/home/u/\x01d\nno\n",    // control character
	} {
		if _, err := ParseProbe(bad); err == nil {
			t.Errorf("ParseProbe(%q) succeeded", bad)
		}
	}
}

func TestCheckRemoteDir(t *testing.T) {
	for _, ok := range []string{"~/.cache/syngoid/1.0.0", "/opt/it's a dir", "/a~b"} {
		if err := checkRemoteDir(ok); err != nil {
			t.Errorf("checkRemoteDir(%q) = %v", ok, err)
		}
	}
	for _, bad := range []string{"", "/a\nb", "/a\x00b", "/a\tb", "/café", "/a\x7fb", `/a\b`} {
		err := checkRemoteDir(bad)
		if err == nil {
			t.Errorf("checkRemoteDir(%q) succeeded", bad)
			continue
		}
		if bad != "" && !strings.Contains(err.Error(), fmt.Sprintf("%q", bad)) {
			t.Errorf("checkRemoteDir(%q) = %v, does not name the value", bad, err)
		}
	}
}

// TestCommandsUnderCsh runs the remote commands the way a csh login shell
// does, which rejects a newline inside single quotes.
func TestCommandsUnderCsh(t *testing.T) {
	csh, err := exec.LookPath("csh")
	if err != nil {
		t.Skip("no csh")
	}
	home := t.TempDir()
	run := func(cmd string, stdin string) string {
		t.Helper()
		c := exec.Command(csh, "-f", "-c", cmd)
		c.Env = append(os.Environ(), "HOME="+home)
		c.Stdin = strings.NewReader(stdin)
		out, err := c.CombinedOutput()
		if err != nil {
			t.Fatalf("csh -c %s: %v: %s", cmd, err, out)
		}
		return string(out)
	}
	pr, err := ParseProbe(run(ProbeCommand("~/it's a dir"), ""))
	if err != nil {
		t.Fatal(err)
	}
	if want := home + "/it's a dir"; pr.Dir != want {
		t.Fatalf("Dir = %q, want %q", pr.Dir, want)
	}
	script := "#!/bin/sh\necho \"ran $1\"\n"
	run(PushCommand(pr.Dir, int64(len(script))), script)
	if out := run(ServeCommand(pr.Dir), ""); out != "ran serve\n" {
		t.Errorf("serve output = %q", out)
	}
}
