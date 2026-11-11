package cli

import (
	"reflect"
	"strings"
	"testing"

	"github.com/piotaixr/syngoid/internal/platform"
)

func TestParseFetch(t *testing.T) {
	p := parse(t, "fetch", "--platform", "linux/arm64", "--platform", "freebsd/amd64")
	if p.Command != CmdFetch || p.Fetch == nil || p.Options != nil {
		t.Fatalf("Parse = %+v", p)
	}
	want := []platform.Platform{{OS: "linux", Arch: "arm64"}, {OS: "freebsd", Arch: "amd64"}}
	if !reflect.DeepEqual(p.Fetch.Platforms, want) || p.Fetch.All {
		t.Errorf("Fetch = %+v, want platforms %v", p.Fetch, want)
	}
	if p.Fetch.DownloadURL != defaultDownloadURL || p.Fetch.Dir != "" {
		t.Errorf("defaults: %+v", p.Fetch)
	}

	f := parse(t, "fetch", "--all", "--dir", "d", "--download-url", "u", "--quiet").Fetch
	if !f.All || f.Dir != "d" || f.DownloadURL != "u" || !f.Quiet || f.Debug || len(f.Platforms) != 0 {
		t.Errorf("Fetch = %+v", f)
	}
	if f := parse(t, "fetch", "--all", "--debug").Fetch; !f.Debug {
		t.Errorf("Fetch = %+v, want Debug", f)
	}
}

func TestParseFetchErrors(t *testing.T) {
	for _, tt := range []struct {
		args []string
		want string
	}{
		{[]string{"fetch"}, "fetch needs --platform OS/ARCH or --all"},
		{[]string{"fetch", "--all", "--platform", "linux/arm64"}, "fetch needs --platform OS/ARCH or --all"},
		{[]string{"fetch", "--all", "x"}, "fetch takes no arguments"},
		{[]string{"fetch", "--platform", "plan9/amd64"}, ""},
		{[]string{"-r", "fetch"}, "fetch takes no options"},
	} {
		err := parseErr(t, tt.args...)
		want := tt.want
		if want == "" {
			_, perr := platform.Parse("plan9/amd64")
			want = perr.Error()
		}
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Parse(%q) error = %v, want %q", tt.args, err, want)
		}
	}
}
