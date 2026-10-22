package perms

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func load(t *testing.T, name string) Delegations {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	d, err := ParseAllow(string(b))
	if err != nil {
		t.Fatalf("ParseAllow(%s): %v", name, err)
	}
	return d
}

func keys(m map[string]bool) string {
	var ks []string
	for k, v := range m {
		if v {
			ks = append(ks, k)
		}
	}
	sort.Strings(ks)
	return strings.Join(ks, ",")
}

// capturedOS are the platforms with captured zfs allow output in
// testdata/<os> (grants.txt lists the zfs allow commands that produced it).
var capturedOS = []string{"linux", "freebsd", "illumos"}

// TestEffectiveCaptured resolves the grants of grants.txt from the captured
// zfs allow output of syngoidfx/fx/a and its clone syngoidfx/fx/a/c. The pool
// grants mount to sgxgrp (local+descendent) and defines the unused @poolset;
// fx/a grants sgxuser snapshot,hold and @set1 (create,mount,receive)
// local+descendent and release local only, sgxgrp send local+descendent and
// destroy descendent only, everyone bookmark, and create-time
// create,mount,snapshot; the clone grants sgxuser rename locally.
func TestEffectiveCaptured(t *testing.T) {
	grp := []string{"sgxgrp"}
	tests := []struct {
		name, file, dataset, user string
		groups                    []string
		want                      string
	}{
		{"self", "allow_fx_a.txt", "syngoidfx/fx/a", "sgxuser", grp, "bookmark,create,hold,mount,receive,release,send,snapshot"},
		{"self without groups", "allow_fx_a.txt", "syngoidfx/fx/a", "sgxuser", nil, "bookmark,create,hold,mount,receive,release,snapshot"},
		{"other user", "allow_fx_a.txt", "syngoidfx/fx/a", "alice", nil, "bookmark"},
		{"other user in group", "allow_fx_a.txt", "syngoidfx/fx/a", "alice", grp, "bookmark,mount,send"},
		// The clone gains the descendent destroy and its own rename, and
		// loses the release that is local to fx/a.
		{"descendant", "allow_fx_a_c.txt", "syngoidfx/fx/a/c", "sgxuser", grp, "bookmark,create,destroy,hold,mount,receive,rename,send,snapshot"},
		{"parent of the clone", "allow_fx_a_c.txt", "syngoidfx/fx/a", "sgxuser", grp, "bookmark,create,hold,mount,receive,release,send,snapshot"},
		{"descendant group only", "allow_fx_a_c.txt", "syngoidfx/fx/a/c", "alice", grp, "bookmark,destroy,mount,send"},
		{"prefix sibling", "allow_fx_a.txt", "syngoidfx/fx/ab", "sgxuser", grp, "mount"},
		{"pool", "allow_fx_a.txt", "syngoidfx", "sgxuser", grp, "mount"},
	}
	for _, osName := range capturedOS {
		t.Run(osName, func(t *testing.T) {
			for _, tt := range tests {
				d := load(t, filepath.Join(osName, tt.file))
				if got := keys(Effective(d, tt.dataset, tt.user, tt.groups)); got != tt.want {
					t.Errorf("%s: got %s, want %s", tt.name, got, tt.want)
				}
			}
		})
	}
}

func TestParseAllowEmpty(t *testing.T) {
	d, err := ParseAllow("")
	if err != nil {
		t.Fatal(err)
	}
	if got := Effective(d, "tank", "backup", []string{"g"}); len(got) != 0 {
		t.Errorf("got %s", keys(got))
	}
}

func TestSetResolution(t *testing.T) {
	out := `---- Permissions on tank/a/b ----
Local permissions:
	user u @inner,@missing,@own
Permission sets:
	@own hold
---- Permissions on tank/a ----
Permission sets:
	@inner @outer,send
	@outer receive,@inner
	@own destroy
`
	d, err := ParseAllow(out)
	if err != nil {
		t.Fatal(err)
	}
	// @inner resolves from the ancestor; its nested @outer and the cycle back to
	// @inner terminate; @own prefers the same-section definition; @missing is empty.
	got := Effective(d, "tank/a/b", "u", nil)
	if keys(got) != "hold,receive,send" {
		t.Errorf("got %s", keys(got))
	}
}

func TestDatasetNameWithSpaces(t *testing.T) {
	d, err := ParseAllow("---- Permissions on tank/my data ----\nLocal permissions:\n\teveryone send\n")
	if err != nil {
		t.Fatal(err)
	}
	if got := Effective(d, "tank/my data", "x", nil); keys(got) != "send" {
		t.Errorf("got %s", keys(got))
	}
}

func TestPrincipalNameWithSpaces(t *testing.T) {
	d, err := ParseAllow("---- Permissions on tank ----\nLocal+Descendent permissions:\n" +
		"\tgroup domain users destroy\n\tuser jane  doe send,hold\n")
	if err != nil {
		t.Fatal(err)
	}
	if got := Effective(d, "tank/x", "x", []string{"domain users"}); keys(got) != "destroy" {
		t.Errorf("group domain users: got %s, want destroy", keys(got))
	}
	if got := Effective(d, "tank", "jane  doe", nil); keys(got) != "hold,send" {
		t.Errorf("user jane  doe: got %s, want hold,send", keys(got))
	}
	if got := Effective(d, "tank", "x", []string{"domain"}); keys(got) != "" {
		t.Errorf("group domain: got %s, want nothing", keys(got))
	}
}

func TestUnknownSubsectionTolerated(t *testing.T) {
	d, err := ParseAllow("---- Permissions on tank ----\nWeird permissions:\n\tfoo bar baz\nLocal permissions:\n\teveryone send\n")
	if err != nil {
		t.Fatal(err)
	}
	if got := Effective(d, "tank", "x", nil); keys(got) != "send" {
		t.Errorf("got %s", keys(got))
	}
}

func TestParseAllowErrors(t *testing.T) {
	for name, in := range map[string]string{
		"entry before section":    "\tuser a send\n",
		"entry before subsection": "---- Permissions on tank ----\n\tuser a send\n",
		"unknown kind":            "---- Permissions on tank ----\nLocal permissions:\n\tbogus a send\n",
		"short user entry":        "---- Permissions on tank ----\nLocal permissions:\n\tuser a\n",
		"set without at":          "---- Permissions on tank ----\nPermission sets:\n\tfoo send\n",
	} {
		if _, err := ParseAllow(in); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func FuzzParseAllow(f *testing.F) {
	for _, osName := range capturedOS {
		for _, n := range []string{"allow_fx_a.txt", "allow_fx_a_c.txt"} {
			b, err := os.ReadFile(filepath.Join("testdata", osName, n))
			if err != nil {
				f.Fatal(err)
			}
			f.Add(string(b))
		}
	}
	f.Add("")
	f.Add("\t\n---- Permissions on  ----\n@")
	f.Fuzz(func(t *testing.T, s string) {
		d, err := ParseAllow(s)
		if err != nil {
			return
		}
		Effective(d, "tank/x", "u", []string{"g"})
	})
}

func TestHeaderWithoutDashes(t *testing.T) {
	out := "---- Permissions on tank/a\nLocal permissions:\n\tuser u send\n" +
		"---- Permissions on tank/a/b\nLocal permissions:\n\tuser u hold\n"
	d, err := ParseAllow(out)
	if err != nil {
		t.Fatal(err)
	}
	if got := Effective(d, "tank/a", "u", nil); keys(got) != "send" {
		t.Errorf("tank/a: got %s, want send", keys(got))
	}
	if got := Effective(d, "tank/a/b", "u", nil); keys(got) != "hold" {
		t.Errorf("tank/a/b: got %s, want hold", keys(got))
	}
}

func TestMalformedLinesError(t *testing.T) {
	for name, in := range map[string]string{
		"malformed header":     "---- Permissions on tank ----\nLocal permissions:\n\tuser a send\n---- Permissions of tank ----\n\tuser a hold\n",
		"header-like colon":    "---- Permissions on tank ----\n---- bogus:\n",
		"nonindented no colon": "---- Permissions on tank ----\nLocal permissions:\ngarbage\n\tuser a send\n",
	} {
		if _, err := ParseAllow(in); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestNearestAncestorSetWins(t *testing.T) {
	out := `---- Permissions on tank/a/b/c ----
Local permissions:
	user u @x
---- Permissions on tank/a/b ----
Permission sets:
	@x parent
---- Permissions on tank/a ----
Permission sets:
	@x grandparent
`
	d, err := ParseAllow(out)
	if err != nil {
		t.Fatal(err)
	}
	if got := Effective(d, "tank/a/b/c", "u", nil); keys(got) != "parent" {
		t.Errorf("got %s, want parent", keys(got))
	}
}
