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

func TestEffectiveDescendant(t *testing.T) {
	d := load(t, "allow_basic.txt")
	got := Effective(d, "tank/backups/vm", "backup", []string{"backup", "zfsadm"})
	for _, p := range []string{"hold", "create", "mount", "receive", "release", "destroy", "bookmark"} {
		if !got[p] {
			t.Errorf("missing %q in %s", p, keys(got))
		}
	}
	if got["snapshot"] {
		t.Errorf("snapshot (local-only on ancestor) must not apply: %s", keys(got))
	}
}

func TestEffectiveSelfLocalOnly(t *testing.T) {
	d := load(t, "allow_basic.txt")
	got := Effective(d, "tank/backups", "backup", []string{"backup", "zfsadm"})
	if !got["snapshot"] {
		t.Errorf("snapshot missing: %s", keys(got))
	}
	if got["destroy"] {
		t.Errorf("descendent-only destroy must not apply to self: %s", keys(got))
	}
	if got["hold"] {
		t.Errorf("hold on a child must not apply to parent: %s", keys(got))
	}
}

func TestEffectiveOtherUser(t *testing.T) {
	d := load(t, "allow_basic.txt")
	got := Effective(d, "tank/backups/vm", "alice", nil)
	if keys(got) != "bookmark" {
		t.Errorf("got %s, want bookmark", keys(got))
	}
}

func TestEffectiveUnrelatedDataset(t *testing.T) {
	d := load(t, "allow_basic.txt")
	got := Effective(d, "tank/backupsX", "backup", []string{"zfsadm"})
	if len(got) != 0 {
		t.Errorf("prefix-sibling dataset must get nothing, got %s", keys(got))
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

func TestParseAllowIgnoresCreateTime(t *testing.T) {
	d := load(t, "allow_createtime.txt")
	got := Effective(d, "tank/data", "backup", nil)
	if keys(got) != "send" {
		t.Errorf("got %s, want send", keys(got))
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
	for _, n := range []string{"allow_basic.txt", "allow_createtime.txt"} {
		b, err := os.ReadFile(filepath.Join("testdata", n))
		if err != nil {
			f.Fatal(err)
		}
		f.Add(string(b))
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
