package cli

import (
	"strings"
	"testing"
	"time"
)

func TestParseAt(t *testing.T) {
	zone := time.FixedZone("test", 2*3600)
	for _, c := range []struct{ in, want string }{
		{"2026-11-10", "2026-11-10T00:00:00+02:00"},
		{"2026-11-10T03:00", "2026-11-10T03:00:00+02:00"},
		{"2026-11-10 03:00", "2026-11-10T03:00:00+02:00"},
		{"2026-11-10T03:00:05", "2026-11-10T03:00:05+02:00"},
		{"2026-11-10T03:00:05Z", "2026-11-10T03:00:05Z"},
		{"2026-11-10T03:00:05-05:00", "2026-11-10T03:00:05-05:00"},
	} {
		got, err := ParseAt(c.in, zone)
		if err != nil {
			t.Errorf("ParseAt(%q): %v", c.in, err)
			continue
		}
		want, err := time.Parse(time.RFC3339, c.want)
		if err != nil {
			t.Fatal(err)
		}
		if !got.Equal(want) {
			t.Errorf("ParseAt(%q) = %s, want %s", c.in, got.Format(time.RFC3339), c.want)
		}
	}
	for _, bad := range []string{"", "yesterday", "10/11/2026", "2026-11-10T25:00", "2026-13-01"} {
		_, err := ParseAt(bad, zone)
		want := `--at: invalid time "` + bad + `" (want 2026-11-10, 2026-11-10T03:00, 2026-11-10T03:00:00 or RFC 3339 with an offset)`
		if err == nil || err.Error() != want {
			t.Errorf("ParseAt(%q) error = %v, want %q", bad, err, want)
		}
	}
}

func TestParseRestore(t *testing.T) {
	p := parse(t, "restore", "-r", "--snapshot", "daily", "backup/data", "tank/data")
	o := p.Options
	if p.Command != CmdRestore || p.Options.Restore == nil {
		t.Fatalf("Command = %q, Restore = %v", p.Command, o.Restore)
	}
	if o.Restore.Snapshot != "daily" || o.Restore.At != nil || o.Restore.List {
		t.Errorf("Restore = %+v", o.Restore)
	}
	if !o.Recursive || !o.NoSyncSnap || !o.NoStream || !o.NoRollback || !o.NoCloneHandling || o.NoResume {
		t.Errorf("forced options wrong: %+v", o)
	}

	p = parse(t, "restore", "--at", "2026-11-10T03:00:00Z", "backup/data", "tank/data")
	want := time.Date(2026, 11, 10, 3, 0, 0, 0, time.UTC)
	if at := p.Options.Restore.At; at == nil || !at.Equal(want) {
		t.Errorf("At = %v, want %v", at, want)
	}

	p = parse(t, "restore", "backup/data", "tank/data")
	if r := p.Options.Restore; r == nil || r.At != nil || r.Snapshot != "" || r.List {
		t.Errorf("Restore = %+v", r)
	}

	p = parse(t, "restore", "--list", "-r", "backup/data")
	o = p.Options
	if !o.Restore.List || o.Source.Dataset != "backup/data" || o.Target != (Location{}) || !o.Recursive {
		t.Errorf("list options = %+v", o)
	}

	if p = parse(t, "tank/data", "backup/data"); p.Options.Restore != nil {
		t.Errorf("sync has Restore = %+v", p.Options.Restore)
	}
}

func TestParseRestoreRejects(t *testing.T) {
	type tc struct {
		args []string
		want string
	}
	cases := []tc{
		{[]string{"restore", "--at", "2026-11-10", "--snapshot", "x", "S", "T"}, "--at and --snapshot are mutually exclusive"},
		{[]string{"restore", "--snapshot", "backup/data@daily", "S", "T"}, `--snapshot: invalid name "backup/data@daily" (want the part after @)`},
		{[]string{"restore", "--snapshot", "", "S", "T"}, "--snapshot: invalid name"},
		{[]string{"restore", "--snapshot", "a/b", "S", "T"}, "--snapshot: invalid name"},
		{[]string{"restore", "--snapshot", "a#b", "S", "T"}, "--snapshot: invalid name"},
		{[]string{"restore", "--at", "yesterday", "S", "T"}, "--at: invalid time"},
		{[]string{"restore", "S"}, "restore requires exactly SOURCE and TARGET (got 1 arguments)"},
		{[]string{"restore", "--list", "S", "T"}, "restore --list takes only SOURCE (got 2 arguments)"},
		{[]string{"--at", "2026-11-10", "S", "T"}, "--at requires the restore command"},
		{[]string{"--snapshot", "x", "S", "T"}, "--snapshot requires the restore command"},
		{[]string{"--list", "S", "T"}, "--list requires the restore command"},
		{[]string{"permissions", "--at", "2026-11-10", "S", "T"}, "--at requires the restore command"},
		{[]string{"restore", "tank/data", "tank/data/sub"}, "is inside SOURCE"},
		{[]string{"restore", "tank/data/sub", "tank/data"}, "is inside TARGET"},
	}
	for _, f := range [][]string{
		{"--identifier", "x"},
		{"--no-stream"},
		{"--no-sync-snap"},
		{"--keep-sync-snap"},
		{"--create-bookmark"},
		{"--use-hold"},
		{"--no-rollback"},
		{"--delete-target-snapshots"},
		{"--force-delete"},
		{"--include-snaps", "x"},
		{"--exclude-snaps", "x"},
		{"--no-clone-handling"},
		{"--no-resume"},
		{"--dumpsnaps"},
		{"--sshport", "22"},
		{"--sshkey", "k"},
		{"--sshcipher", "c"},
		{"--sshoption", "o"},
		{"--sshconfig", "c"},
		{"--mbuffer-size", "1M"},
		{"--no-privilege-elevation"},
		{"--no-command-checks"},
		{"--monitor-version"},
	} {
		args := append(append([]string{"restore"}, f...), "S", "T")
		cases = append(cases, tc{args, f[0] + " is not available with restore"})
	}
	for _, f := range [][]string{
		{"--dry-run"},
		{"--jobs", "2"},
		{"--preserve-properties"},
		{"--at", "2026-11-10"},
		{"--snapshot", "x"},
		{"--target-sudo"},
	} {
		args := append(append([]string{"restore", "--list"}, f...), "S")
		cases = append(cases, tc{args, f[0] + " is not available with restore --list"})
	}
	for _, c := range cases {
		if err := parseErr(t, c.args...); !strings.Contains(err.Error(), c.want) {
			t.Errorf("Parse(%q) error = %q, want it to contain %q", c.args, err, c.want)
		}
	}
}

func TestParseRestoreListAccepts(t *testing.T) {
	p := parse(t, "restore", "--list", "-r", "--skip-parent", "--exclude-datasets", "x", "--sudo", "--source-sudo",
		"-o", "Port=22", "--ssh-config", "c", "--bin-store", "d", "--remote-bin-dir", "r", "--no-download",
		"--download-url", "u", "--debug", "--quiet", "nas:backup/data")
	if !p.Options.Restore.List || p.Options.Source.Host != "nas" {
		t.Errorf("options = %+v", p.Options)
	}
}
