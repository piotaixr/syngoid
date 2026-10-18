package cli

import (
	"reflect"
	"strings"
	"testing"

	"github.com/piotaixr/syngoid/internal/stream"
	"github.com/piotaixr/syngoid/internal/zfs"
)

func parse(t *testing.T, args ...string) Parsed {
	t.Helper()
	p, err := Parse(args, func(string) bool { return false })
	if err != nil {
		t.Fatalf("Parse(%q): %v", args, err)
	}
	return p
}

func parseErr(t *testing.T, args ...string) error {
	t.Helper()
	p, err := Parse(args, func(string) bool { return false })
	if err == nil {
		t.Fatalf("Parse(%q) = %+v, want error", args, p)
	}
	return err
}

func TestParseDefaults(t *testing.T) {
	p := parse(t, "src", "dst")
	if p.Command != CmdSync {
		t.Errorf("Command = %q, want sync", p.Command)
	}
	o := p.Options
	if o.Compress != stream.ZstdFast {
		t.Errorf("Compress = %q", o.Compress)
	}
	if o.BufferSize != 16<<20 {
		t.Errorf("BufferSize = %d", o.BufferSize)
	}
	if o.Progress != stream.ProgressAuto {
		t.Errorf("Progress = %q", o.Progress)
	}
	if o.Source.Dataset != "src" || o.Target.Dataset != "dst" {
		t.Errorf("locations = %+v %+v", o.Source, o.Target)
	}
	if len(p.Warnings) != 0 {
		t.Errorf("warnings = %q", p.Warnings)
	}
}

func TestParseSubcommands(t *testing.T) {
	tests := []struct {
		args []string
		want Command
	}{
		{[]string{"permissions", "a", "b"}, CmdPermissions},
		{[]string{"fetch"}, CmdFetch},
		{[]string{"version"}, CmdVersion},
		{[]string{"--version"}, CmdVersion},
		{[]string{"serve"}, CmdServe},
		{[]string{"sync", "a", "b"}, CmdSync},
		{[]string{"-r", "sync", "a", "b"}, CmdSync},
		{[]string{"a", "b", "-r"}, CmdSync},
	}
	for _, tt := range tests {
		if got := parse(t, tt.args...).Command; got != tt.want {
			t.Errorf("Parse(%q).Command = %q, want %q", tt.args, got, tt.want)
		}
	}
}

func TestParseSubcommandArity(t *testing.T) {
	for _, args := range [][]string{
		{"fetch", "x"},
		{"serve", "x"},
		{"version", "x"},
		{"fetch", "-r"},
		{"sync", "a"},
		{"sync"},
		{"permissions", "a"},
		{"a"},
		{"a", "b", "c"},
		{"--version", "a"},
	} {
		_ = parseErr(t, args...)
	}
}

func TestParseDeprecatedAliases(t *testing.T) {
	p := parse(t, "--sshport", "2222", "--sshkey", "k", "--sshcipher", "c", "--sshoption", "A=B",
		"--sshconfig", "f", "--mbuffer-size", "32M", "--compress", "pigz-fast",
		"--no-privilege-elevation", "--no-command-checks", "--monitor-version", "a", "b")
	o := p.Options
	if want := []string{"Port=2222", "IdentityFile=k", "Ciphers=c", "A=B"}; !reflect.DeepEqual(o.SSHOptions, want) {
		t.Errorf("SSHOptions = %q, want %q", o.SSHOptions, want)
	}
	if o.SSHConfig != "f" {
		t.Errorf("SSHConfig = %q", o.SSHConfig)
	}
	if o.BufferSize != 32<<20 {
		t.Errorf("BufferSize = %d", o.BufferSize)
	}
	if o.Compress != stream.Gzip {
		t.Errorf("Compress = %q", o.Compress)
	}
	if len(p.Warnings) != 10 {
		t.Fatalf("got %d warnings, want 10: %q", len(p.Warnings), p.Warnings)
	}
	for _, want := range []string{
		"--sshport is deprecated, use -o Port=2222",
		"--compress=pigz-fast is deprecated, using gzip",
		"--no-privilege-elevation is ignored: delegation is the default",
	} {
		found := false
		for _, w := range p.Warnings {
			found = found || w == want
		}
		if !found {
			t.Errorf("missing warning %q in %q", want, p.Warnings)
		}
	}
}

func TestParseCompress(t *testing.T) {
	p := parse(t, "--compress=default", "a", "b")
	if p.Options.Compress != stream.ZstdFast || len(p.Warnings) != 0 {
		t.Errorf("default: %q %q", p.Options.Compress, p.Warnings)
	}
	p = parse(t, "--compress=lz4", "a", "b")
	if p.Options.Compress != stream.LZ4 || len(p.Warnings) != 0 {
		t.Errorf("lz4: %q %q", p.Options.Compress, p.Warnings)
	}
	err := parseErr(t, "--compress=bogus", "a", "b")
	if !strings.Contains(err.Error(), "--compress") {
		t.Errorf("error %q does not name flag", err)
	}
}

func TestParseRemovedFlags(t *testing.T) {
	tests := []struct{ flag, want string }{
		{"--source-bwlimit=1m", "bandwidth limiting is not supported in syngoid"},
		{"--target-bwlimit=1m", "bandwidth limiting is not supported in syngoid"},
		{"--pv-options=x", "--progress"},
		{"--insecure-direct-connection=a:1", "not supported"},
		{"--exclude=x", "--exclude-datasets"},
	}
	for _, tt := range tests {
		err := parseErr(t, tt.flag, "a", "b")
		if !strings.Contains(err.Error(), tt.want) {
			t.Errorf("%s: error %q does not contain %q", tt.flag, err, tt.want)
		}
	}
}

func TestParseValidation(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"identifier", []string{"--identifier=bad/id", "a", "b"}, "--identifier"},
		{"send R recursive", []string{"--sendoptions=R", "-r", "a", "b"}, "zfs send -R and --recursive aren't compatible"},
		{"bad sendoptions", []string{"--sendoptions=o", "a", "b"}, "--sendoptions"},
		{"bad recvoptions", []string{"--recvoptions=1", "a", "b"}, "--recvoptions"},
		{"bad regex", []string{"--exclude-snaps=(", "a", "b"}, "--exclude-snaps"},
		{"bad exclude-datasets regex", []string{"--exclude-datasets=(", "a", "b"}, "--exclude-datasets"},
		{"bad include regex", []string{"--include-snaps=(", "a", "b"}, "--include-snaps"},
		{"confirm dry-run", []string{"--confirm", "--dry-run", "a", "b"}, "--confirm and --dry-run are mutually exclusive"},
		{"bookmark", []string{"--create-bookmark", "a", "b"}, "--create-bookmark requires --no-sync-snap"},
		{"bad progress", []string{"--progress=x", "a", "b"}, "--progress"},
		{"bad buffer", []string{"--buffer-size=abc", "a", "b"}, "--buffer-size"},
		{"bad source", []string{"nas:", "b"}, "SOURCE"},
		{"bad target", []string{"a", "nas:"}, "TARGET"},
		{"unknown flag", []string{"--nope", "a", "b"}, "nope"},
		{"same dataset", []string{"tank/data", "tank/data"}, "SOURCE and TARGET are the same dataset: tank/data"},
		{"target inside source", []string{"tank/data", "tank/data/bak"}, "TARGET tank/data/bak is inside SOURCE tank/data"},
		{"source inside target", []string{"tank/data/sub", "tank/data"}, "SOURCE tank/data/sub is inside TARGET tank/data"},
		{"pool root and child", []string{"tank", "tank/bak"}, "TARGET tank/bak is inside SOURCE tank"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := parseErr(t, tt.args...)
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error %q does not contain %q", err, tt.want)
			}
		})
	}
}

func TestParseOverlapAllowed(t *testing.T) {
	for _, args := range [][]string{
		{"tank/data", "tank/database"},
		{"tank/data", "backup/data"},
		{"nas:tank/data", "tank/data"},
		{"nas:tank/data", "nas:tank/data/bak"},
	} {
		if _, err := Parse(args, func(string) bool { return false }); err != nil {
			t.Errorf("Parse(%q): %v", args, err)
		}
	}
}

func TestParseOptions(t *testing.T) {
	p := parse(t, "-r", "--skip-parent", "--identifier=id", "--no-stream", "--no-sync-snap", "--keep-sync-snap",
		"--create-bookmark", "--use-hold", "--preserve-recordsize", "--preserve-properties", "--no-rollback",
		"--delete-target-snapshots", "--force-delete", "--no-resume", "--no-clone-handling", "--dumpsnaps",
		"--debug", "--quiet", "--exclude-datasets=x", "--exclude-datasets=y", "--include-snaps=^a", "--exclude-snaps=b$",
		"--sendoptions=w", "--recvoptions=u o compression=lz4", "--progress=plain", "-o", "Port=1", "--ssh-option", "B=2",
		"--ssh-config=cfg", "--source-sudo", "-n", "--no-download", "--download-url=u", "--bin-store=d1", "--bin-store=d2",
		"--remote-bin-dir=r", "--otel-endpoint=e", "user@h:a", "b")
	o := p.Options
	if !o.Recursive || !o.SkipParent || o.Identifier != "id" || !o.NoStream || !o.NoSyncSnap || !o.KeepSyncSnap ||
		!o.CreateBookmark || !o.UseHold || !o.PreserveRecordsize || !o.PreserveProperties || !o.NoRollback ||
		!o.DeleteTargetSnapshots || !o.ForceDelete || !o.NoResume || !o.NoCloneHandling || !o.DumpSnaps ||
		!o.Debug || !o.Quiet || !o.DryRun || !o.NoDownload {
		t.Errorf("bool options not all set: %+v", o)
	}
	if len(o.ExcludeDatasets) != 2 || len(o.IncludeSnaps) != 1 || len(o.ExcludeSnaps) != 1 {
		t.Errorf("regex counts: %d %d %d", len(o.ExcludeDatasets), len(o.IncludeSnaps), len(o.ExcludeSnaps))
	}
	if !o.ExcludeSnaps[0].MatchString("ab") || o.ExcludeSnaps[0].MatchString("ba") {
		t.Error("exclude-snaps regex not anchored as given")
	}
	if want := []zfs.Opt{{Flag: 'w'}}; !reflect.DeepEqual(o.SendOpts, want) {
		t.Errorf("SendOpts = %v", o.SendOpts)
	}
	if want := []zfs.Opt{{Flag: 'u'}, {Flag: 'o', Value: "compression=lz4"}}; !reflect.DeepEqual(o.RecvOpts, want) {
		t.Errorf("RecvOpts = %v", o.RecvOpts)
	}
	if o.Progress != stream.ProgressPlain {
		t.Errorf("Progress = %q", o.Progress)
	}
	if want := []string{"Port=1", "B=2"}; !reflect.DeepEqual(o.SSHOptions, want) {
		t.Errorf("SSHOptions = %q", o.SSHOptions)
	}
	if o.SSHConfig != "cfg" || o.DownloadURL != "u" || o.RemoteBinDir != "r" || o.OTelEndpoint != "e" {
		t.Errorf("string options: %+v", o)
	}
	if !o.SourceSudo || o.TargetSudo {
		t.Errorf("sudo = %v %v", o.SourceSudo, o.TargetSudo)
	}
	if want := []string{"d1", "d2"}; !reflect.DeepEqual(o.BinStores, want) {
		t.Errorf("BinStores = %q", o.BinStores)
	}
	if !o.Source.IsRemote() || o.Source.User != "user" || o.Source.Host != "h" || o.Target.IsRemote() {
		t.Errorf("locations = %+v %+v", o.Source, o.Target)
	}
}

func TestParseSudo(t *testing.T) {
	o := parse(t, "--sudo", "a", "b").Options
	if !o.SourceSudo || !o.TargetSudo {
		t.Errorf("--sudo: %v %v", o.SourceSudo, o.TargetSudo)
	}
	o = parse(t, "--target-sudo", "a", "b").Options
	if o.SourceSudo || !o.TargetSudo {
		t.Errorf("--target-sudo: %v %v", o.SourceSudo, o.TargetSudo)
	}
}

func TestParseSSHCipherShort(t *testing.T) {
	p := parse(t, "-c", "aes", "a", "b")
	if want := []string{"Ciphers=aes"}; !reflect.DeepEqual(p.Options.SSHOptions, want) {
		t.Errorf("SSHOptions = %q", p.Options.SSHOptions)
	}
	if len(p.Warnings) != 1 {
		t.Errorf("warnings = %q", p.Warnings)
	}
}

func TestParseBufferSize(t *testing.T) {
	tests := []struct {
		in   string
		want int64
	}{
		{"16M", 16 << 20},
		{"512K", 512 << 10},
		{"1G", 1 << 30},
		{"1048576", 1 << 20},
		{"1t", 1 << 40},
		{"2m", 2 << 20},
		{"4MiB", 4 << 20},
		{"8KB", 8 << 10},
	}
	for _, tt := range tests {
		p := parse(t, "--buffer-size="+tt.in, "a", "b")
		if p.Options.BufferSize != tt.want {
			t.Errorf("%s: got %d, want %d", tt.in, p.Options.BufferSize, tt.want)
		}
	}
	for _, bad := range []string{"abc", "", "0", "-1", "1X", "99999999999T"} {
		err := parseErr(t, "--buffer-size="+bad, "a", "b")
		if !strings.Contains(err.Error(), "--buffer-size") {
			t.Errorf("%q: error %q does not name flag", bad, err)
		}
	}
}
