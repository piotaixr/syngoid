package cli

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/pflag"

	"github.com/piotaixr/syngoid/internal/zfs"
)

// RestoreOptions is set on Options for the restore command only.
type RestoreOptions struct {
	At       *time.Time // nil unless --at
	Snapshot string     // "" unless --snapshot
	List     bool       // --list: Options.Target is the zero Location
}

// ParseAt parses an --at value; a value without an offset is in loc.
func ParseAt(s string, loc *time.Location) (time.Time, error) {
	local := strings.Replace(s, " ", "T", 1)
	for _, layout := range []string{"2006-01-02", "2006-01-02T15:04", "2006-01-02T15:04:05"} {
		if t, err := time.ParseInLocation(layout, local, loc); err == nil {
			return t, nil
		}
	}
	if t, err := time.Parse(time.RFC3339, local); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("--at: invalid time %q (want 2026-11-10, 2026-11-10T03:00, 2026-11-10T03:00:00 or RFC 3339 with an offset)", s)
}

// restoreRejected are the flags that make no sense for restore.
var restoreRejected = []string{
	"identifier", "no-stream", "no-sync-snap", "keep-sync-snap", "create-bookmark", "use-hold",
	"no-rollback", "delete-target-snapshots", "force-delete", "include-snaps", "exclude-snaps",
	"no-clone-handling", "no-resume", "dumpsnaps",
	"sshport", "sshkey", "sshcipher", "sshoption", "sshconfig", "mbuffer-size",
	"no-privilege-elevation", "no-command-checks", "monitor-version",
}

// restoreListAllowed are the flags accepted together with restore --list.
var restoreListAllowed = map[string]bool{
	"list": true, "recursive": true, "skip-parent": true, "exclude-datasets": true,
	"sudo": true, "source-sudo": true, "ssh-option": true, "ssh-config": true,
	"bin-store": true, "remote-bin-dir": true, "no-download": true, "download-url": true,
	"debug": true, "quiet": true,
}

// checkRestoreFlags validates the restore-specific flags against the command
// and returns the restore options (nil unless cmd is restore).
func (r *raw) checkRestoreFlags(fs *pflag.FlagSet, cmd Command, pos []string) (*RestoreOptions, error) {
	if cmd != CmdRestore {
		for _, name := range []string{"at", "snapshot", "list"} {
			if fs.Changed(name) {
				return nil, fmt.Errorf("--%s requires the restore command", name)
			}
		}
		return nil, nil
	}
	for _, name := range restoreRejected {
		if fs.Changed(name) {
			return nil, fmt.Errorf("--%s is not available with restore", name)
		}
	}
	if r.list {
		var bad string
		fs.Visit(func(f *pflag.Flag) {
			if bad == "" && !restoreListAllowed[f.Name] {
				bad = f.Name
			}
		})
		if bad != "" {
			return nil, fmt.Errorf("--%s is not available with restore --list", bad)
		}
		if len(pos) != 1 {
			return nil, fmt.Errorf("restore --list takes only SOURCE (got %d arguments)", len(pos))
		}
	} else if len(pos) != 2 {
		return nil, fmt.Errorf("restore requires exactly SOURCE and TARGET (got %d arguments)", len(pos))
	}
	sendOpts, err := zfs.ParseOpts(r.sendOptions)
	if err != nil {
		return nil, fmt.Errorf("--sendoptions: %w", err)
	}
	for _, so := range sendOpts {
		if so.Flag == 'R' {
			return nil, errors.New("--sendoptions R is not available with restore: restore sends one snapshot per dataset")
		}
	}
	ro := &RestoreOptions{List: r.list}
	if fs.Changed("at") && fs.Changed("snapshot") {
		return nil, errors.New("--at and --snapshot are mutually exclusive")
	}
	if fs.Changed("at") {
		at, err := ParseAt(r.at, time.Local)
		if err != nil {
			return nil, err
		}
		ro.At = &at
	}
	if fs.Changed("snapshot") {
		if r.snapshot == "" || strings.ContainsAny(r.snapshot, "@/#") {
			return nil, fmt.Errorf("--snapshot: invalid name %q (want the part after @)", r.snapshot)
		}
		ro.Snapshot = r.snapshot
	}
	return ro, nil
}
