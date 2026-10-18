package engine

import (
	"slices"
	"sort"

	"github.com/piotaixr/syngoid/internal/cli"
	"github.com/piotaixr/syngoid/internal/zfs"
)

// PreservePropsDenylist lists the properties --preserve-properties never
// passes to the receive (spec §8.6).
var PreservePropsDenylist = []string{
	"available", "compressratio", "createtxg", "creation", "clones", "defer_destroy",
	"encryptionroot", "filesystem_count", "keystatus", "guid", "logicalreferenced",
	"logicalused", "mounted", "objsetid", "origin", "receive_resume_token", "redact_snaps",
	"referenced", "refcompressratio", "snapshot_count", "type", "used", "usedbychildren",
	"usedbydataset", "usedbyrefreservation", "usedbysnapshots", "userrefs",
	"snapshots_changed", "volblocksize", "written", "version", "volsize",
	"casesensitivity", "normalization", "utf8only",
}

// PreservedPropKeys returns, sorted, the locally-set properties that
// --preserve-properties passes to the receive: all but the denylisted ones
// and, for an unprivileged Linux target, mountpoint and canmount. Preflight
// and RecvArgs both use it so the checked permissions match what is set.
func PreservedPropKeys(localProps map[string]string, unprivLinux bool) []string {
	keys := make([]string, 0, len(localProps))
	for k := range localProps {
		if slices.Contains(PreservePropsDenylist, k) || unprivLinux && (k == "mountpoint" || k == "canmount") {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// UnprivDroppedProps returns, sorted, the locally-set properties among
// mountpoint and canmount, which --preserve-properties cannot set on an
// unprivileged Linux target.
func UnprivDroppedProps(localProps map[string]string) []string {
	var out []string
	for _, k := range []string{"canmount", "mountpoint"} {
		if _, ok := localProps[k]; ok {
			out = append(out, k)
		}
	}
	return out
}

// RecvArgs builds the zfs receive flags: the filtered --recvoptions first,
// then -s, -F and -u as applicable, then -o key=value pairs sorted by key.
// With --preserve-properties the pairs are the locally-set source properties
// not on the denylist; otherwise --preserve-recordsize adds the source
// recordsize for the full send of a filesystem.
func RecvArgs(o *cli.Options, full bool, sourceType string, localProps map[string]string, recordsize string, unprivLinux bool) []string {
	args := zfs.FilterOpts(o.RecvOpts, zfs.RecvAllowed)
	if !o.NoResume {
		args = append(args, "-s")
	}
	if !o.NoRollback {
		args = append(args, "-F")
	}
	if unprivLinux && !slices.ContainsFunc(o.RecvOpts, func(op zfs.Opt) bool { return op.Flag == 'u' }) {
		args = append(args, "-u")
	}
	switch {
	case o.PreserveProperties:
		keys := PreservedPropKeys(localProps, unprivLinux)
		for _, k := range keys {
			args = append(args, "-o", k+"="+localProps[k])
		}
	case o.PreserveRecordsize && full && sourceType == "filesystem" && recordsize != "":
		args = append(args, "-o", "recordsize="+recordsize)
	}
	return args
}
