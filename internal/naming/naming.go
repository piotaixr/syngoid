// Package naming builds snapshot, hold and bookmark names that are
// byte-compatible with syncoid 2.2.0, so syngoid can take over existing
// replications.
package naming

import (
	"fmt"
	"regexp"
	"time"
)

const (
	snapPrefix  = "syncoid_"
	stampLayout = "2006-01-02:15:04:05"
)

var validIdentifier = regexp.MustCompile(`^[a-zA-Z0-9_:.-]+$`)

// SyncSnapName returns the sync snapshot name for host and identifier at
// time t. The GMT offset is taken from the zone of t.
func SyncSnapName(identifier, host string, t time.Time) string {
	_, off := t.Zone()
	sign := ""
	if off < 0 {
		sign = "-"
		off = -off
	}
	hours := off / 3600
	minutes := off/60 - hours*60
	stamp := t.Format(stampLayout) + "-GMT" + sign + twoDigits(hours) + ":" + twoDigits(minutes)
	return snapPrefix + identPrefix(identifier) + host + "_" + stamp
}

// HoldTag returns the hold tag syncoid places on sync snapshots.
func HoldTag(identifier, host string) string {
	return snapPrefix + identPrefix(identifier) + host
}

// SyncSnapPattern matches sync snapshot names produced for identifier and
// host, and nothing else. Identifier and host are matched literally.
func SyncSnapPattern(identifier, host string) *regexp.Regexp {
	return regexp.MustCompile(`^` + regexp.QuoteMeta(snapPrefix+identPrefix(identifier)+host) +
		`_\d{4}-\d{2}-\d{2}:\d{2}:\d{2}:\d{2}-GMT-?\d{2}:\d{2}$`)
}

// BookmarkFallback returns the bookmark name used for snap when the snapshot
// GUID is known: the snapshot name followed by the first six characters of
// the GUID (the whole GUID if it is shorter).
func BookmarkFallback(snap, guid string) string {
	if len(guid) > 6 {
		guid = guid[:6]
	}
	return snap + guid
}

// ValidIdentifier reports whether s may be used as a syncoid identifier.
func ValidIdentifier(s string) bool {
	return validIdentifier.MatchString(s)
}

func identPrefix(identifier string) string {
	if identifier == "" {
		return ""
	}
	return identifier + "_"
}

func twoDigits(n int) string {
	return fmt.Sprintf("%02d", n)
}
