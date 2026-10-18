// Package buildinfo carries version information stamped in at link time.
package buildinfo

// ProtocolVersion is the version of the syngoid wire protocol.
const ProtocolVersion = 1

// These variables are set with -ldflags "-X ...".
var (
	// Version is the release version, or "dev" for local builds.
	Version = "dev"
	// Commit is the VCS revision the binary was built from.
	Commit = "unknown"
	// Dirty is "true" when the build tree had uncommitted changes.
	Dirty = ""
)

// IsDev reports whether this is a development or dirty build.
func IsDev() bool {
	return Version == "dev" || Dirty == "true"
}
