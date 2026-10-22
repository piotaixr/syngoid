package agent

import (
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

// agentDir matches the names of uploaded agent directories: a release
// version or "dev", optionally followed by a build hash prefix. It excludes
// every other entry of the cache, such as the initiator's "bin".
var agentDir = regexp.MustCompile(`^(dev|[0-9]+\.[0-9]+\.[0-9]+[0-9A-Za-z.-]*)(-[0-9a-f]{8})?$`)

// PruneSiblings removes the stale agent directories next to the one holding
// exe. It acts only when exe lives in $home/.cache/syngoid/<dir> with <dir>
// named like an agent directory, and keeps that directory plus the sibling
// agent directory with the newest modification time; siblings sharing that
// time are all kept. Only real directories are considered; files and symlinks are left alone.
// Problems are logged at debug level.
func PruneSiblings(exe, home string, log *slog.Logger) {
	if exe == "" || home == "" {
		return
	}
	// The kernel reports exe with symlinks resolved but $HOME may reach it
	// through one (FreeBSD's /home is /usr/home), so compare resolved paths.
	cur := filepath.Dir(filepath.Clean(exe))
	if !agentDir.MatchString(filepath.Base(cur)) {
		return
	}
	base, err := filepath.EvalSymlinks(filepath.Join(home, ".cache", "syngoid"))
	if err != nil {
		log.Debug("prune: " + err.Error())
		return
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(cur))
	if err != nil {
		log.Debug("prune: " + err.Error())
		return
	}
	if parent != base {
		return
	}
	entries, err := os.ReadDir(base)
	if err != nil {
		log.Debug("prune: " + err.Error())
		return
	}
	type sibling struct {
		name string
		mod  time.Time
	}
	var sibs []sibling
	var newest time.Time
	for _, e := range entries {
		if !e.IsDir() || e.Name() == filepath.Base(cur) || !agentDir.MatchString(e.Name()) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			log.Debug("prune: " + err.Error())
			continue
		}
		mod := info.ModTime()
		sibs = append(sibs, sibling{e.Name(), mod})
		if mod.After(newest) {
			newest = mod
		}
	}
	for _, s := range sibs {
		if s.mod.Equal(newest) {
			continue
		}
		if err := os.RemoveAll(filepath.Join(base, s.name)); err != nil {
			log.Debug("prune: " + err.Error())
			continue
		}
		log.Debug("pruned " + filepath.Join(base, s.name))
	}
}
