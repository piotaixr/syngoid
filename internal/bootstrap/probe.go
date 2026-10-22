//go:build unix

package bootstrap

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/piotaixr/syngoid/internal/sshx"
)

// probeScript expands a leading "~/" of the bin dir $1 against $HOME and
// prints six lines: uname -s, uname -m, isainfo -k (illumos) or "-", $HOME,
// the absolute bin dir, and whether <dir>/syngoid is executable. It is one
// line: a csh login shell rejects a newline inside single quotes.
const probeScript = `D=$1; case $D in "~/"*) D=$HOME/${D#"~/"};; esac; ` +
	`uname -s; uname -m; ` +
	`if [ "$(uname -s)" = SunOS ]; then isainfo -k; else echo -; fi; ` +
	`printf '%s\n' "$HOME" "$D"; ` +
	`if [ -x "$D/syngoid" ]; then echo yes; else echo no; fi`

// pushScript atomically installs its stdin as $1/syngoid, mode 0700, in a
// 0700 directory (also when it already existed with another mode), only when exactly $2 bytes arrived: a push cut short ends
// cat with a plain EOF. On failure the temporary file is removed. The
// unquoted $n drops the padding BSD wc prints.
const pushScript = `umask 077; T="$1/.syngoid.tmp.$$"; mkdir -p "$1" && chmod 700 "$1" && cat > "$T" && n=$(wc -c < "$T") && [ $n -eq "$2" ] && chmod 700 "$T" && mv -f "$T" "$1/syngoid" || { rm -f "$T"; exit 1; }`

// ProbeCommand returns the remote command probing the platform and the bin
// dir, which may start with "~/".
func ProbeCommand(dir string) string {
	return sshx.ShellJoin("sh", "-c", probeScript, "syngoid-probe", dir)
}

// PushCommand returns the remote command installing its stdin, size bytes,
// as <dir>/syngoid; dir is the probe's absolute Dir.
func PushCommand(dir string, size int64) string {
	return sshx.ShellJoin("sh", "-c", pushScript, "syngoid-push", dir, strconv.FormatInt(size, 10))
}

// ServeCommand returns the remote command starting the agent in dir, the
// probe's absolute Dir.
func ServeCommand(dir string) string {
	return sshx.ShellJoin(dir+"/syngoid", "serve")
}

// ProbeResult is the parsed output of the probe.
type ProbeResult struct {
	Platform
	Home, Dir  string // remote $HOME and the absolute bin dir
	Executable bool   // whether Dir/syngoid is executable
}

// ParseProbe parses the probe's output: exactly six lines, a supported
// platform, an absolute and valid bin dir and a yes/no flag.
func ParseProbe(out string) (ProbeResult, error) {
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(lines) != 6 {
		return ProbeResult{}, fmt.Errorf("probe: unexpected output %q", out)
	}
	isa := lines[2]
	if isa == "-" {
		isa = ""
	}
	p, err := ParsePlatform(lines[0], lines[1], isa)
	if err != nil {
		return ProbeResult{}, err
	}
	r := ProbeResult{Platform: p, Home: lines[3], Dir: lines[4]}
	if !strings.HasPrefix(r.Dir, "/") {
		return ProbeResult{}, fmt.Errorf("probe: remote bin dir %q is not absolute", r.Dir)
	}
	if err := checkRemoteDir(r.Dir); err != nil {
		return ProbeResult{}, err
	}
	switch lines[5] {
	case "yes":
		r.Executable = true
	case "no":
	default:
		return ProbeResult{}, fmt.Errorf("probe: unexpected output %q", out)
	}
	return r, nil
}

// checkRemoteDir rejects a remote bin dir that is empty or holds anything
// but printable ASCII, or that holds a backslash. The serve command runs in
// the remote login shell, which may be csh or fish, where a quoted newline
// or control character does not survive and fish reads \\ and \' as
// escapes inside single quotes.
func checkRemoteDir(dir string) error {
	if dir == "" {
		return fmt.Errorf("remote bin dir is empty")
	}
	for i := 0; i < len(dir); i++ {
		if c := dir[i]; c < 0x20 || c > 0x7e || c == '\\' {
			return fmt.Errorf("invalid remote bin dir %q: only printable ASCII characters other than \\ are allowed", dir)
		}
	}
	return nil
}
