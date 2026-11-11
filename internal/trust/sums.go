package trust

import (
	"fmt"
	"strings"
)

// ParseSums parses a SHA256SUMS file into a map from file name to lowercase
// hex digest. Lines are "<64 lowercase hex>" followed by two spaces (or a
// space and "*" for binary mode) and the name; blank lines are skipped.
func ParseSums(b []byte) (map[string]string, error) {
	sums := map[string]string{}
	for i, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSuffix(line, "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		digest, name, ok := splitSumsLine(line)
		if !ok {
			return nil, fmt.Errorf("SHA256SUMS line %d: malformed", i+1)
		}
		if _, dup := sums[name]; dup {
			return nil, fmt.Errorf("SHA256SUMS: duplicate entry %s", name)
		}
		sums[name] = digest
	}
	return sums, nil
}

func splitSumsLine(line string) (digest, name string, ok bool) {
	if len(line) < 64+2+1 {
		return "", "", false
	}
	digest, rest := line[:64], line[64:]
	for _, c := range digest {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return "", "", false
		}
	}
	if rest[0] != ' ' || (rest[1] != ' ' && rest[1] != '*') {
		return "", "", false
	}
	return digest, rest[2:], true
}
