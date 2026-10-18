package zfs

import (
	"errors"
	"fmt"
	"strings"
)

// Allow-lists of single-letter options per command kind.
const (
	SendAllowedStream     = "LPVRXbcehpsvw"
	SendAllowedBookmark   = "LVcew"
	SendAllowedResume     = "PVev"
	EstimateAllowedStream = "LVRXbcehpsw"
	EstimateAllowedResume = "Ve"
	RecvAllowed           = "hoxuv"
)

// Opt is one parsed command-line option. Value is only set for the flags
// that take an argument (-o and -x).
type Opt struct {
	Flag  byte
	Value string
}

// takesValue reports whether flag consumes the next word as its argument.
func takesValue(flag byte) bool { return flag == 'o' || flag == 'x' }

// ParseOpts parses the space-separated option mini-syntax: each word is a
// run of single-letter flags, and "o" and "x" take the following word as
// their value and must end their word. An empty string yields no options.
func ParseOpts(s string) ([]Opt, error) {
	words := strings.Fields(s)
	var opts []Opt
	for i := 0; i < len(words); i++ {
		w := words[i]
		for j := 0; j < len(w); j++ {
			f := w[j]
			if (f < 'a' || f > 'z') && (f < 'A' || f > 'Z') {
				return nil, fmt.Errorf("invalid option %q in %q", string(f), w)
			}
			if !takesValue(f) {
				opts = append(opts, Opt{Flag: f})
				continue
			}
			if j != len(w)-1 {
				return nil, fmt.Errorf("option -%c must be the last letter of its word %q", f, w)
			}
			if i+1 >= len(words) {
				return nil, errors.New("option -" + string(f) + " requires a value")
			}
			i++
			opts = append(opts, Opt{Flag: f, Value: words[i]})
		}
	}
	return opts, nil
}

// FilterOpts returns the argv form of the options whose flag appears in
// allowed, in their original order. A valued option yields two elements.
func FilterOpts(opts []Opt, allowed string) []string {
	var out []string
	for _, o := range opts {
		if strings.IndexByte(allowed, o.Flag) < 0 {
			continue
		}
		out = append(out, "-"+string(o.Flag))
		if takesValue(o.Flag) {
			out = append(out, o.Value)
		}
	}
	return out
}
