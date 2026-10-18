// Package cli parses the syngoid command line into typed options.
package cli

import (
	"errors"
	"fmt"
	"strings"
)

// Location is a SOURCE or TARGET argument: a dataset, optionally on a
// remote host reached over SSH.
type Location struct {
	User, Host, Dataset string
}

// IsRemote reports whether the location is on another host.
func (l Location) IsRemote() bool { return l.Host != "" }

// String renders the location as [user@]host:dataset, or just the dataset
// when local.
func (l Location) String() string {
	if !l.IsRemote() {
		return l.Dataset
	}
	if l.User != "" {
		return l.User + "@" + l.Host + ":" + l.Dataset
	}
	return l.Host + ":" + l.Dataset
}

// ParseLocation parses a SOURCE or TARGET argument. An argument whose first
// ':' comes before any '/' is remote, unless it has no '@' and a local pool
// named like its first path component exists (pool names may contain ':').
// localPoolExists may be nil, meaning no local pool exists.
func ParseLocation(arg string, localPoolExists func(string) bool) (Location, error) {
	if arg == "" {
		return Location{}, errors.New("empty location")
	}
	colon := strings.IndexByte(arg, ':')
	if colon < 0 || (strings.IndexByte(arg, '/') >= 0 && strings.IndexByte(arg, '/') < colon) {
		return Location{Dataset: arg}, nil
	}
	prefix, dataset := arg[:colon], arg[colon+1:]
	var loc Location
	if at := strings.LastIndexByte(prefix, '@'); at >= 0 {
		loc.User, loc.Host = prefix[:at], prefix[at+1:]
	} else {
		pool, _, _ := strings.Cut(arg, "/")
		if localPoolExists != nil && localPoolExists(pool) {
			return Location{Dataset: arg}, nil
		}
		loc.Host = prefix
	}
	if loc.Host == "" {
		return Location{}, fmt.Errorf("%q: empty host name", arg)
	}
	if dataset == "" {
		return Location{}, fmt.Errorf("%q: empty dataset", arg)
	}
	loc.Dataset = dataset
	return loc, nil
}
