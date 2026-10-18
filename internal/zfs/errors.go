// Package zfs wraps the zfs command line and classifies its failures.
package zfs

import (
	"errors"
	"regexp"
	"strings"
)

// Sentinel errors returned (directly or via errors.Is) by Classify.
var (
	// ErrPartialState means the target holds partially received state that
	// must be cleared or resumed before a new receive can proceed.
	ErrPartialState = errors.New("destination contains partially-complete receive state")
	// ErrResumeSourceGone means a resumable send refers to a source that no
	// longer exists, so the resume token cannot be used.
	ErrResumeSourceGone = errors.New("resume source no longer exists")
	// ErrNoDataset means the referenced dataset does not exist.
	ErrNoDataset = errors.New("dataset does not exist")
	// ErrBookmarksUnsupported means the operation cannot use bookmarks on
	// this dataset or ZFS version.
	ErrBookmarksUnsupported = errors.New("bookmarks are not supported for this operation")
)

// DestinationExistsError reports that a receive target already exists.
type DestinationExistsError struct {
	Name string
}

func (e *DestinationExistsError) Error() string {
	return "destination already exists: " + e.Name
}

var destinationExistsRE = regexp.MustCompile(`(?m)^cannot restore to (.+): destination already exists$`)

// Classify maps zfs stderr output to a typed error. It returns nil when the
// output is not recognised. When several patterns match, they are checked in
// the order: DestinationExists, ResumeSourceGone, PartialState, NoDataset,
// BookmarksUnsupported.
func Classify(stderr string) error {
	if m := destinationExistsRE.FindStringSubmatch(stderr); m != nil {
		return &DestinationExistsError{Name: m[1]}
	}
	if strings.Contains(stderr, "cannot resume send") && strings.Contains(stderr, "no longer exists") {
		return ErrResumeSourceGone
	}
	if strings.Contains(stderr, "partially-complete state") || strings.Contains(stderr, "dataset is busy") {
		return ErrPartialState
	}
	if strings.Contains(stderr, "dataset does not exist") {
		return ErrNoDataset
	}
	if strings.Contains(stderr, "invalid type 'bookmark'") || strings.Contains(stderr, "operation not applicable to datasets of this type") {
		return ErrBookmarksUnsupported
	}
	return nil
}
