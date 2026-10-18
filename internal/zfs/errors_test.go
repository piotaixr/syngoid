package zfs_test

import (
	"errors"
	"testing"

	"github.com/piotaixr/syngoid/internal/zfs"
)

func TestClassify(t *testing.T) {
	tests := []struct {
		name     string
		stderr   string
		want     error
		wantDest string
	}{
		{
			name:   "partial state",
			stderr: `cannot receive incremental stream: destination tank/b contains partially-complete state from "zfs receive -s".`,
			want:   zfs.ErrPartialState,
		},
		{
			name:   "dataset busy",
			stderr: "cannot receive new filesystem stream: dataset is busy",
			want:   zfs.ErrPartialState,
		},
		{
			name:   "resume source gone initial send",
			stderr: "cannot resume send: 'tank/a@s1' used in the initial send no longer exists",
			want:   zfs.ErrResumeSourceGone,
		},
		{
			name:   "resume source gone incremental",
			stderr: "cannot resume send: incremental source 0x1f2e no longer exists",
			want:   zfs.ErrResumeSourceGone,
		},
		{
			name:     "destination exists",
			stderr:   "warning: x\ncannot restore to tank/b@s2: destination already exists\n",
			wantDest: "tank/b@s2",
		},
		{
			name:     "destination exists with colons in snapshot name",
			stderr:   "cannot restore to tank/b@syncoid_h_2026-10-18:04:17:00-GMT+00:00: destination already exists\n",
			wantDest: "tank/b@syncoid_h_2026-10-18:04:17:00-GMT+00:00",
		},
		{
			name:   "no dataset",
			stderr: "cannot open 'tank/x': dataset does not exist",
			want:   zfs.ErrNoDataset,
		},
		{
			name:   "bookmarks unsupported invalid type",
			stderr: "invalid type 'bookmark'",
			want:   zfs.ErrBookmarksUnsupported,
		},
		{
			name:   "bookmarks unsupported not applicable",
			stderr: "operation not applicable to datasets of this type",
			want:   zfs.ErrBookmarksUnsupported,
		},
		{
			name:   "unrecognised",
			stderr: "something else",
			want:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := zfs.Classify(tt.stderr)
			if tt.wantDest != "" {
				var de *zfs.DestinationExistsError
				if !errors.As(got, &de) {
					t.Fatalf("Classify(%q) = %v, want *DestinationExistsError", tt.stderr, got)
				}
				if de.Name != tt.wantDest {
					t.Errorf("DestinationExistsError.Name = %q, want %q", de.Name, tt.wantDest)
				}
				return
			}
			if tt.want == nil {
				if got != nil {
					t.Fatalf("Classify(%q) = %v, want nil", tt.stderr, got)
				}
				return
			}
			if !errors.Is(got, tt.want) {
				t.Fatalf("Classify(%q) = %v, want %v", tt.stderr, got, tt.want)
			}
		})
	}
}

func TestDestinationExistsErrorMessage(t *testing.T) {
	err := &zfs.DestinationExistsError{Name: "tank/b@s2"}
	if got, want := err.Error(), "destination already exists: tank/b@s2"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}
