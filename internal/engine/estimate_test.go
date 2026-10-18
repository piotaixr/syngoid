package engine

import (
	"context"
	"reflect"
	"testing"

	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/endpoint/fake"
	"github.com/piotaixr/syngoid/internal/zfs"
)

func TestSendSpecFor(t *testing.T) {
	opts := mustOpts(t, "Pv")
	s := DatasetState{Source: "tank/data"}
	tests := []struct {
		a    Action
		want zfs.SendSpec
	}{
		{SendFull{Snap: "a"}, zfs.SendSpec{Dataset: "tank/data", To: "a", Options: opts}},
		{SendIncremental{From: "a", To: "b", Intermediates: true}, zfs.SendSpec{Dataset: "tank/data", FromSnap: "a", To: "b", Intermediates: true, Options: opts}},
		{SendFromBookmark{Bookmark: "bm", To: "b"}, zfs.SendSpec{Dataset: "tank/data", FromBookmark: "bm", To: "b", Options: opts}},
		{SendClone{Origin: "tank/o@x", To: "b"}, zfs.SendSpec{Dataset: "tank/data", FromOrigin: "tank/o@x", To: "b", Options: opts}},
		{Resume{Token: "tok"}, zfs.SendSpec{Dataset: "tank/data", ResumeToken: "tok", Options: opts}},
	}
	for _, tt := range tests {
		if got := SendSpecFor(s, tt.a, opts); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%T: got %+v, want %+v", tt.a, got, tt.want)
		}
	}
}

func TestEstimateAction(t *testing.T) {
	w := fake.NewWorld()
	w.Create("tank/data", "")
	w.Snap("tank/data", "a")
	w.Snap("tank/data", "b")
	w.MarkBookmark("tank/data", "a", "bm")
	w.Dataset("tank/data").Used = 5000
	src := fake.New(w, endpoint.HostInfo{})
	ctx := context.Background()
	s := DatasetState{Source: "tank/data", NewSyncSnap: "sync"}
	const mib = 1 << 20

	tests := []struct {
		name       string
		a          Action
		wantBytes  int64
		wantApprox bool
	}{
		{"existing full", SendFull{Snap: "a"}, mib, false},
		{"existing incremental", SendIncremental{From: "a", To: "b"}, mib, false},
		{"new sync incremental uses written@", SendIncremental{From: "a", To: "sync"}, 1 * mib, true},
		{"new sync from bookmark fails written#", SendFromBookmark{Bookmark: "bm", To: "sync"}, 0, true},
		{"new sync full uses referenced", SendFull{Snap: "sync"}, 5000, true},
		{"new sync clone uses referenced", SendClone{Origin: "x@y", To: "sync"}, 5000, true},
		{"new sync incremental missing base", SendIncremental{From: "zz", To: "sync"}, 0, true},
		{"non-send", PruneSyncSnaps{Side: Source}, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, approx := EstimateAction(ctx, src, s, tt.a, nil)
			if n != tt.wantBytes || approx != tt.wantApprox {
				t.Errorf("got (%d, %v), want (%d, %v)", n, approx, tt.wantBytes, tt.wantApprox)
			}
		})
	}
	t.Run("resume with unknown token", func(t *testing.T) {
		n, approx := EstimateAction(ctx, src, s, Resume{Token: "nope"}, nil)
		if n != 0 || approx {
			t.Errorf("got (%d, %v), want (0, false)", n, approx)
		}
	})
}
