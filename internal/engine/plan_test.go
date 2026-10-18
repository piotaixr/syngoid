package engine

import (
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/piotaixr/syngoid/internal/cli"
	"github.com/piotaixr/syngoid/internal/zfs"
)

const (
	planSrc  = "tank/a"
	planTgt  = "backup/a"
	planSync = "syncoid_nas_T"
	ownOld   = "syncoid_nas_2026-01-01:00:00:00-GMT00:00"
	otherOld = "syncoid_nas2_2026-01-01:00:00:00-GMT00:00"
)

var (
	snapA = zfs.Snapshot{Name: "a", GUID: "ga", CreateTxg: 10, Creation: 100}
	snapB = zfs.Snapshot{Name: "b", GUID: "gb", CreateTxg: 20, Creation: 200}
	snapC = zfs.Snapshot{Name: "c", GUID: "gc", CreateTxg: 30, Creation: 300}
	bmX   = zfs.Bookmark{Name: "bm", GUID: "gx", CreateTxg: 15, Creation: 150}
)

func abc() []zfs.Snapshot { return []zfs.Snapshot{snapA, snapB, snapC} }

// baseState is the default fixture: source has a,b,c, target absent, a sync
// snapshot is planned.
func baseState() DatasetState {
	return DatasetState{
		Source:      planSrc,
		Target:      planTgt,
		SourceSnaps: abc(),
		NewSyncSnap: planSync,
		TargetUsed:  1 << 30,
	}
}

func baseOpts() PlanOptions { return PlanOptions{Hostname: "nas"} }

func TestPlan(t *testing.T) {
	cowardly := "Target " + planTgt + " exists but has no snapshots matching with " + planSrc + "!\n" +
		"Replication to target would require destroying existing\n" +
		"target. Cowardly refusing to destroy your existing target."
	hint := "\n\nNOTE: Target " + planTgt + " dataset is < 64MB used - did you mistakenly run\n" +
		"`zfs create " + planTgt + "` on the target? ZFS initial\n" +
		"replication must be to a NON EXISTENT DATASET, which will\n" +
		"then be CREATED BY the initial replication process."

	tests := []struct {
		name        string
		state       func(*DatasetState)
		opts        func(*PlanOptions)
		want        []Action
		wantStatus  Status
		wantMsg     string
		msgContains []string
		msgLacks    []string
		lastAction  Action
	}{
		{
			name:       "sync=false",
			state:      func(s *DatasetState) { s.SyncProp = "false" },
			wantStatus: StatusSkipped,
			wantMsg:    "Skipping dataset (syncoid:sync=false): tank/a...",
		},
		{
			name:        "sync host list excludes",
			state:       func(s *DatasetState) { s.SyncProp = "db1, db2" },
			wantStatus:  StatusSkipped,
			wantMsg:     "Skipping dataset (syncoid:sync doesn't include nas): tank/a...",
			msgContains: []string{"syncoid:sync doesn't include nas"},
		},
		{
			name:  "sync host list includes with spaces",
			state: func(s *DatasetState) { s.SyncProp = "db1, nas" },
			want:  []Action{SendFull{Snap: "a"}, SendIncremental{From: "a", To: planSync, Intermediates: true}},
		},
		{
			name:  "sync true",
			state: func(s *DatasetState) { s.SyncProp = "true" },
			want:  []Action{SendFull{Snap: "a"}, SendIncremental{From: "a", To: planSync, Intermediates: true}},
		},
		{
			name:  "sync dash",
			state: func(s *DatasetState) { s.SyncProp = "-" },
			want:  []Action{SendFull{Snap: "a"}, SendIncremental{From: "a", To: planSync, Intermediates: true}},
		},
		{
			name: "first sync",
			want: []Action{SendFull{Snap: "a"}, SendIncremental{From: "a", To: planSync, Intermediates: true}},
		},
		{
			name: "first sync no-stream",
			opts: func(o *PlanOptions) { o.NoStream = true },
			want: []Action{SendFull{Snap: planSync}},
		},
		{
			name:  "first sync no snaps",
			state: func(s *DatasetState) { s.SourceSnaps = nil },
			want:  []Action{SendFull{Snap: planSync}},
		},
		{
			name: "no-sync-snap no snaps",
			state: func(s *DatasetState) {
				s.SourceSnaps = nil
				s.NewSyncSnap = ""
			},
			opts:       func(o *PlanOptions) { o.NoSyncSnap = true },
			wantStatus: StatusSoft,
			wantMsg:    "no snapshots exist on source tank/a, and you asked for --no-sync-snap.",
		},
		{
			name: "incremental",
			state: func(s *DatasetState) {
				s.TargetExists = true
				s.TargetSnaps = []zfs.Snapshot{snapA, snapB}
			},
			want: []Action{SendIncremental{From: "b", To: planSync, Intermediates: true}},
		},
		{
			name: "incremental no-stream",
			state: func(s *DatasetState) {
				s.TargetExists = true
				s.TargetSnaps = []zfs.Snapshot{snapA, snapB}
			},
			opts: func(o *PlanOptions) { o.NoStream = true },
			want: []Action{SendIncremental{From: "b", To: planSync, Intermediates: false}},
		},
		{
			name: "filtered chain",
			state: func(s *DatasetState) {
				s.TargetExists = true
				s.TargetSnaps = []zfs.Snapshot{snapA}
			},
			opts: func(o *PlanOptions) { o.FiltersActive = true },
			want: []Action{
				SendIncremental{From: "a", To: "b"},
				SendIncremental{From: "b", To: "c"},
				SendIncremental{From: "c", To: planSync},
			},
		},
		{
			name: "filtered no-stream is a single send",
			state: func(s *DatasetState) {
				s.TargetExists = true
				s.TargetSnaps = []zfs.Snapshot{snapA}
			},
			opts: func(o *PlanOptions) {
				o.FiltersActive = true
				o.NoStream = true
			},
			want: []Action{SendIncremental{From: "a", To: planSync}},
		},
		{
			name: "nothing to do",
			state: func(s *DatasetState) {
				s.NewSyncSnap = ""
				s.TargetExists = true
				s.TargetSnaps = abc()
			},
			opts:       func(o *PlanOptions) { o.NoSyncSnap = true },
			wantStatus: StatusNothing,
			wantMsg:    "no snapshots on source newer than c on target. Nothing to do, not syncing.",
		},
		{
			name: "resume",
			state: func(s *DatasetState) {
				s.ResumeToken = "tok"
				s.TargetExists = true
				s.TargetSnaps = []zfs.Snapshot{snapA, snapB}
			},
			want: []Action{Resume{Token: "tok"}},
		},
		{
			name: "resume ignored",
			state: func(s *DatasetState) {
				s.ResumeToken = "tok"
				s.TargetExists = true
				s.TargetSnaps = []zfs.Snapshot{snapA, snapB}
			},
			opts: func(o *PlanOptions) { o.NoResume = true },
			want: []Action{SendIncremental{From: "b", To: planSync, Intermediates: true}},
		},
		{
			name: "bookmark fallback",
			state: func(s *DatasetState) {
				s.TargetExists = true
				s.TargetSnaps = []zfs.Snapshot{{Name: "x", GUID: "gx", CreateTxg: 5}}
				s.SourceBookmarks = []zfs.Bookmark{bmX}
			},
			want: []Action{
				SendFromBookmark{Bookmark: "bm", To: "b"},
				SendIncremental{From: "b", To: planSync, Intermediates: true},
			},
		},
		{
			name: "bookmark no-stream",
			state: func(s *DatasetState) {
				s.TargetExists = true
				s.TargetSnaps = []zfs.Snapshot{{Name: "x", GUID: "gx", CreateTxg: 5}}
				s.SourceBookmarks = []zfs.Bookmark{bmX}
			},
			opts: func(o *PlanOptions) { o.NoStream = true },
			want: []Action{SendFromBookmark{Bookmark: "bm", To: planSync}},
		},
		{
			name: "bookmark with no later source snapshot",
			state: func(s *DatasetState) {
				s.TargetExists = true
				s.TargetSnaps = []zfs.Snapshot{{Name: "x", GUID: "gx", CreateTxg: 5}}
				s.SourceBookmarks = []zfs.Bookmark{{Name: "bm", GUID: "gx", CreateTxg: 99, Creation: 999}}
			},
			want: []Action{SendFromBookmark{Bookmark: "bm", To: planSync}},
		},
		{
			name: "no match",
			state: func(s *DatasetState) {
				s.TargetExists = true
				s.TargetSnaps = []zfs.Snapshot{{Name: "z", GUID: "gz"}}
				s.TargetUsed = 1 << 20
			},
			wantStatus:  StatusCritical,
			wantMsg:     cowardly + hint,
			msgContains: []string{"Cowardly refusing to destroy your existing target.", "did you mistakenly run"},
		},
		{
			name: "no match large",
			state: func(s *DatasetState) {
				s.TargetExists = true
				s.TargetSnaps = []zfs.Snapshot{{Name: "z", GUID: "gz"}}
				s.TargetUsed = 1 << 30
			},
			wantStatus: StatusCritical,
			wantMsg:    cowardly,
			msgLacks:   []string{"did you mistakenly run"},
		},
		{
			name: "force delete",
			state: func(s *DatasetState) {
				s.TargetExists = true
				s.TargetSnaps = []zfs.Snapshot{{Name: "z", GUID: "gz"}}
			},
			opts: func(o *PlanOptions) { o.ForceDelete = true },
			want: []Action{ForceDestroyTarget{}},
		},
		{
			name: "force delete pool root",
			state: func(s *DatasetState) {
				s.TargetExists = true
				s.TargetSnaps = []zfs.Snapshot{{Name: "z", GUID: "gz"}}
				s.TargetIsPoolRoot = true
			},
			opts:       func(o *PlanOptions) { o.ForceDelete = true },
			wantStatus: StatusCritical,
			wantMsg:    cowardly,
		},
		{
			name: "clone",
			state: func(s *DatasetState) {
				s.Origin = "tank/o@s0"
				s.OriginInScope = true
			},
			want: []Action{
				SendClone{Origin: "tank/o@s0", To: "a"},
				SendIncremental{From: "a", To: planSync, Intermediates: true},
			},
		},
		{
			name:  "clone origin out of scope",
			state: func(s *DatasetState) { s.Origin = "tank/o@s0" },
			want:  []Action{SendFull{Snap: "a"}, SendIncremental{From: "a", To: planSync, Intermediates: true}},
		},
		{
			name: "holds",
			state: func(s *DatasetState) {
				s.TargetExists = true
				s.TargetSnaps = []zfs.Snapshot{snapA, snapB}
			},
			opts: func(o *PlanOptions) { o.UseHold = true },
			want: []Action{
				SendIncremental{From: "b", To: planSync, Intermediates: true},
				Hold{Side: Source, Snap: planSync},
				Release{Side: Source, Snap: "b"},
				Hold{Side: Target, Snap: planSync},
				Release{Side: Target, Snap: "b"},
			},
		},
		{
			name: "holds first sync",
			opts: func(o *PlanOptions) { o.UseHold = true },
			want: []Action{
				SendFull{Snap: "a"},
				SendIncremental{From: "a", To: planSync, Intermediates: true},
				Hold{Side: Source, Snap: planSync},
				Hold{Side: Target, Snap: planSync},
			},
		},
		{
			name: "holds bookmark path release target snapshot",
			state: func(s *DatasetState) {
				s.TargetExists = true
				s.TargetSnaps = []zfs.Snapshot{{Name: "x", GUID: "gx", CreateTxg: 5}}
				s.SourceBookmarks = []zfs.Bookmark{bmX}
			},
			opts: func(o *PlanOptions) {
				o.UseHold = true
				o.NoStream = true
			},
			want: []Action{
				SendFromBookmark{Bookmark: "bm", To: planSync},
				Hold{Side: Source, Snap: planSync},
				Release{Side: Source, Snap: "x"},
				Hold{Side: Target, Snap: planSync},
				Release{Side: Target, Snap: "x"},
			},
		},
		{
			name: "bookmark create",
			state: func(s *DatasetState) {
				s.NewSyncSnap = ""
				s.TargetExists = true
				s.TargetSnaps = []zfs.Snapshot{snapA}
			},
			opts: func(o *PlanOptions) {
				o.NoSyncSnap = true
				o.CreateBookmark = true
			},
			want: []Action{
				SendIncremental{From: "a", To: "c", Intermediates: true},
				CreateBookmark{Snap: "c"},
			},
		},
		{
			name: "prune",
			state: func(s *DatasetState) {
				s.SourceSnaps = []zfs.Snapshot{snapA, {Name: ownOld, GUID: "go", CreateTxg: 40}, {Name: otherOld, GUID: "gn", CreateTxg: 50}}
				s.TargetExists = true
				s.TargetSnaps = []zfs.Snapshot{snapA, {Name: ownOld, GUID: "go", CreateTxg: 40}}
			},
			want: []Action{
				SendIncremental{From: ownOld, To: planSync, Intermediates: true},
				PruneSyncSnaps{Side: Source, Snaps: []string{ownOld}},
				PruneSyncSnaps{Side: Target, Snaps: []string{ownOld}},
			},
		},
		{
			name: "keep sync snap",
			state: func(s *DatasetState) {
				s.SourceSnaps = []zfs.Snapshot{snapA, {Name: ownOld, GUID: "go", CreateTxg: 40}, {Name: otherOld, GUID: "gn", CreateTxg: 50}}
				s.TargetExists = true
				s.TargetSnaps = []zfs.Snapshot{snapA, {Name: ownOld, GUID: "go", CreateTxg: 40}}
			},
			opts: func(o *PlanOptions) { o.KeepSyncSnap = true },
			want: []Action{SendIncremental{From: ownOld, To: planSync, Intermediates: true}},
		},
		{
			name: "delete target snaps",
			state: func(s *DatasetState) {
				s.TargetExists = true
				s.TargetSnaps = []zfs.Snapshot{snapA, snapB}
			},
			opts: func(o *PlanOptions) { o.DeleteTargetSnapshots = true },
			want: []Action{
				SendIncremental{From: "b", To: planSync, Intermediates: true},
				DestroyTargetSnaps{},
			},
			lastAction: DestroyTargetSnaps{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := baseState()
			if tc.state != nil {
				tc.state(&s)
			}
			o := baseOpts()
			if tc.opts != nil {
				tc.opts(&o)
			}
			p := PlanDataset(s, o)

			if p.Source != planSrc || p.Target != planTgt {
				t.Errorf("Source/Target = %q/%q, want %q/%q", p.Source, p.Target, planSrc, planTgt)
			}
			if p.Status != tc.wantStatus {
				t.Errorf("Status = %v, want %v (message %q)", p.Status, tc.wantStatus, p.Message)
			}
			if p.Message != tc.wantMsg {
				t.Errorf("Message = %q, want %q", p.Message, tc.wantMsg)
			}
			for _, sub := range tc.msgContains {
				if !strings.Contains(p.Message, sub) {
					t.Errorf("Message %q lacks %q", p.Message, sub)
				}
			}
			for _, sub := range tc.msgLacks {
				if strings.Contains(p.Message, sub) {
					t.Errorf("Message %q contains %q", p.Message, sub)
				}
			}
			if !reflect.DeepEqual(p.Actions, tc.want) {
				t.Errorf("Actions = %#v, want %#v", p.Actions, tc.want)
			}
			if tc.lastAction != nil {
				if len(p.Actions) == 0 || !reflect.DeepEqual(p.Actions[len(p.Actions)-1], tc.lastAction) {
					t.Errorf("last action = %#v, want %#v", p.Actions, tc.lastAction)
				}
			}
		})
	}
}

func TestActionDescribe(t *testing.T) {
	tests := []struct {
		a           Action
		want        string
		destructive bool
	}{
		{SendFull{Snap: "s"}, "send full @s", false},
		{SendIncremental{From: "f", To: "t"}, "send incremental @f → @t", false},
		{SendIncremental{From: "f", To: "t", Intermediates: true}, "send incremental @f → @t (with intermediates)", false},
		{SendFromBookmark{Bookmark: "b", To: "t"}, "send incremental #b → @t", false},
		{SendClone{Origin: "tank/o@s0", To: "t"}, "send clone of tank/o@s0 → @t", false},
		{Resume{Token: "tok"}, "resume interrupted receive", false},
		{Hold{Side: Source, Snap: "s"}, "hold @s on source", false},
		{Hold{Side: Target, Snap: "s"}, "hold @s on target", false},
		{Release{Side: Source, Snap: "s"}, "release @s on source", false},
		{Release{Side: Target, Snap: "s"}, "release @s on target", false},
		{CreateBookmark{Snap: "s"}, "create bookmark #s", false},
		{PruneSyncSnaps{Side: Source, Snaps: []string{"x", "y"}}, "prune 2 sync snapshot(s) on source", false},
		{PruneSyncSnaps{Side: Target, Snaps: []string{"x"}}, "prune 1 sync snapshot(s) on target", false},
		{DestroyTargetSnaps{}, "destroy target snapshots missing on source", false},
		{ForceDestroyTarget{}, "DESTROY target recursively (--force-delete)", true},
	}
	for _, tc := range tests {
		if got := tc.a.Describe(); got != tc.want {
			t.Errorf("%#v.Describe() = %q, want %q", tc.a, got, tc.want)
		}
		if got := tc.a.Destructive(); got != tc.destructive {
			t.Errorf("%#v.Destructive() = %v, want %v", tc.a, got, tc.destructive)
		}
	}
}

func TestPlanOptionsFrom(t *testing.T) {
	o := &cli.Options{
		Identifier:            "id",
		NoStream:              true,
		NoSyncSnap:            true,
		KeepSyncSnap:          true,
		CreateBookmark:        true,
		UseHold:               true,
		DeleteTargetSnapshots: true,
		ForceDelete:           true,
		NoResume:              true,
	}
	want := PlanOptions{
		NoStream: true, NoSyncSnap: true, KeepSyncSnap: true, CreateBookmark: true,
		UseHold: true, DeleteTargetSnapshots: true, ForceDelete: true, NoResume: true,
		Hostname: "nas", Identifier: "id",
	}
	if got := PlanOptionsFrom(o, "nas"); got != want {
		t.Errorf("PlanOptionsFrom = %+v, want %+v", got, want)
	}

	for _, f := range []func(*cli.Options){
		func(o *cli.Options) { o.IncludeSnaps = []*regexp.Regexp{regexp.MustCompile("x")} },
		func(o *cli.Options) { o.ExcludeSnaps = []*regexp.Regexp{regexp.MustCompile("x")} },
	} {
		o := &cli.Options{}
		f(o)
		if !PlanOptionsFrom(o, "nas").FiltersActive {
			t.Errorf("FiltersActive = false with snapshot filters %+v", o)
		}
	}
	if PlanOptionsFrom(&cli.Options{}, "nas").FiltersActive {
		t.Error("FiltersActive = true without snapshot filters")
	}
}
