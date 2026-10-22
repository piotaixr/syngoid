package rpc

import (
	"errors"
	"testing"

	"github.com/piotaixr/syngoid/internal/zfs"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name   string
		method string
		params any
		field  string // "" means valid
	}{
		{"exists with space", MethodExists, DatasetParams{Dataset: "tank/my data"}, ""},
		{"exists pointer", MethodExists, &DatasetParams{Dataset: "tank/my data"}, ""},
		{"exists flag", MethodExists, DatasetParams{Dataset: "-oops"}, "dataset"},
		{"exists empty component", MethodExists, DatasetParams{Dataset: "tank//a"}, "dataset"},
		{"exists empty", MethodExists, DatasetParams{}, "dataset"},
		{"exists snapshot name", MethodExists, DatasetParams{Dataset: "tank/a@s"}, "dataset"},
		{"exists too long", MethodExists, DatasetParams{Dataset: string(make([]byte, 256))}, "dataset"},
		{"list_tree newline", MethodListTree, DatasetParams{Dataset: "tank\n-r"}, "dataset"},
		{"destroy_dataset ok", MethodDestroyDataset, DatasetParams{Dataset: "tank/a"}, ""},
		{"destroy ok", MethodDestroy, DestroyParams{Dataset: "tank/a", Snaps: []string{"s1", "s2"}}, ""},
		{"destroy flag snap", MethodDestroy, DestroyParams{Dataset: "tank/a", Snaps: []string{"s1", "-r"}}, "snaps"},
		{"destroy full snap", MethodDestroy, DestroyParams{Dataset: "tank/a", Snaps: []string{"tank/a@s1"}}, "snaps"},
		{"destroy bad dataset", MethodDestroy, DestroyParams{Dataset: "-r", Snaps: []string{"s1"}}, "dataset"},
		{"snapshot ok", MethodSnapshot, SnapshotParams{Names: []string{"tank/a@s"}}, ""},
		{"snapshot no at", MethodSnapshot, SnapshotParams{Names: []string{"tank/a"}}, "names"},
		{"snapshot flag", MethodSnapshot, SnapshotParams{Names: []string{"-r"}}, "names"},
		{"snapshot two ats", MethodSnapshot, SnapshotParams{Names: []string{"tank/a@s@t"}}, "names"},
		{"snapshot flag short", MethodSnapshot, SnapshotParams{Names: []string{"tank/a@-s"}}, "names"},
		{"hold flag tag", MethodHold, HoldParams{Tag: "-d", Snapshot: "tank/a@s"}, "tag"},
		{"hold ok", MethodHold, HoldParams{Tag: "syncoid_nas", Snapshot: "tank/a@s"}, ""},
		{"hold empty tag", MethodHold, HoldParams{Snapshot: "tank/a@s"}, "tag"},
		{"hold control tag", MethodHold, HoldParams{Tag: "a\x00b", Snapshot: "tank/a@s"}, "tag"},
		{"hold long tag", MethodHold, HoldParams{Tag: string(make([]byte, 256)), Snapshot: "tank/a@s"}, "tag"},
		{"hold bad snapshot", MethodHold, HoldParams{Tag: "t", Snapshot: "tank/a#s"}, "snapshot"},
		{"release ok", MethodRelease, HoldParams{Tag: "t", Snapshot: "tank/a@s"}, ""},
		{"release flag tag", MethodRelease, HoldParams{Tag: "-r", Snapshot: "tank/a@s"}, "tag"},
		{"bookmark ok", MethodBookmark, BookmarkParams{Snapshot: "tank/a@s", Bookmark: "tank/a#b"}, ""},
		{"bookmark not a bookmark", MethodBookmark, BookmarkParams{Snapshot: "tank/a@s", Bookmark: "tank/a@b"}, "bookmark"},
		{"bookmark bad snapshot", MethodBookmark, BookmarkParams{Snapshot: "-x@s", Bookmark: "tank/a#b"}, "snapshot"},
		{"send ok", MethodSend, SendParams{Spec: zfs.SendSpec{Dataset: "tank/a", To: "s2", FromSnap: "s1"}}, ""},
		{"send bad dataset", MethodSend, SendParams{Spec: zfs.SendSpec{Dataset: "-R"}}, "spec.dataset"},
		{"send bad to", MethodSend, SendParams{Spec: zfs.SendSpec{Dataset: "tank/a", To: "-x"}}, "spec.to"},
		{"send bad from_snap", MethodSend, SendParams{Spec: zfs.SendSpec{Dataset: "tank/a", FromSnap: "a/b"}}, "spec.from_snap"},
		{"send bad from_bookmark", MethodSend, SendParams{Spec: zfs.SendSpec{Dataset: "tank/a", FromBookmark: "-b"}}, "spec.from_bookmark"},
		{"send origin ok", MethodSend, SendParams{Spec: zfs.SendSpec{Dataset: "tank/a", FromOrigin: "tank/o@s"}}, ""},
		{"send origin short", MethodSend, SendParams{Spec: zfs.SendSpec{Dataset: "tank/a", FromOrigin: "s"}}, "spec.from_origin"},
		{"send bad token", MethodSend, SendParams{Spec: zfs.SendSpec{Dataset: "tank/a", ResumeToken: "-t"}}, "spec.resume_token"},
		{"send ok token", MethodSend, SendParams{Spec: zfs.SendSpec{Dataset: "tank/a", ResumeToken: "1-abc0-def"}}, ""},
		{"send option R", MethodSend, SendParams{Spec: zfs.SendSpec{Dataset: "tank/a", Options: []zfs.Opt{{Flag: 'R'}}}}, ""},
		{"send option o", MethodSend, SendParams{Spec: zfs.SendSpec{Dataset: "tank/a", Options: []zfs.Opt{{Flag: 'o'}}}}, "spec.options"},
		{"send option with value", MethodSend, SendParams{Spec: zfs.SendSpec{Dataset: "tank/a", Options: []zfs.Opt{{Flag: 'R', Value: "x"}}}}, "spec.options"},
		{"send_estimate bad", MethodSendEstimate, SendEstimateParams{Spec: zfs.SendSpec{Dataset: "tank/a", Options: []zfs.Opt{{Flag: 'o'}}}}, "spec.options"},
		{"send_estimate ok", MethodSendEstimate, SendEstimateParams{Spec: zfs.SendSpec{Dataset: "tank/a"}}, ""},
		{"receive ok", MethodReceive, ReceiveParams{Spec: zfs.RecvSpec{Dataset: "tank/b", Args: []string{"-s", "-F", "-o", "compression=lz4", "-x", "mountpoint"}}}, ""},
		{"receive bad dataset", MethodReceive, ReceiveParams{Spec: zfs.RecvSpec{Dataset: "-F"}}, "spec.dataset"},
		{"receive bad o value", MethodReceive, ReceiveParams{Spec: zfs.RecvSpec{Dataset: "tank/b", Args: []string{"-o", "-x=1"}}}, "spec.args"},
		{"receive long flag", MethodReceive, ReceiveParams{Spec: zfs.RecvSpec{Dataset: "tank/b", Args: []string{"--force"}}}, "spec.args"},
		{"receive newline value", MethodReceive, ReceiveParams{Spec: zfs.RecvSpec{Dataset: "tank/b", Args: []string{"-o", "a=b\nc"}}}, "spec.args"},
		{"receive NUL value", MethodReceive, ReceiveParams{Spec: zfs.RecvSpec{Dataset: "tank/b", Args: []string{"-o", "a=b\x00c"}}}, "spec.args"},
		{"receive o without equals", MethodReceive, ReceiveParams{Spec: zfs.RecvSpec{Dataset: "tank/b", Args: []string{"-o", "compression"}}}, "spec.args"},
		{"receive o dangling", MethodReceive, ReceiveParams{Spec: zfs.RecvSpec{Dataset: "tank/b", Args: []string{"-o"}}}, "spec.args"},
		{"receive x dangling", MethodReceive, ReceiveParams{Spec: zfs.RecvSpec{Dataset: "tank/b", Args: []string{"-x"}}}, "spec.args"},
		{"receive x flag", MethodReceive, ReceiveParams{Spec: zfs.RecvSpec{Dataset: "tank/b", Args: []string{"-x", "-F"}}}, "spec.args"},
		{"receive bare arg", MethodReceive, ReceiveParams{Spec: zfs.RecvSpec{Dataset: "tank/b", Args: []string{"tank/c"}}}, "spec.args"},
		{"receive u h v", MethodReceive, ReceiveParams{Spec: zfs.RecvSpec{Dataset: "tank/b", Args: []string{"-u", "-h", "-v"}}}, ""},
		{"receive o value with spaces and dash", MethodReceive, ReceiveParams{Spec: zfs.RecvSpec{Dataset: "tank/b", Args: []string{"-o", "syncoid:x=-a b"}}}, ""},
		{"get_props ok", MethodGetProps, GetPropsParams{Dataset: "tank/a", Props: []string{"syncoid:sync", "used"}}, ""},
		{"get_props flag", MethodGetProps, GetPropsParams{Dataset: "tank/a", Props: []string{"syncoid:sync", "-H"}}, "props"},
		{"get_props bad dataset", MethodGetProps, GetPropsParams{Dataset: "-r", Props: []string{"used"}}, "dataset"},
		{"get_props upper", MethodGetProps, GetPropsParams{Dataset: "tank/a", Props: []string{"Used"}}, "props"},
		{"receive_busy ok", MethodReceiveBusy, ReceiveBusyParams{Datasets: []string{"tank/a", "tank/b"}}, ""},
		{"receive_busy bad", MethodReceiveBusy, ReceiveBusyParams{Datasets: []string{"tank/a", "-r"}}, "datasets"},
		{"info", MethodInfo, nil, ""},
		{"shutdown", MethodShutdown, nil, ""},
		{"cancel", MethodCancel, CancelParams{ID: 7}, ""},
		{"unknown method", "rm_rf", nil, "params"},
		{"hello is not validated here", MethodHello, Hello{}, "params"},
		{"wrong params type", MethodExists, HoldParams{}, "params"},
		{"nil params", MethodExists, nil, "params"},
		{"nil pointer params", MethodExists, (*DatasetParams)(nil), "params"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(tt.method, tt.params)
			if tt.field == "" {
				if err != nil {
					t.Fatalf("Validate = %v, want nil", err)
				}
				return
			}
			var ie *InvalidError
			if !errors.As(err, &ie) {
				t.Fatalf("Validate = %v (%T), want *InvalidError", err, err)
			}
			if ie.Field != tt.field {
				t.Errorf("Field = %q, want %q (value %q)", ie.Field, tt.field, ie.Value)
			}
		})
	}
}

func TestValidateReportsOffendingValue(t *testing.T) {
	err := Validate(MethodDestroy, DestroyParams{Dataset: "tank/a", Snaps: []string{"s1", "-r"}})
	var ie *InvalidError
	if !errors.As(err, &ie) || ie.Value != "-r" {
		t.Fatalf("Validate = %v, want InvalidError with Value -r", err)
	}
}
