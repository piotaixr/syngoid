package perms

import (
	"reflect"
	"testing"
)

func names(reqs []Requirement) []string {
	out := make([]string, len(reqs))
	for i, r := range reqs {
		out[i] = r.String()
	}
	return out
}

func checkReq(t *testing.T, got []Requirement, want []string) {
	t.Helper()
	g := names(got)
	if len(g) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(g, want) {
		t.Errorf("requirements = %v, want %v", g, want)
	}
}

func TestRequiredSourceDefault(t *testing.T) {
	got := Required(Source, Needs{})
	checkReq(t, got, []string{"send", "snapshot", "mount", "destroy", "hold"})
	if !reflect.DeepEqual(got[0].AnyOf, []string{"send"}) {
		t.Errorf("send AnyOf = %v", got[0].AnyOf)
	}
}

func TestRequiredSourceRaw(t *testing.T) {
	got := Required(Source, Needs{RawSend: true})
	want := []string{"send", "send:raw", "send:encrypted"}
	if !reflect.DeepEqual(got[0].AnyOf, want) {
		t.Errorf("first AnyOf = %v, want %v", got[0].AnyOf, want)
	}
	if got[0].String() != "send or send:raw or send:encrypted" {
		t.Errorf("String() = %q", got[0].String())
	}
}

func TestRequiredSourceNoSyncSnap(t *testing.T) {
	checkReq(t, Required(Source, Needs{NoSyncSnap: true}), []string{"send", "hold"})
	checkReq(t, Required(Source, Needs{NoSyncSnap: true, CreateBookmark: true, UseHold: true}),
		[]string{"send", "hold", "release", "bookmark"})
	checkReq(t, Required(Source, Needs{KeepSyncSnap: true}), []string{"send", "snapshot", "mount", "hold"})
}

// zfs send -I (the default without --no-stream) takes temporary holds, so the
// source needs hold; plain -i and full sends do not.
func TestRequiredSourceNoStream(t *testing.T) {
	checkReq(t, Required(Source, Needs{NoStream: true}), []string{"send", "snapshot", "mount", "destroy"})
	checkReq(t, Required(Source, Needs{NoStream: true, UseHold: true}),
		[]string{"send", "snapshot", "mount", "destroy", "hold", "release"})
}

// zfs receive -F needs no rollback delegation (observed on OpenZFS, spec §14
// item 1).
func TestRequiredTargetDefault(t *testing.T) {
	checkReq(t, Required(Target, Needs{}), []string{"create", "mount", "receive", "destroy"})
}

func TestRequiredTargetNoRollbackKeep(t *testing.T) {
	got := Required(Target, Needs{NoRollback: true, KeepSyncSnap: true})
	checkReq(t, got, []string{"create", "mount", "receive or receive:append"})
	if !reflect.DeepEqual(got[2].AnyOf, []string{"receive", "receive:append"}) {
		t.Errorf("receive AnyOf = %v", got[2].AnyOf)
	}
}

func TestRequiredTargetDestroyTriggers(t *testing.T) {
	base := Needs{NoSyncSnap: true, NoRollback: true}
	want := []string{"create", "mount", "receive or receive:append"}
	checkReq(t, Required(Target, base), want)
	d := base
	d.DeleteTargetSnapshots = true
	checkReq(t, Required(Target, d), append(append([]string{}, want...), "destroy"))
	f := base
	f.ForceDelete = true
	checkReq(t, Required(Target, f), append(append([]string{}, want...), "destroy"))
}

func TestRequiredTargetProps(t *testing.T) {
	got := Required(Target, Needs{
		PreserveRecordsize: true, Props: []string{"compression", "atime"}, UserProps: true, UseHold: true,
	})
	checkReq(t, got, []string{
		"create", "mount", "receive", "destroy", "hold", "release",
		"recordsize", "compression", "atime", "userprop",
	})
}

func TestRequiredDeduplicates(t *testing.T) {
	got := Required(Target, Needs{Props: []string{"mount", "compression", "compression", "destroy"}})
	checkReq(t, got, []string{"create", "mount", "receive", "destroy", "compression"})
}

func TestMissing(t *testing.T) {
	have := map[string]bool{"send:raw": true, "snapshot": true, "mount": true}
	req := Required(Source, Needs{RawSend: true})
	checkReq(t, Missing(have, req), []string{"destroy", "hold"})
	checkReq(t, Missing(map[string]bool{"send": true, "snapshot": true, "mount": true, "destroy": true, "hold": true}, req), nil)
}

func TestGrantCommand(t *testing.T) {
	req := []Requirement{
		{AnyOf: []string{"send"}},
		{AnyOf: []string{"snapshot"}},
		{AnyOf: []string{"mount"}},
		{AnyOf: []string{"destroy"}},
		{AnyOf: []string{"receive", "receive:append"}},
	}
	want := "zfs allow -u backup send,snapshot,mount,destroy,receive tank/data"
	if got := GrantCommand("backup", "tank/data", req); got != want {
		t.Errorf("GrantCommand = %q, want %q", got, want)
	}
	if got := GrantCommand("backup", "tank/data", nil); got != "" {
		t.Errorf("empty GrantCommand = %q, want empty", got)
	}
}
