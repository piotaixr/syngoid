package engine

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/piotaixr/syngoid/internal/cli"
	"github.com/piotaixr/syngoid/internal/endpoint"
	"github.com/piotaixr/syngoid/internal/endpoint/fake"
	"github.com/piotaixr/syngoid/internal/perms"
	"github.com/piotaixr/syngoid/internal/zfs"
)

// allowFor renders `zfs allow` output granting perms to user on ds, locally
// and to descendents.
func allowFor(ds, user, perms string) string {
	return "---- Permissions on " + ds + " ----------------------------------------\n" +
		"Local+Descendent permissions:\n\tuser " + user + " " + perms + "\n"
}

// pfEndpoint counts Allow calls and can fail GetProps.
type pfEndpoint struct {
	endpoint.Endpoint
	allows   atomic.Int32
	getProps error
}

func (c *pfEndpoint) Allow(ctx context.Context, ds string) (string, error) {
	c.allows.Add(1)
	return c.Endpoint.Allow(ctx, ds)
}

func (c *pfEndpoint) GetProps(ctx context.Context, ds string, props ...string) (map[string]string, error) {
	if c.getProps != nil {
		return nil, c.getProps
	}
	return c.Endpoint.GetProps(ctx, ds, props...)
}

type pfEnv struct {
	sw, tw   *fake.World
	src, dst *fake.Endpoint
	o        *cli.Options
	states   []DatasetState
}

// newPfEnv has tank/data on the source and only the pool backup on the
// target, replicating tank/data to the absent backup/data.
func newPfEnv() *pfEnv {
	e := &pfEnv{sw: fake.NewWorld(), tw: fake.NewWorld(), o: &cli.Options{}}
	e.sw.Create("tank", "")
	e.sw.Create("tank/data", "")
	e.tw.Create("backup", "")
	e.src = fake.New(e.sw, endpoint.HostInfo{OS: "linux", User: "backup", UID: 1000})
	e.dst = fake.New(e.tw, endpoint.HostInfo{OS: "linux", User: "backup", UID: 1000})
	e.o.Source.Dataset, e.o.Target.Dataset = "tank/data", "backup/data"
	e.states = []DatasetState{{Source: "tank/data", Target: "backup/data"}}
	return e
}

func (e *pfEnv) run(t *testing.T, src, dst endpoint.Endpoint) PreflightReport {
	t.Helper()
	r, err := Preflight(context.Background(), src, dst, e.o, e.states)
	if err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	return r
}

func TestPreflightRootSkips(t *testing.T) {
	e := newPfEnv()
	e.src = fake.New(e.sw, endpoint.HostInfo{OS: "linux", User: "root", UID: 0})
	e.dst = fake.New(e.tw, endpoint.HostInfo{OS: "linux", User: "root", UID: 0})
	cs, cd := &pfEndpoint{Endpoint: e.src}, &pfEndpoint{Endpoint: e.dst}
	r := e.run(t, cs, cd)
	if !r.OK() {
		t.Fatalf("not OK: %+v", r)
	}
	if cs.allows.Load()+cd.allows.Load() != 0 {
		t.Errorf("Allow was called for root")
	}
	if !r.Sides[0].Root || !r.Sides[1].Root {
		t.Errorf("Root flags = %v %v", r.Sides[0].Root, r.Sides[1].Root)
	}
}

func TestPreflightSudo(t *testing.T) {
	e := newPfEnv()
	e.o.SourceSudo, e.o.TargetSudo = true, true
	cs, cd := &pfEndpoint{Endpoint: e.src}, &pfEndpoint{Endpoint: e.dst}
	r := e.run(t, cs, cd)
	if !r.OK() || !r.Sides[0].Sudo || !r.Sides[1].Sudo {
		t.Fatalf("report = %+v", r)
	}
	if cs.allows.Load()+cd.allows.Load() != 0 {
		t.Errorf("Allow was called under sudo")
	}

	cd.getProps = &zfs.CommandError{Argv: []string{"sudo", "-n", "zfs"}, ExitCode: 1, Stderr: "sudo: a password is required\n"}
	r = e.run(t, cs, cd)
	if r.OK() {
		t.Fatal("OK despite sudo failure")
	}
	if got := r.Sides[1].SudoErr; got != "sudo: a password is required" {
		t.Errorf("SudoErr = %q", got)
	}
	if r.Sides[0].SudoErr != "" {
		t.Errorf("source SudoErr = %q", r.Sides[0].SudoErr)
	}
	var buf bytes.Buffer
	r.Render(&buf, false)
	want := "CRITICAL ERROR: missing ZFS permissions\n" +
		"  [target]  sudo -n zfs failed: sudo: a password is required\n"
	if buf.String() != want {
		t.Errorf("Render =\n%s\nwant\n%s", buf.String(), want)
	}
}

func TestPreflightMissing(t *testing.T) {
	e := newPfEnv()
	e.src.Faults.Allow = map[string]string{"tank/data": allowFor("tank/data", "backup", "send,snapshot,mount")}
	e.dst.Faults.Allow = map[string]string{"backup": allowFor("backup", "backup", "create,mount,rollback,destroy")}
	r := e.run(t, e.src, e.dst)
	if r.OK() {
		t.Fatal("OK despite missing grants")
	}
	if r.Sides[1].GrantDataset != "backup" || r.Sides[0].GrantDataset != "tank/data" {
		t.Errorf("GrantDataset = %q, %q", r.Sides[0].GrantDataset, r.Sides[1].GrantDataset)
	}
	var buf bytes.Buffer
	r.Render(&buf, false)
	want := `CRITICAL ERROR: missing ZFS permissions
  source tank/data: destroy
  target backup/data: receive
Grant them (as root) with:
  [source]  zfs allow -u backup destroy,mount tank/data
  [target]  zfs allow -u backup receive,create,mount backup
Alternatively, run with --sudo (requires a sudoers rule for zfs).
`
	if buf.String() != want {
		t.Errorf("Render =\n%s\nwant\n%s", buf.String(), want)
	}
}

func TestPreflightOK(t *testing.T) {
	e := newPfEnv()
	e.src.Faults.Allow = map[string]string{"tank/data": allowFor("tank/data", "backup", "send,snapshot,mount,destroy")}
	e.dst.Faults.Allow = map[string]string{"backup": allowFor("backup", "backup", "create,mount,receive,rollback,destroy")}
	r := e.run(t, e.src, e.dst)
	if !r.OK() {
		t.Fatalf("not OK: %+v", r)
	}
	var buf bytes.Buffer
	r.Render(&buf, false)
	if buf.Len() != 0 {
		t.Errorf("Render of OK report = %q", buf.String())
	}
}

func TestPreflightChecksEachDatasetAndAncestorOnce(t *testing.T) {
	e := newPfEnv()
	e.sw.Create("tank/data/a", "")
	e.tw.Create("backup/data", "")
	e.states = []DatasetState{
		{Source: "tank/data", Target: "backup/data", TargetExists: true},
		{Source: "tank/data/a", Target: "backup/data/a"},
		{Source: "tank/data/b", Target: "backup/data/b"},
	}
	cd := &pfEndpoint{Endpoint: e.dst}
	srcAllow := allowFor("tank/data", "backup", "send,snapshot,mount,destroy")
	e.src.Faults.Allow = map[string]string{"tank/data": srcAllow, "tank/data/a": srcAllow, "tank/data/b": srcAllow}
	e.dst.Faults.Allow = map[string]string{"backup/data": allowFor("backup/data", "backup", "create,mount,receive,rollback,destroy")}
	r := e.run(t, e.src, cd)
	if !r.OK() {
		t.Fatalf("not OK: %+v", r)
	}
	// backup/data exists and is checked once; the absent children both fall
	// back to it.
	if n := cd.allows.Load(); n != 1 {
		t.Errorf("target Allow calls = %d, want 1", n)
	}
}

func TestPreflightPreserveProps(t *testing.T) {
	e := newPfEnv()
	e.o.PreserveProperties = true
	e.sw.SetProp("tank/data", "compression", "lz4")
	e.sw.SetProp("tank/data", "atime", "off")
	e.sw.SetProp("tank/data", "guid", "123")
	e.dst.Faults.Allow = map[string]string{"backup": allowFor("backup", "backup", "create,mount,receive,rollback,destroy")}
	e.src.Faults.Allow = map[string]string{"tank/data": allowFor("tank/data", "backup", "send,snapshot,mount,destroy")}
	r := e.run(t, e.src, e.dst)
	if r.OK() {
		t.Fatal("OK despite missing property grants")
	}
	var got []string
	for _, m := range r.Sides[1].Missing[0].Missing {
		got = append(got, m.String())
	}
	if want := []string{"atime", "compression"}; !reflect.DeepEqual(got, want) {
		t.Errorf("missing = %v, want %v", got, want)
	}
	if !strings.Contains(grantNames(r.Sides[1].Grant), "atime,compression") || strings.Contains(grantNames(r.Sides[1].Grant), "guid") {
		t.Errorf("Grant = %q", grantNames(r.Sides[1].Grant))
	}
}

func grantNames(reqs []perms.Requirement) string {
	return strings.TrimPrefix(perms.GrantCommand("u", "d", reqs), "zfs allow -u u ")
}

func TestPreflightPreservePropsUserAndUnprivLinux(t *testing.T) {
	e := newPfEnv()
	e.o.PreserveProperties = true
	e.sw.SetProp("tank/data", "mountpoint", "/x")
	e.sw.SetProp("tank/data", "canmount", "on")
	e.sw.SetProp("tank/data", "compression", "lz4")
	e.sw.SetProp("tank/data", "org:note", "hi")
	e.dst.Faults.Allow = map[string]string{"backup": allowFor("backup", "backup", "create,mount,receive,rollback,destroy")}
	e.src.Faults.Allow = map[string]string{"tank/data": allowFor("tank/data", "backup", "send,snapshot,mount,destroy")}
	r := e.run(t, e.src, e.dst)
	var got []string
	for _, m := range r.Sides[1].Missing[0].Missing {
		got = append(got, m.String())
	}
	if want := []string{"compression", "userprop"}; !reflect.DeepEqual(got, want) {
		t.Errorf("missing = %v, want %v", got, want)
	}
}

func TestNeedsFrom(t *testing.T) {
	o := &cli.Options{
		NoSyncSnap: true, UseHold: true, NoRollback: true, PreserveRecordsize: true,
		SendOpts: []zfs.Opt{{Flag: 'w'}},
		RecvOpts: []zfs.Opt{{Flag: 'o', Value: "compression=off"}, {Flag: 'u'}, {Flag: 'o', Value: "atime=off"}, {Flag: 'o', Value: "compression=on"}},
	}
	n := NeedsFrom(o, []string{"atime", "quota"}, true)
	want := perms.Needs{
		NoSyncSnap: true, UseHold: true, NoRollback: true, PreserveRecordsize: true, RawSend: true,
		Props: []string{"atime", "quota", "compression"}, UserProps: true,
	}
	if !reflect.DeepEqual(n, want) {
		t.Errorf("NeedsFrom = %+v, want %+v", n, want)
	}
}

func TestPreflightRenderFull(t *testing.T) {
	e := newPfEnv()
	e.src.Faults.Allow = map[string]string{"tank/data": allowFor("tank/data", "backup", "send,snapshot,mount,destroy")}
	e.dst.Faults.Allow = map[string]string{"backup": allowFor("backup", "backup", "create,mount,receive,rollback,destroy")}
	r := e.run(t, e.src, e.dst)
	var buf bytes.Buffer
	r.Render(&buf, true)
	want := `Full minimal grants:
  [source]  zfs allow -u backup send,snapshot,mount,destroy tank/data
  [target]  zfs allow -u backup create,mount,receive,rollback,destroy backup
`
	if buf.String() != want {
		t.Errorf("Render =\n%s\nwant\n%s", buf.String(), want)
	}

	e.o.SourceSudo = true
	r = e.run(t, e.src, e.dst)
	buf.Reset()
	r.Render(&buf, true)
	if strings.Contains(buf.String(), "[source]") {
		t.Errorf("sudo side listed in full grants:\n%s", buf.String())
	}
}

func TestPreflightMissingFullAndLabels(t *testing.T) {
	e := newPfEnv()
	e.dst.LabelValue = "backup@nas"
	r := e.run(t, e.src, e.dst)
	var buf bytes.Buffer
	r.Render(&buf, true)
	out := buf.String()
	for _, want := range []string{
		"  target backup/data: create, mount, receive, rollback, destroy\n",
		"  [backup@nas]  zfs allow -u backup create,mount,receive,rollback,destroy backup\n",
		"Full minimal grants:\n  [source]  zfs allow -u backup send,snapshot,mount,destroy tank/data\n  [backup@nas]  zfs allow",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in\n%s", want, out)
		}
	}
}
