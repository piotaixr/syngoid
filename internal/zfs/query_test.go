package zfs_test

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/piotaixr/syngoid/internal/zfs"
	"github.com/piotaixr/syngoid/internal/zfs/zfstest"
)

// fixture reads testdata/name.
func fixture(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

const (
	treeArgs = "zfs list -Hp -o name,type,origin,used,receive_resume_token,syncoid:sync -t filesystem,volume"
	snapHead = "zfs get -Hp -t snapshot -o name,property,value guid,createtxg,creation"
	bmHead   = "zfs get -Hp -t bookmark -o name,property,value guid,createtxg,creation"
	allHead  = "zfs get -Hp"
	allTail  = "-o name,property,value all"
)

func newClient(resp map[string]zfstest.Response) (*zfs.Client, *zfstest.FakeExec) {
	fe := &zfstest.FakeExec{Responses: resp}
	return &zfs.Client{Exec: fe}, fe
}

// TestParseTreeResumeToken covers a dataset with a resume token, which the
// captured tree (TestParseCapturedTree) does not have.
func TestParseTreeResumeToken(t *testing.T) {
	c, _ := newClient(map[string]zfstest.Response{
		treeArgs + " -r tank/data": {Stdout: fixture(t, "tree.txt")},
	})
	got, err := c.ListTree(context.Background(), "tank/data", true)
	if err != nil {
		t.Fatal(err)
	}
	want := []zfs.DatasetInfo{
		{Name: "tank/data", Type: "filesystem", Used: 2097152, ResumeToken: "1-abcdef-token", SyncProp: "false"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
}

func TestParseTreeNameWithSpace(t *testing.T) {
	c, _ := newClient(map[string]zfstest.Response{
		treeArgs + " tank/my data": {Stdout: "tank/my data\tfilesystem\t-\t8192\t-\t-\n"},
	})
	got, err := c.ListTree(context.Background(), "tank/my data", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Name != "tank/my data" {
		t.Fatalf("got %+v", got)
	}
}

func TestParseTreeBadLine(t *testing.T) {
	c, _ := newClient(map[string]zfstest.Response{
		treeArgs + " tank": {Stdout: "tank\tfilesystem\n"},
	})
	_, err := c.ListTree(context.Background(), "tank", false)
	if err == nil || !strings.Contains(err.Error(), `"tank\tfilesystem"`) {
		t.Fatalf("err = %v", err)
	}
}

func TestListTreeNoDataset(t *testing.T) {
	c, _ := newClient(map[string]zfstest.Response{
		treeArgs + " nope": {Exit: 1, Stderr: "cannot open 'nope': dataset does not exist\n"},
	})
	_, err := c.ListTree(context.Background(), "nope", false)
	if !errors.Is(err, zfs.ErrNoDataset) {
		t.Fatalf("err = %v", err)
	}
}

func TestSnapshotTieBreakAcrossDatasetsAndCap(t *testing.T) {
	var sb strings.Builder
	add := func(name string) {
		sb.WriteString(name + "\tguid\t1\n" + name + "\tcreatetxg\t-\n" + name + "\tcreation\t5\n")
	}
	add("a/x@s0")
	add("b/y@s0")
	for i := 0; i < 1005; i++ {
		add("b/y@t" + strings.Repeat("x", i%7) + string(rune('A'+i%26)) + strings.Repeat("0", i/26))
	}
	c, _ := newClient(map[string]zfstest.Response{snapHead + " -r a": {Stdout: sb.String()}})
	got, err := c.ListSnapshots(context.Background(), "a", true)
	if err != nil {
		t.Fatal(err)
	}
	if got["a/x"][0].Creation != 5000 || got["b/y"][0].Creation != 5001 || got["a/x"][0].CreateTxg != 0 {
		t.Fatalf("got %+v %+v", got["a/x"], got["b/y"][0])
	}
	last := got["b/y"]
	if c := last[len(last)-1].Creation; c != 5999 {
		t.Fatalf("cap: last creation = %d, want 5999", c)
	}
}

func TestListSnapshotsFallback(t *testing.T) {
	all := "tank/a\ttype\tfilesystem\n" +
		"tank/a\tguid\t9\n" +
		"tank/a@s1\ttype\tsnapshot\n" +
		"tank/a@s1\tguid\t10\n" +
		"tank/a@s1\tcreatetxg\t5\n" +
		"tank/a@s1\tcreation\t1700000000\n" +
		"tank/a#b1\ttype\tbookmark\n" +
		"tank/a#b1\tguid\t11\n" +
		"tank/a#b1\tcreation\t1700000001\n"
	c, fe := newClient(map[string]zfstest.Response{
		snapHead + " -d 1 tank/a":                {Exit: 2, Stderr: "invalid option 't'\n"},
		allHead + " -d 1 " + allTail + " tank/a": {Stdout: all},
	})
	got, err := c.ListSnapshots(context.Background(), "tank/a", false)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]zfs.Snapshot{"tank/a": {{Name: "s1", GUID: "10", CreateTxg: 5, Creation: 1700000000000}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v", got)
	}
	if len(fe.Calls) != 2 {
		t.Fatalf("calls = %v", fe.Calls)
	}
}

func TestListSnapshotsNoDatasetNoFallback(t *testing.T) {
	c, fe := newClient(map[string]zfstest.Response{
		snapHead + " -r nope": {Exit: 1, Stderr: "cannot open 'nope': dataset does not exist\n"},
	})
	_, err := c.ListSnapshots(context.Background(), "nope", true)
	if !errors.Is(err, zfs.ErrNoDataset) || len(fe.Calls) != 1 {
		t.Fatalf("err = %v calls = %v", err, fe.Calls)
	}
}

// TestListBookmarks lists the captured Linux bookmarks b1 and b2, which carry
// the guid, txg and creation second of the snapshots s1 and s2 they were made
// from.
func TestListBookmarks(t *testing.T) {
	c, _ := newClient(map[string]zfstest.Response{
		bmHead + " -r syngoidfx":   {Stdout: fixture(t, "linux/bookmarks.txt")},
		snapHead + " -r syngoidfx": {Stdout: fixture(t, "linux/snaps.txt")},
	})
	ctx := context.Background()
	got, err := c.ListBookmarks(ctx, "syngoidfx", true)
	if err != nil {
		t.Fatal(err)
	}
	snaps, err := c.ListSnapshots(ctx, "syngoidfx", true)
	if err != nil {
		t.Fatal(err)
	}
	const ds = "syngoidfx/fx/a"
	if len(got) != 1 || len(got[ds]) != 2 {
		t.Fatalf("got %+v", got)
	}
	for i, name := range []string{"b1", "b2"} {
		b, s := got[ds][i], snaps[ds][i]
		if b.Name != name || b.GUID != s.GUID || b.CreateTxg != s.CreateTxg || b.Creation/1000 != s.Creation/1000 {
			t.Errorf("bookmark %+v does not match snapshot %+v", b, s)
		}
	}
}

func TestListBookmarksUnsupported(t *testing.T) {
	c, _ := newClient(map[string]zfstest.Response{
		bmHead + " -d 1 tank": {Exit: 2, Stderr: "cannot get properties: invalid type 'bookmark'\n"},
	})
	got, err := c.ListBookmarks(context.Background(), "tank", false)
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("got %v, %v", got, err)
	}
}

func TestExistsTrue(t *testing.T) {
	c, _ := newClient(map[string]zfstest.Response{
		"zfs list -H -o name tank": {Stdout: "tank\n"},
	})
	ok, err := c.Exists(context.Background(), "tank")
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
}

func TestExistsFalseOnNoDataset(t *testing.T) {
	c, _ := newClient(map[string]zfstest.Response{
		"zfs list -H -o name nope": {Exit: 1, Stderr: "cannot open 'nope': dataset does not exist\n"},
	})
	ok, err := c.Exists(context.Background(), "nope")
	if err != nil || ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
}

func TestExistsErrorOnOther(t *testing.T) {
	c, _ := newClient(map[string]zfstest.Response{
		"zfs list -H -o name tank": {Exit: 1, Stderr: "permission denied\n"},
	})
	ok, err := c.Exists(context.Background(), "tank")
	if err == nil || ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
}

func TestGetPropsAndLocalProps(t *testing.T) {
	c, _ := newClient(map[string]zfstest.Response{
		"zfs get -Hp -o property,value guid,syncoid:sync tank/a": {Stdout: "guid\t42\nsyncoid:sync\t-\n"},
		"zfs get -Hp -s local -o property,value all tank/a":      {Stdout: "compression\tlz4\nsyncoid:sync\tnas,db1\n"},
	})
	got, err := c.GetProps(context.Background(), "tank/a", "guid", "syncoid:sync")
	if err != nil || !reflect.DeepEqual(got, map[string]string{"guid": "42", "syncoid:sync": "-"}) {
		t.Fatalf("got %v, %v", got, err)
	}
	got, err = c.LocalProps(context.Background(), "tank/a")
	if err != nil || !reflect.DeepEqual(got, map[string]string{"compression": "lz4", "syncoid:sync": "nas,db1"}) {
		t.Fatalf("got %v, %v", got, err)
	}
}

func TestQueryArgv(t *testing.T) {
	for _, sudo := range []bool{false, true} {
		for _, rec := range []bool{false, true} {
			fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{}}
			c := &zfs.Client{Exec: fe, Sudo: sudo}
			pre := "zfs"
			if sudo {
				pre = "sudo -n zfs"
			}
			depth, treeDepth := "-d 1 ", ""
			if rec {
				depth, treeDepth = "-r ", "-r "
			}
			ctx := context.Background()
			// Failures are irrelevant; only the issued argv is checked.
			_, _ = c.ListTree(ctx, "tank/a", rec)
			_, _ = c.ListBookmarks(ctx, "tank/a", rec)
			_, _ = c.ListSnapshots(ctx, "tank/a", rec)
			_, _ = c.GetProps(ctx, "tank/a", "guid", "type")
			_, _ = c.LocalProps(ctx, "tank/a")
			_, _ = c.Exists(ctx, "tank/a")
			var got []string
			for _, call := range fe.Calls {
				got = append(got, strings.Join(call, " "))
			}
			want := []string{
				pre + strings.TrimPrefix(treeArgs, "zfs") + " " + treeDepth + "tank/a",
				pre + strings.TrimPrefix(bmHead, "zfs") + " " + depth + "tank/a",
				pre + strings.TrimPrefix(snapHead, "zfs") + " " + depth + "tank/a",
				pre + strings.TrimPrefix(allHead, "zfs") + " " + depth + allTail + " tank/a",
				pre + " get -Hp -o property,value guid,type tank/a",
				pre + " get -Hp -s local -o property,value all tank/a",
				pre + " list -H -o name tank/a",
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("sudo=%v rec=%v\ngot  %q\nwant %q", sudo, rec, got, want)
			}
		}
	}
}
