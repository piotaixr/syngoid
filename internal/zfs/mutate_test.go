package zfs_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/piotaixr/syngoid/internal/zfs"
	"github.com/piotaixr/syngoid/internal/zfs/zfstest"
)

func TestMutateArgv(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		call func(c *zfs.Client) error
		want []string
	}{
		{"snapshot", func(c *zfs.Client) error { return c.Snapshot(ctx, []string{"tank/a@s1", "tank/b@s1"}) }, []string{"zfs", "snapshot", "tank/a@s1", "tank/b@s1"}},
		{"destroy", func(c *zfs.Client) error { return c.Destroy(ctx, "tank/a", []string{"x", "y", "z"}) }, []string{"zfs", "destroy", "tank/a@x,y,z"}},
		{"destroy dataset", func(c *zfs.Client) error { return c.DestroyDataset(ctx, "tank/a", false) }, []string{"zfs", "destroy", "tank/a"}},
		{"destroy dataset recursive", func(c *zfs.Client) error { return c.DestroyDataset(ctx, "tank/a", true) }, []string{"zfs", "destroy", "-r", "tank/a"}},
		{"hold", func(c *zfs.Client) error { return c.Hold(ctx, "syncoid_h", "tank/a@s1") }, []string{"zfs", "hold", "syncoid_h", "tank/a@s1"}},
		{"release", func(c *zfs.Client) error { return c.Release(ctx, "syncoid_h", "tank/a@s1") }, []string{"zfs", "release", "syncoid_h", "tank/a@s1"}},
		{"bookmark", func(c *zfs.Client) error { return c.Bookmark(ctx, "tank/a@s1", "tank/a#s1") }, []string{"zfs", "bookmark", "tank/a@s1", "tank/a#s1"}},
		{"abort resume", func(c *zfs.Client) error { return c.AbortResume(ctx, "tank/a") }, []string{"zfs", "receive", "-A", "tank/a"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := ""
			for i, a := range tt.want {
				if i > 0 {
					key += " "
				}
				key += a
			}
			c, fe := newClient(map[string]zfstest.Response{key: {}})
			if err := tt.call(c); err != nil {
				t.Fatal(err)
			}
			if len(fe.Calls) != 1 || !reflect.DeepEqual(fe.Calls[0], tt.want) {
				t.Errorf("calls = %v, want [%v]", fe.Calls, tt.want)
			}
		})
	}
}

func TestMutateSudoAndError(t *testing.T) {
	fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{
		"sudo -n zfs hold t tank/a@s1": {Exit: 1, Stderr: "dataset does not exist"},
	}}
	c := &zfs.Client{Exec: fe, Sudo: true}
	err := c.Hold(context.Background(), "t", "tank/a@s1")
	var ce *zfs.CommandError
	if !errors.As(err, &ce) || !errors.Is(err, zfs.ErrNoDataset) {
		t.Fatalf("err = %v, want classified CommandError", err)
	}
}

func TestMutateEmptyIsNoop(t *testing.T) {
	c, fe := newClient(nil)
	ctx := context.Background()
	if err := c.Snapshot(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Destroy(ctx, "tank/a", nil); err != nil {
		t.Fatal(err)
	}
	if len(fe.Calls) != 0 {
		t.Errorf("unexpected calls %v", fe.Calls)
	}
}
