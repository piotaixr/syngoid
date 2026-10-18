package zfs_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/piotaixr/syngoid/internal/zfs"
	"github.com/piotaixr/syngoid/internal/zfs/zfstest"
)

func TestClientSudoPrefix(t *testing.T) {
	if got, want := (&zfs.Client{Sudo: true}).Argv("list", "-H"), []string{"sudo", "-n", "zfs", "list", "-H"}; !reflect.DeepEqual(got, want) {
		t.Errorf("sudo argv = %v, want %v", got, want)
	}
	if got, want := (&zfs.Client{}).Argv("list", "-H"), []string{"zfs", "list", "-H"}; !reflect.DeepEqual(got, want) {
		t.Errorf("argv = %v, want %v", got, want)
	}
}

func TestClientOutput(t *testing.T) {
	fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{
		"zfs list -H":     {Stdout: "tank/a\n"},
		"zfs list -H bad": {Stderr: "cannot open 'bad': dataset does not exist\n", Exit: 1},
	}}
	c := &zfs.Client{Exec: fe}
	out, err := c.Output(context.Background(), "list", "-H")
	if err != nil || out != "tank/a\n" {
		t.Fatalf("out=%q err=%v", out, err)
	}
	_, err = c.Output(context.Background(), "list", "-H", "bad")
	var ce *zfs.CommandError
	if !errors.As(err, &ce) || !errors.Is(err, zfs.ErrNoDataset) {
		t.Fatalf("err = %v", err)
	}
	if len(fe.Calls) != 2 {
		t.Errorf("calls = %v", fe.Calls)
	}
	if _, err := c.Output(context.Background(), "unknown"); err == nil {
		t.Error("unknown argv should error")
	}
}

func TestFakeExecBlockAndStdout(t *testing.T) {
	fe := &zfstest.FakeExec{Responses: map[string]zfstest.Response{"zfs hang": {Block: true}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := fe.Run(ctx, zfs.Cmd{Argv: []string{"zfs", "hang"}})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v", err)
	}
}
