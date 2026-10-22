//go:build integration

package integration

import (
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// throughputBytes is the size of the dataset TestITRemoteThroughput sends.
const throughputBytes = 1 << 30

// requireAvailable skips the test unless pool has at least size bytes free.
// Pools created by an older setup-zfs.sh are only 1 GiB.
func requireAvailable(t *testing.T, pool string, size int64) {
	t.Helper()
	avail, err := strconv.ParseInt(zfs(t, "get", "-Hp", "-o", "value", "available", pool), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	if avail < size {
		t.Skipf("pool %s has %d MiB free, need %d MiB: recreate the pools with scripts/ci/setup-zfs.sh (4G images)", pool, avail>>20, size>>20)
	}
}

// baselineSend replicates src@snap into the dataset dst of itUser@localhost
// with the plain pipe `zfs send | ssh sudo -n zfs receive`, and returns how
// long it took.
func baselineSend(t *testing.T, src, snap, dst string) time.Duration {
	t.Helper()
	send := zfsCommand("", "send", src+"@"+snap)
	out, err := send.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	recv := exec.Command("ssh", "-o", "BatchMode=yes", itUser+"@"+remoteHost,
		"sudo", "-n", zfsPath, "receive", dst)
	recv.Dir = "/"
	recv.Stdin = out
	var stderr strings.Builder
	send.Stderr, recv.Stderr = &stderr, &stderr

	start := time.Now()
	if err := recv.Start(); err != nil {
		t.Fatal(err)
	}
	if err := send.Start(); err != nil {
		_ = recv.Process.Kill()
		_ = recv.Wait()
		t.Fatal(err)
	}
	sendErr := send.Wait()
	recvErr := recv.Wait()
	d := time.Since(start)
	if sendErr != nil || recvErr != nil {
		t.Fatalf("baseline zfs send | ssh zfs receive: send: %v, receive: %v: %s", sendErr, recvErr, strings.TrimSpace(stderr.String()))
	}
	return d
}

// TestITRemoteThroughput compares a replication through syngoid's yamux
// session over ssh with a bare `zfs send | ssh zfs receive`, both of 1 GiB of
// incompressible data to itUser@localhost through sudo (spec §14 item 2).
// Loopback and file-backed pools are no 10 GbE link: the test only guards
// against the mux costing a large share of what plain ssh achieves, and logs
// both rates on a THROUGHPUT line to record them.
func TestITRemoteThroughput(t *testing.T) {
	beginRemote(t)
	requireAvailable(t, srcPool, throughputBytes+throughputBytes/8)
	requireAvailable(t, dstPool, 2*throughputBytes+throughputBytes/4)
	src := srcPool + "/t"
	createSource(t, src, throughputBytes)
	zfs(t, "snapshot", src+"@s")

	// Read the stream once beforehand so both runs read it from the same
	// (warm) cache.
	if err := zfsCommand("", "send", src+"@s").Run(); err != nil {
		t.Fatalf("warm-up zfs send: %v", err)
	}

	baselineDst := dstPool + "/b"
	baseline := baselineSend(t, src, "s", baselineDst)
	requireSameGUID(t, src, baselineDst, "s")

	syngoidDst := dstPool + "/t"
	start := time.Now()
	syngoidOK(t, "--compress=none", "--no-sync-snap", "--target-sudo", src, remoteLoc(itUser, syngoidDst))
	elapsed := time.Since(start)
	requireSameGUID(t, src, syngoidDst, "s")
	if g, w := fileSHA256(t, filepath.Join(mountpoint(t, syngoidDst), "data")), fileSHA256(t, filepath.Join(mountpoint(t, src), "data")); g != w {
		t.Fatal("the data differs on the target")
	}

	rate := func(d time.Duration) float64 { return throughputBytes / 1e6 / d.Seconds() }
	t.Logf("THROUGHPUT syngoid=%.1f baseline=%.1f MB/s (1 GiB, loopback; syngoid %s, baseline %s)",
		rate(elapsed), rate(baseline), elapsed.Round(time.Millisecond), baseline.Round(time.Millisecond))
	if rate(elapsed) < rate(baseline)/2 {
		t.Fatalf("syngoid moved %.1f MB/s, below half of the baseline's %.1f MB/s", rate(elapsed), rate(baseline))
	}
}
