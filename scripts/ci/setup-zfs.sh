#!/usr/bin/env bash
# Prepares an Ubuntu host for the integration suite (test/integration):
# installs the ZFS tools, creates the file-backed pools syngoid-src and
# syngoid-dst (1 GiB sparse files) and the unprivileged user syngoid-it used
# by the delegation tests. Idempotent. Re-executes itself through sudo.
set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
	exec sudo "$0" "$@"
fi

img_dir=/var/tmp/syngoid-it
pools=(syngoid-src syngoid-dst)
it_user=syngoid-it

export DEBIAN_FRONTEND=noninteractive
if ! command -v zfs >/dev/null 2>&1; then
	apt-get update -q
	apt-get install -y -q zfsutils-linux
fi
if ! modprobe zfs; then
	# Some cloud kernels ship the zfs module in the extra modules package.
	apt-get install -y -q "linux-modules-extra-$(uname -r)"
	modprobe zfs
fi

mkdir -p "$img_dir"
for pool in "${pools[@]}"; do
	if zpool list -H -o name "$pool" >/dev/null 2>&1; then
		echo "pool $pool already exists"
		continue
	fi
	truncate -s 1G "$img_dir/$pool.img"
	zpool create -f "$pool" "$img_dir/$pool.img"
done

if ! id "$it_user" >/dev/null 2>&1; then
	useradd --system --no-create-home --shell /usr/sbin/nologin "$it_user"
fi

zfs version
zpool list "${pools[@]}"
