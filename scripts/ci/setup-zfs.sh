#!/usr/bin/env bash
# Prepares an Ubuntu host for the integration suite (test/integration):
# installs the ZFS tools, creates the file-backed pools syngoid-src and
# syngoid-dst (1 GiB sparse files) and the unprivileged user syngoid-it used
# by the delegation tests, and installs Perl syncoid 2.2.0 (/usr/local/bin/syncoid)
# for the interoperability tests. Idempotent. Re-executes itself through sudo.
set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
	exec sudo "$0" "$@"
fi

img_dir=/var/tmp/syngoid-it
pools=(syngoid-src syngoid-dst)
it_user=syngoid-it

export DEBIAN_FRONTEND=noninteractive
# Refresh the package lists once, before any install: a stale image index
# makes apt-get install fail with 404s.
apt-get update -q
if ! command -v zfs >/dev/null 2>&1; then
	apt-get install -y -q zfsutils-linux
fi
if ! modprobe zfs; then
	# Some cloud kernels ship the zfs module in the extra modules package.
	apt-get install -y -q "linux-modules-extra-$(uname -r)"
	modprobe zfs
fi

# Perl syncoid 2.2.0, for the compat tests. It needs Capture::Tiny; its default
# lzo compression (lzop) is not used, the tests pass --compress=none.
# The download is pinned by its sha256 and only installed when it matches.
syncoid=/usr/local/bin/syncoid
syncoid_url=https://raw.githubusercontent.com/jimsalterjrs/sanoid/v2.2.0/syncoid
syncoid_sha256=5f2965b9b48165df3f507f1b473c8fe74a5ba088a59d412c7d2c746969dfa1e5
if [ ! -x "$syncoid" ]; then
	apt-get install -y -q libcapture-tiny-perl curl
	tmp=$(mktemp -d)
	trap 'rm -rf "$tmp"' EXIT
	curl -fsSL -o "$tmp/syncoid" "$syncoid_url"
	echo "$syncoid_sha256  $tmp/syncoid" | sha256sum -c -
	install -m 0755 "$tmp/syncoid" "$syncoid"
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
