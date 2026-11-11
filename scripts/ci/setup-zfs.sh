#!/usr/bin/env bash
# Prepares an Ubuntu host for the integration suite (test/integration):
# installs the ZFS tools, creates the file-backed pools syngoid-src and
# syngoid-dst (4 GiB sparse files) and the unprivileged user syngoid-it used
# by the delegation tests, and installs Perl syncoid 2.2.0 (/usr/local/bin/syncoid)
# for the interoperability tests. For the remote tests it also sets up
# key-based ssh from root to syngoid-it@localhost (known_hosts included, no
# passphrase), and lets syngoid-it run zfs through `sudo -n`
# (/etc/sudoers.d/syngoid-it).
#
# The remote tests need an sshd on localhost:22. This script installs and
# starts one (openssh-server, enabled at boot, listening on every interface,
# with the distribution's defaults, which allow password logins) only under
# GitHub Actions or when SYNGOID_IT_SSHD=1 is set; on any other machine it
# requires an sshd that already listens on localhost:22.
# Idempotent. Re-executes itself through sudo.
set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
	# sudo resets the environment; pass the sshd opt-in through.
	exec sudo env "GITHUB_ACTIONS=${GITHUB_ACTIONS:-}" "SYNGOID_IT_SSHD=${SYNGOID_IT_SSHD:-}" "$0" "$@"
fi

img_dir=/var/tmp/syngoid-it
pools=(syngoid-src syngoid-dst)
it_user=syngoid-it

export DEBIAN_FRONTEND=noninteractive

# GitHub's Ubuntu runners fetch packages through a mirror list
# (mirror+file:/etc/apt/apt-mirrors.txt) that names only ubuntu.com hosts, so
# an ubuntu.com outage fails the whole job. Keep the listed mirrors whose host
# resolves and add independent full mirrors after them. Packages stay verified
# against the Ubuntu archive key whichever mirror serves them.
mirror_list=/etc/apt/apt-mirrors.txt
fallback_mirrors=(
	https://mirrors.edge.kernel.org/ubuntu/
	https://mirror.us.leaseweb.net/ubuntu/
	https://ftp.halifax.rwth-aachen.de/ubuntu/
)
if [ -f "$mirror_list" ] &&
	grep -rqsF "mirror+file:$mirror_list" /etc/apt/sources.list /etc/apt/sources.list.d; then
	new_list=$(mktemp)
	prio=0
	while IFS= read -r line; do
		url=${line%%[[:space:]]*}
		host=${url#*://}
		host=${host%%/*}
		case " ${fallback_mirrors[*]} " in *" $url "*) continue ;; esac
		if [ -n "$host" ] && getent hosts "$host" >/dev/null; then
			prio=$((prio + 1))
			printf '%s\tpriority:%d\n' "$url" "$prio" >>"$new_list"
		else
			echo "apt mirror $url does not resolve; leaving it out" >&2
		fi
	done <"$mirror_list"
	for url in "${fallback_mirrors[@]}"; do
		prio=$((prio + 1))
		printf '%s\tpriority:%d\n' "$url" "$prio" >>"$new_list"
	done
	install -m 0644 "$new_list" "$mirror_list"
	rm -f "$new_list"
	cat "$mirror_list"
fi
apt_opts=(-q -o Acquire::Retries=3)

# Refresh the package lists once, before any install: a stale image index
# makes apt-get install fail with 404s.
apt-get "${apt_opts[@]}" update
if ! command -v zfs >/dev/null 2>&1; then
	apt-get "${apt_opts[@]}" install -y zfsutils-linux
fi
if ! modprobe zfs; then
	# Some cloud kernels ship the zfs module in the extra modules package.
	apt-get "${apt_opts[@]}" install -y "linux-modules-extra-$(uname -r)"
	modprobe zfs
fi

# Perl syncoid 2.2.0, for the compat tests. It needs Capture::Tiny; its default
# lzo compression (lzop) is not used, the tests pass --compress=none.
# The download is pinned by its sha256 and only installed when it matches.
syncoid=/usr/local/bin/syncoid
syncoid_url=https://raw.githubusercontent.com/jimsalterjrs/sanoid/v2.2.0/syncoid
syncoid_sha256=5f2965b9b48165df3f507f1b473c8fe74a5ba088a59d412c7d2c746969dfa1e5
if [ ! -x "$syncoid" ]; then
	apt-get "${apt_opts[@]}" install -y libcapture-tiny-perl curl
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
	truncate -s 4G "$img_dir/$pool.img"
	zpool create -f "$pool" "$img_dir/$pool.img"
done

# syngoid-it is also an ssh login: it needs a home (the remote tests push the
# agent to ~/.cache/syngoid), a real shell and an account sshd does not treat
# as locked ("*" is not a valid hash, so no password login is possible).
it_home=/home/$it_user
if ! id "$it_user" >/dev/null 2>&1; then
	useradd --system --create-home --home-dir "$it_home" --shell /bin/bash "$it_user"
fi
if [ "$(getent passwd "$it_user" | cut -d: -f7)" != /bin/bash ] ||
	[ "$(getent passwd "$it_user" | cut -d: -f6)" != "$it_home" ]; then
	# An account created by an older version of this script.
	usermod --home "$it_home" --shell /bin/bash "$it_user"
fi
usermod --password '*' "$it_user"
install -d -m 0750 -o "$it_user" -g "$(id -gn "$it_user")" "$it_home"

# sshd on localhost for the remote tests. Installing and starting a server is
# only done where that is expected: CI, or an explicit opt-in.
if [ "${GITHUB_ACTIONS:-}" = true ] || [ "${SYNGOID_IT_SSHD:-}" = 1 ]; then
	if [ ! -x /usr/sbin/sshd ] || ! command -v ssh-keyscan >/dev/null 2>&1; then
		apt-get "${apt_opts[@]}" install -y openssh-server openssh-client
	fi
	ssh-keygen -A
	mkdir -p /run/sshd
	if ! { systemctl start ssh || service ssh start; } >/dev/null 2>&1; then
		/usr/sbin/sshd
	fi
fi
sshd_up=
if command -v ssh-keyscan >/dev/null 2>&1; then
	for _ in $(seq 20); do
		if [ -n "$(ssh-keyscan -T 2 localhost 2>/dev/null)" ]; then
			sshd_up=1
			break
		fi
		sleep 1
	done
fi
if [ -z "$sshd_up" ]; then
	cat >&2 <<'MSG'
error: the remote integration tests need an ssh server on localhost:22 (and the
ssh client tools), and none answers.

Either start an sshd yourself and rerun this script, or let the script install
and start one by running it with SYNGOID_IT_SSHD=1:

    SYNGOID_IT_SSHD=1 scripts/ci/setup-zfs.sh

That installs openssh-server, enables it at boot and starts it on every network
interface with the distribution's default configuration, which allows password
logins for every account. Use it only on a throwaway machine, or firewall port
22. Under GitHub Actions the script does this on its own.
MSG
	exit 1
fi

# Root's key logs in as syngoid-it; the initiator runs as root.
install -d -m 0700 /root/.ssh
if [ ! -f /root/.ssh/id_ed25519 ]; then
	ssh-keygen -q -t ed25519 -N '' -f /root/.ssh/id_ed25519
fi
pubkey=$(ssh-keygen -y -f /root/.ssh/id_ed25519)
authorize() { # authorize FILE OWNER: append the public key unless present
	local file=$1 owner=$2
	install -d -m 0700 -o "$owner" -g "$(id -gn "$owner")" "$(dirname "$file")"
	touch "$file"
	if ! grep -qxF "$pubkey" "$file"; then
		echo "$pubkey" >>"$file"
	fi
	chown "$owner:$(id -gn "$owner")" "$file"
	chmod 0600 "$file"
}
authorize "$it_home/.ssh/authorized_keys" "$it_user"
touch /root/.ssh/known_hosts
if ! ssh-keygen -F localhost -f /root/.ssh/known_hosts >/dev/null; then
	ssh-keyscan -H localhost >>/root/.ssh/known_hosts
fi

# The --sudo tests: syngoid-it may run exactly the zfs binary as root.
zfs_bin=$(command -v zfs)
sudo_rule="$it_user ALL=(root) NOPASSWD: $zfs_bin"
zfs_real=$(readlink -f "$zfs_bin")
if [ "$zfs_real" != "$zfs_bin" ]; then
	sudo_rule="$sudo_rule, $zfs_real"
fi
sudoers_tmp=$(mktemp)
echo "$sudo_rule" >"$sudoers_tmp"
if ! visudo -cf "$sudoers_tmp" ||
	! install -m 0440 -o root -g root "$sudoers_tmp" "/etc/sudoers.d/$it_user"; then
	rm -f "$sudoers_tmp"
	exit 1
fi
rm -f "$sudoers_tmp"

# Fail here, with a clear message, rather than in every remote test.
ssh -o BatchMode=yes -o ConnectTimeout=10 "$it_user@localhost" true
sudo -n -u "$it_user" -- sudo -n "$zfs_bin" version >/dev/null

zfs version
zpool list "${pools[@]}"
