#!/bin/sh
# Captures real zfs, zfs allow and ps output for the parser tests (spec 14.3,
# 14.4). Usage: capture-fixtures.sh OUTDIR     (as root; Linux, FreeBSD, illumos)
#
# It creates the throw-away pool "syngoidfx" on a 256 MiB file vdev (mounted
# under a temporary altroot, never on /), a user and a group for the grant
# cases, builds the datasets described below, and writes into OUTDIR the
# output of exactly the argv syngoid runs:
#
#   tree.txt               zfs list -Hp -o name,type,origin,used,receive_resume_token,syncoid:sync -t filesystem,volume -r syngoidfx
#   tree_single.txt        the same without -r, on "syngoidfx/fx/my data"
#   snaps.txt              zfs get -Hp -r -t snapshot -o name,property,value guid,createtxg,creation syngoidfx
#   snaps_d1.txt           the same with -d 1 instead of -r, on syngoidfx/fx/a
#   snaps_all.txt          zfs get -Hp -r -o name,property,value all syngoidfx   (the ListSnapshots fallback)
#   bookmarks.txt          zfs get -Hp -r -t bookmark ... syngoidfx
#   props.txt, props_local.txt   GetProps (recordsize) and LocalProps of syngoidfx/fx/a
#   allow_fx_a.txt, allow_fx_a_c.txt   zfs allow on syngoidfx/fx/a and its clone syngoidfx/fx/a/c
#   estimate_full.txt, estimate_inc.txt, estimate_inc_I.txt, estimate_resume.txt
#                          zfs send -nvP, -i, -I and -t
#   stderr_*.txt           stderr of the failing receives and sends (partial state,
#                          destination exists, no dataset, resume source gone)
#   ps_ww.txt, ps_A.txt, ps_e.txt
#                          ps -wwAo args=, ps -Ao args= and ps -eo args while a
#                          process shows "... zfs receive -s syngoidfx/busy/..."
#                          (a 218-character target) in its argument list; on
#                          illumos ps_ww fails (no -w), which is itself the fixture
#   grants.txt             the zfs allow commands that were run
#   commands.txt           every capture with its exit status
#   manifest.txt           uname -a and the zfs version
#
# stdout of a capture goes to its file, non-empty stderr to "<file>.stderr".
# A failing capture never aborts the run; its exit status is in commands.txt.
# POSIX sh only: it runs under dash (Ubuntu), /bin/sh (FreeBSD) and ksh93
# (OmniOS). The pool, the user, the group and the temp dir are removed on exit.

set -eu

if [ $# -ne 1 ]; then
	echo "usage: $0 OUTDIR" >&2
	exit 2
fi
if [ "$(id -u)" -ne 0 ]; then
	echo "$0: must run as root" >&2
	exit 2
fi

mkdir -p "$1"
out=$(cd "$1" && pwd)
# Never mix fresh captures with stale files.
if [ -n "$(ls -A "$out")" ]; then
	echo "$0: $out is not empty" >&2
	exit 2
fi

# zfs prints locale-dependent text; syngoid runs it with LC_ALL=C.
LC_ALL=C
export LC_ALL
PATH="$PATH:/sbin:/usr/sbin:/usr/local/sbin:/bin:/usr/bin"
export PATH

os=$(uname -s)
pool=syngoidfx
usr=sgxuser
grp=sgxgrp
fx="$pool/fx"

# Kept short: the fake receive process below carries this path in its args,
# and only its long target name should make that ps line long.
work=$(mktemp -d /var/tmp/sgx.XXXXXX)
alt="$work/alt"
img="$work/vdev"
made_pool=
made_grp=
made_usr=
fake_pid=

cleanup() {
	set +e
	trap '' HUP INT TERM
	[ -z "$fake_pid" ] || kill "$fake_pid" 2>/dev/null
	[ -z "$made_pool" ] || zpool destroy -f "$pool" >/dev/null 2>&1
	if [ -n "$made_usr" ]; then
		case $os in
		FreeBSD) pw userdel "$usr" >/dev/null 2>&1 ;;
		*) userdel "$usr" >/dev/null 2>&1 ;;
		esac
	fi
	if [ -n "$made_grp" ]; then
		case $os in
		FreeBSD) pw groupdel "$grp" >/dev/null 2>&1 ;;
		*) groupdel "$grp" >/dev/null 2>&1 ;;
		esac
	fi
	rm -rf "$work"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

# cap NAME CMD...: stdout to NAME, non-empty stderr to NAME.stderr, status to
# commands.txt. Never fails.
cap() {
	_n=$1
	shift
	if "$@" >"$out/$_n" 2>"$out/$_n.stderr"; then _rc=0; else _rc=$?; fi
	[ -s "$out/$_n.stderr" ] || rm -f "$out/$_n.stderr"
	echo "$_n rc=$_rc: $*" >>"$out/commands.txt"
}

# errcap NAME CMD...: stderr to NAME, stdout dropped, status to commands.txt.
# Never fails.
errcap() {
	_n=$1
	shift
	if "$@" >/dev/null 2>"$out/$_n"; then _rc=0; else _rc=$?; fi
	echo "$_n rc=$_rc: $*" >>"$out/commands.txt"
}

# grant ARGS...: zfs allow ARGS, logged in grants.txt. Never fails.
grant() {
	if zfs allow "$@" >/dev/null 2>&1; then _rc=0; else _rc=$?; fi
	echo "zfs allow $* (rc=$_rc)" >>"$out/grants.txt"
}

# Receives that end in the state under test. The pipelines return the status
# of zfs receive; the sends' own stderr is not part of the capture.
recv_truncated() { # STREAMFILE DEST: leave a resumable partial receive in DEST
	# Cut a file, not a pipe: dd counts reads, and a pipe returns short ones.
	dd if="$1" bs=65536 count=16 2>/dev/null | zfs receive -s "$2" >/dev/null 2>&1
}
recv_full() { # SNAP DEST FLAGS...
	_snap=$1
	_dst=$2
	shift 2
	zfs send "$_snap" 2>/dev/null | zfs receive "$@" "$_dst"
}
recv_resume() { # TOKEN DEST
	zfs send -t "$1" 2>/dev/null | zfs receive -s "$2"
}
recv_inc() { # FROM TO DEST FLAGS...
	_from=$1
	_to=$2
	_dst=$3
	shift 3
	zfs send -i "$_from" "$_to" 2>/dev/null | zfs receive "$@" "$_dst"
}

: >"$out/commands.txt"
: >"$out/grants.txt"

# --- pool, user, group ------------------------------------------------------

case $os in
SunOS) mkfile -n 256m "$img" ;;
*) truncate -s 256M "$img" ;;
esac
zpool create -f -R "$alt" -O compression=off -O atime=off "$pool" "$img"
made_pool=1

case $os in
FreeBSD)
	pw groupadd "$grp"
	made_grp=1
	pw useradd "$usr" -g "$grp" -d /nonexistent -s /usr/sbin/nologin
	made_usr=1
	;;
SunOS)
	groupadd "$grp"
	made_grp=1
	useradd -g "$grp" -d /nonexistent -s /bin/false "$usr"
	made_usr=1
	;;
*)
	groupadd "$grp"
	made_grp=1
	useradd -M -g "$grp" -d /nonexistent -s /usr/sbin/nologin "$usr"
	made_usr=1
	;;
esac

# --- datasets ---------------------------------------------------------------

zfs create "$fx"
zfs create "$fx/a"
dd if=/dev/urandom of="$alt/$fx/a/file1" bs=1024 count=4096 2>/dev/null
zfs snapshot "$fx/a@s1"
sleep 1
dd if=/dev/urandom of="$alt/$fx/a/file2" bs=1024 count=1024 2>/dev/null
# s2 and s3 want the same creation second (the tie-break fixture). zfs refuses
# two snapshots of one filesystem in a single command, so take them back to
# back and retake both when a second boundary fell between them. Not fatal if
# it never works out.
same_second=no
n=0
while [ "$n" -lt 10 ]; do
	n=$((n + 1))
	zfs snapshot "$fx/a@s2"
	zfs snapshot "$fx/a@s3"
	c2=$(zfs get -Hp -o value creation "$fx/a@s2")
	c3=$(zfs get -Hp -o value creation "$fx/a@s3")
	if [ "$c2" = "$c3" ]; then
		same_second=yes
		break
	fi
	[ "$n" -lt 10 ] || break # keep the last pair
	zfs destroy "$fx/a@s3"
	zfs destroy "$fx/a@s2"
done
echo "same-second snapshot pair fx/a@s2, fx/a@s3: $same_second (attempts: $n)" >>"$out/commands.txt"
zfs clone -o syncoid:sync=nas,db1 "$fx/a@s1" "$fx/a/c"
zfs bookmark "$fx/a@s1" "$fx/a#b1"
zfs bookmark "$fx/a@s2" "$fx/a#b2"
zfs create -s -V 8m "$fx/vol"
zfs snapshot "$fx/vol@syncoid_fxhost_2026-10-18:10:00:00-GMT02:00"
zfs create -o syncoid:sync=false "$fx/nosync"
zfs create "$fx/my data"
zfs snapshot "$fx/my data@m1"

# --- grants -----------------------------------------------------------------

# Pool level: a permission set and a group grant, inherited by everything below.
grant -s @poolset create,mount "$pool"
grant -g "$grp" mount "$pool"
# fx/a: one case per kind of grantee and scope.
grant -u "$usr" snapshot,hold "$fx/a"
grant -g "$grp" send "$fx/a"
grant -e bookmark "$fx/a"
grant -s @set1 create,mount,receive "$fx/a"
grant -l -u "$usr" release "$fx/a"
grant -d -g "$grp" destroy "$fx/a"
grant -l -d -u "$usr" @set1 "$fx/a"
grant -c create,mount,snapshot "$fx/a"
# The clone gets its own local grant on top of what it inherits.
grant -l -u "$usr" rename "$fx/a/c"

# --- listings ---------------------------------------------------------------

cap tree.txt zfs list -Hp -o name,type,origin,used,receive_resume_token,syncoid:sync -t filesystem,volume -r "$pool"
cap tree_single.txt zfs list -Hp -o name,type,origin,used,receive_resume_token,syncoid:sync -t filesystem,volume "$fx/my data"
cap snaps.txt zfs get -Hp -r -t snapshot -o name,property,value guid,createtxg,creation "$pool"
cap snaps_d1.txt zfs get -Hp -d 1 -t snapshot -o name,property,value guid,createtxg,creation "$fx/a"
cap snaps_all.txt zfs get -Hp -r -o name,property,value all "$pool"
cap bookmarks.txt zfs get -Hp -r -t bookmark -o name,property,value guid,createtxg,creation "$pool"
cap props.txt zfs get -Hp -o property,value recordsize "$fx/a"
cap props_local.txt zfs get -Hp -s local -o property,value all "$fx/a"
cap allow_fx_a.txt zfs allow "$fx/a"
cap allow_fx_a_c.txt zfs allow "$fx/a/c"
cap estimate_full.txt zfs send -nvP "$fx/a@s1"
cap estimate_inc.txt zfs send -nvP -i "$fx/a@s1" "$fx/a@s2"
cap estimate_inc_I.txt zfs send -nvP -I "$fx/a@s1" "$fx/a@s3"

# --- failing receives and sends ---------------------------------------------

# A source with enough data to cut in the middle, and a resumable partial
# receive of it.
zfs create "$fx/r"
dd if=/dev/urandom of="$alt/$fx/r/file" bs=1024 count=4096 2>/dev/null
zfs snapshot "$fx/r@r1"
zfs send "$fx/r@r1" >"$work/r1.stream"
recv_truncated "$work/r1.stream" "$pool/rx" || true
token=$(zfs get -Hp -o value receive_resume_token "$pool/rx" 2>/dev/null || echo -)
if [ -z "$token" ] || [ "$token" = - ]; then
	echo "$0: the truncated receive left no resume token; the resume fixtures would be wrong" >&2
	exit 1
fi

cap estimate_resume.txt zfs send -nvP -t "$token"
errcap stderr_partial_state.txt recv_full "$fx/r@r1" "$pool/rx" -s -F
errcap stderr_partial_state_noF.txt recv_full "$fx/r@r1" "$pool/rx" -s

# The destination already holds the snapshot being received (fx/r@r1, the
# one the token resumes): a resumed receive and two plain ones. Which of them
# produces "cannot restore to ...: destination already exists" is what the
# captured files show.
recv_full "$fx/r@r1" "$pool/exist" >/dev/null 2>&1
errcap stderr_dest_exists.txt recv_resume "$token" "$pool/exist"
errcap stderr_dest_exists_plain.txt recv_full "$fx/r@r1" "$pool/exist" -s -F
errcap stderr_dest_exists_noF.txt recv_full "$fx/r@r1" "$pool/exist" -s
# None of the three above says "cannot restore to X: destination already
# exists" (run 39853662415). That is libzfs' report of a receive the kernel
# refuses because the destination already has the target snapshot name: here
# an incremental s1..s2 onto a copy of s1 that has its own @s2.
recv_full "$fx/a@s1" "$pool/conflict" >/dev/null 2>&1
zfs snapshot "$pool/conflict@s2"
errcap stderr_dest_exists_snap.txt recv_inc "$fx/a@s1" "$fx/a@s2" "$pool/conflict" -s -F

errcap stderr_no_dataset.txt zfs list -H -o name "$pool/nope"
errcap stderr_no_dataset_get.txt zfs get -Hp -r -t snapshot -o name,property,value guid,createtxg,creation "$pool/nope"
errcap stderr_recv_no_parent.txt recv_full "$fx/a@s1" "$pool/nope/child" -s -F

# Resume after the source snapshot is gone. zfs send -t falls back to any
# snapshot carrying the token's guid, so the received copies of fx/r@r1 go
# first (syngoidfx/exist; on illumos the -F receive into syngoidfx/rx
# completed too).
zfs destroy -r "$pool/exist"
zfs destroy -r "$pool/rx"
zfs destroy "$fx/r@r1"
errcap stderr_resume_source_gone.txt zfs send -t "$token"

# --- ps ---------------------------------------------------------------------

# A process whose argument list ends in "zfs receive -s <long target>". It is
# a shell script named zfs (so there is no way to rename a binary portably),
# run by path, hence ps shows "/bin/sh <work>/b/zfs receive -s <target>" (the
# interpreter first, plus the script's own "sleep" child). The target is a
# valid but nonexistent 218-character dataset name, longer than FreeBSD's
# 79-column ps line without -ww and illumos' 80-byte args: the fixtures show
# where each ps form cuts a long receive line. Fixtures of this shape only
# prove the ps output format, header line, width and quoting; matching a
# receive is tested over the captured line.
busy="$pool/busy"
for _c in 1 2 3 4 5 6; do
	busy="$busy/component-$_c-of-a-long-target-name"
done
mkdir "$work/b"
printf '#!/bin/sh\nsleep 5\n' >"$work/b/zfs"
chmod 755 "$work/b/zfs"
"$work/b/zfs" receive -s "$busy" &
fake_pid=$!
sleep 1 # let the forked shell exec the script
cap ps_ww.txt ps -wwAo args=
cap ps_A.txt ps -Ao args=
cap ps_e.txt ps -eo args
kill "$fake_pid" 2>/dev/null || true
wait "$fake_pid" 2>/dev/null || true
fake_pid=

# --- manifest ---------------------------------------------------------------

{
	uname -a
	if zfs version >/dev/null 2>&1; then
		zfs version
	else
		zfs upgrade -v 2>&1 | head -1
	fi
} >"$out/manifest.txt" 2>&1

echo "fixtures written to $out"
