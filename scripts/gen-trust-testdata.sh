#!/usr/bin/env bash
# Regenerates internal/trust/testdata: two passwordless test key pairs, a
# SHA256SUMS-style file and two signatures of it (trusted comments
# "syngoid 1.2.3" and "syngoid 1.2.2"). Secret keys are discarded.
#
# Needs `minisign` on PATH; run it under mise:
#   mise exec minisign@0.12 -- scripts/gen-trust-testdata.sh
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
out="$root/internal/trust/testdata"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

minisign -G -W -p "$tmp/test.pub" -s "$tmp/test.key" >/dev/null
minisign -G -W -p "$tmp/other.pub" -s "$tmp/other.key" >/dev/null

d1=$(printf 'linux-amd64' | openssl dgst -sha256 -r | cut -d' ' -f1)
d2=$(printf 'freebsd-amd64' | openssl dgst -sha256 -r | cut -d' ' -f1)
printf '%s  syngoid_1.2.3_linux_amd64\n%s  syngoid_1.2.3_freebsd_amd64\n' "$d1" "$d2" >"$tmp/sums.txt"

minisign -S -s "$tmp/test.key" -m "$tmp/sums.txt" -t "syngoid 1.2.3" -x "$tmp/sums.txt.minisig" </dev/null >/dev/null
minisign -S -s "$tmp/test.key" -m "$tmp/sums.txt" -t "syngoid 1.2.2" -x "$tmp/sums-other-version.txt.minisig" </dev/null >/dev/null

mkdir -p "$out"
cp "$tmp/test.pub" "$tmp/other.pub" "$tmp/sums.txt" \
  "$tmp/sums.txt.minisig" "$tmp/sums-other-version.txt.minisig" "$out/"
