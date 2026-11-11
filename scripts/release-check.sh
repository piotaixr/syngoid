#!/usr/bin/env bash
# Builds a signed GoReleaser snapshot into dist/ with a throwaway
# password-protected minisign key, then verifies it with syngoid's own
# verifier (TestReleaseCheck in internal/trust). The password is piped to
# minisign, so no terminal is needed.
#
# Needs go, goreleaser and minisign on PATH; run it as `mise run release-check`.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

pw=$(openssl rand -hex 32)
printf '%s\n%s\n' "$pw" "$pw" | minisign -G -p "$tmp/k.pub" -s "$tmp/k.key"

MINISIGN_KEY_FILE="$tmp/k.key" MINISIGN_PASSWORD="$pw" goreleaser release --snapshot --clean

SYNGOID_RELEASECHECK_PUBKEY="$tmp/k.pub" go test -count=1 -tags releasecheck -run TestReleaseCheck ./internal/trust
