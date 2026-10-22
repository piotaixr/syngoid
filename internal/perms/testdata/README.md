`<os>/allow_*.txt` is real `zfs allow` output captured by `scripts/ci/capture-fixtures.sh`
(spec §14.3); `<os>/grants.txt` lists the `zfs allow` commands that produced it. The
system and zfs version are in `internal/zfs/testdata/<os>/manifest.txt`.
