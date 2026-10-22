package zfs

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The testdata/<os> directories hold real output captured by
// scripts/ci/capture-fixtures.sh (manifest.txt names the system and the zfs
// version, commands.txt the argv and exit status of every capture). The
// expectations below come from what the script created, not from the files.

// capturedOS are the platforms with a captured fixture set.
var capturedOS = []string{"linux", "freebsd", "illumos"}

const (
	fxPool    = "syngoidfx"
	fxA       = "syngoidfx/fx/a"
	fxMyData  = "syngoidfx/fx/my data"
	fxVol     = "syngoidfx/fx/vol"
	fxVolSnap = "syncoid_fxhost_2026-10-18:10:00:00-GMT02:00"
	mib       = 1 << 20
)

func readCaptured(t *testing.T, osName, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", osName, name))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// captureStatus returns the exit status commands.txt records for capture name.
func captureStatus(t *testing.T, osName, name string) int {
	t.Helper()
	re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(name) + ` rc=(\d+): `)
	m := re.FindStringSubmatch(readCaptured(t, osName, "commands.txt"))
	if m == nil {
		t.Fatalf("%s: no %s in commands.txt", osName, name)
	}
	rc, err := strconv.Atoi(m[1])
	if err != nil {
		t.Fatal(err)
	}
	return rc
}

// sameSecondPair reports whether the capture got fx/a@s2 and fx/a@s3 into
// the same creation second.
func sameSecondPair(t *testing.T, osName string) bool {
	t.Helper()
	m := regexp.MustCompile(`(?m)^same-second snapshot pair fx/a@s2, fx/a@s3: (yes|no) `).
		FindStringSubmatch(readCaptured(t, osName, "commands.txt"))
	if m == nil {
		t.Fatalf("%s: no same-second line in commands.txt", osName)
	}
	return m[1] == "yes"
}

func TestParseCapturedTree(t *testing.T) {
	want := []DatasetInfo{
		{Name: fxPool, Type: "filesystem"},
		{Name: "syngoidfx/fx", Type: "filesystem"},
		{Name: fxA, Type: "filesystem"},
		{Name: "syngoidfx/fx/a/c", Type: "filesystem", Origin: fxA + "@s1", SyncProp: "nas,db1"},
		{Name: fxMyData, Type: "filesystem"},
		{Name: "syngoidfx/fx/nosync", Type: "filesystem", SyncProp: "false"},
		{Name: fxVol, Type: "volume"},
	}
	for _, osName := range capturedOS {
		t.Run(osName, func(t *testing.T) {
			got, err := parseTree(readCaptured(t, osName, "tree.txt"))
			if err != nil {
				t.Fatal(err)
			}
			for i := range got {
				// The unmodified clone uses nothing; everything else holds data.
				if clone := got[i].Origin != ""; clone != (got[i].Used == 0) {
					t.Errorf("%s: used = %d", got[i].Name, got[i].Used)
				}
				got[i].Used = 0
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got  %+v\nwant %+v", got, want)
			}

			single, err := parseTree(readCaptured(t, osName, "tree_single.txt"))
			if err != nil {
				t.Fatal(err)
			}
			if len(single) != 1 || single[0].Name != fxMyData || single[0].Type != "filesystem" {
				t.Fatalf("single = %+v", single)
			}
		})
	}
}

func TestParseCapturedProps(t *testing.T) {
	for _, osName := range capturedOS {
		t.Run(osName, func(t *testing.T) {
			got, err := parsePropValues(readCaptured(t, osName, "props.txt"))
			if err != nil || !reflect.DeepEqual(got, map[string]string{"recordsize": "131072"}) {
				t.Fatalf("props = %v, %v", got, err)
			}
			// fx/a has no local property: compression and atime are set on the pool.
			got, err = parsePropValues(readCaptured(t, osName, "props_local.txt"))
			if err != nil || len(got) != 0 {
				t.Fatalf("local props = %v, %v", got, err)
			}
		})
	}
}

// checkCapturedSnapshots checks a snapshot listing of the whole pool.
func checkCapturedSnapshots(t *testing.T, osName string, got map[string][]Snapshot) {
	t.Helper()
	wantNames := map[string][]string{
		fxA:      {"s1", "s2", "s3"},
		fxMyData: {"m1"},
		fxVol:    {fxVolSnap},
	}
	names := map[string][]string{}
	guids := map[string]bool{}
	creations := map[uint64]bool{}
	for ds, list := range got {
		for _, s := range list {
			names[ds] = append(names[ds], s.Name)
			if _, err := strconv.ParseUint(s.GUID, 10, 64); err != nil || guids[s.GUID] {
				t.Errorf("%s@%s: bad or repeated guid %q", ds, s.Name, s.GUID)
			}
			guids[s.GUID] = true
			if creations[s.Creation] {
				t.Errorf("%s@%s: creation %d not unique in the listing", ds, s.Name, s.Creation)
			}
			creations[s.Creation] = true
		}
	}
	if !reflect.DeepEqual(names, wantNames) {
		t.Fatalf("names = %q, want %q", names, wantNames)
	}

	a := got[fxA]
	s1, s2, s3 := a[0], a[1], a[2]
	if s1.CreateTxg >= s2.CreateTxg || s2.CreateTxg >= s3.CreateTxg {
		t.Errorf("createtxg not increasing: %d %d %d", s1.CreateTxg, s2.CreateTxg, s3.CreateTxg)
	}
	// The script sleeps a second between s1 and s2.
	if s1.Creation/1000 >= s2.Creation/1000 {
		t.Errorf("s1 creation %d not before s2 %d", s1.Creation, s2.Creation)
	}
	if sameSecondPair(t, osName) {
		// Same second: s2 keeps counter 0, s3 gets the next one.
		if s2.Creation%1000 != 0 || s3.Creation != s2.Creation+1 {
			t.Errorf("tie-break: s2 = %d, s3 = %d, want s3 = s2+1 and s2%%1000 = 0", s2.Creation, s3.Creation)
		}
	} else if s3.Creation <= s2.Creation {
		t.Errorf("s3 creation %d not after s2 %d", s3.Creation, s2.Creation)
	}
}

func TestParseCapturedSnapshots(t *testing.T) {
	for _, osName := range capturedOS {
		t.Run(osName, func(t *testing.T) {
			// The "all" listing (the ListSnapshots fallback) works everywhere.
			all, err := parseSnapshots(readCaptured(t, osName, "snaps_all.txt"), "snapshot")
			if err != nil {
				t.Fatal(err)
			}
			checkCapturedSnapshots(t, osName, all)

			// The typed listing was captured with the property list before
			// -r / -d 1; illumos zfs stops parsing options at the first
			// operand and rejects that order (see TestClassifyCaptured).
			wantTyped := osName != "illumos"
			for _, name := range []string{"snaps.txt", "snaps_d1.txt"} {
				if ok := captureStatus(t, osName, name) == 0; ok != wantTyped {
					t.Fatalf("%s succeeded = %v, want %v", name, ok, wantTyped)
				}
			}
			if !wantTyped {
				return
			}
			typed, err := parseSnapshots(readCaptured(t, osName, "snaps.txt"), "")
			if err != nil {
				t.Fatal(err)
			}
			checkCapturedSnapshots(t, osName, typed)
			if !reflect.DeepEqual(typed, all) {
				t.Errorf("typed and all listings differ:\n%+v\n%+v", typed, all)
			}
			d1, err := parseSnapshots(readCaptured(t, osName, "snaps_d1.txt"), "")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(d1, map[string][]Snapshot{fxA: typed[fxA]}) {
				t.Errorf("-d 1 listing = %+v, want only %s %+v", d1, fxA, typed[fxA])
			}
		})
	}
}

func TestParseCapturedBookmarks(t *testing.T) {
	for _, osName := range capturedOS {
		t.Run(osName, func(t *testing.T) {
			all := readCaptured(t, osName, "snaps_all.txt")
			snaps, err := parseSnapshots(all, "snapshot")
			if err != nil {
				t.Fatal(err)
			}
			listings := map[string]string{"all": all}
			// Same argv order problem as the typed snapshot listing.
			wantTyped := osName != "illumos"
			if ok := captureStatus(t, osName, "bookmarks.txt") == 0; ok != wantTyped {
				t.Fatalf("bookmarks.txt succeeded = %v, want %v", ok, wantTyped)
			}
			if wantTyped {
				listings["typed"] = readCaptured(t, osName, "bookmarks.txt")
			}
			for kind, out := range listings {
				wantType := ""
				if kind == "all" {
					wantType = "bookmark"
				}
				got, err := parseGuidProps(out, '#', wantType)
				if err != nil {
					t.Fatal(err)
				}
				// b1 and b2 are bookmarks of s1 and s2: same guid, txg and creation.
				if len(got) != 1 || len(got[fxA]) != 2 {
					t.Fatalf("%s: got %+v", kind, got)
				}
				for i, name := range []string{"b1", "b2"} {
					b, s := got[fxA][i], snaps[fxA][i]
					if b.Name != name || b.GUID != s.GUID || b.CreateTxg != s.CreateTxg || b.Creation/1000 != s.Creation/1000 {
						t.Errorf("%s: bookmark %+v does not match snapshot %+v", kind, b, s)
					}
				}
			}
		})
	}
}

func TestParseEstimateCaptured(t *testing.T) {
	// Sizes follow from the data the script wrote: 4 MiB of random data
	// before s1, 1 MiB more before s2, nothing between s2 and s3, and a
	// 4 MiB fx/r@r1 whose receive was cut after 1 MiB.
	tests := []struct {
		file     string
		resume   bool
		min, max int64
	}{
		{"estimate_full.txt", false, 4 * mib, 5 * mib},
		{"estimate_inc.txt", false, 1 * mib, 1*mib + mib/2},
		{"estimate_inc_I.txt", false, 1 * mib, 1*mib + mib/2},
		{"estimate_resume.txt", true, 2 * mib, 4 * mib},
	}
	for _, osName := range capturedOS {
		t.Run(osName, func(t *testing.T) {
			var inc int64
			for _, tt := range tests {
				got := parseEstimate(readCaptured(t, osName, tt.file), tt.resume)
				if got < tt.min || got >= tt.max {
					t.Errorf("%s: %d not in [%d, %d)", tt.file, got, tt.min, tt.max)
				}
				switch tt.file {
				case "estimate_inc.txt":
					inc = got
				case "estimate_inc_I.txt":
					if got <= inc {
						t.Errorf("-I s1..s3 (%d) not larger than -i s1 s2 (%d)", got, inc)
					}
				}
			}
		})
	}
}

func TestClassifyCaptured(t *testing.T) {
	// none means Classify must not recognise the message.
	var none error
	notIllumos := func(want error) map[string]error {
		return map[string]error{"linux": want, "freebsd": want, "illumos": none}
	}
	all := func(want error) map[string]error {
		return map[string]error{"linux": want, "freebsd": want, "illumos": want}
	}
	tests := []struct {
		file string
		want map[string]error
		// emptyOn lists the platforms where the capture succeeded, leaving
		// no stderr.
		emptyOn []string
	}{
		// illumos receives a full stream with -F over partial state.
		{"stderr_partial_state.txt", notIllumos(ErrPartialState), []string{"illumos"}},
		// "destination 'X' exists / must specify -F": not a partial-state report.
		{"stderr_partial_state_noF.txt", all(none), nil},
		// None of the destination-exists variants yields "cannot restore to
		// X: destination already exists": resuming onto an existing dataset
		// ("Resuming recv on existing destination", illumos: "invalid backup
		// stream"), a full stream onto one with snapshots, and one without -F.
		{"stderr_dest_exists.txt", all(none), nil},
		{"stderr_dest_exists_plain.txt", all(none), nil},
		{"stderr_dest_exists_noF.txt", all(none), nil},
		{"stderr_no_dataset.txt", all(ErrNoDataset), nil},
		// illumos rejects the captured argv order before looking the dataset up.
		{"stderr_no_dataset_get.txt", notIllumos(ErrNoDataset), nil},
		{"stderr_recv_no_parent.txt", all(ErrNoDataset), nil},
		// The capture did not reach the state: zfs send -t found the token's
		// snapshot by guid in syngoidfx/exist@r1, a received copy of it.
		{"stderr_resume_source_gone.txt", all(none), capturedOS},
		{"snaps.txt.stderr", map[string]error{"illumos": none}, nil},
		{"snaps_d1.txt.stderr", map[string]error{"illumos": none}, nil},
		{"bookmarks.txt.stderr", map[string]error{"illumos": none}, nil},
	}
	for _, osName := range capturedOS {
		t.Run(osName, func(t *testing.T) {
			for _, tt := range tests {
				want, ok := tt.want[osName]
				if !ok {
					continue
				}
				stderr := readCaptured(t, osName, tt.file)
				capture := strings.TrimSuffix(tt.file, ".stderr")
				wantEmpty := false
				for _, o := range tt.emptyOn {
					wantEmpty = wantEmpty || o == osName
				}
				if wantEmpty != (captureStatus(t, osName, capture) == 0) || wantEmpty != (stderr == "") {
					t.Errorf("%s: rc %d, stderr %q; want success = %v", tt.file, captureStatus(t, osName, capture), stderr, wantEmpty)
				}
				got := Classify(stderr)
				if want == nil {
					if got != nil {
						t.Errorf("%s: Classify = %v, want nil", tt.file, got)
					}
				} else if !errors.Is(got, want) {
					t.Errorf("%s: Classify = %v, want %v", tt.file, got, want)
				}
			}
		})
	}
}
