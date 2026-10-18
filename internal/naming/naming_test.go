package naming_test

import (
	"testing"
	"time"

	"github.com/piotaixr/syngoid/internal/naming"
)

func TestSyncSnapName(t *testing.T) {
	plus2 := time.FixedZone("", 2*3600)
	minus5 := time.FixedZone("", -5*3600)
	at := func(loc *time.Location) time.Time {
		return time.Date(2026, 10, 18, 14, 3, 7, 0, loc)
	}

	cases := []struct {
		name       string
		identifier string
		host       string
		when       time.Time
		want       string
	}{
		{"utc+2", "", "nas", at(plus2), "syncoid_nas_2026-10-18:14:03:07-GMT02:00"},
		{"utc-5", "", "nas", at(minus5), "syncoid_nas_2026-10-18:14:03:07-GMT-05:00"},
		{"identifier", "offsite", "nas", at(plus2), "syncoid_offsite_nas_2026-10-18:14:03:07-GMT02:00"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := naming.SyncSnapName(c.identifier, c.host, c.when); got != c.want {
				t.Errorf("SyncSnapName = %q, want %q", got, c.want)
			}
		})
	}
}

func TestSyncSnapNameHalfHourOffsets(t *testing.T) {
	cases := []struct {
		name string
		off  int
		want string
	}{
		{"utc+5:30", 5*3600 + 30*60, "syncoid_nas_2026-10-18:14:03:07-GMT05:30"},
		{"utc-3:30", -(3*3600 + 30*60), "syncoid_nas_2026-10-18:14:03:07-GMT-03:30"},
		{"utc", 0, "syncoid_nas_2026-10-18:14:03:07-GMT00:00"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			loc := time.FixedZone("", c.off)
			when := time.Date(2026, 10, 18, 14, 3, 7, 0, loc)
			if got := naming.SyncSnapName("", "nas", when); got != c.want {
				t.Errorf("SyncSnapName = %q, want %q", got, c.want)
			}
		})
	}
}

func TestHoldTag(t *testing.T) {
	cases := []struct {
		identifier, host, want string
	}{
		{"", "nas", "syncoid_nas"},
		{"offsite", "nas", "syncoid_offsite_nas"},
	}
	for _, c := range cases {
		if got := naming.HoldTag(c.identifier, c.host); got != c.want {
			t.Errorf("HoldTag(%q, %q) = %q, want %q", c.identifier, c.host, got, c.want)
		}
	}
}

func TestPrunePatternStrict(t *testing.T) {
	re := naming.SyncSnapPattern("", "nas")
	matches := []string{
		"syncoid_nas_2026-10-18:14:03:07-GMT02:00",
		"syncoid_nas_2026-10-18:14:03:07-GMT-05:00",
	}
	rejects := []string{
		"syncoid_nas2_2026-10-18:14:03:07-GMT02:00",
		"syncoid_offsite_nas_2026-10-18:14:03:07-GMT02:00",
		"syncoid_nas_manual",
	}
	for _, s := range matches {
		if !re.MatchString(s) {
			t.Errorf("pattern should match %q", s)
		}
	}
	for _, s := range rejects {
		if re.MatchString(s) {
			t.Errorf("pattern should not match %q", s)
		}
	}
}

func TestPrunePatternQuotesMeta(t *testing.T) {
	const stamp = "_2026-10-18:14:03:07-GMT02:00"
	if naming.SyncSnapPattern("a.b", "nas").MatchString("syncoid_aXb_nas" + stamp) {
		t.Error("identifier a.b pattern must not match syncoid_aXb_nas_...")
	}
	if !naming.SyncSnapPattern("a.b", "nas").MatchString("syncoid_a.b_nas" + stamp) {
		t.Error("identifier a.b pattern should match syncoid_a.b_nas_...")
	}
	if naming.SyncSnapPattern("", "db.lan").MatchString("syncoid_dbXlan" + stamp) {
		t.Error("host db.lan pattern must not match syncoid_dbXlan_...")
	}
}

func TestBookmarkFallback(t *testing.T) {
	if got, want := naming.BookmarkFallback("pool/ds@snap", "1234567890"), "pool/ds@snap123456"; got != want {
		t.Errorf("BookmarkFallback = %q, want %q", got, want)
	}
	if got, want := naming.BookmarkFallback("pool/ds@snap", "123"), "pool/ds@snap123"; got != want {
		t.Errorf("BookmarkFallback short guid = %q, want %q", got, want)
	}
}

func TestValidIdentifier(t *testing.T) {
	for _, ok := range []string{"off-site_1:x.y", "offsite"} {
		if !naming.ValidIdentifier(ok) {
			t.Errorf("ValidIdentifier(%q) = false, want true", ok)
		}
	}
	for _, bad := range []string{"bad/id", "with space", ""} {
		if naming.ValidIdentifier(bad) {
			t.Errorf("ValidIdentifier(%q) = true, want false", bad)
		}
	}
}
