package cli

import (
	"testing"
)

func TestParseLocation(t *testing.T) {
	exists := func(p string) bool { return p == "pool:x" }
	tests := []struct {
		arg     string
		want    Location
		remote  bool
		str     string
		wantErr bool
	}{
		{arg: "tank/a", want: Location{Dataset: "tank/a"}, str: "tank/a"},
		{arg: "root@nas:tank/a", want: Location{User: "root", Host: "nas", Dataset: "tank/a"}, remote: true, str: "root@nas:tank/a"},
		{arg: "@nas:tank/a", want: Location{Host: "nas", Dataset: "tank/a"}, remote: true, str: "nas:tank/a"},
		{arg: "nas:tank/a", want: Location{Host: "nas", Dataset: "tank/a"}, remote: true, str: "nas:tank/a"},
		{arg: "pool:x/a", want: Location{Dataset: "pool:x/a"}, str: "pool:x/a"},
		{arg: "tank/a:b", want: Location{Dataset: "tank/a:b"}, str: "tank/a:b"},
		{arg: "nas:", wantErr: true},
		{arg: "root@nas:", wantErr: true},
		{arg: ":tank", wantErr: true},
		{arg: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.arg, func(t *testing.T) {
			got, err := ParseLocation(tt.arg, exists)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseLocation(%q) = %+v, want error", tt.arg, got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
			if got.IsRemote() != tt.remote {
				t.Errorf("IsRemote = %v, want %v", got.IsRemote(), tt.remote)
			}
			if got.String() != tt.str {
				t.Errorf("String = %q, want %q", got.String(), tt.str)
			}
		})
	}
}

func TestParseLocationNilPoolCheck(t *testing.T) {
	got, err := ParseLocation("pool:x/a", nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := (Location{Host: "pool", Dataset: "x/a"}); got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}
