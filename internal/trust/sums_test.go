package trust

import "testing"

func TestParseSums(t *testing.T) {
	got, err := ParseSums(readFile(t, "sums.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d entries", len(got))
	}
	if got["syngoid_1.2.3_linux_amd64"] != "abb35c616421af72198ad7c2aeeef38516f08f6a7afb2a728cf0068a8a712ddc" {
		t.Fatalf("wrong digest: %v", got)
	}

	hex := "abb35c616421af72198ad7c2aeeef38516f08f6a7afb2a728cf0068a8a712ddc"
	star, err := ParseSums([]byte("\n" + hex + " *file\n\n"))
	if err != nil || star["file"] != hex {
		t.Fatalf("star form: %v %v", star, err)
	}
}

func TestParseSumsRejects(t *testing.T) {
	hex := "abb35c616421af72198ad7c2aeeef38516f08f6a7afb2a728cf0068a8a712ddc"
	tests := []struct{ name, in, want string }{
		{"uppercase", "ABB35C616421AF72198AD7C2AEEEF38516F08F6A7AFB2A728CF0068A8A712DDC  f\n", "SHA256SUMS line 1: malformed"},
		{"short", hex[:63] + "  f\n", "SHA256SUMS line 1: malformed"},
		{"one space", hex + " f\n", "SHA256SUMS line 1: malformed"},
		{"empty name", hex + "  \n", "SHA256SUMS line 1: malformed"},
		{"second line", hex + "  a\njunk\n", "SHA256SUMS line 2: malformed"},
		{"duplicate", hex + "  a\n" + hex + "  a\n", "SHA256SUMS: duplicate entry a"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseSums([]byte(tc.in))
			if err == nil || err.Error() != tc.want {
				t.Fatalf("got %v, want %q", err, tc.want)
			}
		})
	}
}
