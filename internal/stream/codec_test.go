package stream

import (
	"bytes"
	"io"
	"math/rand/v2"
	"testing"
)

var allCodecs = []Codec{None, ZstdFast, Zstd, LZ4, Gzip}

func TestParseCodecLegacy(t *testing.T) {
	tests := []struct {
		in         string
		want       Codec
		deprecated bool
	}{
		{"zstd-fast", ZstdFast, false},
		{"zstdmt-fast", ZstdFast, true},
		{"zstd", Zstd, false},
		{"zstd-slow", Zstd, true},
		{"zstdmt-slow", Zstd, true},
		{"xz", Zstd, true},
		{"gzip", Gzip, false},
		{"pigz-fast", Gzip, true},
		{"pigz-slow", Gzip, true},
		{"lzo", LZ4, true},
		{"lz4", LZ4, false},
		{"none", None, false},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, dep, err := ParseCodec(tt.in)
			if err != nil {
				t.Fatalf("ParseCodec(%q) error: %v", tt.in, err)
			}
			if got != tt.want || dep != tt.deprecated {
				t.Fatalf("ParseCodec(%q) = (%q, %v), want (%q, %v)", tt.in, got, dep, tt.want, tt.deprecated)
			}
		})
	}

	if _, _, err := ParseCodec("bogus"); err == nil {
		t.Fatal("ParseCodec(bogus) succeeded, want error")
	}
}

func roundTrip(t testing.TB, c Codec, data []byte) {
	t.Helper()
	var buf bytes.Buffer
	enc, err := NewEncoder(c, &buf)
	if err != nil {
		t.Fatalf("NewEncoder(%s): %v", c, err)
	}
	if _, err := enc.Write(data); err != nil {
		t.Fatalf("%s write: %v", c, err)
	}
	if err := enc.Close(); err != nil {
		t.Fatalf("%s encoder close: %v", c, err)
	}
	dec, err := NewDecoder(c, &buf)
	if err != nil {
		t.Fatalf("NewDecoder(%s): %v", c, err)
	}
	got, err := io.ReadAll(dec)
	if err != nil {
		t.Fatalf("%s read: %v", c, err)
	}
	if err := dec.Close(); err != nil {
		t.Fatalf("%s decoder close: %v", c, err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("%s round trip mismatch: got %d bytes, want %d", c, len(got), len(data))
	}
}

// testData returns deterministic data mixing repeated and random blocks.
func testData(size int) []byte {
	r := rand.New(rand.NewPCG(1, 2))
	const block = 4096
	repeated := make([]byte, block)
	for i := range repeated {
		repeated[i] = byte(r.IntN(256))
	}
	data := make([]byte, 0, size)
	for len(data) < size {
		if r.IntN(2) == 0 {
			data = append(data, repeated...)
			continue
		}
		b := make([]byte, block)
		for i := range b {
			b[i] = byte(r.IntN(256))
		}
		data = append(data, b...)
	}
	return data[:size]
}

func TestCodecRoundTrip(t *testing.T) {
	data := testData(8 << 20)
	for _, c := range allCodecs {
		t.Run(string(c), func(t *testing.T) {
			roundTrip(t, c, data)
		})
	}
}

func TestEncoderCloseKeepsWriterOpen(t *testing.T) {
	for _, c := range allCodecs {
		t.Run(string(c), func(t *testing.T) {
			w := &closeTracker{}
			enc, err := NewEncoder(c, w)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := enc.Write([]byte("hello")); err != nil {
				t.Fatal(err)
			}
			if err := enc.Close(); err != nil {
				t.Fatal(err)
			}
			if w.closed {
				t.Fatal("encoder Close closed the underlying writer")
			}
			if w.Len() == 0 {
				t.Fatal("encoder Close wrote nothing")
			}
		})
	}
}

type closeTracker struct {
	bytes.Buffer
	closed bool
}

func (c *closeTracker) Close() error { c.closed = true; return nil }

func TestUnknownCodec(t *testing.T) {
	if _, err := NewEncoder("bogus", io.Discard); err == nil {
		t.Error("NewEncoder(bogus) succeeded, want error")
	}
	if _, err := NewDecoder("bogus", bytes.NewReader(nil)); err == nil {
		t.Error("NewDecoder(bogus) succeeded, want error")
	}
}

func FuzzCodecRoundTrip(f *testing.F) {
	f.Add([]byte(""))
	f.Add([]byte("hello hello hello hello"))
	f.Add(testData(10000))
	f.Fuzz(func(t *testing.T, data []byte) {
		for _, c := range allCodecs {
			roundTrip(t, c, data)
		}
	})
}
