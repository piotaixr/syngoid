package trust

import "testing"

func FuzzParseSignature(f *testing.F) {
	f.Add(readFile(f, "sums.txt.minisig"))
	f.Add(readFile(f, "sums-other-version.txt.minisig"))
	f.Fuzz(func(t *testing.T, b []byte) { _, _ = ParseSignature(b) })
}

func FuzzParsePublicKey(f *testing.F) {
	f.Add(string(readFile(f, "test.pub")))
	f.Add(string(readFile(f, "other.pub")))
	f.Fuzz(func(t *testing.T, s string) { _, _ = ParsePublicKey(s) })
}

func FuzzParseSums(f *testing.F) {
	f.Add(readFile(f, "sums.txt"))
	f.Fuzz(func(t *testing.T, b []byte) { _, _ = ParseSums(b) })
}
