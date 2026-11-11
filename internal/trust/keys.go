package trust

import "sync"

// embeddedKeys are the minisign public keys (the .pub text or its bare base64
// line) that may sign releases: the current key and the next one, so a key
// rotation ships before the old key retires.
//
// TODO(maintainer): paste the current and next public keys (plan Task 0).
// While the list is empty Verify fails closed: every signature is rejected.
var embeddedKeys = []string{}

var (
	keysOnce sync.Once
	keysMu   sync.Mutex
	keys     []PublicKey
)

// trustedKeys returns the keys Verify trusts. The embedded constants are
// parsed once; a malformed one panics, which TestEmbeddedKeys turns into a
// test failure instead of a runtime surprise.
func trustedKeys() []PublicKey {
	keysOnce.Do(func() {
		parsed := make([]PublicKey, 0, len(embeddedKeys))
		for _, text := range embeddedKeys {
			k, err := ParsePublicKey(text)
			if err != nil {
				panic("trust: malformed embedded public key: " + err.Error())
			}
			parsed = append(parsed, k)
		}
		keysMu.Lock()
		keys = parsed
		keysMu.Unlock()
	})
	keysMu.Lock()
	defer keysMu.Unlock()
	return keys
}

// Cleanuper is the part of testing.TB that ReplaceKeysForTesting needs; it
// keeps the testing package out of production code.
type Cleanuper interface {
	Helper()
	Cleanup(func())
}

// ReplaceKeysForTesting swaps the trusted keys for the given ones and
// restores the previous keys when the test ends. It is the only way to change
// them: there is no runtime, flag or environment override.
func ReplaceKeysForTesting(tb Cleanuper, replacement ...PublicKey) {
	tb.Helper()
	previous := trustedKeys()
	keysMu.Lock()
	keys = replacement
	keysMu.Unlock()
	tb.Cleanup(func() {
		keysMu.Lock()
		keys = previous
		keysMu.Unlock()
	})
}
