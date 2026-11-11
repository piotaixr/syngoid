package trust

import "sync"

// currentKey is the release signing key with key ID 1D1D0F872FE988EE.
const currentKey = "RWTuiOkvhw8dHTEwi6hY70j47/rWnbonaUhx49PVddfCjdvzH449i33s"

// nextKey is the release signing key with key ID 7AA1ABAB97755335, trusted
// ahead of rotation so a key change ships before the old key retires.
const nextKey = "RWQ1U3WXq6uheoyHZzKTMH6KbMQtx3HEPp6igyCk4+vcudZUDDHGio80"

// embeddedKeys are the minisign public keys (bare base64 lines) that may sign
// releases: the current key first, then the next one.
var embeddedKeys = []string{currentKey, nextKey}

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
