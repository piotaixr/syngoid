package rpc

import (
	"testing"
	"time"
)

// SetCancelGrace sets cancelGrace to d for the duration of t.
func SetCancelGrace(t testing.TB, d time.Duration) {
	old := cancelGrace
	cancelGrace = d
	t.Cleanup(func() { cancelGrace = old })
}
