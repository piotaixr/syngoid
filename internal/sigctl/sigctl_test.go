package sigctl

import (
	"context"
	"os"
	"slices"
	"sync"
	"syscall"
	"testing"
)

func TestControllerSequence(t *testing.T) {
	tests := []struct {
		name      string
		arm       bool
		disarm    bool
		sigs      []os.Signal
		wantDrain bool
		wantCtx   bool
		wantExits []int
	}{
		{"armed INT", true, false, []os.Signal{syscall.SIGINT}, true, false, nil},
		{"armed INT INT", true, false, []os.Signal{syscall.SIGINT, syscall.SIGINT}, true, true, nil},
		{"armed INT INT INT", true, false, []os.Signal{syscall.SIGINT, syscall.SIGINT, syscall.SIGINT}, true, true, []int{2}},
		{"unarmed INT", false, false, []os.Signal{syscall.SIGINT}, false, true, nil},
		{"unarmed INT INT", false, false, []os.Signal{syscall.SIGINT, syscall.SIGINT}, false, true, []int{2}},
		{"armed TERM", true, false, []os.Signal{syscall.SIGTERM}, false, true, nil},
		{"armed HUP", true, false, []os.Signal{syscall.SIGHUP}, false, true, nil},
		{"armed INT TERM", true, false, []os.Signal{syscall.SIGINT, syscall.SIGTERM}, true, true, nil},
		{"armed disarmed INT", true, true, []os.Signal{syscall.SIGINT}, false, true, nil},
		{"cancelled TERM", false, false, []os.Signal{syscall.SIGTERM, syscall.SIGTERM, syscall.SIGHUP}, false, true, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var exits []int
			c, ctx := New(context.Background(), func(code int) { exits = append(exits, code) })
			if tt.arm {
				disarm := c.Arm()
				if tt.disarm {
					disarm()
				}
			}
			for _, s := range tt.sigs {
				c.Handle(s)
			}
			drained := false
			select {
			case <-c.Drain():
				drained = true
			default:
			}
			if drained != tt.wantDrain {
				t.Errorf("drain closed = %v, want %v", drained, tt.wantDrain)
			}
			if got := ctx.Err() != nil; got != tt.wantCtx {
				t.Errorf("ctx cancelled = %v, want %v", got, tt.wantCtx)
			}
			if !slices.Equal(exits, tt.wantExits) {
				t.Errorf("exits = %v, want %v", exits, tt.wantExits)
			}
		})
	}
}

func TestControllerConcurrentHandle(t *testing.T) {
	var mu sync.Mutex
	var exits []int
	c, _ := New(context.Background(), func(code int) { mu.Lock(); exits = append(exits, code); mu.Unlock() })
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			disarm := c.Arm()
			c.Handle(syscall.SIGINT)
			disarm()
			c.Handle(syscall.SIGTERM)
		}()
	}
	wg.Wait()
	c.Handle(syscall.SIGINT)
	mu.Lock()
	defer mu.Unlock()
	if len(exits) > 1 {
		t.Errorf("exit called %d times, want at most once", len(exits))
	}
}

func TestStopWithoutListenAndIdempotent(t *testing.T) {
	c, _ := New(context.Background(), func(int) {})
	c.Stop()
	c.Stop()
}

func TestListenReceivesSignalsUntilStop(t *testing.T) {
	c, ctx := New(context.Background(), func(int) {})
	done := make(chan struct{})
	go func() { c.Listen(); close(done) }()
	<-c.listening
	if err := syscall.Kill(os.Getpid(), syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
	c.Stop()
	<-done
}
