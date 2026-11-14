// Package sigctl turns SIGINT, SIGTERM and SIGHUP into the drain, abort and
// exit stages of design §5.1.
package sigctl

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// Controller owns the run context and the drain channel.
type Controller struct {
	exit   func(int)
	cancel context.CancelFunc
	drain  chan struct{}

	mu        sync.Mutex
	armed     int
	drained   bool
	cancelled bool
	exited    bool

	// listening is closed once Listen has registered its signal channel.
	listening chan struct{}
	stop      chan struct{}
	stopOnce  sync.Once
	sigs      chan os.Signal
}

// New returns a controller and the run context it cancels on abort. exit is
// called at most once, with code 2, on the last-resort signal.
func New(parent context.Context, exit func(code int)) (*Controller, context.Context) {
	ctx, cancel := context.WithCancel(parent)
	return &Controller{
		exit:      exit,
		cancel:    cancel,
		drain:     make(chan struct{}),
		listening: make(chan struct{}),
		stop:      make(chan struct{}),
		sigs:      make(chan os.Signal, 4),
	}, ctx
}

// Handle applies one signal to the state machine.
func (c *Controller) Handle(sig os.Signal) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if sig != syscall.SIGINT {
		c.abort()
		return
	}
	switch {
	case c.cancelled:
		if !c.exited {
			c.exited = true
			c.exit(2)
		}
	case c.drained:
		c.abort()
	case c.armed > 0:
		c.drained = true
		close(c.drain)
	default:
		c.abort()
	}
}

// abort cancels the run context; c.mu must be held.
func (c *Controller) abort() {
	if c.cancelled {
		return
	}
	c.cancelled = true
	c.cancel()
}

// Listen handles SIGINT, SIGTERM and SIGHUP until Stop. It keeps running
// after an abort so that a further Ctrl-C can force the exit.
func (c *Controller) Listen() {
	signal.Notify(c.sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(c.sigs)
	close(c.listening)
	for {
		select {
		case s := <-c.sigs:
			c.Handle(s)
		case <-c.stop:
			return
		}
	}
}

// Stop unregisters the signal handler and ends Listen. It is safe to call
// without Listen and more than once.
func (c *Controller) Stop() {
	c.stopOnce.Do(func() {
		signal.Stop(c.sigs)
		close(c.stop)
	})
}

// Arm makes draining possible until the returned function is called.
func (c *Controller) Arm() (disarm func()) {
	c.mu.Lock()
	c.armed++
	c.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			c.armed--
			c.mu.Unlock()
		})
	}
}

// Drain is closed when the user asked to drain.
func (c *Controller) Drain() <-chan struct{} { return c.drain }
