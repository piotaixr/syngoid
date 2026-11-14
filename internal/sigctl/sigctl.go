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

	life    sync.Mutex // guards started and stopped
	started bool
	stopped bool
	stop    chan struct{}
	sigs    chan os.Signal
}

// New returns a controller and the run context it cancels on abort. exit is
// called at most once, with code 2, on the last-resort signal.
func New(parent context.Context, exit func(code int)) (*Controller, context.Context) {
	ctx, cancel := context.WithCancel(parent)
	return &Controller{
		exit:   exit,
		cancel: cancel,
		drain:  make(chan struct{}),
		stop:   make(chan struct{}),
		sigs:   make(chan os.Signal, 4),
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

// Start registers for SIGINT, SIGTERM and SIGHUP before it returns, so that
// no signal takes its default action once Start has been called, and handles
// them in a goroutine until Stop. Handling continues after an abort so that a
// further Ctrl-C can force the exit. Start after Stop, or a second Start, does
// nothing.
func (c *Controller) Start() {
	c.life.Lock()
	defer c.life.Unlock()
	if c.started || c.stopped {
		return
	}
	c.started = true
	signal.Notify(c.sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go c.loop()
}

// loop applies received signals until Stop.
func (c *Controller) loop() {
	for {
		select {
		case s := <-c.sigs:
			c.Handle(s)
		case <-c.stop:
			return
		}
	}
}

// Stop unregisters the signal handler and ends the Start goroutine. It is
// safe to call before Start and more than once.
func (c *Controller) Stop() {
	c.life.Lock()
	defer c.life.Unlock()
	if c.stopped {
		return
	}
	c.stopped = true
	signal.Stop(c.sigs)
	close(c.stop)
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
