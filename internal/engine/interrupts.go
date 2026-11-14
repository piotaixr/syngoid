package engine

// Interrupts lets the user drain a run (design §5).
type Interrupts interface {
	Arm() (disarm func())   // draining is possible from now until disarm
	Drain() <-chan struct{} // closed when the user asked to drain
}
