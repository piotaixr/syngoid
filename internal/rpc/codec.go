package rpc

import (
	"encoding/json"
	"io"
	"sync"
)

// Codec reads and writes Messages as JSON lines on one connection.
type Codec struct {
	wmu sync.Mutex
	enc *json.Encoder
	dec *json.Decoder
}

// NewCodec returns a Codec over rw.
func NewCodec(rw io.ReadWriter) *Codec {
	return &Codec{enc: json.NewEncoder(rw), dec: json.NewDecoder(rw)}
}

// Write sends m as one line. It is safe for concurrent use.
func (c *Codec) Write(m Message) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.enc.Encode(m)
}

// Read returns the next message, or io.EOF at a clean end of input. There
// is no line-length limit (a list_snapshots result can be tens of MiB). It
// must not be called concurrently.
func (c *Codec) Read() (Message, error) {
	var m Message
	err := c.dec.Decode(&m)
	return m, err
}
