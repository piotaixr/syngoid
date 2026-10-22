package rpc

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
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

// dataHeader is the first line of every data stream.
type dataHeader struct {
	RequestID uint64 `json:"request_id"`
}

// WriteDataHeader writes the {"request_id":N} line that opens a data stream.
func WriteDataHeader(w io.Writer, id uint64) error {
	_, err := fmt.Fprintf(w, "{\"request_id\":%d}\n", id)
	return err
}

// ReadDataHeader reads the header line of a data stream. r must have the
// default 4096-byte buffer; a longer line, a request id of 0 or invalid JSON
// is an error. The bytes after the header stay in r for the caller.
func ReadDataHeader(r *bufio.Reader) (uint64, error) {
	line, err := r.ReadSlice('\n')
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			return 0, errors.New("rpc: data stream header line too long")
		}
		if err == io.EOF && len(line) > 0 {
			err = io.ErrUnexpectedEOF
		}
		return 0, err
	}
	var h dataHeader
	if err := json.Unmarshal(line, &h); err != nil {
		return 0, fmt.Errorf("rpc: bad data stream header: %w", err)
	}
	if h.RequestID == 0 {
		return 0, errors.New("rpc: data stream header has no request_id")
	}
	return h.RequestID, nil
}
