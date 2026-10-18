package stream

import (
	"context"
	"errors"
	"io"
)

// chunkSize is the size of one queued buffer in Copy.
const chunkSize = 128 << 10

// chunk is one read result travelling from the reader to the writer.
type chunk struct {
	buf []byte
	n   int
	err error // terminal read error, io.EOF included; buf is unused when set
}

// Copy copies src to dst through an in-memory queue of 128 KiB chunks holding
// at most bufferSize bytes (at least one chunk), so that a slow reader and a
// slow writer overlap instead of stalling each other, as mbuffer does. It
// returns the number of bytes written to dst.
//
// On src EOF every queued chunk is written before Copy returns (count, nil).
// A read error other than EOF, a write error, or cancellation of ctx (reported
// as ctx.Err()) ends the copy. Copy never closes dst or src.
//
// The reader goroutine stops after any error, but a src.Read that blocks
// forever cannot be interrupted: Copy still returns on cancellation, and that
// goroutine lives until the Read returns.
func Copy(ctx context.Context, dst io.Writer, src io.Reader, bufferSize int64) (int64, error) {
	depth := int(bufferSize / chunkSize)
	if depth < 1 {
		depth = 1
	}
	queue := make(chan chunk, depth)
	free := make(chan []byte, depth+2)
	stop := make(chan struct{})
	defer close(stop)

	go readLoop(src, queue, free, stop)

	var written int64
	for {
		select {
		case <-ctx.Done():
			return written, ctx.Err()
		case c := <-queue:
			if c.n > 0 {
				n, err := dst.Write(c.buf[:c.n])
				written += int64(n)
				if err != nil {
					return written, err
				}
				if n < c.n {
					return written, io.ErrShortWrite
				}
				select {
				case free <- c.buf:
				default:
				}
			}
			if c.err != nil {
				if errors.Is(c.err, io.EOF) {
					return written, nil
				}
				return written, c.err
			}
		}
	}
}

// readLoop reads src into chunks and queues them until a read error (EOF
// included, queued as the final chunk) or until stop is closed.
func readLoop(src io.Reader, queue chan<- chunk, free chan []byte, stop <-chan struct{}) {
	for {
		var buf []byte
		select {
		case buf = <-free:
		default:
			buf = make([]byte, chunkSize)
		}
		n, err := src.Read(buf)
		select {
		case queue <- chunk{buf: buf, n: n, err: err}:
		case <-stop:
			return
		}
		if err != nil {
			return
		}
	}
}
