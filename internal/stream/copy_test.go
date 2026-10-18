package stream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

func TestCopyTransfersAll(t *testing.T) {
	const size = 10 << 20
	data := make([]byte, size)
	rand.New(rand.NewSource(1)).Read(data)

	var dst bytes.Buffer
	n, err := Copy(context.Background(), &dst, bytes.NewReader(data), 1<<20)
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if n != size {
		t.Fatalf("count = %d, want %d", n, size)
	}
	if !bytes.Equal(dst.Bytes(), data) {
		t.Fatal("output differs from input")
	}
}

func TestCopyTinyBuffer(t *testing.T) {
	data := bytes.Repeat([]byte("abc"), 100000)
	var dst bytes.Buffer
	n, err := Copy(context.Background(), &dst, bytes.NewReader(data), 0)
	if err != nil || n != int64(len(data)) || !bytes.Equal(dst.Bytes(), data) {
		t.Fatalf("n=%d err=%v equal=%v", n, err, bytes.Equal(dst.Bytes(), data))
	}
}

// blockingReader never returns from Read until released. The goroutine reading
// from it cannot be interrupted, so the test releases it on cleanup.
type blockingReader struct{ release chan struct{} }

func (b blockingReader) Read([]byte) (int, error) {
	<-b.release
	return 0, io.EOF
}

func TestCopyCancel(t *testing.T) {
	src := blockingReader{release: make(chan struct{})}
	t.Cleanup(func() { close(src.release) })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := Copy(ctx, io.Discard, src, 1<<20)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Copy did not return within 1s of cancel")
	}
}

// endlessReader yields zeros forever and counts the reads made.
type endlessReader struct{ reads atomic.Int64 }

func (e *endlessReader) Read(p []byte) (int, error) {
	e.reads.Add(1)
	return len(p), nil
}

// failAfterWriter fails once limit bytes have been accepted.
type failAfterWriter struct {
	limit, written int
	err            error
}

func (f *failAfterWriter) Write(p []byte) (int, error) {
	if f.written+len(p) > f.limit {
		return 0, f.err
	}
	f.written += len(p)
	return len(p), nil
}

func TestCopyWriterError(t *testing.T) {
	boom := errors.New("boom")
	src := &endlessReader{}
	dst := &failAfterWriter{limit: 1 << 20, err: boom}

	readerExited := make(chan struct{})
	_, err := copyQueued(context.Background(), dst, src, 1<<20, func() { close(readerExited) })
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}

	// The reader is blocked on a full queue when the writer fails; it must
	// notice the stop signal and exit.
	select {
	case <-readerExited:
	case <-time.After(2 * time.Second):
		t.Fatal("reader goroutine did not exit after writer error")
	}
}

func TestCopyReaderError(t *testing.T) {
	boom := errors.New("read boom")
	src := io.MultiReader(bytes.NewReader([]byte("hello")), errReader{boom})
	var dst bytes.Buffer
	n, err := Copy(context.Background(), &dst, src, 1<<20)
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	if n != 5 || dst.String() != "hello" {
		t.Fatalf("n=%d dst=%q, want buffered data drained", n, dst.String())
	}
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

func TestCopyCancelStopsReader(t *testing.T) {
	src := &endlessReader{}
	ctx, cancel := context.WithCancel(context.Background())
	// A writer that blocks until cancelled lets the queue fill up.
	dst := writerFunc(func(p []byte) (int, error) {
		<-ctx.Done()
		return len(p), nil
	})
	readerExited := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := copyQueued(ctx, dst, src, 1<<20, func() { close(readerExited) })
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	select {
	case <-readerExited:
	case <-time.After(2 * time.Second):
		t.Fatal("reader goroutine did not exit after cancel")
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }
