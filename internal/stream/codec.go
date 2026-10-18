// Package stream provides the in-process stream codecs and copy helpers that
// replace syncoid's external lzop/zstd/pigz/lz4/xz pipes.
package stream

import (
	"fmt"
	"io"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// Codec names a stream compression algorithm.
type Codec string

// Supported codecs.
const (
	None     Codec = "none"
	ZstdFast Codec = "zstd-fast"
	Zstd     Codec = "zstd"
	LZ4      Codec = "lz4"
	Gzip     Codec = "gzip"
)

// gzipLevel is the compression level used for Gzip.
const gzipLevel = 3

// ParseCodec maps a codec name, including the legacy syncoid names, to a
// Codec. deprecated is true when s is a legacy name that differs from the
// canonical name of the returned codec.
func ParseCodec(s string) (c Codec, deprecated bool, err error) {
	switch s {
	case "none", "zstd-fast", "zstd", "lz4", "gzip":
		return Codec(s), false, nil
	case "zstdmt-fast":
		return ZstdFast, true, nil
	case "zstd-slow", "zstdmt-slow", "xz":
		return Zstd, true, nil
	case "pigz-fast", "pigz-slow":
		return Gzip, true, nil
	case "lzo":
		return LZ4, true, nil
	}
	return "", false, fmt.Errorf("unknown compression %q", s)
}

// nopWriteCloser adds a no-op Close to a writer.
type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

// NewEncoder returns a compressing writer for c. Closing it flushes all
// pending output to w but does not close w.
func NewEncoder(c Codec, w io.Writer) (io.WriteCloser, error) {
	switch c {
	case None:
		return nopWriteCloser{w}, nil
	case ZstdFast:
		return zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedDefault))
	case Zstd:
		return zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	case LZ4:
		return lz4.NewWriter(w), nil
	case Gzip:
		return gzip.NewWriterLevel(w, gzipLevel)
	}
	return nil, fmt.Errorf("unknown codec %q", c)
}

// readOnly exposes only Read of an lz4 reader, hiding its WriteTo, which
// returns io.EOF as an error when called after a Read reached the end.
type readOnly struct{ r io.Reader }

func (d readOnly) Read(p []byte) (int, error) { return d.r.Read(p) }
func (readOnly) Close() error                 { return nil }

// zstdDecoder adapts zstd.Decoder, whose Close returns nothing, to io.ReadCloser.
type zstdDecoder struct{ *zstd.Decoder }

func (d zstdDecoder) Close() error {
	d.Decoder.Close()
	return nil
}

// NewDecoder returns a decompressing reader for c. Closing it releases decoder
// resources only; r is not closed.
func NewDecoder(c Codec, r io.Reader) (io.ReadCloser, error) {
	switch c {
	case None:
		return io.NopCloser(r), nil
	case ZstdFast, Zstd:
		d, err := zstd.NewReader(r)
		if err != nil {
			return nil, err
		}
		return zstdDecoder{d}, nil
	case LZ4:
		return readOnly{lz4.NewReader(r)}, nil
	case Gzip:
		return gzip.NewReader(r)
	}
	return nil, fmt.Errorf("unknown codec %q", c)
}
