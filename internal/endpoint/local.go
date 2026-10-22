package endpoint

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/piotaixr/syngoid/internal/stream"
	"github.com/piotaixr/syngoid/internal/zfs"
)

// Local is the Endpoint for the host the process runs on.
type Local struct {
	*zfs.Client
	exec zfs.Exec
}

var _ Endpoint = (*Local)(nil)

// NewLocal returns a Local endpoint. c runs zfs commands (honouring its sudo
// setting); exec runs the helper commands id and ps directly.
func NewLocal(c *zfs.Client, exec zfs.Exec) *Local {
	return &Local{Client: c, exec: exec}
}

// Label returns "" for the local host.
func (l *Local) Label() string { return "" }

// run executes argv through the plain Exec and returns its trimmed stdout.
func (l *Local) run(ctx context.Context, argv ...string) (string, error) {
	var out strings.Builder
	if err := l.exec.Run(ctx, zfs.Cmd{Argv: argv, Stdout: &out}); err != nil {
		return "", err
	}
	return strings.TrimSpace(out.String()), nil
}

// Info gathers the local user, uid, groups and ZFS version.
func (l *Local) Info(ctx context.Context) (HostInfo, error) {
	info := HostInfo{OS: runtime.GOOS, Arch: runtime.GOARCH}
	var err error
	if info.User, err = l.run(ctx, "id", "-un"); err != nil {
		return HostInfo{}, err
	}
	uid, err := l.run(ctx, "id", "-u")
	if err != nil {
		return HostInfo{}, err
	}
	if info.UID, err = strconv.Atoi(uid); err != nil {
		return HostInfo{}, fmt.Errorf("parse uid %q: %w", uid, err)
	}
	groups, err := l.run(ctx, "id", "-Gn")
	if err != nil {
		return HostInfo{}, err
	}
	info.Groups = strings.Fields(groups)
	if v, err := l.Output(ctx, "version"); err == nil {
		info.ZFSVersion, _, _ = strings.Cut(strings.TrimLeft(v, "\n"), "\n")
		info.ZFSVersion = strings.TrimSpace(info.ZFSVersion)
	}
	return info, nil
}

// Allow returns the raw output of `zfs allow ds`.
func (l *Local) Allow(ctx context.Context, ds string) (string, error) {
	return l.Output(ctx, "allow", ds)
}

// processArgs lists the argument list of every process, one per line. It
// prefers `ps -Ao args=` (no header line); when that fails, as on a ps that
// rejects the "args=" header suppression, it falls back to `ps -eo args` and
// drops the header line.
func (l *Local) processArgs(ctx context.Context) ([]string, error) {
	var out strings.Builder
	err := l.exec.Run(ctx, zfs.Cmd{Argv: []string{"ps", "-Ao", "args="}, Stdout: &out})
	if err == nil {
		return strings.Split(out.String(), "\n"), nil
	}
	if ctx.Err() != nil {
		return nil, err
	}
	out.Reset()
	if err2 := l.exec.Run(ctx, zfs.Cmd{Argv: []string{"ps", "-eo", "args"}, Stdout: &out}); err2 != nil {
		return nil, errors.Join(err, err2)
	}
	_, rest, _ := strings.Cut(out.String(), "\n")
	return strings.Split(rest, "\n"), nil
}

// ReceiveBusy lists processes once and reports which datasets are the target
// of a running zfs receive.
func (l *Local) ReceiveBusy(ctx context.Context, datasets []string) (map[string]bool, error) {
	lines, err := l.processArgs(ctx)
	if err != nil {
		return nil, err
	}
	for i, ln := range lines {
		lines[i] = strings.TrimRight(ln, " \t\r")
	}
	busy := make(map[string]bool, len(datasets))
	for _, ds := range datasets {
		re := regexp.MustCompile(`zfs *(receive|recv)[^/]*` + regexp.QuoteMeta(ds) + `$`)
		busy[ds] = false
		for _, ln := range lines {
			if re.MatchString(ln) {
				busy[ds] = true
				break
			}
		}
	}
	return busy, nil
}

// countWriter reports the cumulative number of bytes written through it.
type countWriter struct {
	w        io.Writer
	n        int64
	progress func(int64)
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	if c.progress != nil && n > 0 {
		c.progress(c.n)
	}
	return n, err
}

// gateWriter forwards to w until discard is called, then drops all writes.
type gateWriter struct {
	w       io.Writer
	discard atomic.Bool
}

func (g *gateWriter) Write(p []byte) (int, error) {
	if g.discard.Load() {
		return len(p), nil
	}
	return g.w.Write(p)
}

// Send runs zfs send, encoding the stream with o.Codec into w. Progress is
// reported in raw (pre-encoding) bytes. The encoder is flushed and closed
// only when zfs send succeeded; on failure its resources are released
// without writing anything further to w, so a truncated stream never gets a
// valid trailer and a stalled consumer cannot block the return.
func (l *Local) Send(ctx context.Context, s zfs.SendSpec, o SendOpts, w io.Writer) error {
	gate := &gateWriter{w: w}
	enc, err := stream.NewEncoder(o.Codec, gate)
	if err != nil {
		return err
	}
	if err := l.Client.Send(ctx, s, &countWriter{w: enc, progress: o.Progress}); err != nil {
		gate.discard.Store(true)
		_ = enc.Close()
		return err
	}
	return enc.Close()
}

// errReceiveDone replaces the feeder's write errors: the only way writing
// into the receive's stdin pipe fails is that Receive closed its read end
// after zfs receive exited, so such errors are never reported.
var errReceiveDone = errors.New("endpoint: receive finished")

// receiveFeed is the write end of zfs receive's stdin pipe.
type receiveFeed struct{ w *os.File }

func (f receiveFeed) Write(p []byte) (int, error) {
	n, err := f.w.Write(p)
	if err != nil {
		err = errReceiveDone
	}
	return n, err
}

// Receive decodes r with codec and feeds the result to zfs receive through
// an OS pipe, so zfs receive owns its stdin and an early failure returns
// without waiting for r. The decoder is built inside the feeding goroutine
// because some decoders read the stream header on construction. A genuine
// read or decode error wins over the receive error it causes; otherwise the
// receive error is returned unchanged. When the receive fails, the feeder
// may still be blocked reading r; the caller unblocks it by closing r.
func (l *Local) Receive(ctx context.Context, s zfs.RecvSpec, codec stream.Codec, r io.Reader) error {
	pr, pw, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("endpoint: receive pipe: %w", err)
	}
	decErr := make(chan error, 1)
	go func() {
		err := decodeInto(receiveFeed{pw}, codec, r)
		if errors.Is(err, errReceiveDone) {
			err = nil
		}
		decErr <- err // before closing pw so a receive failure sees it
		_ = pw.Close()
	}()
	recvErr := l.Client.Receive(ctx, s, pr)
	// Unblock the feeder if receive stopped reading early: its writes now
	// fail with EPIPE.
	_ = pr.Close()
	if recvErr != nil {
		select {
		case err := <-decErr:
			if err != nil {
				return err
			}
		default:
		}
		return recvErr
	}
	return <-decErr
}

// decodeInto copies the decoded form of r into w.
func decodeInto(w io.Writer, codec stream.Codec, r io.Reader) error {
	dec, err := stream.NewDecoder(codec, r)
	if err != nil {
		return err
	}
	defer func() { _ = dec.Close() }()
	_, err = io.Copy(w, dec)
	return err
}
