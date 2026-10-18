package endpoint

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"runtime"
	"strconv"
	"strings"

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

// ReceiveBusy lists processes once and reports which datasets are the target
// of a running zfs receive.
func (l *Local) ReceiveBusy(ctx context.Context, datasets []string) (map[string]bool, error) {
	var out strings.Builder
	if err := l.exec.Run(ctx, zfs.Cmd{Argv: []string{"ps", "-Ao", "args="}, Stdout: &out}); err != nil {
		return nil, err
	}
	lines := strings.Split(out.String(), "\n")
	for i, ln := range lines {
		lines[i] = strings.TrimRight(ln, " \t\r")
	}
	busy := make(map[string]bool, len(datasets))
	for _, ds := range datasets {
		re, err := regexp.Compile(`zfs *(receive|recv)[^/]*` + regexp.QuoteMeta(ds) + `$`)
		if err != nil {
			return nil, err
		}
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

// Send runs zfs send, encoding the stream with o.Codec into w. Progress is
// reported in raw (pre-encoding) bytes.
func (l *Local) Send(ctx context.Context, s zfs.SendSpec, o SendOpts, w io.Writer) error {
	enc, err := stream.NewEncoder(o.Codec, w)
	if err != nil {
		return err
	}
	sendErr := l.Client.Send(ctx, s, &countWriter{w: enc, progress: o.Progress})
	// Always close to release encoder resources; only flush errors matter
	// when the send itself succeeded.
	closeErr := enc.Close()
	if sendErr != nil {
		return sendErr
	}
	return closeErr
}

// Receive decodes r with codec and feeds the result to zfs receive. The
// decoder is built inside the feeding goroutine because some decoders read
// the stream header on construction.
func (l *Local) Receive(ctx context.Context, s zfs.RecvSpec, codec stream.Codec, r io.Reader) error {
	pr, pw := io.Pipe()
	decErr := make(chan error, 1)
	go func() {
		err := decodeInto(pw, codec, r)
		decErr <- err // before closing pw so a receive failure sees it
		pw.CloseWithError(err)
	}()
	recvErr := l.Client.Receive(ctx, s, pr)
	// Unblock the feeder if receive stopped reading early.
	pr.CloseWithError(io.ErrClosedPipe)
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
