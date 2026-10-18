package zfs

import (
	"context"
	"io"
	"strconv"
	"strings"
)

// minEstimate is the smallest size reported for a known, non-empty stream.
const minEstimate = 4096

// estimateAllowedBookmark is the set of options valid both for sending from
// a bookmark and for a size estimate of a stream.
var estimateAllowedBookmark = intersect(SendAllowedBookmark, EstimateAllowedStream)

func intersect(a, b string) string {
	var sb strings.Builder
	for i := 0; i < len(a); i++ {
		if strings.IndexByte(b, a[i]) >= 0 {
			sb.WriteByte(a[i])
		}
	}
	return sb.String()
}

// SendSpec describes one zfs send. Options are unfiltered; SendArgs and
// EstimateArgs apply the allow-list matching the kind of send.
type SendSpec struct {
	Dataset string
	// To is the target snapshot short name.
	To string
	// FromSnap, FromBookmark and FromOrigin (a full snapshot name) select
	// the incremental source; at most one is set.
	FromSnap     string
	FromBookmark string
	FromOrigin   string
	// Intermediates sends -I instead of -i for a snapshot-based increment.
	Intermediates bool
	// ResumeToken, when set, sends with -t and ignores the other fields.
	ResumeToken string
	Options     []Opt
}

// SendArgs returns the zfs send argv (without the leading "zfs").
func SendArgs(s SendSpec) []string {
	allowed := SendAllowedStream
	switch {
	case s.ResumeToken != "":
		allowed = SendAllowedResume
	case s.FromBookmark != "":
		allowed = SendAllowedBookmark
	}
	return append(append([]string{"send"}, FilterOpts(s.Options, allowed)...), streamArgs(s)...)
}

// EstimateArgs returns the zfs send -nvP argv (without the leading "zfs")
// that reports the size of the stream SendArgs would produce.
func EstimateArgs(s SendSpec) []string {
	allowed := EstimateAllowedStream
	switch {
	case s.ResumeToken != "":
		allowed = EstimateAllowedResume
	case s.FromBookmark != "":
		allowed = estimateAllowedBookmark
	}
	args := append([]string{"send"}, FilterOpts(s.Options, allowed)...)
	return append(append(args, "-nvP"), streamArgs(s)...)
}

// streamArgs returns the stream-selecting arguments of a send.
func streamArgs(s SendSpec) []string {
	snap := s.Dataset + "@" + s.To
	inc := "-i"
	if s.Intermediates {
		inc = "-I"
	}
	switch {
	case s.ResumeToken != "":
		return []string{"-t", s.ResumeToken}
	case s.FromBookmark != "":
		return []string{"-i", s.Dataset + "#" + s.FromBookmark, snap}
	case s.FromSnap != "":
		return []string{inc, s.Dataset + "@" + s.FromSnap, snap}
	case s.FromOrigin != "":
		return []string{"-i", s.FromOrigin, snap}
	}
	return []string{snap}
}

// RecvSpec describes one zfs receive; Args are the already-built flags.
type RecvSpec struct {
	Dataset string
	Args    []string
}

// Send runs zfs send for s, writing the stream to w.
func (c *Client) Send(ctx context.Context, s SendSpec, w io.Writer) error {
	return c.Exec.Run(ctx, Cmd{Argv: c.Argv(SendArgs(s)...), Stdout: w})
}

// Receive runs zfs receive for s, reading the stream from r.
func (c *Client) Receive(ctx context.Context, s RecvSpec, r io.Reader) error {
	args := append([]string{"receive"}, s.Args...)
	return c.Exec.Run(ctx, Cmd{Argv: c.Argv(append(args, s.Dataset)...), Stdin: r})
}

// SendEstimate returns the estimated stream size in bytes, at least 4096
// when known; 0 means unknown, including when the command fails.
func (c *Client) SendEstimate(ctx context.Context, s SendSpec) (int64, error) {
	out, err := c.Output(ctx, EstimateArgs(s)...)
	if err != nil {
		return 0, nil
	}
	return parseEstimate(out, s.ResumeToken != ""), nil
}

// parseEstimate reads the size from the last line of zfs send -nvP output.
func parseEstimate(out string, resume bool) int64 {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	last := strings.TrimSpace(lines[len(lines)-1])
	if resume {
		if i := strings.LastIndexByte(last, '\t'); i >= 0 {
			last = last[i+1:]
		}
	} else {
		last = strings.TrimSpace(strings.TrimPrefix(last, "size"))
	}
	n, err := strconv.ParseInt(last, 10, 64)
	switch {
	case err != nil || n <= 0:
		return 0
	case n < minEstimate:
		return minEstimate
	}
	return n
}
