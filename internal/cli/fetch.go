package cli

import (
	"errors"
	"io"

	"github.com/spf13/pflag"

	"github.com/piotaixr/syngoid/internal/platform"
)

// FetchOptions holds the settings of the fetch command.
type FetchOptions struct {
	Platforms    []platform.Platform
	All          bool
	Dir          string
	DownloadURL  string
	Quiet, Debug bool
}

// newFetchFlagSet declares the fetch flags, storing into platforms and f.
func newFetchFlagSet(platforms *[]string, f *FetchOptions) *pflag.FlagSet {
	fs := pflag.NewFlagSet("syngoid fetch", pflag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.SortFlags = false
	fs.StringArrayVar(platforms, "platform", nil, "platform to fetch, as OS/ARCH (repeatable)")
	fs.BoolVar(&f.All, "all", false, "fetch every release platform")
	fs.StringVar(&f.Dir, "dir", "", "store directory (default: the user cache)")
	fs.StringVar(&f.DownloadURL, "download-url", defaultDownloadURL, "agent download URL template")
	fs.BoolVar(&f.Quiet, "quiet", false, "only log warnings and errors")
	fs.BoolVar(&f.Debug, "debug", false, "debug logging")
	return fs
}

// FetchUsage returns the flag descriptions for the fetch help text.
func FetchUsage() string {
	var (
		platforms []string
		f         FetchOptions
	)
	return newFetchFlagSet(&platforms, &f).FlagUsages()
}

// parseFetch parses the arguments after the fetch subcommand. For --help it
// returns ErrHelp together with Command set to CmdFetch, so the caller can
// print the fetch usage.
func parseFetch(args []string) (Parsed, error) {
	var (
		platforms []string
		f         FetchOptions
	)
	fs := newFetchFlagSet(&platforms, &f)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, pflag.ErrHelp) {
			return Parsed{Command: CmdFetch}, err
		}
		return Parsed{}, err
	}
	if fs.NArg() > 0 {
		return Parsed{}, errors.New("fetch takes no arguments")
	}
	if f.All == (len(platforms) > 0) {
		return Parsed{}, errors.New("fetch needs --platform OS/ARCH or --all")
	}
	for _, s := range platforms {
		p, err := platform.Parse(s)
		if err != nil {
			return Parsed{}, err
		}
		f.Platforms = append(f.Platforms, p)
	}
	return Parsed{Command: CmdFetch, Fetch: &f}, nil
}
