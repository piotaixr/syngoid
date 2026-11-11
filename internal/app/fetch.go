package app

import (
	"context"
	"fmt"

	"github.com/piotaixr/syngoid/internal/buildinfo"
	"github.com/piotaixr/syngoid/internal/cli"
	"github.com/piotaixr/syngoid/internal/dist"
	"github.com/piotaixr/syngoid/internal/logx"
	"github.com/piotaixr/syngoid/internal/platform"
)

// runFetch implements the fetch command: it pre-seeds verified agent
// binaries for this exact version into the user cache or f.Dir. Every
// requested platform is attempted; any failure makes the exit code 2.
func runFetch(ctx context.Context, env Env, f *cli.FetchOptions) int {
	if buildinfo.IsDev() {
		return criticalf(env, "fetch needs a release build")
	}
	log := logx.New(logx.Config{Stdout: env.Stdout, Stderr: env.Stderr, Quiet: f.Quiet, Debug: f.Debug})
	var store *dist.Store
	if f.Dir != "" {
		store = &dist.Store{Dir: f.Dir, Version: buildinfo.Version, URLTemplate: f.DownloadURL, Log: log}
	} else {
		var err error
		if store, err = dist.NewUserCache(buildinfo.Version, f.DownloadURL, log); err != nil {
			return criticalf(env, "%v", err)
		}
	}
	platforms := f.Platforms
	if f.All {
		platforms = platform.Supported()
	}
	code := 0
	for _, p := range platforms {
		if _, ok := store.Lookup(p); ok {
			log.Info(fmt.Sprintf("%s already present", p))
			continue
		}
		if _, err := store.Download(ctx, p); err != nil {
			log.Log(ctx, logx.LevelCritical, fmt.Sprintf("%s: %v", p, err))
			code = 2
		}
	}
	return code
}
