// Command syngoid replicates ZFS datasets; it is a Go rewrite of syncoid.
package main

import (
	"context"
	"os"
	"time"

	"github.com/piotaixr/syngoid/internal/app"
)

func main() {
	env := app.Env{
		Stdin:       os.Stdin,
		Stdout:      os.Stdout,
		Stderr:      os.Stderr,
		StdinIsTTY:  isTTY(os.Stdin),
		StderrIsTTY: isTTY(os.Stderr),
		Hostname:    os.Hostname,
		Now:         time.Now,
	}
	os.Exit(app.Main(context.Background(), os.Args[1:], env))
}

// isTTY reports whether f is a character device (a terminal).
func isTTY(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}
