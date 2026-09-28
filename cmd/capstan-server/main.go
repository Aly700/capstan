// Command capstan-server serves Capstan, applies migrations, and manages API keys.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	// Keep the daily budget's America/Toronto boundary available in minimal images.
	_ "time/tzdata"
)

func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
}
func main() {
	ctx, stop := signalContext()
	defer stop()
	if err := run(ctx, os.Args[1:], os.Getenv, os.Stdin, os.Stdout, os.Stderr, productionDependencies()); err != nil {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error("capstan-server failed", "error", err)
		os.Exit(1)
	}
}
