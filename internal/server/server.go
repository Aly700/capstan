package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// Serve owns the listener and background loops until shutdown. A Server is served
// once. Cancel ctx to close the listener, release long polls, stop maintenance,
// and give ordinary RPCs up to 25 seconds to finish before cancelling them.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	rpcCtx, stopRPCs := context.WithCancel(context.Background())
	defer stopRPCs()
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	httpServer := &http.Server{
		Handler: s, Protocols: protocols,
		ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second,
		BaseContext: func(net.Listener) context.Context { return rpcCtx },
		ErrorLog:    slog.NewLogLogger(s.logger.Handler(), slog.LevelError),
	}
	loopCtx, stopLoops := context.WithCancel(context.Background())
	defer stopLoops()
	httpServer.RegisterOnShutdown(func() {
		s.cancelPolls()
		stopLoops()
	})
	loopsDone := make(chan struct{})
	go func() { defer close(loopsDone); s.RunLoops(loopCtx) }()
	served := make(chan error, 1)
	go func() { served <- httpServer.Serve(listener) }()
	s.logger.Info("server listening", "address", listener.Addr().String())
	var serveErr error
	select {
	case serveErr = <-served:
	case <-ctx.Done():
	}
	s.draining.Store(true)
	// Shutdown owns listener closure. Its hook releases polls and loops after
	// new connections have been stopped, while ordinary RPCs keep their contexts.
	drainCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	shutdownErr := httpServer.Shutdown(drainCtx)
	if shutdownErr != nil {
		stopRPCs()
		_ = httpServer.Close()
	}
	select {
	case <-loopsDone:
	case <-drainCtx.Done():
		if shutdownErr == nil {
			shutdownErr = drainCtx.Err()
		}
	}
	if shutdownErr != nil {
		return fmt.Errorf("server shutdown: %w", shutdownErr)
	}
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) && !(ctx.Err() != nil && errors.Is(serveErr, net.ErrClosed)) {
		return fmt.Errorf("server serve: %w", serveErr)
	}
	s.logger.Info("server stopped")
	return nil
}
