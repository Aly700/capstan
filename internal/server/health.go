package server

import (
	"context"
	"net/http"
	"time"
)

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("ok\n"))
}
func (s *Server) readiness(w http.ResponseWriter, r *http.Request) {
	if s.ready == nil || s.draining.Load() {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := s.ready(ctx); err != nil || ctx.Err() != nil {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	s.health(w, r)
}
