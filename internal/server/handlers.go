// Package server exposes the engine through authenticated Connect services.
package server

import (
	"context"
	"log/slog"
	"maps"
	"net/http"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	rpc "github.com/Aly700/capstan/gen/capstan/v1/capstanv1connect"
	"github.com/Aly700/capstan/internal/config"
	"github.com/Aly700/capstan/internal/engine"
	"github.com/Aly700/capstan/internal/server/ui"
	"github.com/Aly700/capstan/internal/store"
)

// Notifier is the notification portion of store.Store. A nil notifier is useful
// for polling-only backends; the periodic re-check still discovers tasks.
type Notifier interface {
	Subscribe(kind store.TaskKind, queue string) (<-chan struct{}, func())
	SubscribeRun(runID string) (<-chan struct{}, func())
}

var _ Notifier = (store.Store)(nil)

type Options struct {
	Logger *slog.Logger
	Ready  func(context.Context) error
}

type Server struct {
	metrics   *metrics
	ready     func(context.Context) error
	pollStop  context.Context
	stopPolls context.CancelFunc
	draining  atomic.Bool
	cfg       config.Config
	api       engine.API
	notifier  Notifier
	logger    *slog.Logger
	handler   http.Handler
}

// New mounts both services without opening a listener or starting background work.
// The API, notifier and logger remain independent of any concrete store.
func New(cfg config.Config, api engine.API, notifier Notifier, opts Options) *Server {
	if cfg.PollTimeout <= 0 {
		cfg.PollTimeout = 20 * time.Second
	}
	if cfg.PollTimeout > 30*time.Second {
		cfg.PollTimeout = 30 * time.Second
	}
	if cfg.MaxMessageBytes <= 0 {
		cfg.MaxMessageBytes = 4 << 20
	}
	cfg.APIKeyHashes = maps.Clone(cfg.APIKeyHashes)
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{cfg: cfg, api: api, notifier: notifier, logger: logger}
	s.metrics = newMetrics()
	s.ready = opts.Ready
	s.pollStop, s.stopPolls = context.WithCancel(context.Background())
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.health)
	mux.HandleFunc("/readyz", s.readiness)
	mux.Handle("/metrics", s.metrics)
	mux.Handle("/ui/", ui.NewHandler())
	options := []connect.HandlerOption{connect.WithReadMaxBytes(cfg.MaxMessageBytes), connect.WithInterceptors(s.measure(), s.authenticate(), s.errors())}
	mux.Handle(rpc.NewWorkerServiceHandler(s, options...))
	mux.Handle(rpc.NewClientServiceHandler(s, options...))
	s.handler = mux
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.handler.ServeHTTP(w, r) }

func (s *Server) cancelPolls() { s.stopPolls() }
