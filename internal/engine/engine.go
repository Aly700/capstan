// Engine implements the frozen API over transactional storage.
package engine

import (
	"context"
	"errors"
	"maps"
	"math"
	"time"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
	"google.golang.org/protobuf/proto"
)

// Engine implements API over a store.Store.
type Engine struct {
	deps Deps
	cfg  Config
}

// New builds an engine. deps.Store is required; deps.Clock defaults to SystemClock.
func New(deps Deps, cfg Config) (*Engine, error) {
	if deps.Store == nil {
		return nil, errors.New("engine: store is required")
	}
	if deps.Clock == nil {
		deps.Clock = SystemClock{}
	}
	cfg = cfg.WithDefaults()
	if cfg.DefaultTaskTimeout <= 0 || cfg.DefaultTaskTimeout > 10*time.Minute || cfg.TaskRetryInitial <= 0 || cfg.TaskRetryMax <= 0 || cfg.MaxHistoryEvents <= 0 || cfg.GatePollInitial <= 0 || cfg.GatePollMax <= 0 || cfg.DailyCapUSD <= 0 || math.IsNaN(cfg.DailyCapUSD) || math.IsInf(cfg.DailyCapUSD, 0) || !validRetry(cfg.DefaultRetry) {
		return nil, Invalid("invalid engine configuration")
	}
	for _, p := range cfg.ModelPrices {
		for _, rate := range []float64{p.Input, p.Output, p.CacheRead, p.CacheWrite} {
			if rate < 0 || math.IsNaN(rate) || math.IsInf(rate, 0) {
				return nil, Invalid("invalid model price")
			}
		}
	}
	cfg.ModelPrices = maps.Clone(cfg.ModelPrices)
	if cfg.DefaultRetry != nil {
		cfg.DefaultRetry = proto.Clone(cfg.DefaultRetry).(*capstanv1.RetryPolicy)
	}
	return &Engine{deps: deps, cfg: cfg}, nil
}

var _ API = (*Engine)(nil)

var errNotImplemented = errors.New("engine: not implemented yet (engine lane)")

func (e *Engine) ResolveApproval(ctx context.Context, identity string, req *capstanv1.ResolveApprovalRequest) (*capstanv1.ResolveApprovalResponse, error) {
	return nil, errNotImplemented
}

func (e *Engine) ReserveAICall(ctx context.Context, req *capstanv1.ReserveAICallRequest) (*capstanv1.ReserveAICallResponse, error) {
	return nil, errNotImplemented
}

func (e *Engine) FinishAICall(ctx context.Context, req *capstanv1.FinishAICallRequest) (*capstanv1.FinishAICallResponse, error) {
	return nil, errNotImplemented
}

func (e *Engine) ProcessDueApprovals(ctx context.Context, limit int) (int, error) {
	return 0, errNotImplemented
}

func (e *Engine) TimeoutRuns(ctx context.Context, limit int) (int, error) {
	return 0, errNotImplemented
}

func (e *Engine) NextWakeup(ctx context.Context) (at time.Time, ok bool, err error) {
	return time.Time{}, false, errNotImplemented
}
