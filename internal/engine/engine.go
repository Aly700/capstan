// Engine implements the frozen API over transactional storage.
package engine

import (
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
