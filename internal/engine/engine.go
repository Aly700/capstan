// Engine is the concrete implementation of API. Implemented by the engine lane; this file is
// a compile-time stub so other lanes can build against engine.New from the start.
package engine

import (
	"context"
	"errors"
	"time"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
)

var errNotImplemented = errors.New("engine: not implemented yet (engine lane)")

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
	return &Engine{deps: deps, cfg: cfg.WithDefaults()}, nil
}

var _ API = (*Engine)(nil)

func (e *Engine) StartRun(ctx context.Context, identity string, req *capstanv1.StartRunRequest) (*capstanv1.StartRunResponse, error) {
	return nil, errNotImplemented
}

func (e *Engine) SignalRun(ctx context.Context, identity string, req *capstanv1.SignalRunRequest) (*capstanv1.SignalRunResponse, error) {
	return nil, errNotImplemented
}

func (e *Engine) CancelRun(ctx context.Context, identity string, req *capstanv1.CancelRunRequest) (*capstanv1.CancelRunResponse, error) {
	return nil, errNotImplemented
}

func (e *Engine) ResumeRun(ctx context.Context, identity string, req *capstanv1.ResumeRunRequest) (*capstanv1.ResumeRunResponse, error) {
	return nil, errNotImplemented
}

func (e *Engine) DescribeRun(ctx context.Context, req *capstanv1.DescribeRunRequest) (*capstanv1.DescribeRunResponse, error) {
	return nil, errNotImplemented
}

func (e *Engine) ListRuns(ctx context.Context, req *capstanv1.ListRunsRequest) (*capstanv1.ListRunsResponse, error) {
	return nil, errNotImplemented
}

func (e *Engine) GetHistory(ctx context.Context, req *capstanv1.GetHistoryRequest) (*capstanv1.GetHistoryResponse, error) {
	return nil, errNotImplemented
}

func (e *Engine) ResolveApproval(ctx context.Context, identity string, req *capstanv1.ResolveApprovalRequest) (*capstanv1.ResolveApprovalResponse, error) {
	return nil, errNotImplemented
}

func (e *Engine) PollWorkflowTask(ctx context.Context, req *capstanv1.PollWorkflowTaskRequest) (resp *capstanv1.PollWorkflowTaskResponse, found bool, err error) {
	return nil, false, errNotImplemented
}

func (e *Engine) CompleteWorkflowTask(ctx context.Context, req *capstanv1.CompleteWorkflowTaskRequest) (*capstanv1.CompleteWorkflowTaskResponse, error) {
	return nil, errNotImplemented
}

func (e *Engine) FailWorkflowTask(ctx context.Context, req *capstanv1.FailWorkflowTaskRequest) (*capstanv1.FailWorkflowTaskResponse, error) {
	return nil, errNotImplemented
}

func (e *Engine) PollActivityTask(ctx context.Context, req *capstanv1.PollActivityTaskRequest) (resp *capstanv1.PollActivityTaskResponse, found bool, err error) {
	return nil, false, errNotImplemented
}

func (e *Engine) CompleteActivityTask(ctx context.Context, req *capstanv1.CompleteActivityTaskRequest) (*capstanv1.CompleteActivityTaskResponse, error) {
	return nil, errNotImplemented
}

func (e *Engine) FailActivityTask(ctx context.Context, req *capstanv1.FailActivityTaskRequest) (*capstanv1.FailActivityTaskResponse, error) {
	return nil, errNotImplemented
}

func (e *Engine) HeartbeatActivityTask(ctx context.Context, req *capstanv1.HeartbeatActivityTaskRequest) (*capstanv1.HeartbeatActivityTaskResponse, error) {
	return nil, errNotImplemented
}

func (e *Engine) ReserveAICall(ctx context.Context, req *capstanv1.ReserveAICallRequest) (*capstanv1.ReserveAICallResponse, error) {
	return nil, errNotImplemented
}

func (e *Engine) FinishAICall(ctx context.Context, req *capstanv1.FinishAICallRequest) (*capstanv1.FinishAICallResponse, error) {
	return nil, errNotImplemented
}

func (e *Engine) FireDueTimers(ctx context.Context, limit int) (int, error) {
	return 0, errNotImplemented
}

func (e *Engine) ProcessDueTasks(ctx context.Context, limit int) (int, error) {
	return 0, errNotImplemented
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
