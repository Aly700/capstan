// Package engine is the heart of Capstan: it turns commands into events, keeps the run
// row in step with the history, and owns every state transition. It performs no network
// I/O inside a store transaction and reads time only through its Clock.
//
// This file is the frozen contract between the engine and the server. The concrete
// *Engine type implementing API lives in engine.go (lane: engine).
package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/proto"
)

// Error vocabulary. The server maps these to Connect codes with errors.Is; wrap them with
// fmt.Errorf("%w: detail", ErrX) to add context.
var (
	ErrNotFound           = errors.New("not found")             // CodeNotFound
	ErrAlreadyExists      = errors.New("already exists")        // CodeAlreadyExists
	ErrInvalidArgument    = errors.New("invalid argument")      // CodeInvalidArgument
	ErrStaleTask          = errors.New("stale task token")      // CodeFailedPrecondition
	ErrRunClosed          = errors.New("run is closed")         // CodeFailedPrecondition
	ErrFailedPrecondition = errors.New("failed precondition")   // CodeFailedPrecondition
	ErrBudgetExceeded     = errors.New("daily AI cap exceeded") // CodeResourceExhausted
)

// Invalid returns an ErrInvalidArgument with a message.
func Invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidArgument, fmt.Sprintf(format, args...))
}

// API is everything the server calls. Request and response types are the protocol's own,
// so the server's handlers are thin: authenticate, long-poll where needed, call, map errors.
type API interface {
	// ---- client ----
	StartRun(ctx context.Context, identity string, req *capstanv1.StartRunRequest) (*capstanv1.StartRunResponse, error)
	SignalRun(ctx context.Context, identity string, req *capstanv1.SignalRunRequest) (*capstanv1.SignalRunResponse, error)
	CancelRun(ctx context.Context, identity string, req *capstanv1.CancelRunRequest) (*capstanv1.CancelRunResponse, error)
	ResumeRun(ctx context.Context, identity string, req *capstanv1.ResumeRunRequest) (*capstanv1.ResumeRunResponse, error)
	DescribeRun(ctx context.Context, req *capstanv1.DescribeRunRequest) (*capstanv1.DescribeRunResponse, error)
	ListRuns(ctx context.Context, req *capstanv1.ListRunsRequest) (*capstanv1.ListRunsResponse, error)
	GetHistory(ctx context.Context, req *capstanv1.GetHistoryRequest) (*capstanv1.GetHistoryResponse, error)
	ResolveApproval(ctx context.Context, identity string, req *capstanv1.ResolveApprovalRequest) (*capstanv1.ResolveApprovalResponse, error)

	// ---- worker ----
	// PollWorkflowTask and PollActivityTask never block: they claim a task if one is
	// available and return found=false otherwise. The server implements the long poll.
	PollWorkflowTask(ctx context.Context, req *capstanv1.PollWorkflowTaskRequest) (resp *capstanv1.PollWorkflowTaskResponse, found bool, err error)
	CompleteWorkflowTask(ctx context.Context, req *capstanv1.CompleteWorkflowTaskRequest) (*capstanv1.CompleteWorkflowTaskResponse, error)
	FailWorkflowTask(ctx context.Context, req *capstanv1.FailWorkflowTaskRequest) (*capstanv1.FailWorkflowTaskResponse, error)
	PollActivityTask(ctx context.Context, req *capstanv1.PollActivityTaskRequest) (resp *capstanv1.PollActivityTaskResponse, found bool, err error)
	CompleteActivityTask(ctx context.Context, req *capstanv1.CompleteActivityTaskRequest) (*capstanv1.CompleteActivityTaskResponse, error)
	FailActivityTask(ctx context.Context, req *capstanv1.FailActivityTaskRequest) (*capstanv1.FailActivityTaskResponse, error)
	HeartbeatActivityTask(ctx context.Context, req *capstanv1.HeartbeatActivityTaskRequest) (*capstanv1.HeartbeatActivityTaskResponse, error)
	ReserveAICall(ctx context.Context, req *capstanv1.ReserveAICallRequest) (*capstanv1.ReserveAICallResponse, error)
	FinishAICall(ctx context.Context, req *capstanv1.FinishAICallRequest) (*capstanv1.FinishAICallResponse, error)

	// ---- background work, driven by the server's loops or by the fault lab ----
	// Each processes at most limit items, each item in its own transaction, and returns how
	// many it handled. None of them performs network I/O except ProcessDueApprovals, which
	// calls the Gate strictly outside any transaction.
	FireDueTimers(ctx context.Context, limit int) (int, error)
	ProcessDueTasks(ctx context.Context, limit int) (int, error)
	ProcessDueApprovals(ctx context.Context, limit int) (int, error)
	TimeoutRuns(ctx context.Context, limit int) (int, error)
	// NextWakeup returns the earliest time any background work becomes due, or ok=false.
	NextWakeup(ctx context.Context) (at time.Time, ok bool, err error)
}

// Clock is the engine's only source of time.
type Clock interface{ Now() time.Time }

// SystemClock reads the wall clock in UTC.
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

// GateClient is how the engine learns the fate of approvals that live in AgentOps Gate.
type GateClient interface {
	// ApprovalStatus calls GET /approvals/{id}. Status is one of PENDING, APPROVED,
	// DENIED, EXPIRED exactly as the Gate returns it.
	ApprovalStatus(ctx context.Context, approvalID string) (GateApproval, error)
}

type GateApproval struct {
	Status    string
	DecidedBy string
	DecidedAt time.Time
}

// ModelPrice is USD per million tokens.
type ModelPrice struct {
	Input      float64 `json:"input"`
	Output     float64 `json:"output"`
	CacheRead  float64 `json:"cache_read"`
	CacheWrite float64 `json:"cache_write"`
}

// DefaultModelPrices is the built-in price table (USD per million tokens), overridable with
// CAPSTAN_MODEL_PRICES. Cache reads bill at 10% of input, cache writes at 125%.
var DefaultModelPrices = map[string]ModelPrice{
	"claude-opus-5":    {Input: 5, Output: 25, CacheRead: 0.5, CacheWrite: 6.25},
	"claude-sonnet-5":  {Input: 2, Output: 10, CacheRead: 0.2, CacheWrite: 2.5},
	"claude-haiku-4-5": {Input: 1, Output: 5, CacheRead: 0.1, CacheWrite: 1.25},
	"claude-fable-5-1": {Input: 10, Output: 50, CacheRead: 0.25, CacheWrite: 12.5},
}

// Config holds the engine's tunables. Zero values are replaced by the defaults below.
type Config struct {
	DefaultTaskTimeout time.Duration          // 10s
	TaskRetryInitial   time.Duration          // 1s: backoff for failed or timed-out workflow tasks
	TaskRetryMax       time.Duration          // 60s
	DefaultRetry       *capstanv1.RetryPolicy // activities without a policy: 1s, x2, max 100s, unlimited
	MaxHistoryEvents   int64                  // 50_000: a run exceeding it fails with a clear error
	DailyCapUSD        float64                // 2.00
	CapLocation        *time.Location         // America/Toronto: defines "today" for the cap
	ModelPrices        map[string]ModelPrice  // DefaultModelPrices
	GatePollInitial    time.Duration          // 15s
	GatePollMax        time.Duration          // 5m
}

// WithDefaults returns a copy of c with every zero field set to its default.
func (c Config) WithDefaults() Config {
	if c.DefaultTaskTimeout == 0 {
		c.DefaultTaskTimeout = 10 * time.Second
	}
	if c.TaskRetryInitial == 0 {
		c.TaskRetryInitial = time.Second
	}
	if c.TaskRetryMax == 0 {
		c.TaskRetryMax = time.Minute
	}
	if c.MaxHistoryEvents == 0 {
		c.MaxHistoryEvents = 50_000
	}
	if c.DailyCapUSD == 0 {
		c.DailyCapUSD = 2.00
	}
	if c.CapLocation == nil {
		loc, err := time.LoadLocation("America/Toronto")
		if err != nil {
			loc = time.UTC
		}
		c.CapLocation = loc
	}
	if c.ModelPrices == nil {
		c.ModelPrices = DefaultModelPrices
	}
	if c.GatePollInitial == 0 {
		c.GatePollInitial = 15 * time.Second
	}
	if c.GatePollMax == 0 {
		c.GatePollMax = 5 * time.Minute
	}
	return c
}

// Deps are the engine's collaborators. Gate may be nil; GATE approvals then only resolve
// by timeout or ResolveApproval.
type Deps struct {
	Store store.Store
	Clock Clock
	Gate  GateClient
}

// EncodeToken serialises a task token for a worker.
func EncodeToken(t *capstanv1.TaskToken) []byte {
	b, err := proto.Marshal(t)
	if err != nil {
		panic(fmt.Sprintf("engine: marshal task token: %v", err))
	}
	return b
}

// DecodeToken parses a worker-supplied token. Malformed input is ErrInvalidArgument.
func DecodeToken(b []byte) (*capstanv1.TaskToken, error) {
	if len(b) == 0 {
		return nil, Invalid("empty task token")
	}
	t := &capstanv1.TaskToken{}
	if err := proto.Unmarshal(b, t); err != nil {
		return nil, Invalid("malformed task token")
	}
	if t.GetRunId() == "" || t.GetTaskId() <= 0 {
		return nil, Invalid("incomplete task token")
	}
	return t, nil
}

// IdempotencyKey is the stable key handed to every attempt of one activity.
func IdempotencyKey(runID string, seq int64) string {
	return fmt.Sprintf("%s/%d", runID, seq)
}
