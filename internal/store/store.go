// Package store defines the persistence contract every Capstan component meets at.
//
// Two implementations exist: pgstore (PostgreSQL, production) and memstore (in memory,
// used by engine unit tests and the fault lab). Both must pass the shared conformance suite
// in store/storetest. This file is a frozen contract for the build; change it only with
// a note in docs/decisions.md.
//
// Rules every implementation honours:
//
//   - Everything happens inside InTx. A Tx is never used after its function returns.
//   - InTx serialises conflicting work the way PostgreSQL does with row locks: GetRun with
//     forUpdate=true blocks other transactions that lock the same run until commit.
//   - History is append-only and gap-free. AppendEvents rejects ids that are not exactly
//     last+1, last+2, ... with ErrConflict, and nothing ever updates or deletes an event.
//   - Notify inside a transaction is delivered only if the transaction commits.
//   - Time is always passed in by the caller. Stores never read the clock.
package store

import (
	"context"
	"errors"
	"time"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
)

var (
	ErrNotFound      = errors.New("store: not found")
	ErrAlreadyExists = errors.New("store: already exists")
	// ErrConflict means a write violated an ordering invariant, for example a history
	// append whose first id is not last_event_id+1.
	ErrConflict = errors.New("store: conflict")
)

// Store is the entry point. Implementations are safe for concurrent use.
type Store interface {
	// InTx runs fn in one transaction. If fn returns an error the transaction rolls back
	// and InTx returns that error unchanged (so errors.Is works on sentinels).
	InTx(ctx context.Context, fn func(Tx) error) error

	// Subscribe returns a channel that receives a value (non-blocking, coalesced) whenever a
	// committed transaction called Notify for this kind and queue. cancel releases it.
	Subscribe(kind TaskKind, queue string) (ch <-chan struct{}, cancel func())

	// SubscribeRun returns a channel that receives a value (non-blocking, coalesced) when
	// a committed transaction called NotifyRunClosed for runID. Every subscriber for the
	// run is woken. Notifications are hints: callers must re-read durable run state and
	// retain a fallback check. cancel releases the subscription and is idempotent.
	SubscribeRun(runID string) (ch <-chan struct{}, cancel func())

	Close() error
}

// Tx is the set of operations available inside a transaction.
type Tx interface {
	// ---- runs ----

	// InsertRun creates a run. ErrAlreadyExists if run_id is taken.
	InsertRun(r *Run) error
	// GetRun returns a run. forUpdate locks it until the transaction ends. ErrNotFound.
	GetRun(runID string, forUpdate bool) (*Run, error)
	// UpdateRun overwrites every mutable field of the run identified by r.RunID.
	UpdateRun(r *Run) error
	// ListRuns returns runs ordered by run_id ascending, starting after f.AfterRunID.
	ListRuns(f RunFilter) ([]*Run, error)
	// RunsPastDeadline returns open runs (running or blocked) whose RunDeadline is set and
	// <= now, ordered by deadline, at most limit, skipping rows locked by others.
	RunsPastDeadline(now time.Time, limit int) ([]*Run, error)

	// ---- history ----

	// AppendEvents appends events whose EventId values must continue the run's history
	// contiguously. It does not update the run row; callers update LastEventID themselves
	// in the same transaction.
	AppendEvents(runID string, events []*capstanv1.HistoryEvent) error
	// ReadHistory returns events with event_id > afterEventID in order, at most limit
	// (limit <= 0 means all).
	ReadHistory(runID string, afterEventID int64, limit int) ([]*capstanv1.HistoryEvent, error)

	// ---- inbox: external events that arrived while a workflow task was in flight ----

	// PushInbox buffers an event for the run. The event's EventId is ignored and assigned
	// when the inbox is flushed into history.
	PushInbox(runID string, ev *capstanv1.HistoryEvent) error
	// DrainInbox removes and returns the run's buffered events in arrival order.
	DrainInbox(runID string) ([]*capstanv1.HistoryEvent, error)
	// InboxSize returns how many events are buffered for the run.
	InboxSize(runID string) (int, error)

	// ---- tasks ----

	// InsertTask stores a task and assigns t.ID (positive, unique, increasing).
	InsertTask(t *Task) error
	// ClaimTask atomically picks the oldest claimable task of this kind on this queue:
	// VisibleAt <= now and not leased. It sets LeasedUntil = now+lease, WorkerID and
	// StartedAt = now, persists that, and returns it. Returns (nil, nil) when none.
	// Rows locked by other transactions are skipped, never waited on.
	ClaimTask(kind TaskKind, queue string, now time.Time, lease time.Duration, workerID string) (*Task, error)
	// GetTask returns a task by id. forUpdate locks it. ErrNotFound when it no longer exists.
	GetTask(id int64, forUpdate bool) (*Task, error)
	// UpdateTask overwrites every mutable field of the task identified by t.ID.
	UpdateTask(t *Task) error
	// DeleteTask removes a task. Deleting a missing task is not an error.
	DeleteTask(id int64) error
	// RunTasks returns every task of a run, ordered by id.
	RunTasks(runID string) ([]*Task, error)
	// DueTasks returns tasks whose CheckAt is set and <= now, ordered by CheckAt, at most
	// limit, skipping rows locked by others. CheckAt is the earliest deadline the engine
	// must act on for that task (lease expiry, heartbeat, schedule-to-start,
	// schedule-to-close, or retry visibility).
	DueTasks(now time.Time, limit int) ([]*Task, error)

	// ---- timers ----

	// InsertTimer stores a timer. ErrAlreadyExists if (run_id, seq) exists.
	InsertTimer(t *Timer) error
	// DeleteTimer removes a timer and reports whether it existed.
	DeleteTimer(runID string, seq int64) (bool, error)
	// DueTimers returns timers with DueAt <= now ordered by DueAt, at most limit, skipping
	// rows locked by others.
	DueTimers(now time.Time, limit int) ([]*Timer, error)
	// RunTimers returns every timer of a run.
	RunTimers(runID string) ([]*Timer, error)

	// ---- approvals ----

	// InsertApproval stores an approval. ErrAlreadyExists if (run_id, approval_id) exists.
	InsertApproval(a *Approval) error
	// GetApproval returns an approval. forUpdate locks it. ErrNotFound.
	GetApproval(runID, approvalID string, forUpdate bool) (*Approval, error)
	// UpdateApproval overwrites every mutable field of the approval.
	UpdateApproval(a *Approval) error
	// RunApprovals returns every approval of a run.
	RunApprovals(runID string) ([]*Approval, error)
	// DueApprovals returns pending approvals whose CheckAt <= now ordered by CheckAt, at
	// most limit, skipping rows locked by others.
	DueApprovals(now time.Time, limit int) ([]*Approval, error)

	// ---- signal dedupe ----

	// RecordSignalRequest remembers (run_id, request_id). ErrAlreadyExists if seen before.
	RecordSignalRequest(runID, requestID string) error

	// ---- model-call ledger ----

	// LockBudget serialises budget reservations across the whole store until the
	// transaction ends (pg_advisory_xact_lock in PostgreSQL).
	LockBudget() error
	// InsertAICall stores a ledger row and assigns c.ID.
	InsertAICall(c *AICall) error
	// GetAICall returns a ledger row. forUpdate locks it. ErrNotFound.
	GetAICall(id int64, forUpdate bool) (*AICall, error)
	// UpdateAICall overwrites every mutable field of the ledger row.
	UpdateAICall(c *AICall) error
	// SpentSince sums CostUSD over rows with At >= since, counting reserved rows at their
	// EstimateUSD, so spend in flight counts against the cap.
	SpentSince(since time.Time) (float64, error)
	// RunCost sums CostUSD (reserved rows at EstimateUSD) for one run.
	RunCost(runID string) (float64, error)

	// ---- notifications ----

	// Notify schedules a wake-up for Subscribe(kind, queue), delivered on commit.
	Notify(kind TaskKind, queue string)
	// NotifyRunClosed schedules a wake-up for every SubscribeRun(runID) subscriber,
	// delivered only on commit. It does not change the run's state.
	NotifyRunClosed(runID string)
}

// ---- records ----

type TaskKind int8

const (
	TaskWorkflow TaskKind = 1
	TaskActivity TaskKind = 2
)

func (k TaskKind) String() string {
	switch k {
	case TaskWorkflow:
		return "workflow"
	case TaskActivity:
		return "activity"
	}
	return "unknown"
}

// Run is the mutable summary of a run. The history is the source of truth; this row is a
// projection the engine keeps in step with it inside the same transaction.
type Run struct {
	RunID        string
	WorkflowType string
	TaskQueue    string
	Status       capstanv1.RunStatus
	Input        *capstanv1.Payload
	Result       *capstanv1.Payload // set when completed
	Failure      *capstanv1.Failure // set when failed, timed out, or blocked
	TaskTimeout  time.Duration      // workflow task start-to-close
	RunTimeout   time.Duration      // 0 means none
	RunDeadline  time.Time          // zero means none; StartedAt + RunTimeout
	StartedAt    time.Time
	ClosedAt     time.Time // zero while open
	LastEventID  int64

	// WorkflowTaskID is the id of the run's scheduled-or-in-flight workflow task, 0 if none.
	// A run has at most one workflow task at a time.
	WorkflowTaskID int64
	// InFlight is true between TaskStarted and the task's completion, failure, or timeout.
	// While true, external events go to the inbox instead of the history.
	InFlight bool

	CancelRequested    bool
	ContinuedFromRunID string
	ContinuedToRunID   string
	Identity           string // who started it
}

// Open reports whether the run can still change.
func (r *Run) Open() bool {
	return r.Status == capstanv1.RunStatus_RUN_STATUS_RUNNING ||
		r.Status == capstanv1.RunStatus_RUN_STATUS_BLOCKED
}

type RunFilter struct {
	Status       capstanv1.RunStatus // UNSPECIFIED means any
	WorkflowType string              // empty means any
	AfterRunID   string
	Limit        int
}

// Task is a unit of work handed to a worker: either a workflow task or one attempt of an
// activity.
type Task struct {
	ID               int64
	Kind             TaskKind
	RunID            string
	TaskQueue        string
	ScheduledEventID int64 // TaskScheduled (workflow) or ActivityScheduled (activity)
	Attempt          int32 // 1 for the first attempt

	VisibleAt   time.Time // not claimable before this (retry backoff)
	LeasedUntil time.Time // zero when not claimed
	WorkerID    string
	StartedAt   time.Time // zero when not claimed
	ScheduledAt time.Time // when the activity (or workflow task) was first scheduled
	CheckAt     time.Time // earliest deadline the engine must act on; zero means none

	// Activity-only fields.
	Activity         *capstanv1.ActivityScheduledAttributes // the scheduled spec: type, input, timeouts, retry
	LastHeartbeatAt  time.Time
	HeartbeatDetails *capstanv1.Payload
	LastFailure      *capstanv1.Failure
	CancelRequested  bool

	// Workflow-task-only fields.
	StartedEventID int64 // TaskStarted event id while in flight
}

type Timer struct {
	RunID          string
	Seq            int64
	StartedEventID int64
	DueAt          time.Time
}

type ApprovalStatus int8

const (
	ApprovalPending  ApprovalStatus = 1
	ApprovalApproved ApprovalStatus = 2
	ApprovalDenied   ApprovalStatus = 3
	ApprovalExpired  ApprovalStatus = 4
)

type Approval struct {
	RunID            string
	ApprovalID       string
	Seq              int64
	RequestedEventID int64
	Source           capstanv1.ApprovalSource
	GateDecisionID   string
	Status           ApprovalStatus
	DueAt            time.Time // zero means no timeout
	CheckAt          time.Time // next time the engine must look: timeout or next Gate poll
	GatePolls        int32     // how many times the Gate has been polled (backoff input)
	RequestedAt      time.Time
	ResolvedAt       time.Time
	Resolver         string
	Choice           string
	Note             string
}

type AICallStatus int8

const (
	AICallReserved AICallStatus = 1
	AICallFinished AICallStatus = 2
	AICallFailed   AICallStatus = 3
)

type AICall struct {
	ID               int64
	RunID            string
	ActivitySeq      int64
	Model            string
	Status           AICallStatus
	Bounded          bool // both token bounds were priced by the server; false for legacy or unknown models
	EstimateUSD      float64
	CostUSD          float64 // 0 while reserved
	InputTokens      int64
	OutputTokens     int64
	CacheReadTokens  int64
	CacheWriteTokens int64
	ErrorCode        string
	At               time.Time // reservation time; used for the daily window
	FinishedAt       time.Time
}
