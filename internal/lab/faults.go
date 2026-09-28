package lab

import (
	"context"
	"errors"
	"sync"
	"time"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
)

var (
	// ErrInjected is a database failure raised after an operation or before commit.
	ErrInjected = errors.New("lab: injected database failure")
	// ErrServerCrash is the panic value for a simulated server crash. Recover outside
	// InTx so the underlying store has unwound and discarded its transaction first.
	ErrServerCrash = errors.New("lab: injected server crash")
)

// FaultStore adds one-shot transaction faults to a store. Unarmed transactions and
// subscriptions retain the underlying store's behavior. It is safe for concurrent use.
type FaultStore struct {
	store.Store
	mu    sync.Mutex
	armed *faultArm
	hits  int
}

type faultArm struct {
	transaction int
	after       int
	crash       bool
}

type faultTx struct {
	base      store.Tx
	owner     *FaultStore
	remaining int
	crash     bool
	fired     bool
}

var _ store.Store = (*FaultStore)(nil)
var _ store.Tx = (*faultTx)(nil)

// NewFaultStore wraps base without changing its ownership or lifetime.
func NewFaultStore(base store.Store) *FaultStore { return &FaultStore{Store: base} }

// Arm replaces the pending fault. The next transaction callback to enter consumes it.
// The fault fires once that many Tx calls succeed, including reads and Notify. If the
// callback succeeds sooner, it fires before commit instead. A callback that returns an
// earlier error consumes the arm without injecting. after must be positive.
//
// Both failure kinds occur after the underlying operation, permitting a mutation to
// complete before rollback. Database faults return ErrInjected; crash faults panic with
// ErrServerCrash. Notify has no error return, so its database faults panic with ErrInjected.
func (s *FaultStore) Arm(after int, crash bool) {
	s.ArmTransaction(1, after, crash)
}

// ArmTransaction targets the given upcoming transaction callback (one-based).
// Earlier callbacks run unchanged and count even when they return an error. Calls
// rejected before entering their callback do not count. The selected callback uses
// Arm's operation-count and rollback rules; Disarm cancels an unreached selection.
// Both transaction and after must be positive.
func (s *FaultStore) ArmTransaction(transaction, after int, crash bool) {
	if transaction < 1 {
		panic("lab: fault transaction count must be positive")
	}
	if after < 1 {
		panic("lab: fault operation count must be positive")
	}
	s.mu.Lock()
	s.armed = &faultArm{transaction: transaction, after: after, crash: crash}
	s.mu.Unlock()
}

// Disarm cancels a pending arm. An arm already consumed by a transaction is unchanged.
func (s *FaultStore) Disarm() {
	s.mu.Lock()
	s.armed = nil
	s.mu.Unlock()
}

// Hits returns the total number of faults injected since this wrapper was created.
func (s *FaultStore) Hits() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits
}

// InTx leaves rollback and panic unwinding to the underlying store.
func (s *FaultStore) InTx(ctx context.Context, fn func(store.Tx) error) error {
	return s.Store.InTx(ctx, func(base store.Tx) error {
		s.mu.Lock()
		arm := s.armed
		if arm != nil {
			arm.transaction--
			if arm.transaction > 0 {
				arm = nil
			} else {
				s.armed = nil
			}
		}
		s.mu.Unlock()
		if arm == nil {
			return fn(base)
		}
		tx := &faultTx{base: base, owner: s, remaining: arm.after, crash: arm.crash}
		if err := fn(tx); err != nil {
			return err
		}
		// Also reject commit if the callback ignored an injected error.
		return tx.inject()
	})
}

func (tx *faultTx) inject() error {
	if !tx.fired {
		tx.fired = true
		tx.owner.mu.Lock()
		tx.owner.hits++
		tx.owner.mu.Unlock()
	}
	if tx.crash {
		panic(ErrServerCrash)
	}
	return ErrInjected
}

func (tx *faultTx) after(err error) error {
	if err != nil || tx.fired {
		return err
	}
	tx.remaining--
	if tx.remaining == 0 {
		return tx.inject()
	}
	return nil
}

func (tx *faultTx) InsertRun(r *store.Run) error { return tx.after(tx.base.InsertRun(r)) }
func (tx *faultTx) GetRun(id string, forUpdate bool) (*store.Run, error) {
	value, err := tx.base.GetRun(id, forUpdate)
	return value, tx.after(err)
}
func (tx *faultTx) UpdateRun(r *store.Run) error { return tx.after(tx.base.UpdateRun(r)) }
func (tx *faultTx) ListRuns(filter store.RunFilter) ([]*store.Run, error) {
	value, err := tx.base.ListRuns(filter)
	return value, tx.after(err)
}
func (tx *faultTx) RunsPastDeadline(now time.Time, limit int) ([]*store.Run, error) {
	value, err := tx.base.RunsPastDeadline(now, limit)
	return value, tx.after(err)
}
func (tx *faultTx) AppendEvents(id string, events []*capstanv1.HistoryEvent) error {
	return tx.after(tx.base.AppendEvents(id, events))
}
func (tx *faultTx) ReadHistory(id string, after int64, limit int) ([]*capstanv1.HistoryEvent, error) {
	value, err := tx.base.ReadHistory(id, after, limit)
	return value, tx.after(err)
}
func (tx *faultTx) PushInbox(id string, event *capstanv1.HistoryEvent) error {
	return tx.after(tx.base.PushInbox(id, event))
}
func (tx *faultTx) DrainInbox(id string) ([]*capstanv1.HistoryEvent, error) {
	value, err := tx.base.DrainInbox(id)
	return value, tx.after(err)
}
func (tx *faultTx) InboxSize(id string) (int, error) {
	value, err := tx.base.InboxSize(id)
	return value, tx.after(err)
}
func (tx *faultTx) InsertTask(task *store.Task) error { return tx.after(tx.base.InsertTask(task)) }
func (tx *faultTx) ClaimTask(kind store.TaskKind, queue string, now time.Time, lease time.Duration, worker string) (*store.Task, error) {
	value, err := tx.base.ClaimTask(kind, queue, now, lease, worker)
	return value, tx.after(err)
}
func (tx *faultTx) GetTask(id int64, forUpdate bool) (*store.Task, error) {
	value, err := tx.base.GetTask(id, forUpdate)
	return value, tx.after(err)
}
func (tx *faultTx) UpdateTask(task *store.Task) error { return tx.after(tx.base.UpdateTask(task)) }
func (tx *faultTx) DeleteTask(id int64) error         { return tx.after(tx.base.DeleteTask(id)) }
func (tx *faultTx) RunTasks(id string) ([]*store.Task, error) {
	value, err := tx.base.RunTasks(id)
	return value, tx.after(err)
}
func (tx *faultTx) DueTasks(now time.Time, limit int) ([]*store.Task, error) {
	value, err := tx.base.DueTasks(now, limit)
	return value, tx.after(err)
}
func (tx *faultTx) InsertTimer(timer *store.Timer) error { return tx.after(tx.base.InsertTimer(timer)) }
func (tx *faultTx) DeleteTimer(id string, seq int64) (bool, error) {
	value, err := tx.base.DeleteTimer(id, seq)
	return value, tx.after(err)
}
func (tx *faultTx) DueTimers(now time.Time, limit int) ([]*store.Timer, error) {
	value, err := tx.base.DueTimers(now, limit)
	return value, tx.after(err)
}
func (tx *faultTx) RunTimers(id string) ([]*store.Timer, error) {
	value, err := tx.base.RunTimers(id)
	return value, tx.after(err)
}
func (tx *faultTx) InsertApproval(approval *store.Approval) error {
	return tx.after(tx.base.InsertApproval(approval))
}
func (tx *faultTx) GetApproval(id, approvalID string, forUpdate bool) (*store.Approval, error) {
	value, err := tx.base.GetApproval(id, approvalID, forUpdate)
	return value, tx.after(err)
}
func (tx *faultTx) UpdateApproval(approval *store.Approval) error {
	return tx.after(tx.base.UpdateApproval(approval))
}
func (tx *faultTx) RunApprovals(id string) ([]*store.Approval, error) {
	value, err := tx.base.RunApprovals(id)
	return value, tx.after(err)
}
func (tx *faultTx) DueApprovals(now time.Time, limit int) ([]*store.Approval, error) {
	value, err := tx.base.DueApprovals(now, limit)
	return value, tx.after(err)
}
func (tx *faultTx) RecordSignalRequest(id, requestID string) error {
	return tx.after(tx.base.RecordSignalRequest(id, requestID))
}
func (tx *faultTx) LockBudget() error { return tx.after(tx.base.LockBudget()) }
func (tx *faultTx) InsertAICall(call *store.AICall) error {
	return tx.after(tx.base.InsertAICall(call))
}
func (tx *faultTx) GetAICall(id int64, forUpdate bool) (*store.AICall, error) {
	value, err := tx.base.GetAICall(id, forUpdate)
	return value, tx.after(err)
}
func (tx *faultTx) UpdateAICall(call *store.AICall) error {
	return tx.after(tx.base.UpdateAICall(call))
}
func (tx *faultTx) SpentSince(since time.Time) (float64, error) {
	value, err := tx.base.SpentSince(since)
	return value, tx.after(err)
}
func (tx *faultTx) RunCost(id string) (float64, error) {
	value, err := tx.base.RunCost(id)
	return value, tx.after(err)
}
func (tx *faultTx) Notify(kind store.TaskKind, queue string) {
	tx.base.Notify(kind, queue)
	if err := tx.after(nil); err != nil {
		panic(err)
	}
}
