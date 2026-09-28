// Package memstore implements store.Store with serial transactions and isolated snapshots.
// It backs engine unit tests and the fault lab.
package memstore

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/proto"
)

var errClosed = errors.New("memstore: closed")

type queueKey struct {
	kind  store.TaskKind
	queue string
}
type timerKey struct {
	runID string
	seq   int64
}
type stringKey struct{ runID, id string }
type memory struct {
	mu                     sync.Mutex
	active                 bool
	ready                  chan struct{}
	closed                 bool
	data                   *snapshot
	nextTaskID, nextCallID int64
	subs                   map[queueKey]map[chan struct{}]struct{}
}
type snapshot struct {
	runs      map[string]*store.Run
	history   map[string][]*capstanv1.HistoryEvent
	inbox     map[string][]*capstanv1.HistoryEvent
	tasks     map[int64]*store.Task
	timers    map[timerKey]*store.Timer
	approvals map[stringKey]*store.Approval
	signals   map[stringKey]struct{}
	calls     map[int64]*store.AICall
}
type transaction struct {
	ctx           context.Context
	owner         *memory
	data          *snapshot
	notifications map[queueKey]struct{}
}

var _ store.Store = (*memory)(nil)
var _ store.Tx = (*transaction)(nil)

// New returns an empty in-memory store.
func New() store.Store {
	return &memory{
		ready: make(chan struct{}),
		data: &snapshot{
			runs:      make(map[string]*store.Run),
			history:   make(map[string][]*capstanv1.HistoryEvent),
			inbox:     make(map[string][]*capstanv1.HistoryEvent),
			tasks:     make(map[int64]*store.Task),
			timers:    make(map[timerKey]*store.Timer),
			approvals: make(map[stringKey]*store.Approval),
			signals:   make(map[stringKey]struct{}),
			calls:     make(map[int64]*store.AICall),
		},
		subs: make(map[queueKey]map[chan struct{}]struct{}),
	}
}

// begin uses one global mutex to serialize transactions. Waiting on ready instead of
// holding the mutex lets cancellation and subscription changes proceed during a callback.
func (s *memory) begin(ctx context.Context) (*snapshot, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return nil, errClosed
		}
		if !s.active {
			s.active = true
			data := s.data.clone()
			s.mu.Unlock()
			return data, nil
		}
		ready := s.ready
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ready:
		}
	}
}
func (s *memory) wakeLocked() {
	close(s.ready)
	s.ready = make(chan struct{})
}
func (s *memory) InTx(ctx context.Context, fn func(store.Tx) error) error {
	data, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		s.mu.Lock()
		s.active = false
		s.wakeLocked()
		s.mu.Unlock()
	}()
	tx := &transaction{ctx: ctx, owner: s, data: data, notifications: make(map[queueKey]struct{})}
	if err := fn(tx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return errClosed
	}
	s.data = data
	for key := range tx.notifications {
		for ch := range s.subs[key] {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}
	return nil
}
func (s *memory) Subscribe(kind store.TaskKind, queue string) (<-chan struct{}, func()) {
	key := queueKey{kind, queue}
	ch := make(chan struct{}, 1)
	s.mu.Lock()
	if s.closed {
		close(ch)
		s.mu.Unlock()
		return ch, func() {}
	}
	if s.subs[key] == nil {
		s.subs[key] = make(map[chan struct{}]struct{})
	}
	s.subs[key][ch] = struct{}{}
	s.mu.Unlock()
	var once sync.Once
	return ch, func() {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if _, ok := s.subs[key][ch]; ok {
				delete(s.subs[key], ch)
				close(ch)
				if len(s.subs[key]) == 0 {
					delete(s.subs, key)
				}
			}
		})
	}
}
func (s *memory) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.wakeLocked()
	for _, subscribers := range s.subs {
		for ch := range subscribers {
			close(ch)
		}
	}
	clear(s.subs)
	return nil
}
func cloneMessage[T proto.Message](v T) T {
	if !v.ProtoReflect().IsValid() {
		return v
	}
	return proto.Clone(v).(T)
}
func cloneRun(v *store.Run) *store.Run {
	c := *v
	c.Input = cloneMessage(v.Input)
	c.Result = cloneMessage(v.Result)
	c.Failure = cloneMessage(v.Failure)
	c.RunDeadline = c.RunDeadline.UTC()
	c.StartedAt = c.StartedAt.UTC()
	c.ClosedAt = c.ClosedAt.UTC()
	return &c
}
func cloneTask(v *store.Task) *store.Task {
	c := *v
	c.Activity = cloneMessage(v.Activity)
	c.HeartbeatDetails = cloneMessage(v.HeartbeatDetails)
	c.LastFailure = cloneMessage(v.LastFailure)
	c.VisibleAt = c.VisibleAt.UTC()
	c.LeasedUntil = c.LeasedUntil.UTC()
	c.StartedAt = c.StartedAt.UTC()
	c.ScheduledAt = c.ScheduledAt.UTC()
	c.CheckAt = c.CheckAt.UTC()
	c.LastHeartbeatAt = c.LastHeartbeatAt.UTC()
	return &c
}
func cloneTimer(v *store.Timer) *store.Timer {
	c := *v
	c.DueAt = c.DueAt.UTC()
	return &c
}
func cloneApproval(v *store.Approval) *store.Approval {
	c := *v
	c.DueAt = c.DueAt.UTC()
	c.CheckAt = c.CheckAt.UTC()
	c.RequestedAt = c.RequestedAt.UTC()
	c.ResolvedAt = c.ResolvedAt.UTC()
	return &c
}
func cloneCall(v *store.AICall) *store.AICall {
	c := *v
	c.At = c.At.UTC()
	c.FinishedAt = c.FinishedAt.UTC()
	return &c
}
func cloneEvents(v []*capstanv1.HistoryEvent) []*capstanv1.HistoryEvent {
	out := make([]*capstanv1.HistoryEvent, len(v))
	for i, e := range v {
		out[i] = cloneMessage(e)
	}
	return out
}
func cloneMap[K comparable, V any](v map[K]V, clone func(V) V) map[K]V {
	out := make(map[K]V, len(v))
	for k, value := range v {
		out[k] = clone(value)
	}
	return out
}
func (s *snapshot) clone() *snapshot {
	return &snapshot{
		runs:      cloneMap(s.runs, cloneRun),
		history:   cloneMap(s.history, cloneEvents),
		inbox:     cloneMap(s.inbox, cloneEvents),
		tasks:     cloneMap(s.tasks, cloneTask),
		timers:    cloneMap(s.timers, cloneTimer),
		approvals: cloneMap(s.approvals, cloneApproval),
		signals:   cloneMap(s.signals, func(v struct{}) struct{} { return v }),
		calls:     cloneMap(s.calls, cloneCall),
	}
}
func limitRows[T any](rows []T, limit int) []T {
	if limit > 0 && len(rows) > limit {
		return rows[:limit]
	}
	return rows
}
func (tx *transaction) check() error { return tx.ctx.Err() }
func (tx *transaction) requireRun(id string) error {
	if err := tx.check(); err != nil {
		return err
	}
	if tx.data.runs[id] == nil {
		return store.ErrNotFound
	}
	return nil
}
func (tx *transaction) InsertRun(r *store.Run) error {
	if err := tx.check(); err != nil {
		return err
	}
	if tx.data.runs[r.RunID] != nil {
		return store.ErrAlreadyExists
	}
	tx.data.runs[r.RunID] = cloneRun(r)
	return nil
}
func (tx *transaction) GetRun(id string, _ bool) (*store.Run, error) {
	if err := tx.requireRun(id); err != nil {
		return nil, err
	}
	return cloneRun(tx.data.runs[id]), nil
}
func (tx *transaction) UpdateRun(r *store.Run) error {
	if err := tx.requireRun(r.RunID); err != nil {
		return err
	}
	tx.data.runs[r.RunID] = cloneRun(r)
	return nil
}
func (tx *transaction) ListRuns(f store.RunFilter) ([]*store.Run, error) {
	if err := tx.check(); err != nil {
		return nil, err
	}
	var rows []*store.Run
	for _, r := range tx.data.runs {
		if r.RunID > f.AfterRunID && (f.Status == capstanv1.RunStatus_RUN_STATUS_UNSPECIFIED || r.Status == f.Status) && (f.WorkflowType == "" || r.WorkflowType == f.WorkflowType) {
			rows = append(rows, cloneRun(r))
		}
	}
	slices.SortFunc(rows, func(a, b *store.Run) int { return compare(a.RunID, b.RunID) })
	return limitRows(rows, f.Limit), nil
}
func (tx *transaction) RunsPastDeadline(now time.Time, limit int) ([]*store.Run, error) {
	if err := tx.check(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}
	var rows []*store.Run
	for _, r := range tx.data.runs {
		if r.Open() && !r.RunDeadline.IsZero() && !r.RunDeadline.After(now) {
			rows = append(rows, cloneRun(r))
		}
	}
	slices.SortFunc(rows, func(a, b *store.Run) int {
		if c := a.RunDeadline.Compare(b.RunDeadline); c != 0 {
			return c
		}
		return compare(a.RunID, b.RunID)
	})
	return limitRows(rows, limit), nil
}
func (tx *transaction) AppendEvents(id string, events []*capstanv1.HistoryEvent) error {
	if err := tx.requireRun(id); err != nil {
		return err
	}
	last := int64(len(tx.data.history[id]))
	for i, e := range events {
		if e == nil || e.EventId != last+int64(i)+1 {
			return store.ErrConflict
		}
	}
	tx.data.history[id] = append(tx.data.history[id], cloneEvents(events)...)
	return nil
}
func (tx *transaction) ReadHistory(id string, after int64, limit int) ([]*capstanv1.HistoryEvent, error) {
	if err := tx.check(); err != nil {
		return nil, err
	}
	var rows []*capstanv1.HistoryEvent
	for _, e := range tx.data.history[id] {
		if e.EventId > after {
			rows = append(rows, cloneMessage(e))
			if limit > 0 && len(rows) == limit {
				break
			}
		}
	}
	return rows, nil
}
func (tx *transaction) PushInbox(id string, e *capstanv1.HistoryEvent) error {
	if err := tx.requireRun(id); err != nil {
		return err
	}
	if e == nil {
		return store.ErrConflict
	}
	c := cloneMessage(e)
	c.EventId = 0
	tx.data.inbox[id] = append(tx.data.inbox[id], c)
	return nil
}
func (tx *transaction) DrainInbox(id string) ([]*capstanv1.HistoryEvent, error) {
	if err := tx.check(); err != nil {
		return nil, err
	}
	rows := cloneEvents(tx.data.inbox[id])
	delete(tx.data.inbox, id)
	return rows, nil
}
func (tx *transaction) InboxSize(id string) (int, error) {
	if err := tx.check(); err != nil {
		return 0, err
	}
	return len(tx.data.inbox[id]), nil
}
func (tx *transaction) InsertTask(t *store.Task) error {
	if err := tx.requireRun(t.RunID); err != nil {
		return err
	}
	tx.owner.mu.Lock()
	tx.owner.nextTaskID++
	t.ID = tx.owner.nextTaskID
	tx.owner.mu.Unlock()
	tx.data.tasks[t.ID] = cloneTask(t)
	return nil
}
func (tx *transaction) ClaimTask(kind store.TaskKind, queue string, now time.Time, lease time.Duration, worker string) (*store.Task, error) {
	if err := tx.check(); err != nil {
		return nil, err
	}
	var oldest *store.Task
	for _, t := range tx.data.tasks {
		if t.Kind != kind || t.TaskQueue != queue || t.VisibleAt.After(now) || !t.LeasedUntil.IsZero() {
			continue
		}
		if oldest == nil || t.VisibleAt.Before(oldest.VisibleAt) || (t.VisibleAt.Equal(oldest.VisibleAt) && t.ID < oldest.ID) {
			oldest = t
		}
	}
	if oldest == nil {
		return nil, nil
	}
	oldest.LeasedUntil = now.Add(lease).UTC()
	oldest.WorkerID = worker
	oldest.StartedAt = now.UTC()
	return cloneTask(oldest), nil
}
func (tx *transaction) GetTask(id int64, _ bool) (*store.Task, error) {
	if err := tx.check(); err != nil {
		return nil, err
	}
	t := tx.data.tasks[id]
	if t == nil {
		return nil, store.ErrNotFound
	}
	return cloneTask(t), nil
}
func (tx *transaction) UpdateTask(t *store.Task) error {
	if err := tx.check(); err != nil {
		return err
	}
	if tx.data.tasks[t.ID] == nil {
		return store.ErrNotFound
	}
	tx.data.tasks[t.ID] = cloneTask(t)
	return nil
}
func (tx *transaction) DeleteTask(id int64) error {
	if err := tx.check(); err != nil {
		return err
	}
	delete(tx.data.tasks, id)
	return nil
}
func (tx *transaction) RunTasks(id string) ([]*store.Task, error) {
	if err := tx.check(); err != nil {
		return nil, err
	}
	var rows []*store.Task
	for _, t := range tx.data.tasks {
		if t.RunID == id {
			rows = append(rows, cloneTask(t))
		}
	}
	slices.SortFunc(rows, func(a, b *store.Task) int { return compare(a.ID, b.ID) })
	return rows, nil
}
func (tx *transaction) DueTasks(now time.Time, limit int) ([]*store.Task, error) {
	if err := tx.check(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}
	var rows []*store.Task
	for _, t := range tx.data.tasks {
		if !t.CheckAt.IsZero() && !t.CheckAt.After(now) {
			rows = append(rows, cloneTask(t))
		}
	}
	slices.SortFunc(rows, func(a, b *store.Task) int {
		if c := a.CheckAt.Compare(b.CheckAt); c != 0 {
			return c
		}
		return compare(a.ID, b.ID)
	})
	return limitRows(rows, limit), nil
}
func (tx *transaction) InsertTimer(t *store.Timer) error {
	if err := tx.requireRun(t.RunID); err != nil {
		return err
	}
	key := timerKey{t.RunID, t.Seq}
	if tx.data.timers[key] != nil {
		return store.ErrAlreadyExists
	}
	tx.data.timers[key] = cloneTimer(t)
	return nil
}
func (tx *transaction) DeleteTimer(id string, seq int64) (bool, error) {
	if err := tx.check(); err != nil {
		return false, err
	}
	key := timerKey{id, seq}
	_, ok := tx.data.timers[key]
	delete(tx.data.timers, key)
	return ok, nil
}
func (tx *transaction) DueTimers(now time.Time, limit int) ([]*store.Timer, error) {
	if err := tx.check(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}
	var rows []*store.Timer
	for _, t := range tx.data.timers {
		if !t.DueAt.After(now) {
			rows = append(rows, cloneTimer(t))
		}
	}
	slices.SortFunc(rows, func(a, b *store.Timer) int {
		if c := a.DueAt.Compare(b.DueAt); c != 0 {
			return c
		}
		if c := compare(a.RunID, b.RunID); c != 0 {
			return c
		}
		return compare(a.Seq, b.Seq)
	})
	return limitRows(rows, limit), nil
}
func (tx *transaction) RunTimers(id string) ([]*store.Timer, error) {
	if err := tx.check(); err != nil {
		return nil, err
	}
	var rows []*store.Timer
	for _, t := range tx.data.timers {
		if t.RunID == id {
			rows = append(rows, cloneTimer(t))
		}
	}
	slices.SortFunc(rows, func(a, b *store.Timer) int { return compare(a.Seq, b.Seq) })
	return rows, nil
}
func (tx *transaction) InsertApproval(a *store.Approval) error {
	if err := tx.requireRun(a.RunID); err != nil {
		return err
	}
	key := stringKey{a.RunID, a.ApprovalID}
	if tx.data.approvals[key] != nil {
		return store.ErrAlreadyExists
	}
	tx.data.approvals[key] = cloneApproval(a)
	return nil
}
func (tx *transaction) GetApproval(id, approvalID string, _ bool) (*store.Approval, error) {
	if err := tx.check(); err != nil {
		return nil, err
	}
	a := tx.data.approvals[stringKey{id, approvalID}]
	if a == nil {
		return nil, store.ErrNotFound
	}
	return cloneApproval(a), nil
}
func (tx *transaction) UpdateApproval(a *store.Approval) error {
	if err := tx.check(); err != nil {
		return err
	}
	key := stringKey{a.RunID, a.ApprovalID}
	if tx.data.approvals[key] == nil {
		return store.ErrNotFound
	}
	tx.data.approvals[key] = cloneApproval(a)
	return nil
}
func (tx *transaction) RunApprovals(id string) ([]*store.Approval, error) {
	if err := tx.check(); err != nil {
		return nil, err
	}
	var rows []*store.Approval
	for _, a := range tx.data.approvals {
		if a.RunID == id {
			rows = append(rows, cloneApproval(a))
		}
	}
	slices.SortFunc(rows, func(a, b *store.Approval) int { return compare(a.ApprovalID, b.ApprovalID) })
	return rows, nil
}
func (tx *transaction) DueApprovals(now time.Time, limit int) ([]*store.Approval, error) {
	if err := tx.check(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}
	var rows []*store.Approval
	for _, a := range tx.data.approvals {
		if a.Status == store.ApprovalPending && !a.CheckAt.IsZero() && !a.CheckAt.After(now) {
			rows = append(rows, cloneApproval(a))
		}
	}
	slices.SortFunc(rows, func(a, b *store.Approval) int {
		if c := a.CheckAt.Compare(b.CheckAt); c != 0 {
			return c
		}
		if c := compare(a.RunID, b.RunID); c != 0 {
			return c
		}
		return compare(a.ApprovalID, b.ApprovalID)
	})
	return limitRows(rows, limit), nil
}
func (tx *transaction) RecordSignalRequest(id, requestID string) error {
	if err := tx.requireRun(id); err != nil {
		return err
	}
	key := stringKey{id, requestID}
	if _, ok := tx.data.signals[key]; ok {
		return store.ErrAlreadyExists
	}
	tx.data.signals[key] = struct{}{}
	return nil
}
func (tx *transaction) LockBudget() error { return tx.check() }
func (tx *transaction) InsertAICall(c *store.AICall) error {
	if err := tx.requireRun(c.RunID); err != nil {
		return err
	}
	tx.owner.mu.Lock()
	tx.owner.nextCallID++
	c.ID = tx.owner.nextCallID
	tx.owner.mu.Unlock()
	tx.data.calls[c.ID] = cloneCall(c)
	return nil
}
func (tx *transaction) GetAICall(id int64, _ bool) (*store.AICall, error) {
	if err := tx.check(); err != nil {
		return nil, err
	}
	c := tx.data.calls[id]
	if c == nil {
		return nil, store.ErrNotFound
	}
	return cloneCall(c), nil
}
func (tx *transaction) UpdateAICall(c *store.AICall) error {
	if err := tx.check(); err != nil {
		return err
	}
	if tx.data.calls[c.ID] == nil {
		return store.ErrNotFound
	}
	tx.data.calls[c.ID] = cloneCall(c)
	return nil
}
func (tx *transaction) callSum(include func(*store.AICall) bool) (float64, error) {
	if err := tx.check(); err != nil {
		return 0, err
	}
	ids := make([]int64, 0, len(tx.data.calls))
	for id := range tx.data.calls {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	var sum float64
	for _, id := range ids {
		c := tx.data.calls[id]
		if include(c) {
			if c.Status == store.AICallReserved {
				sum += c.EstimateUSD
			} else {
				sum += c.CostUSD
			}
		}
	}
	return sum, nil
}
func (tx *transaction) SpentSince(since time.Time) (float64, error) {
	return tx.callSum(func(c *store.AICall) bool { return !c.At.Before(since) })
}
func (tx *transaction) RunCost(id string) (float64, error) {
	return tx.callSum(func(c *store.AICall) bool { return c.RunID == id })
}
func (tx *transaction) Notify(kind store.TaskKind, queue string) {
	tx.notifications[queueKey{kind, queue}] = struct{}{}
}
func compare[T ~string | ~int64](a, b T) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
