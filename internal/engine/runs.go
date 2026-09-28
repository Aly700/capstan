package engine

import (
	"context"
	"encoding/base64"
	"errors"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"regexp"
	"time"
	"unicode/utf8"
)

var runIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,180}$`)

func validName(s string) bool {
	n := utf8.RuneCountInString(s)
	return utf8.ValidString(s) && n > 0 && n <= 200
}
func validDuration(d *durationpb.Duration) bool {
	if d == nil {
		return true
	}
	return d.CheckValid() == nil && d.Seconds >= 0 && d.Nanos >= 0 && durationpb.New(d.AsDuration()).Seconds == d.Seconds && durationpb.New(d.AsDuration()).Nanos == d.Nanos
}
func validRunTimeout(d *durationpb.Duration) bool {
	return validDuration(d) && d.AsDuration()%time.Millisecond == 0
}

func runError(err error) error {
	if errors.Is(err, store.ErrNotFound) {
		return ErrNotFound
	}
	return err
}

func (e *Engine) StartRun(ctx context.Context, identity string, req *v1.StartRunRequest) (*v1.StartRunResponse, error) {
	if !runIDPattern.MatchString(req.GetRunId()) || !validName(req.GetWorkflowType()) || !validName(req.GetTaskQueue()) || !validRunTimeout(req.GetRunTimeout()) || !validRunTimeout(req.GetTaskTimeout()) || req.GetTaskTimeout().AsDuration() > 10*time.Minute {
		return nil, Invalid("invalid run id, workflow type, queue or timeout")
	}
	resp := &v1.StartRunResponse{}
	err := e.inTx(ctx, func(tx store.Tx) error {
		resp = &v1.StartRunResponse{}
		r, err := tx.GetRun(req.RunId, true)
		if errors.Is(err, store.ErrNotFound) {
			now := e.now()
			r = &store.Run{RunID: req.RunId, WorkflowType: req.WorkflowType, TaskQueue: req.TaskQueue, Input: req.Input, Status: v1.RunStatus_RUN_STATUS_RUNNING, StartedAt: now, TaskTimeout: req.GetTaskTimeout().AsDuration(), RunTimeout: req.GetRunTimeout().AsDuration(), Identity: identity}
			if r.TaskTimeout == 0 {
				r.TaskTimeout = e.cfg.DefaultTaskTimeout
			}
			if r.RunTimeout > 0 {
				r.RunDeadline = addDeadline(now, r.RunTimeout)
			}
			err = tx.InsertRun(r)
			if errors.Is(err, store.ErrAlreadyExists) {
				r, err = tx.GetRun(req.RunId, true)
			} else if err == nil {
				resp.Started = true
				if err = e.startHistory(tx, r); err != nil {
					return err
				}
			}
		}
		if err != nil {
			return err
		}
		if r.WorkflowType != req.WorkflowType {
			return ErrAlreadyExists
		}
		resp.Run, err = e.runInfo(tx, r)
		return err
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (e *Engine) startHistory(tx store.Tx, r *store.Run) error {
	ev := &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_RUN_STARTED, Attributes: &v1.HistoryEvent_RunStarted{RunStarted: &v1.RunStartedAttributes{WorkflowType: r.WorkflowType, TaskQueue: r.TaskQueue, Input: r.Input, TaskTimeout: durationpb.New(r.TaskTimeout), RunTimeout: durationpb.New(r.RunTimeout), ContinuedFromRunId: r.ContinuedFromRunID, Identity: r.Identity}}}
	if err := e.appendEvents(tx, r, ev); err != nil {
		return err
	}
	if err := e.scheduleWorkflow(tx, r, 1, e.now()); err != nil {
		return err
	}
	return tx.UpdateRun(r)
}

func (e *Engine) appendEvents(tx store.Tx, r *store.Run, events ...*v1.HistoryEvent) error {
	for i, ev := range events {
		ev.EventId = r.LastEventID + int64(i) + 1
		ev.Time = timestamppb.New(e.now())
	}
	if err := tx.AppendEvents(r.RunID, events); err != nil {
		return err
	}
	r.LastEventID += int64(len(events))
	return nil
}
func (e *Engine) scheduleWorkflow(tx store.Tx, r *store.Run, attempt int32, visible time.Time) error {
	visible = visible.Truncate(time.Microsecond)
	ev := &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_TASK_SCHEDULED, Attributes: &v1.HistoryEvent_TaskScheduled{TaskScheduled: &v1.TaskScheduledAttributes{TaskQueue: r.TaskQueue, StartToCloseTimeout: durationpb.New(r.TaskTimeout), Attempt: attempt}}}
	if err := e.appendEvents(tx, r, ev); err != nil {
		return err
	}
	task := &store.Task{Kind: store.TaskWorkflow, RunID: r.RunID, TaskQueue: r.TaskQueue, ScheduledEventID: ev.EventId, Attempt: attempt, VisibleAt: visible, ScheduledAt: e.now()}
	// Delayed workflow retries wake long pollers when their visibility time arrives.
	if visible.After(task.ScheduledAt) {
		task.CheckAt = visible
	}
	if err := tx.InsertTask(task); err != nil {
		return err
	}
	r.WorkflowTaskID = task.ID
	tx.Notify(store.TaskWorkflow, r.TaskQueue)
	return nil
}
func (e *Engine) runInfo(tx store.Tx, r *store.Run) (*v1.RunInfo, error) {
	cost, err := tx.RunCost(r.RunID)
	if err != nil {
		return nil, err
	}
	tasks, err := tx.RunTasks(r.RunID)
	if err != nil {
		return nil, err
	}
	approvals, err := tx.RunApprovals(r.RunID)
	if err != nil {
		return nil, err
	}
	info := &v1.RunInfo{RunId: r.RunID, WorkflowType: r.WorkflowType, TaskQueue: r.TaskQueue, Status: r.Status, StartedAt: timestamppb.New(r.StartedAt), LastEventId: r.LastEventID, Result: r.Result, Failure: r.Failure, CostUsd: roundUSD(cost), ContinuedAsNewRunId: r.ContinuedToRunID}
	if !r.ClosedAt.IsZero() {
		info.ClosedAt = timestamppb.New(r.ClosedAt)
	}
	for _, task := range tasks {
		if task.Kind == store.TaskActivity {
			info.PendingActivities++
		}
	}
	for _, a := range approvals {
		if a.Status == store.ApprovalPending {
			info.PendingApprovals++
		}
	}
	return info, nil
}
func (e *Engine) DescribeRun(ctx context.Context, req *v1.DescribeRunRequest) (*v1.DescribeRunResponse, error) {
	resp := &v1.DescribeRunResponse{}
	err := e.deps.Store.InTx(ctx, func(tx store.Tx) error {
		r, err := tx.GetRun(req.GetRunId(), false)
		if err != nil {
			return runError(err)
		}
		resp.Run, err = e.runInfo(tx, r)
		return err
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}
func pageSize(n int32, def, maximum int) (int, error) {
	if n < 0 {
		return 0, Invalid("negative page size")
	}
	if n == 0 {
		return def, nil
	}
	return min(int(n), maximum), nil
}
func (e *Engine) ListRuns(ctx context.Context, req *v1.ListRunsRequest) (*v1.ListRunsResponse, error) {
	n, err := pageSize(req.GetPageSize(), 50, 500)
	if err != nil {
		return nil, err
	}
	after, err := base64.RawURLEncoding.DecodeString(req.GetPageToken())
	if err != nil {
		return nil, Invalid("invalid page token")
	}
	resp := &v1.ListRunsResponse{}
	err = e.deps.Store.InTx(ctx, func(tx store.Tx) error {
		runs, err := tx.ListRuns(store.RunFilter{Status: req.GetStatus(), WorkflowType: req.GetWorkflowType(), AfterRunID: string(after), Limit: n + 1})
		if err != nil {
			return err
		}
		if len(runs) > n {
			runs = runs[:n]
			resp.NextPageToken = base64.RawURLEncoding.EncodeToString([]byte(runs[n-1].RunID))
		}
		for _, r := range runs {
			info, err := e.runInfo(tx, r)
			if err != nil {
				return err
			}
			resp.Runs = append(resp.Runs, info)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}
func (e *Engine) GetHistory(ctx context.Context, req *v1.GetHistoryRequest) (*v1.GetHistoryResponse, error) {
	n, err := pageSize(req.GetPageSize(), 1000, 5000)
	if err != nil {
		return nil, err
	}
	if req.GetAfterEventId() < 0 {
		return nil, Invalid("negative event id")
	}
	resp := &v1.GetHistoryResponse{}
	err = e.deps.Store.InTx(ctx, func(tx store.Tx) error {
		if _, err := tx.GetRun(req.GetRunId(), false); err != nil {
			return runError(err)
		}
		events, err := tx.ReadHistory(req.GetRunId(), req.GetAfterEventId(), n+1)
		if err != nil {
			return err
		}
		if len(events) > n {
			events = events[:n]
			resp.More = true
		}
		resp.Events = events
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}
