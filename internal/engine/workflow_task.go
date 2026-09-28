package engine

import (
	"context"
	"errors"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"math"
)

func (e *Engine) PollWorkflowTask(ctx context.Context, req *v1.PollWorkflowTaskRequest) (*v1.PollWorkflowTaskResponse, bool, error) {
	resp := &v1.PollWorkflowTaskResponse{}
	found := false
	err := e.deps.Store.InTx(ctx, func(tx store.Tx) error {
		now := e.deps.Clock.Now()
		task, err := tx.ClaimTask(store.TaskWorkflow, req.GetTaskQueue(), now, e.cfg.DefaultTaskTimeout, req.GetIdentity())
		if err != nil || task == nil {
			return err
		}
		r, err := tx.GetRun(task.RunID, true)
		if err != nil {
			return err
		}
		if r.Status != v1.RunStatus_RUN_STATUS_RUNNING || r.WorkflowTaskID != task.ID {
			return tx.DeleteTask(task.ID)
		}
		ev := &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_TASK_STARTED, Attributes: &v1.HistoryEvent_TaskStarted{TaskStarted: &v1.TaskStartedAttributes{ScheduledEventId: task.ScheduledEventID, Identity: req.GetIdentity()}}}
		if err := e.appendEvents(tx, r, ev); err != nil {
			return err
		}
		r.InFlight = true
		task.StartedEventID = ev.EventId
		task.LeasedUntil = now.Add(r.TaskTimeout)
		task.CheckAt = task.LeasedUntil
		if err := tx.UpdateTask(task); err != nil {
			return err
		}
		if err := tx.UpdateRun(r); err != nil {
			return err
		}
		h, err := tx.ReadHistory(r.RunID, 0, 0)
		if err != nil {
			return err
		}
		resp = &v1.PollWorkflowTaskResponse{RunId: r.RunID, WorkflowType: r.WorkflowType, Attempt: task.Attempt, History: h, TaskToken: EncodeToken(&v1.TaskToken{Kind: v1.TaskKind_TASK_KIND_WORKFLOW, RunId: r.RunID, TaskId: task.ID, Attempt: task.Attempt, ScheduledEventId: task.ScheduledEventID, StartedEventId: task.StartedEventID})}
		found = true
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return resp, found, nil
}

func (e *Engine) workflowToken(tx store.Tx, raw []byte) (*store.Task, *store.Run, error) {
	tok, err := DecodeToken(raw)
	if err != nil {
		return nil, nil, err
	}
	task, err := tx.GetTask(tok.TaskId, true)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil, ErrStaleTask
	}
	if err != nil {
		return nil, nil, err
	}
	if tok.Kind != v1.TaskKind_TASK_KIND_WORKFLOW || task.Kind != store.TaskWorkflow || tok.RunId != task.RunID || tok.Attempt != task.Attempt || tok.ScheduledEventId != task.ScheduledEventID || tok.StartedEventId != task.StartedEventID || tok.Seq != 0 || task.StartedEventID == 0 || task.LeasedUntil.IsZero() || !e.deps.Clock.Now().Before(task.LeasedUntil) {
		return nil, nil, ErrStaleTask
	}
	r, err := tx.GetRun(task.RunID, true)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil, ErrStaleTask
	}
	if err != nil {
		return nil, nil, err
	}
	if r.Status != v1.RunStatus_RUN_STATUS_RUNNING || !r.InFlight || r.WorkflowTaskID != task.ID || r.LastEventID != tok.StartedEventId {
		return nil, nil, ErrStaleTask
	}
	return task, r, nil
}

func (e *Engine) CompleteWorkflowTask(ctx context.Context, req *v1.CompleteWorkflowTaskRequest) (*v1.CompleteWorkflowTaskResponse, error) {
	err := e.deps.Store.InTx(ctx, func(tx store.Tx) error {
		task, r, err := e.workflowToken(tx, req.GetTaskToken())
		if err != nil {
			return err
		}
		if err := e.validateCommands(tx, r, req.GetCommands()); err != nil {
			return err
		}
		ev := &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_TASK_COMPLETED, Attributes: &v1.HistoryEvent_TaskCompleted{TaskCompleted: &v1.TaskCompletedAttributes{ScheduledEventId: task.ScheduledEventID, StartedEventId: task.StartedEventID, Identity: req.GetIdentity(), BuildId: req.GetBuildId()}}}
		if err := e.appendEvents(tx, r, ev); err != nil {
			return err
		}
		for _, cmd := range req.GetCommands() {
			if err := e.applyCommand(tx, r, ev.EventId, cmd); err != nil {
				return err
			}
		}
		if r.Open() {
			flushed, err := e.flush(tx, r)
			if err != nil {
				return err
			}
			r.WorkflowTaskID = 0
			if flushed {
				if err := e.scheduleWorkflow(tx, r, 1, e.deps.Clock.Now()); err != nil {
					return err
				}
			}
		}
		if err := tx.DeleteTask(task.ID); err != nil {
			return err
		}
		r.InFlight = false
		if r.Open() && r.LastEventID > e.cfg.MaxHistoryEvents {
			r.Failure = &v1.Failure{Type: "HistoryLimitExceeded", Message: "run history exceeds configured event limit", NonRetryable: true}
			if err := e.appendEvents(tx, r, &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_RUN_FAILED, Attributes: &v1.HistoryEvent_RunFailed{RunFailed: &v1.RunFailedAttributes{Failure: r.Failure}}}); err != nil {
				return err
			}
			if err := e.closeRun(tx, r, v1.RunStatus_RUN_STATUS_FAILED); err != nil {
				return err
			}
		}
		return tx.UpdateRun(r)
	})
	if err != nil {
		return nil, err
	}
	return &v1.CompleteWorkflowTaskResponse{}, nil
}

func (e *Engine) FailWorkflowTask(ctx context.Context, req *v1.FailWorkflowTaskRequest) (*v1.FailWorkflowTaskResponse, error) {
	err := e.deps.Store.InTx(ctx, func(tx store.Tx) error {
		task, r, err := e.workflowToken(tx, req.GetTaskToken())
		if err != nil {
			return err
		}
		if req.GetCause() != v1.TaskFailedCause_TASK_FAILED_CAUSE_HISTORY_MISMATCH && req.GetCause() != v1.TaskFailedCause_TASK_FAILED_CAUSE_SDK_ERROR {
			return Invalid("unknown workflow task failure cause")
		}
		ev := &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_TASK_FAILED, Attributes: &v1.HistoryEvent_TaskFailed{TaskFailed: &v1.TaskFailedAttributes{ScheduledEventId: task.ScheduledEventID, StartedEventId: task.StartedEventID, Cause: req.Cause, Failure: req.Failure, Identity: req.Identity}}}
		if err := e.appendEvents(tx, r, ev); err != nil {
			return err
		}
		if err := tx.DeleteTask(task.ID); err != nil {
			return err
		}
		r.InFlight = false
		r.WorkflowTaskID = 0
		if _, err := e.flush(tx, r); err != nil {
			return err
		}
		if req.Cause == v1.TaskFailedCause_TASK_FAILED_CAUSE_HISTORY_MISMATCH {
			if err := e.appendEvents(tx, r, &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_RUN_BLOCKED, Attributes: &v1.HistoryEvent_RunBlocked{RunBlocked: &v1.RunBlockedAttributes{TaskFailedEventId: ev.EventId, Failure: req.Failure}}}); err != nil {
				return err
			}
			r.Status = v1.RunStatus_RUN_STATUS_BLOCKED
			r.Failure = req.Failure
		} else {
			attempt := task.Attempt
			if attempt < math.MaxInt32 {
				attempt++
			}
			if err := e.scheduleWorkflow(tx, r, attempt, e.deps.Clock.Now().Add(e.workflowRetryDelay(attempt))); err != nil {
				return err
			}
		}
		return tx.UpdateRun(r)
	})
	if err != nil {
		return nil, err
	}
	return &v1.FailWorkflowTaskResponse{}, nil
}

func (e *Engine) closeRun(tx store.Tx, r *store.Run, status v1.RunStatus) error {
	r.Status = status
	r.ClosedAt = e.deps.Clock.Now()
	r.InFlight = false
	r.WorkflowTaskID = 0
	tasks, err := tx.RunTasks(r.RunID)
	if err != nil {
		return err
	}
	for _, task := range tasks {
		if err := tx.DeleteTask(task.ID); err != nil {
			return err
		}
	}
	timers, err := tx.RunTimers(r.RunID)
	if err != nil {
		return err
	}
	for _, timer := range timers {
		if _, err := tx.DeleteTimer(r.RunID, timer.Seq); err != nil {
			return err
		}
	}
	approvals, err := tx.RunApprovals(r.RunID)
	if err != nil {
		return err
	}
	for _, a := range approvals {
		if a.Status == store.ApprovalPending {
			a.Status = store.ApprovalExpired
			a.ResolvedAt = r.ClosedAt
			a.CheckAt = r.ClosedAt
			a.Resolver = "run closed"
			if err := tx.UpdateApproval(a); err != nil {
				return err
			}
		}
	}
	// A closing command wins over buffered external work; release the unreachable inbox.
	_, err = tx.DrainInbox(r.RunID)
	return err
}
