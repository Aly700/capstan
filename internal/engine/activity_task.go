package engine

import (
	"context"
	"errors"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (e *Engine) PollActivityTask(ctx context.Context, req *v1.PollActivityTaskRequest) (*v1.PollActivityTaskResponse, bool, error) {
	if req == nil || !validName(req.TaskQueue) {
		return nil, false, Invalid("task_queue must contain 1–200 characters")
	}
	response := &v1.PollActivityTaskResponse{}
	found := false
	err := e.inTx(ctx, func(tx store.Tx) error {
		response, found = &v1.PollActivityTaskResponse{}, false
		now := e.now()
		task, err := tx.ClaimTask(store.TaskActivity, req.TaskQueue, now, e.cfg.DefaultTaskTimeout, req.Identity)
		if err != nil || task == nil {
			return err
		}
		run, err := tx.GetRun(task.RunID, true)
		if err != nil {
			return err
		}
		if !run.Open() {
			return tx.DeleteTask(task.ID)
		}
		if task.Activity == nil {
			return Invalid("activity task has no specification")
		}
		// ClaimTask only selects previously unleased tasks. Check scheduling deadlines
		// before accepting that lease, so a delayed sweeper cannot start expired work.
		if activityDeadlinePassed(activityScheduleClose(task), now) || activityDeadlinePassed(activityScheduleStart(task), now) {
			task.LeasedUntil = time.Time{}
			task.StartedAt = time.Time{}
			return e.processDueActivityTask(tx, run, task, now)
		}
		task.LeasedUntil = addDeadline(now, task.Activity.GetStartToCloseTimeout().AsDuration())
		task.LastHeartbeatAt = now
		task.CheckAt = activityCheckAt(task, now)
		if err := tx.UpdateTask(task); err != nil {
			return err
		}
		response = &v1.PollActivityTaskResponse{
			TaskToken: EncodeToken(&v1.TaskToken{Kind: v1.TaskKind_TASK_KIND_ACTIVITY, RunId: task.RunID, TaskId: task.ID, Attempt: task.Attempt, ScheduledEventId: task.ScheduledEventID, Seq: task.Activity.Seq}),
			RunId:     task.RunID, WorkflowType: run.WorkflowType, Seq: task.Activity.Seq, ActivityType: task.Activity.ActivityType, Input: task.Activity.Input,
			Attempt: task.Attempt, ScheduledTime: timestamppb.New(task.ScheduledAt), StartedTime: timestamppb.New(task.StartedAt),
			StartToCloseTimeout: task.Activity.StartToCloseTimeout, HeartbeatTimeout: task.Activity.HeartbeatTimeout, HeartbeatDetails: task.HeartbeatDetails,
			IdempotencyKey: IdempotencyKey(task.RunID, task.Activity.Seq),
		}
		found = true
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return response, found, nil
}

// activityToken rejects expiry even if the reaper has not processed the task yet.
func (e *Engine) activityToken(tx store.Tx, raw []byte) (*store.Task, *store.Run, error) {
	token, err := DecodeToken(raw)
	if err != nil {
		return nil, nil, err
	}
	task, err := tx.GetTask(token.TaskId, true)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil, ErrStaleTask
	}
	if err != nil {
		return nil, nil, err
	}
	now := e.now()
	if token.Kind != v1.TaskKind_TASK_KIND_ACTIVITY || task.Kind != store.TaskActivity || task.Activity == nil ||
		token.RunId != task.RunID || token.Attempt != task.Attempt || token.ScheduledEventId != task.ScheduledEventID ||
		token.StartedEventId != 0 || token.Seq != task.Activity.Seq || task.LeasedUntil.IsZero() ||
		task.StartedAt.IsZero() || !task.LeasedUntil.After(now) || activityDeadlinePassed(activityScheduleClose(task), now) ||
		activityDeadlinePassed(activityHeartbeatDeadline(task), now) {
		return nil, nil, ErrStaleTask
	}
	run, err := tx.GetRun(task.RunID, true)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil, ErrStaleTask
	}
	if err != nil {
		return nil, nil, err
	}
	if !run.Open() {
		return nil, nil, ErrStaleTask
	}
	return task, run, nil
}

func activityDeadlinePassed(deadline, now time.Time) bool {
	return !deadline.IsZero() && !deadline.After(now)
}

func (e *Engine) CompleteActivityTask(ctx context.Context, req *v1.CompleteActivityTaskRequest) (*v1.CompleteActivityTaskResponse, error) {
	if req == nil {
		return nil, Invalid("request is required")
	}
	err := e.inTx(ctx, func(tx store.Tx) error {
		task, run, err := e.activityToken(tx, req.TaskToken)
		if err != nil {
			return err
		}
		if err := tx.DeleteTask(task.ID); err != nil {
			return err
		}
		if err := e.deliver(tx, run, &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_ACTIVITY_COMPLETED, Attributes: &v1.HistoryEvent_ActivityCompleted{ActivityCompleted: &v1.ActivityCompletedAttributes{ScheduledEventId: task.ScheduledEventID, Seq: task.Activity.Seq, Result: req.Result, Attempt: task.Attempt, Identity: req.Identity}}}); err != nil {
			return err
		}
		return tx.UpdateRun(run)
	})
	if err != nil {
		return nil, err
	}
	return &v1.CompleteActivityTaskResponse{}, nil
}

func (e *Engine) FailActivityTask(ctx context.Context, req *v1.FailActivityTaskRequest) (*v1.FailActivityTaskResponse, error) {
	if req == nil || req.Failure == nil {
		return nil, Invalid("failure is required")
	}
	err := e.inTx(ctx, func(tx store.Tx) error {
		task, run, err := e.activityToken(tx, req.TaskToken)
		if err != nil {
			return err
		}
		var event *v1.HistoryEvent
		if task.CancelRequested && req.Failure.Type == "CancelledFailure" {
			event = &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_ACTIVITY_CANCELLED, Attributes: &v1.HistoryEvent_ActivityCancelled{ActivityCancelled: &v1.ActivityCancelledAttributes{ScheduledEventId: task.ScheduledEventID, Seq: task.Activity.Seq, Details: req.Failure.Details}}}
		} else if e.retryActivity(task, req.Failure, e.now()) {
			return tx.UpdateTask(task)
		} else {
			event = &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_ACTIVITY_FAILED, Attributes: &v1.HistoryEvent_ActivityFailed{ActivityFailed: &v1.ActivityFailedAttributes{ScheduledEventId: task.ScheduledEventID, Seq: task.Activity.Seq, Failure: req.Failure, Attempt: task.Attempt, Identity: req.Identity}}}
		}
		if err := tx.DeleteTask(task.ID); err != nil {
			return err
		}
		if err := e.deliver(tx, run, event); err != nil {
			return err
		}
		return tx.UpdateRun(run)
	})
	if err != nil {
		return nil, err
	}
	return &v1.FailActivityTaskResponse{}, nil
}

func (e *Engine) HeartbeatActivityTask(ctx context.Context, req *v1.HeartbeatActivityTaskRequest) (*v1.HeartbeatActivityTaskResponse, error) {
	if req == nil {
		return nil, Invalid("request is required")
	}
	response := &v1.HeartbeatActivityTaskResponse{}
	err := e.inTx(ctx, func(tx store.Tx) error {
		response = &v1.HeartbeatActivityTaskResponse{}
		task, _, err := e.activityToken(tx, req.TaskToken)
		if err != nil {
			return err
		}
		now := e.now()
		task.LastHeartbeatAt = now
		task.HeartbeatDetails = req.Details
		task.CheckAt = activityCheckAt(task, now)
		response.CancelRequested = task.CancelRequested
		return tx.UpdateTask(task)
	})
	if err != nil {
		return nil, err
	}
	return response, nil
}
