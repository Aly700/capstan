package engine

import (
	"context"
	"errors"
	"math"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
)

func (e *Engine) ProcessDueTasks(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, Invalid("limit must be positive")
	}
	count := 0
	for count < limit {
		handled := false
		err := e.inTx(ctx, func(tx store.Tx) error {
			handled = false
			now := e.now()
			tasks, err := tx.DueTasks(now, 1)
			if err != nil || len(tasks) == 0 {
				return err
			}
			task, err := tx.GetTask(tasks[0].ID, true)
			if errors.Is(err, store.ErrNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			if !activityDeadlinePassed(task.CheckAt, now) {
				return nil
			}
			handled = true
			run, err := tx.GetRun(task.RunID, true)
			if err != nil {
				return err
			}
			if !run.Open() {
				return tx.DeleteTask(task.ID)
			}
			if task.Kind == store.TaskWorkflow {
				return e.processDueWorkflowTask(tx, run, task, now)
			}
			return e.processDueActivityTask(tx, run, task, now)
		})
		if err != nil {
			return count, err
		}
		if !handled {
			break
		}
		count++
	}
	return count, nil
}

func (e *Engine) processDueWorkflowTask(tx store.Tx, run *store.Run, task *store.Task, now time.Time) error {
	if task.LeasedUntil.IsZero() {
		if !task.VisibleAt.After(now) {
			tx.Notify(store.TaskWorkflow, task.TaskQueue)
			task.CheckAt = time.Time{}
		} else {
			task.CheckAt = task.VisibleAt
		}
		return tx.UpdateTask(task)
	}
	if task.LeasedUntil.After(now) {
		task.CheckAt = task.LeasedUntil
		return tx.UpdateTask(task)
	}
	if run.WorkflowTaskID != task.ID || !run.InFlight {
		return tx.DeleteTask(task.ID)
	}
	event := &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_TASK_TIMED_OUT, Attributes: &v1.HistoryEvent_TaskTimedOut{TaskTimedOut: &v1.TaskTimedOutAttributes{ScheduledEventId: task.ScheduledEventID, StartedEventId: task.StartedEventID}}}
	if err := e.appendEvents(tx, run, event); err != nil {
		return err
	}
	if err := tx.DeleteTask(task.ID); err != nil {
		return err
	}
	run.InFlight = false
	run.WorkflowTaskID = 0
	if _, err := e.flush(tx, run); err != nil {
		return err
	}
	attempt := task.Attempt
	if attempt < math.MaxInt32 {
		attempt++
	}
	if err := e.scheduleWorkflow(tx, run, attempt, addDeadline(now, e.workflowRetryDelay(attempt))); err != nil {
		return err
	}
	return tx.UpdateRun(run)
}

func (e *Engine) processDueActivityTask(tx store.Tx, run *store.Run, task *store.Task, now time.Time) error {
	timeout := v1.TimeoutType_TIMEOUT_TYPE_UNSPECIFIED
	retry := false
	switch {
	case activityDeadlinePassed(activityScheduleClose(task), now):
		timeout = v1.TimeoutType_TIMEOUT_TYPE_SCHEDULE_TO_CLOSE
	case task.LeasedUntil.IsZero() && activityDeadlinePassed(activityScheduleStart(task), now):
		timeout = v1.TimeoutType_TIMEOUT_TYPE_SCHEDULE_TO_START
	case !task.LeasedUntil.IsZero() && activityDeadlinePassed(activityHeartbeatDeadline(task), now):
		timeout, retry = v1.TimeoutType_TIMEOUT_TYPE_HEARTBEAT, true
	case activityDeadlinePassed(task.LeasedUntil, now):
		timeout, retry = v1.TimeoutType_TIMEOUT_TYPE_START_TO_CLOSE, true
	}
	if timeout == v1.TimeoutType_TIMEOUT_TYPE_UNSPECIFIED {
		if task.LeasedUntil.IsZero() && !task.VisibleAt.After(now) {
			tx.Notify(store.TaskActivity, task.TaskQueue)
		}
		task.CheckAt = activityCheckAt(task, now)
		return tx.UpdateTask(task)
	}
	if retry && e.retryActivity(task, &v1.Failure{Type: "TimeoutFailure", Message: timeout.String()}, now) {
		return tx.UpdateTask(task)
	}
	if err := tx.DeleteTask(task.ID); err != nil {
		return err
	}
	event := &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_ACTIVITY_TIMED_OUT, Attributes: &v1.HistoryEvent_ActivityTimedOut{ActivityTimedOut: &v1.ActivityTimedOutAttributes{ScheduledEventId: task.ScheduledEventID, Seq: task.Activity.Seq, TimeoutType: timeout, LastFailure: task.LastFailure, Attempt: task.Attempt}}}
	if err := e.deliver(tx, run, event); err != nil {
		return err
	}
	return tx.UpdateRun(run)
}
