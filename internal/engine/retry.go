package engine

import (
	"math"
	"slices"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
)

func (e *Engine) workflowRetryDelay(attempt int32) time.Duration {
	return cappedBackoff(e.cfg.TaskRetryInitial, 2, max(int64(attempt)-2, 0), e.cfg.TaskRetryMax)
}

// cappedBackoff clamps before converting to Duration, including default-cap overflow.
func cappedBackoff(initial time.Duration, coefficient float64, exponent int64, maximum time.Duration) time.Duration {
	delay := float64(initial) * math.Pow(coefficient, float64(exponent))
	if math.IsInf(delay, 1) || delay >= float64(maximum) {
		return maximum
	}
	return time.Duration(delay)
}

func (e *Engine) retryActivity(task *store.Task, failure *v1.Failure, now time.Time) bool {
	policy := task.Activity.GetRetryPolicy()
	if policy == nil {
		policy = e.cfg.DefaultRetry
	}
	if failure.GetNonRetryable() || slices.Contains(policy.GetNonRetryableErrorTypes(), failure.GetType()) ||
		(policy.GetMaximumAttempts() > 0 && task.Attempt >= policy.GetMaximumAttempts()) || task.Attempt == math.MaxInt32 {
		return false
	}
	initial := policy.GetInitialInterval().AsDuration()
	if initial <= 0 {
		initial = time.Second
	}
	coefficient := policy.GetBackoffCoefficient()
	if coefficient <= 0 || math.IsNaN(coefficient) {
		coefficient = 2
	}
	maximum := policy.GetMaximumInterval().AsDuration()
	if maximum <= 0 {
		maximum = time.Duration(math.MaxInt64)
		if initial <= time.Duration(math.MaxInt64)/100 {
			maximum = initial * 100
		}
	}
	delay := cappedBackoff(initial, coefficient, max(int64(task.Attempt)-1, 0), maximum)
	visible := now.Add(delay)
	deadline := activityScheduleClose(task)
	if !deadline.IsZero() && visible.After(deadline) {
		return false
	}
	task.Attempt++
	task.VisibleAt = visible
	task.LeasedUntil = time.Time{}
	task.StartedAt = time.Time{}
	task.LastHeartbeatAt = time.Time{}
	task.WorkerID = ""
	task.LastFailure = failure
	task.CheckAt = activityCheckAt(task, now)
	return true
}

func earliestActivityTime(a, b time.Time) time.Time {
	if a.IsZero() || (!b.IsZero() && b.Before(a)) {
		return b
	}
	return a
}

func activityScheduleClose(task *store.Task) time.Time {
	if d := task.Activity.GetScheduleToCloseTimeout().AsDuration(); d > 0 {
		return task.ScheduledAt.Add(d)
	}
	return time.Time{}
}

func activityScheduleStart(task *store.Task) time.Time {
	if d := task.Activity.GetScheduleToStartTimeout().AsDuration(); d > 0 {
		return task.ScheduledAt.Add(d)
	}
	return time.Time{}
}

func activityHeartbeatDeadline(task *store.Task) time.Time {
	if d := task.Activity.GetHeartbeatTimeout().AsDuration(); d > 0 {
		last := task.LastHeartbeatAt
		if last.IsZero() {
			last = task.StartedAt
		}
		return last.Add(d)
	}
	return time.Time{}
}

func activityCheckAt(task *store.Task, now time.Time) time.Time {
	at := activityScheduleClose(task)
	if task.LeasedUntil.IsZero() {
		at = earliestActivityTime(at, activityScheduleStart(task))
		if task.VisibleAt.After(now) {
			at = earliestActivityTime(at, task.VisibleAt)
		}
		return at
	}
	at = earliestActivityTime(at, task.LeasedUntil)
	return earliestActivityTime(at, activityHeartbeatDeadline(task))
}
