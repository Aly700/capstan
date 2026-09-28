package engine

import (
	"context"
	"math"
	"testing"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestActivityRetryPolicy(t *testing.T) {
	tests := []struct {
		name     string
		policy   *v1.RetryPolicy
		failure  *v1.Failure
		deadline time.Duration
	}{
		{"non-retryable", nil, &v1.Failure{NonRetryable: true}, 0},
		{"error-type", &v1.RetryPolicy{NonRetryableErrorTypes: []string{"Fatal"}}, &v1.Failure{Type: "Fatal"}, 0},
		{"attempt-limit", &v1.RetryPolicy{MaximumAttempts: 1}, &v1.Failure{Type: "Error"}, 0},
		{"schedule-close-cutoff", nil, &v1.Failure{Type: "Error"}, 500 * time.Millisecond},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e, _, _ := newTestEngine(t)
			scheduleTestActivity(t, e, "no-retry", func(c *v1.ScheduleActivityCommand) {
				c.RetryPolicy = tc.policy
				if tc.deadline > 0 {
					c.ScheduleToCloseTimeout = durationpb.New(tc.deadline)
				}
			})
			a := mustActivity(t, e)
			if _, err := e.FailActivityTask(context.Background(), &v1.FailActivityTaskRequest{TaskToken: a.TaskToken, Failure: tc.failure}); err != nil {
				t.Fatal(err)
			}
			h := history(t, e, "no-retry")
			wantTypes(t, h,
				v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED,
				v1.EventType_EVENT_TYPE_TASK_COMPLETED, v1.EventType_EVENT_TYPE_ACTIVITY_SCHEDULED,
				v1.EventType_EVENT_TYPE_ACTIVITY_FAILED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED)
		})
	}
	t.Run("backoff-and-cap", func(t *testing.T) {
		e, clock, s := newTestEngine(t)
		scheduleTestActivity(t, e, "backoff", func(c *v1.ScheduleActivityCommand) {
			c.RetryPolicy = &v1.RetryPolicy{MaximumInterval: durationpb.New(4 * time.Second)}
		})
		for i, delay := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 4 * time.Second} {
			a := mustActivity(t, e)
			if a.Attempt != int32(i+1) {
				t.Fatalf("attempt=%d", a.Attempt)
			}
			if _, err := e.FailActivityTask(context.Background(), &v1.FailActivityTaskRequest{TaskToken: a.TaskToken, Failure: &v1.Failure{Type: "Error"}}); err != nil {
				t.Fatal(err)
			}
			row := taskForActivity(t, s, a.TaskToken)
			if !row.VisibleAt.Equal(clock.Now().Add(delay)) || !row.CheckAt.Equal(row.VisibleAt) {
				t.Fatalf("retry delay=%v want=%v", row.VisibleAt.Sub(clock.Now()), delay)
			}
			if _, found, err := e.PollActivityTask(context.Background(), &v1.PollActivityTaskRequest{TaskQueue: "q"}); found || err != nil {
				t.Fatalf("early retry found=%v err=%v", found, err)
			}
			clock.Advance(delay)
		}
	})
}

func TestConfiguredDefaultActivityRetry(t *testing.T) {
	e, clock, s := newTestEngine(t)
	e.cfg.DefaultRetry = &v1.RetryPolicy{InitialInterval: durationpb.New(3 * time.Second), BackoffCoefficient: 3, MaximumAttempts: 2}
	scheduleTestActivity(t, e, "configured-retry", nil)
	a := mustActivity(t, e)
	if _, err := e.FailActivityTask(context.Background(), &v1.FailActivityTaskRequest{TaskToken: a.TaskToken, Failure: &v1.Failure{Type: "Error"}}); err != nil {
		t.Fatal(err)
	}
	row := taskForActivity(t, s, a.TaskToken)
	if !row.VisibleAt.Equal(clock.Now().Add(3 * time.Second)) {
		t.Fatalf("configured default ignored: %v", row.VisibleAt.Sub(clock.Now()))
	}
	clock.Advance(3 * time.Second)
	a = mustActivity(t, e)
	if _, err := e.FailActivityTask(context.Background(), &v1.FailActivityTaskRequest{TaskToken: a.TaskToken, Failure: &v1.Failure{Type: "Error"}}); err != nil {
		t.Fatal(err)
	}
	h := history(t, e, "configured-retry")
	wantTypes(t, h,
		v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED,
		v1.EventType_EVENT_TYPE_TASK_COMPLETED, v1.EventType_EVENT_TYPE_ACTIVITY_SCHEDULED,
		v1.EventType_EVENT_TYPE_ACTIVITY_FAILED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED)
}

func TestRetryDelayArithmetic(t *testing.T) {
	e := &Engine{cfg: Config{}.WithDefaults()}
	for _, tc := range []struct {
		attempt int32
		want    time.Duration
	}{{2, time.Second}, {3, 2 * time.Second}, {4, 4 * time.Second}, {20, time.Minute}, {math.MaxInt32, time.Minute}} {
		if got := e.workflowRetryDelay(tc.attempt); got != tc.want {
			t.Errorf("workflow attempt=%d delay=%v want=%v", tc.attempt, got, tc.want)
		}
	}
	now := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name        string
		initial     time.Duration
		coefficient float64
		attempt     int32
		maximum     time.Duration
		want        time.Duration
	}{
		{"default", 0, 0, 1, 0, time.Second},
		{"coefficient", time.Second, 3, 3, 0, 9 * time.Second},
		{"cap", time.Second, 2, 2000, 4 * time.Second, 4 * time.Second},
		{"overflow-default-cap", time.Duration(math.MaxInt64 / 2), 2, 2, 0, time.Duration(math.MaxInt64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy := &v1.RetryPolicy{BackoffCoefficient: tc.coefficient}
			if tc.initial > 0 {
				policy.InitialInterval = durationpb.New(tc.initial)
			}
			if tc.maximum > 0 {
				policy.MaximumInterval = durationpb.New(tc.maximum)
			}
			task := &store.Task{Attempt: tc.attempt, ScheduledAt: now, Activity: &v1.ActivityScheduledAttributes{RetryPolicy: policy}}
			if !e.retryActivity(task, &v1.Failure{Type: "Error"}, now) {
				t.Fatal("retry unexpectedly disallowed")
			}
			want := now.Add(tc.want).Truncate(time.Microsecond)
			if !task.VisibleAt.Equal(want) {
				t.Errorf("visibility=%v want=%v", task.VisibleAt, want)
			}
		})
	}
}

func TestActivityCheckAtEarliestDeadline(t *testing.T) {
	now := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name string
		task *store.Task
		want time.Time
	}{
		{"idle", &store.Task{ScheduledAt: now, VisibleAt: now, Activity: &v1.ActivityScheduledAttributes{}}, time.Time{}},
		{"schedule-start", &store.Task{ScheduledAt: now, VisibleAt: now.Add(5 * time.Second), Activity: &v1.ActivityScheduledAttributes{ScheduleToStartTimeout: durationpb.New(time.Second)}}, now.Add(time.Second)},
		{"retry-visibility", &store.Task{ScheduledAt: now, VisibleAt: now.Add(time.Second), Activity: &v1.ActivityScheduledAttributes{ScheduleToCloseTimeout: durationpb.New(5 * time.Second)}}, now.Add(time.Second)},
		{"heartbeat", &store.Task{ScheduledAt: now, StartedAt: now, LeasedUntil: now.Add(10 * time.Second), Activity: &v1.ActivityScheduledAttributes{HeartbeatTimeout: durationpb.New(time.Second)}}, now.Add(time.Second)},
		{"schedule-close", &store.Task{ScheduledAt: now, StartedAt: now, LeasedUntil: now.Add(10 * time.Second), Activity: &v1.ActivityScheduledAttributes{ScheduleToCloseTimeout: durationpb.New(time.Second)}}, now.Add(time.Second)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := activityCheckAt(tc.task, now); !got.Equal(tc.want) {
				t.Fatalf("check at=%v want=%v", got, tc.want)
			}
		})
	}
}
