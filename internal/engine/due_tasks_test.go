package engine

import (
	"context"
	"testing"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestWorkflowTaskLeaseExpiry(t *testing.T) {
	e, clock, _ := newTestEngine(t)
	mustStart(t, e, "wf-expiry")
	w := mustPoll(t, e)
	clock.Advance(10 * time.Second)
	if n, err := e.ProcessDueTasks(context.Background(), 1); n != 1 || err != nil {
		t.Fatalf("due: %d %v", n, err)
	}
	h := history(t, e, "wf-expiry")
	wantTypes(t, h,
		v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED,
		v1.EventType_EVENT_TYPE_TASK_TIMED_OUT, v1.EventType_EVENT_TYPE_TASK_SCHEDULED)
	if _, found, err := e.PollWorkflowTask(context.Background(), &v1.PollWorkflowTaskRequest{TaskQueue: "q"}); err != nil || found {
		t.Fatalf("backoff found=%v err=%v", found, err)
	}
	clock.Advance(time.Second)
	r := mustPoll(t, e)
	if r.Attempt != 2 {
		t.Fatalf("attempt=%d", r.Attempt)
	}
	timedOut := r.History[len(r.History)-3].GetTaskTimedOut()
	token, _ := DecodeToken(w.TaskToken)
	if timedOut == nil || timedOut.StartedEventId != token.StartedEventId || timedOut.ScheduledEventId != token.ScheduledEventId {
		t.Fatalf("timeout: %v", timedOut)
	}
}

func TestActivityStartToCloseRetries(t *testing.T) {
	e, clock, _ := newTestEngine(t)
	scheduleTestActivity(t, e, "stc-retry", nil)
	a := mustActivity(t, e)
	clock.Advance(10 * time.Second)
	if n, err := e.ProcessDueTasks(context.Background(), 10); n != 1 || err != nil {
		t.Fatalf("due: %d %v", n, err)
	}
	wantTypes(t, history(t, e, "stc-retry"),
		v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED,
		v1.EventType_EVENT_TYPE_TASK_COMPLETED, v1.EventType_EVENT_TYPE_ACTIVITY_SCHEDULED)
	clock.Advance(time.Second)
	b := mustActivity(t, e)
	if b.Attempt != 2 || b.IdempotencyKey != a.IdempotencyKey {
		t.Fatalf("retry: %v", b)
	}
}

func assertActivityTimeout(t *testing.T, e *Engine, id string, want v1.TimeoutType) {
	t.Helper()
	wantTypes(t, history(t, e, id),
		v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED,
		v1.EventType_EVENT_TYPE_TASK_COMPLETED, v1.EventType_EVENT_TYPE_ACTIVITY_SCHEDULED,
		v1.EventType_EVENT_TYPE_ACTIVITY_TIMED_OUT, v1.EventType_EVENT_TYPE_TASK_SCHEDULED)
	h, err := e.GetHistory(context.Background(), &v1.GetHistoryRequest{RunId: id})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, event := range h.Events {
		if event.Type == v1.EventType_EVENT_TYPE_ACTIVITY_TIMED_OUT {
			n++
			if event.GetActivityTimedOut().TimeoutType != want {
				t.Fatalf("timeout=%v want=%v", event.GetActivityTimedOut().TimeoutType, want)
			}
		}
	}
	if n != 1 {
		t.Fatalf("timeout events=%d", n)
	}
}

func TestActivityHeartbeatTimeout(t *testing.T) {
	e, clock, _ := newTestEngine(t)
	scheduleTestActivity(t, e, "hb-timeout", func(c *v1.ScheduleActivityCommand) {
		c.HeartbeatTimeout = durationpb.New(time.Second)
		c.RetryPolicy = &v1.RetryPolicy{MaximumAttempts: 1}
	})
	mustActivity(t, e)
	clock.Advance(time.Second)
	if n, err := e.ProcessDueTasks(context.Background(), 10); n != 1 || err != nil {
		t.Fatalf("due: %d %v", n, err)
	}
	assertActivityTimeout(t, e, "hb-timeout", v1.TimeoutType_TIMEOUT_TYPE_HEARTBEAT)
}

func TestActivityScheduleToStartTimeout(t *testing.T) {
	e, clock, _ := newTestEngine(t)
	scheduleTestActivity(t, e, "sts-timeout", func(c *v1.ScheduleActivityCommand) { c.ScheduleToStartTimeout = durationpb.New(time.Second) })
	clock.Advance(time.Second)
	if n, err := e.ProcessDueTasks(context.Background(), 10); n != 1 || err != nil {
		t.Fatalf("due: %d %v", n, err)
	}
	assertActivityTimeout(t, e, "sts-timeout", v1.TimeoutType_TIMEOUT_TYPE_SCHEDULE_TO_START)
}

func TestActivityScheduleToCloseTimeout(t *testing.T) {
	for _, leased := range []bool{false, true} {
		t.Run(map[bool]string{false: "unleased", true: "leased"}[leased], func(t *testing.T) {
			e, clock, _ := newTestEngine(t)
			scheduleTestActivity(t, e, "schedule-close", func(c *v1.ScheduleActivityCommand) {
				c.ScheduleToCloseTimeout = durationpb.New(2 * time.Second)
				c.HeartbeatTimeout = durationpb.New(2 * time.Second)
				c.ScheduleToStartTimeout = durationpb.New(2 * time.Second)
			})
			if leased {
				mustActivity(t, e)
			}
			clock.Advance(2 * time.Second)
			if n, err := e.ProcessDueTasks(context.Background(), 10); n != 1 || err != nil {
				t.Fatalf("due: %d %v", n, err)
			}
			assertActivityTimeout(t, e, "schedule-close", v1.TimeoutType_TIMEOUT_TYPE_SCHEDULE_TO_CLOSE)
		})
	}
}

func TestRetryVisibilityNotifies(t *testing.T) {
	e, clock, s := newTestEngine(t)
	scheduleTestActivity(t, e, "retry-notify", nil)
	a := mustActivity(t, e)
	ch, cancel := s.Subscribe(2, "q")
	defer cancel()
	if _, err := e.FailActivityTask(context.Background(), &v1.FailActivityTaskRequest{TaskToken: a.TaskToken, Failure: &v1.Failure{Type: "Transient"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
		t.Fatal("retry notified before visible")
	default:
	}
	clock.Advance(time.Second)
	if n, err := e.ProcessDueTasks(context.Background(), 10); n != 1 || err != nil {
		t.Fatalf("due: %d %v", n, err)
	}
	select {
	case <-ch:
	default:
		t.Fatal("visible retry did not notify")
	}
	if n, err := e.ProcessDueTasks(context.Background(), 10); n != 0 || err != nil {
		t.Fatalf("duplicate visibility: %d %v", n, err)
	}
	mustActivity(t, e)
}

func TestHeartbeatTimeoutRetryRetainsDetails(t *testing.T) {
	e, clock, _ := newTestEngine(t)
	scheduleTestActivity(t, e, "hb-retry", func(c *v1.ScheduleActivityCommand) { c.HeartbeatTimeout = durationpb.New(2 * time.Second) })
	a := mustActivity(t, e)
	if _, err := e.HeartbeatActivityTask(context.Background(), &v1.HeartbeatActivityTaskRequest{TaskToken: a.TaskToken, Details: &v1.Payload{Data: []byte("resume")}}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Second)
	if _, err := e.ProcessDueTasks(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	b := mustActivity(t, e)
	if b.Attempt != 2 || string(b.GetHeartbeatDetails().GetData()) != "resume" {
		t.Fatalf("retry response: %v", b)
	}
	clock.Advance(time.Second)
	if n, err := e.ProcessDueTasks(context.Background(), 10); n != 0 || err != nil {
		t.Fatalf("old heartbeat deadline leaked: %d %v", n, err)
	}
}

func TestDueTasksFromStoredTask(t *testing.T) {
	e, clock, _ := newTestEngine(t)
	seedActivity(t, e, true)
	clock.Advance(10 * time.Second)
	if n, err := e.ProcessDueTasks(context.Background(), 1); n != 1 || err != nil {
		t.Fatalf("due stored task: %d, %v", n, err)
	}
}
