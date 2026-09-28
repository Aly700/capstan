package engine

import (
	"context"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"testing"
	"time"
)

func TestTimerFiresExactlyOnce(t *testing.T) {
	e, c, _ := newTestEngine(t)
	mustStart(t, e, "r")
	mustComplete(t, e, mustPoll(t, e).TaskToken, timerCmd(1, time.Minute))
	if n, err := e.FireDueTimers(context.Background(), 10); n != 0 || err != nil {
		t.Fatalf("early %d %v", n, err)
	}
	c.Advance(time.Minute)
	for _, want := range []int{1, 0} {
		if n, err := e.FireDueTimers(context.Background(), 10); n != want || err != nil {
			t.Fatalf("fire %d %v", n, err)
		}
	}
	wantTypes(t, history(t, e, "r"), v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED, v1.EventType_EVENT_TYPE_TASK_COMPLETED, v1.EventType_EVENT_TYPE_TIMER_STARTED, v1.EventType_EVENT_TYPE_TIMER_FIRED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED)
}
func TestCancelledTimerNeverFires(t *testing.T) {
	e, c, _ := newTestEngine(t)
	mustStart(t, e, "r")
	mustComplete(t, e, mustPoll(t, e).TaskToken, timerCmd(1, time.Minute), &v1.Command{Attributes: &v1.Command_CancelTimer{CancelTimer: &v1.CancelTimerCommand{Seq: 1}}})
	c.Advance(time.Minute)
	if n, err := e.FireDueTimers(context.Background(), 10); n != 0 || err != nil {
		t.Fatalf("cancelled %d %v", n, err)
	}
	wantTypes(t, history(t, e, "r"), v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED, v1.EventType_EVENT_TYPE_TASK_COMPLETED, v1.EventType_EVENT_TYPE_TIMER_STARTED, v1.EventType_EVENT_TYPE_TIMER_CANCELLED)
}
func TestTimerAlreadyFiredCanBeCancelledBeforeInboxFlush(t *testing.T) {
	e, c, _ := newTestEngine(t)
	mustStart(t, e, "r")
	mustComplete(t, e, mustPoll(t, e).TaskToken, timerCmd(1, time.Second))
	if _, err := e.SignalRun(context.Background(), "", &v1.SignalRunRequest{RunId: "r", Name: "s"}); err != nil {
		t.Fatal(err)
	}
	p := mustPoll(t, e)
	c.Advance(time.Second)
	if n, err := e.FireDueTimers(context.Background(), 10); n != 1 || err != nil {
		t.Fatalf("fire %d %v", n, err)
	}
	mustComplete(t, e, p.TaskToken, &v1.Command{Attributes: &v1.Command_CancelTimer{CancelTimer: &v1.CancelTimerCommand{Seq: 1}}})
	wantTypes(t, history(t, e, "r"), v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED, v1.EventType_EVENT_TYPE_TASK_COMPLETED, v1.EventType_EVENT_TYPE_TIMER_STARTED, v1.EventType_EVENT_TYPE_SIGNAL_RECEIVED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED, v1.EventType_EVENT_TYPE_TASK_COMPLETED, v1.EventType_EVENT_TYPE_TIMER_CANCELLED, v1.EventType_EVENT_TYPE_TIMER_FIRED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED)
}
