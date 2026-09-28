package engine

import (
	"bytes"
	"context"
	"errors"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"testing"
)

func TestSignalDedupeByRequestID(t *testing.T) {
	e, _, _ := newTestEngine(t)
	mustStart(t, e, "r")
	req := &v1.SignalRunRequest{RunId: "r", Name: "s", RequestId: "dedupe"}
	if _, err := e.SignalRun(context.Background(), "sender", req); err != nil {
		t.Fatal(err)
	}
	before := historyBytes(t, e, "r")
	if _, err := e.SignalRun(context.Background(), "sender", req); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, historyBytes(t, e, "r")) {
		t.Fatal("duplicate signal")
	}
	wantTypes(t, history(t, e, "r"), v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_SIGNAL_RECEIVED)
}
func TestSignalToClosedRunIsRunClosed(t *testing.T) {
	e, _, _ := newTestEngine(t)
	mustStart(t, e, "r")
	mustComplete(t, e, mustPoll(t, e).TaskToken, completeCmd())
	before := historyBytes(t, e, "r")
	if _, err := e.SignalRun(context.Background(), "", &v1.SignalRunRequest{RunId: "r", Name: "s"}); !errors.Is(err, ErrRunClosed) {
		t.Fatalf("signal %v", err)
	}
	if _, err := e.CancelRun(context.Background(), "", &v1.CancelRunRequest{RunId: "r"}); !errors.Is(err, ErrRunClosed) {
		t.Fatalf("cancel %v", err)
	}
	if !bytes.Equal(before, historyBytes(t, e, "r")) {
		t.Fatal("closed history changed")
	}
}
func TestCancelRunDeliversRequest(t *testing.T) {
	e, _, _ := newTestEngine(t)
	mustStart(t, e, "r")
	p := mustPoll(t, e)
	for range 2 {
		if _, err := e.CancelRun(context.Background(), "client", &v1.CancelRunRequest{RunId: "r", Reason: "stop"}); err != nil {
			t.Fatal(err)
		}
	}
	wantTypes(t, history(t, e, "r"), v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED)
	mustComplete(t, e, p.TaskToken, markerCmd(1))
	wantTypes(t, history(t, e, "r"), v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED, v1.EventType_EVENT_TYPE_TASK_COMPLETED, v1.EventType_EVENT_TYPE_MARKER_RECORDED, v1.EventType_EVENT_TYPE_RUN_CANCEL_REQUESTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED)
}
func TestEventsForBlockedRunAreKeptAndDeliveredAfterResume(t *testing.T) {
	e, _, _ := newTestEngine(t)
	mustStart(t, e, "r")
	mustComplete(t, e, mustPoll(t, e).TaskToken, activityCmd(1))
	a, found, err := e.PollActivityTask(context.Background(), &v1.PollActivityTaskRequest{TaskQueue: "q"})
	if err != nil || !found {
		t.Fatalf("activity %v %v", found, err)
	}
	if _, err := e.SignalRun(context.Background(), "", &v1.SignalRunRequest{RunId: "r", Name: "wake"}); err != nil {
		t.Fatal(err)
	}
	p := mustPoll(t, e)
	if _, err := e.FailWorkflowTask(context.Background(), &v1.FailWorkflowTaskRequest{TaskToken: p.TaskToken, Cause: v1.TaskFailedCause_TASK_FAILED_CAUSE_HISTORY_MISMATCH}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.SignalRun(context.Background(), "", &v1.SignalRunRequest{RunId: "r", Name: "blocked_signal"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.CompleteActivityTask(context.Background(), &v1.CompleteActivityTaskRequest{TaskToken: a.TaskToken}); err != nil {
		t.Fatal(err)
	}
	want := []v1.EventType{v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED, v1.EventType_EVENT_TYPE_TASK_COMPLETED, v1.EventType_EVENT_TYPE_ACTIVITY_SCHEDULED, v1.EventType_EVENT_TYPE_SIGNAL_RECEIVED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED, v1.EventType_EVENT_TYPE_TASK_FAILED, v1.EventType_EVENT_TYPE_RUN_BLOCKED, v1.EventType_EVENT_TYPE_SIGNAL_RECEIVED, v1.EventType_EVENT_TYPE_ACTIVITY_COMPLETED}
	wantTypes(t, history(t, e, "r"), want...)
	if _, found, err := e.PollWorkflowTask(context.Background(), &v1.PollWorkflowTaskRequest{TaskQueue: "q"}); err != nil || found {
		t.Fatalf("blocked poll %v %v", found, err)
	}
	if _, err := e.ResumeRun(context.Background(), "operator", &v1.ResumeRunRequest{RunId: "r", Reason: "fixed"}); err != nil {
		t.Fatal(err)
	}
	want = append(want, v1.EventType_EVENT_TYPE_RUN_RESUMED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED)
	wantTypes(t, history(t, e, "r"), want...)
	p = mustPoll(t, e)
	if len(p.History) != len(want)+1 {
		t.Fatal("resumed task missing history")
	}
}
func TestResumeRequiresBlocked(t *testing.T) {
	e, _, _ := newTestEngine(t)
	mustStart(t, e, "r")
	if _, err := e.ResumeRun(context.Background(), "", &v1.ResumeRunRequest{RunId: "r"}); !errors.Is(err, ErrFailedPrecondition) {
		t.Fatalf("resume %v", err)
	}
}
func TestSignalsCancellationAndResumeMissingRun(t *testing.T) {
	e, _, _ := newTestEngine(t)
	_, a := e.SignalRun(context.Background(), "", &v1.SignalRunRequest{RunId: "missing", Name: "s"})
	_, b := e.CancelRun(context.Background(), "", &v1.CancelRunRequest{RunId: "missing"})
	_, c := e.ResumeRun(context.Background(), "", &v1.ResumeRunRequest{RunId: "missing"})
	for _, err := range []error{a, b, c} {
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("missing %v", err)
		}
	}
}
