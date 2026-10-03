package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestRunTimeoutClosesRun(t *testing.T) {
	e, clock, s := newTestEngine(t)
	if _, err := e.StartRun(context.Background(), "owner", &v1.StartRunRequest{RunId: "timeout", WorkflowType: "wf", TaskQueue: "q", RunTimeout: durationpb.New(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	w := mustPoll(t, e)
	mustComplete(t, e, w.TaskToken, activityCmd(1), timerCmd(2, time.Hour), approvalCmd(3, "approval", v1.ApprovalSource_APPROVAL_SOURCE_HUMAN, time.Hour))
	if n, err := e.TimeoutRuns(context.Background(), 10); n != 0 || err != nil {
		t.Fatalf("early: %d %v", n, err)
	}
	clock.Advance(time.Minute)
	if n, err := e.TimeoutRuns(context.Background(), 10); n != 1 || err != nil {
		t.Fatalf("timeout: %d %v", n, err)
	}
	if n, err := e.TimeoutRuns(context.Background(), 10); n != 0 || err != nil {
		t.Fatalf("repeat: %d %v", n, err)
	}
	if err := s.InTx(context.Background(), func(tx store.Tx) error {
		r, err := tx.GetRun("timeout", false)
		if err != nil {
			return err
		}
		if r.Status != v1.RunStatus_RUN_STATUS_TIMED_OUT || !r.ClosedAt.Equal(clock.Now()) || r.InFlight || r.WorkflowTaskID != 0 {
			t.Errorf("timed out run: %+v", r)
		}
		tasks, err := tx.RunTasks(r.RunID)
		if err != nil {
			return err
		}
		timers, err := tx.RunTimers(r.RunID)
		if err != nil {
			return err
		}
		approvals, err := tx.RunApprovals(r.RunID)
		if err != nil {
			return err
		}
		if len(tasks) != 0 || len(timers) != 0 || len(approvals) != 1 || approvals[0].Status != store.ApprovalExpired {
			t.Errorf("remaining work: tasks=%v timers=%v approvals=%v", tasks, timers, approvals)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	wantTypes(t, history(t, e, "timeout"),
		v1.EventType_EVENT_TYPE_RUN_STARTED,
		v1.EventType_EVENT_TYPE_TASK_SCHEDULED,
		v1.EventType_EVENT_TYPE_TASK_STARTED,
		v1.EventType_EVENT_TYPE_TASK_COMPLETED,
		v1.EventType_EVENT_TYPE_ACTIVITY_SCHEDULED,
		v1.EventType_EVENT_TYPE_TIMER_STARTED,
		v1.EventType_EVENT_TYPE_APPROVAL_REQUESTED,
		v1.EventType_EVENT_TYPE_RUN_TIMED_OUT,
	)
}

func TestRunTimeoutFlushesInFlightInbox(t *testing.T) {
	e, clock, _ := newTestEngine(t)
	if _, err := e.StartRun(context.Background(), "owner", &v1.StartRunRequest{RunId: "timeout", WorkflowType: "wf", TaskQueue: "q", RunTimeout: durationpb.New(time.Second)}); err != nil {
		t.Fatal(err)
	}
	w := mustPoll(t, e)
	for _, name := range []string{"first", "second"} {
		if _, err := e.SignalRun(context.Background(), "owner", &v1.SignalRunRequest{RunId: "timeout", Name: name}); err != nil {
			t.Fatal(err)
		}
	}
	clock.Advance(time.Second)
	if n, err := e.TimeoutRuns(context.Background(), 1); n != 1 || err != nil {
		t.Fatalf("timeout: %d %v", n, err)
	}
	h, err := e.GetHistory(context.Background(), &v1.GetHistoryRequest{RunId: "timeout"})
	if err != nil {
		t.Fatal(err)
	}
	wantTypes(t, history(t, e, "timeout"),
		v1.EventType_EVENT_TYPE_RUN_STARTED,
		v1.EventType_EVENT_TYPE_TASK_SCHEDULED,
		v1.EventType_EVENT_TYPE_TASK_STARTED,
		v1.EventType_EVENT_TYPE_SIGNAL_RECEIVED,
		v1.EventType_EVENT_TYPE_SIGNAL_RECEIVED,
		v1.EventType_EVENT_TYPE_RUN_TIMED_OUT,
	)
	if h.Events[3].GetSignalReceived().GetName() != "first" || h.Events[4].GetSignalReceived().GetName() != "second" {
		t.Fatalf("history: %v", h.Events)
	}
	if _, err := e.CompleteWorkflowTask(context.Background(), &v1.CompleteWorkflowTaskRequest{TaskToken: w.TaskToken}); !errors.Is(err, ErrStaleTask) {
		t.Fatalf("late completion: %v", err)
	}
}

func TestRunTimeoutClosesBlockedRun(t *testing.T) {
	e, clock, _ := newTestEngine(t)
	if _, err := e.StartRun(context.Background(), "owner", &v1.StartRunRequest{RunId: "timeout", WorkflowType: "wf", TaskQueue: "q", RunTimeout: durationpb.New(time.Second)}); err != nil {
		t.Fatal(err)
	}
	w := mustPoll(t, e)
	if _, err := e.FailWorkflowTask(context.Background(), &v1.FailWorkflowTaskRequest{TaskToken: w.TaskToken, Cause: v1.TaskFailedCause_TASK_FAILED_CAUSE_HISTORY_MISMATCH, Failure: &v1.Failure{Type: "HistoryMismatch"}}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	if n, err := e.TimeoutRuns(context.Background(), 1); n != 1 || err != nil {
		t.Fatalf("blocked timeout: %d %v", n, err)
	}
	r, err := e.DescribeRun(context.Background(), &v1.DescribeRunRequest{RunId: "timeout"})
	if err != nil || r.GetRun().GetStatus() != v1.RunStatus_RUN_STATUS_TIMED_OUT {
		t.Fatalf("status: %v %v", r, err)
	}
}

func TestRunTimeoutHonorsLimit(t *testing.T) {
	e, clock, _ := newTestEngine(t)
	for _, id := range []string{"one", "two"} {
		if _, err := e.StartRun(context.Background(), "owner", &v1.StartRunRequest{RunId: id, WorkflowType: "wf", TaskQueue: "q", RunTimeout: durationpb.New(time.Second)}); err != nil {
			t.Fatal(err)
		}
	}
	clock.Advance(time.Second)
	if n, err := e.TimeoutRuns(context.Background(), 0); n != 0 || !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("zero limit: %d %v", n, err)
	}
	for _, limit := range []int{1, 1} {
		if n, err := e.TimeoutRuns(context.Background(), limit); n != limit || err != nil {
			t.Fatalf("limit %d: %d %v", limit, n, err)
		}
	}
}

func TestRunTimeoutRollsBackInboxDrainOnAppendFailure(t *testing.T) {
	e, clock, s := newTestEngine(t)
	if _, err := e.StartRun(context.Background(), "owner", &v1.StartRunRequest{RunId: "timeout", WorkflowType: "wf", TaskQueue: "q", RunTimeout: durationpb.New(time.Second)}); err != nil {
		t.Fatal(err)
	}
	mustPoll(t, e)
	if _, err := e.SignalRun(context.Background(), "owner", &v1.SignalRunRequest{RunId: "timeout", Name: "buffered"}); err != nil {
		t.Fatal(err)
	}
	before := historyBytes(t, e, "timeout")
	clock.Advance(time.Second)
	sentinel := errors.New("injected timeout append failure")
	observed := &observedStore{Store: s, failAppend: 1, failure: sentinel}
	e.deps.Store = observed
	if n, err := e.TimeoutRuns(context.Background(), 1); n != 0 || !errors.Is(err, sentinel) {
		t.Fatalf("timeout fault: %d %v", n, err)
	}
	if observed.transactions != 1 {
		t.Fatalf("timeout transactions=%d", observed.transactions)
	}
	if got := historyBytes(t, e, "timeout"); string(got) != string(before) {
		t.Fatal("timeout history survived rollback")
	}
	if err := s.InTx(context.Background(), func(tx store.Tx) error {
		r, err := tx.GetRun("timeout", false)
		if err != nil {
			return err
		}
		n, err := tx.InboxSize(r.RunID)
		if err != nil {
			return err
		}
		if r.Status != v1.RunStatus_RUN_STATUS_RUNNING || !r.InFlight || n != 1 {
			t.Errorf("timeout rollback: run=%+v inbox=%d", r, n)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if n, err := e.TimeoutRuns(context.Background(), 1); n != 1 || err != nil {
		t.Fatalf("timeout retry: %d %v", n, err)
	}
}
