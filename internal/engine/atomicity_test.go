package engine

import (
	"bytes"
	"context"
	"errors"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"testing"
	"time"
)

func TestFailedAndExpiredWorkflowTasksFlushInboxInOrder(t *testing.T) {
	for _, exit := range []string{"sdk", "mismatch", "timeout"} {
		t.Run(exit, func(t *testing.T) {
			e, c, s := newTestEngine(t)
			mustStart(t, e, "r")
			p := mustPoll(t, e)
			for _, name := range []string{"first", "second"} {
				if _, err := e.SignalRun(context.Background(), "", &v1.SignalRunRequest{RunId: "r", Name: name}); err != nil {
					t.Fatal(err)
				}
			}
			end, after := v1.EventType_EVENT_TYPE_TASK_FAILED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED
			if exit == "timeout" {
				c.Advance(10 * time.Second)
				if n, err := e.ProcessDueTasks(context.Background(), 1); n != 1 || err != nil {
					t.Fatalf("expire %d %v", n, err)
				}
				end = v1.EventType_EVENT_TYPE_TASK_TIMED_OUT
			} else {
				cause := v1.TaskFailedCause_TASK_FAILED_CAUSE_SDK_ERROR
				if exit == "mismatch" {
					cause = v1.TaskFailedCause_TASK_FAILED_CAUSE_HISTORY_MISMATCH
					after = v1.EventType_EVENT_TYPE_RUN_BLOCKED
				}
				if _, err := e.FailWorkflowTask(context.Background(), &v1.FailWorkflowTaskRequest{TaskToken: p.TaskToken, Cause: cause}); err != nil {
					t.Fatal(err)
				}
			}
			wantTypes(t, history(t, e, "r"), v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED, end, v1.EventType_EVENT_TYPE_SIGNAL_RECEIVED, v1.EventType_EVENT_TYPE_SIGNAL_RECEIVED, after)
			h := historyEvents(t, e, "r")
			if h[4].GetSignalReceived().Name != "first" || h[5].GetSignalReceived().Name != "second" {
				t.Fatal("arrival order changed")
			}
			if err := s.InTx(context.Background(), func(tx store.Tx) error {
				n, err := tx.InboxSize("r")
				if err != nil {
					return err
				}
				r, err := tx.GetRun("r", false)
				if err != nil {
					return err
				}
				if n != 0 || r.InFlight {
					t.Fatalf("inbox=%d run=%+v", n, r)
				}
				if exit == "mismatch" {
					if r.Status != v1.RunStatus_RUN_STATUS_BLOCKED || r.WorkflowTaskID != 0 {
						t.Fatalf("blocked %+v", r)
					}
				} else if h[6].GetTaskScheduled().Attempt != 2 {
					t.Fatal("retry attempt")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestClosingCommandDiscardsBufferedEvents(t *testing.T) {
	e, _, s := newTestEngine(t)
	mustStart(t, e, "r")
	p := mustPoll(t, e)
	if _, err := e.SignalRun(context.Background(), "", &v1.SignalRunRequest{RunId: "r", Name: "late"}); err != nil {
		t.Fatal(err)
	}
	mustComplete(t, e, p.TaskToken, completeCmd())
	wantTypes(t, history(t, e, "r"), v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED, v1.EventType_EVENT_TYPE_TASK_COMPLETED, v1.EventType_EVENT_TYPE_RUN_COMPLETED)
	if err := s.InTx(context.Background(), func(tx store.Tx) error {
		n, err := tx.InboxSize("r")
		if n != 0 {
			t.Fatal("unreachable inbox retained")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTimerDeleteAndDeliveryRollBackTogether(t *testing.T) {
	e, c, s := newTestEngine(t)
	mustStart(t, e, "r")
	mustComplete(t, e, mustPoll(t, e).TaskToken, timerCmd(1, time.Second))
	c.Advance(time.Second)
	before := historyBytes(t, e, "r")
	fault := errors.New("timer delivery failed")
	e.deps.Store = &observedStore{Store: s, failAppend: 1, failure: fault}
	if n, err := e.FireDueTimers(context.Background(), 1); n != 0 || !errors.Is(err, fault) {
		t.Fatalf("injection %d %v", n, err)
	}
	if !bytes.Equal(before, historyBytes(t, e, "r")) {
		t.Fatal("partial fire committed")
	}
	if err := s.InTx(context.Background(), func(tx store.Tx) error {
		timers, err := tx.RunTimers("r")
		if len(timers) != 1 {
			t.Fatal("timer deleted despite rollback")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []int{1, 0} {
		if n, err := e.FireDueTimers(context.Background(), 1); n != want || err != nil {
			t.Fatalf("retry %d %v", n, err)
		}
	}
	wantTypes(t, history(t, e, "r"), v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED, v1.EventType_EVENT_TYPE_TASK_COMPLETED, v1.EventType_EVENT_TYPE_TIMER_STARTED, v1.EventType_EVENT_TYPE_TIMER_FIRED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED)
}

func TestTaskExpiryRollbackRestoresLeaseAndInbox(t *testing.T) {
	e, c, s := newTestEngine(t)
	ch, cancel := s.Subscribe(store.TaskWorkflow, "q")
	defer cancel()
	mustStart(t, e, "r")
	awaitNotification(t, ch)
	p := mustPoll(t, e)
	if _, err := e.SignalRun(context.Background(), "", &v1.SignalRunRequest{RunId: "r", Name: "s"}); err != nil {
		t.Fatal(err)
	}
	before := historyBytes(t, e, "r")
	c.Advance(10 * time.Second)
	fault := errors.New("reschedule failed")
	e.deps.Store = &observedStore{Store: s, failAppend: 3, failure: fault}
	if n, err := e.ProcessDueTasks(context.Background(), 1); n != 0 || !errors.Is(err, fault) {
		t.Fatalf("expiry %d %v", n, err)
	}
	if !bytes.Equal(before, historyBytes(t, e, "r")) {
		t.Fatal("failed expiry changed history")
	}
	select {
	case <-ch:
		t.Fatal("rolled back wakeup")
	default:
	}
	tok, _ := DecodeToken(p.TaskToken)
	if err := s.InTx(context.Background(), func(tx store.Tx) error {
		task, err := tx.GetTask(tok.TaskId, false)
		if err != nil {
			return err
		}
		n, err := tx.InboxSize("r")
		if n != 1 || task.LeasedUntil.IsZero() {
			t.Fatal("task or inbox lost")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if n, err := e.ProcessDueTasks(context.Background(), 1); n != 1 || err != nil {
		t.Fatalf("retry %d %v", n, err)
	}
	wantTypes(t, history(t, e, "r"), v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED, v1.EventType_EVENT_TYPE_TASK_TIMED_OUT, v1.EventType_EVENT_TYPE_SIGNAL_RECEIVED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED)
}

func TestContinueAsNewIsAtomicAcrossBothRuns(t *testing.T) {
	e, _, s := newTestEngine(t)
	mustStart(t, e, "r")
	p := mustPoll(t, e)
	before := historyBytes(t, e, "r")
	fault := errors.New("successor append failed")
	e.deps.Store = &observedStore{Store: s, failAppend: 3, failure: fault}
	cmd := &v1.Command{Attributes: &v1.Command_ContinueAsNew{ContinueAsNew: &v1.ContinueAsNewCommand{}}}
	if _, err := e.CompleteWorkflowTask(context.Background(), &v1.CompleteWorkflowTaskRequest{TaskToken: p.TaskToken, Commands: []*v1.Command{cmd}}); !errors.Is(err, fault) {
		t.Fatalf("continue fault %v", err)
	}
	if !bytes.Equal(before, historyBytes(t, e, "r")) {
		t.Fatal("predecessor closed despite rollback")
	}
	if _, err := e.DescribeRun(context.Background(), &v1.DescribeRunRequest{RunId: "r~2"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("successor escaped rollback %v", err)
	}
	mustComplete(t, e, p.TaskToken, cmd)
	wantTypes(t, history(t, e, "r"), v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED, v1.EventType_EVENT_TYPE_TASK_COMPLETED, v1.EventType_EVENT_TYPE_RUN_CONTINUED_AS_NEW)
	wantTypes(t, history(t, e, "r~2"), v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED)
}

func TestSignalDedupeReservationRollsBackWithDelivery(t *testing.T) {
	e, _, s := newTestEngine(t)
	mustStart(t, e, "r")
	before := historyBytes(t, e, "r")
	fault := errors.New("signal append failed")
	e.deps.Store = &observedStore{Store: s, failAppend: 1, failure: fault}
	req := &v1.SignalRunRequest{RunId: "r", Name: "s", RequestId: "once"}
	if _, err := e.SignalRun(context.Background(), "", req); !errors.Is(err, fault) {
		t.Fatalf("signal fault %v", err)
	}
	if !bytes.Equal(before, historyBytes(t, e, "r")) {
		t.Fatal("failed signal changed history")
	}
	for range 2 {
		if _, err := e.SignalRun(context.Background(), "", req); err != nil {
			t.Fatal(err)
		}
	}
	wantTypes(t, history(t, e, "r"), v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_SIGNAL_RECEIVED)
}
