package engine

import (
	"bytes"
	"errors"
	"testing"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/proto"
)

func assertTerminated(t *testing.T, e *Engine, id, reason string) {
	t.Helper()
	h := historyEvents(t, e, id)
	terminal := h[len(h)-1]
	if terminal.Type != v1.EventType_EVENT_TYPE_RUN_FAILED || terminal.GetRunFailed().GetTaskCompletedEventId() != 0 || terminal.GetRunFailed().GetFailure().GetType() != "Terminated" || terminal.GetRunFailed().GetFailure().GetMessage() != reason {
		t.Fatalf("termination event: %v", terminal)
	}
	for i, event := range h {
		if event.EventId != int64(i+1) {
			t.Fatalf("history gap: %v", event)
		}
		if i < len(h)-1 && event.Type == v1.EventType_EVENT_TYPE_RUN_FAILED {
			t.Fatal("duplicate terminal event")
		}
	}
	if err := e.deps.Store.InTx(t.Context(), func(tx store.Tx) error {
		r, err := tx.GetRun(id, false)
		if err != nil {
			return err
		}
		if r.Status != v1.RunStatus_RUN_STATUS_FAILED || r.InFlight || r.WorkflowTaskID != 0 || r.LastEventID != terminal.EventId || r.ClosedAt.IsZero() || !proto.Equal(r.Failure, terminal.GetRunFailed().Failure) {
			t.Fatalf("terminated projection: %+v", r)
		}
		tasks, err := tx.RunTasks(id)
		if err != nil {
			return err
		}
		timers, err := tx.RunTimers(id)
		if err != nil {
			return err
		}
		inbox, err := tx.InboxSize(id)
		if err != nil {
			return err
		}
		approvals, err := tx.RunApprovals(id)
		if err != nil {
			return err
		}
		if len(tasks) != 0 || len(timers) != 0 || inbox != 0 {
			t.Fatalf("work after termination: tasks=%v timers=%v inbox=%d", tasks, timers, inbox)
		}
		for _, a := range approvals {
			if a.Status == store.ApprovalPending {
				t.Fatalf("pending approval: %+v", a)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTerminateRunFlushesInboxAndClosesRunningRun(t *testing.T) {
	e, clock, s := newTestEngine(t)
	mustStart(t, e, "r")
	mustComplete(t, e, mustPoll(t, e).TaskToken,
		activityCmd(1), activityCmd(2), timerCmd(3, time.Second), timerCmd(4, time.Hour),
		approvalCmd(5, "resolved", v1.ApprovalSource_APPROVAL_SOURCE_HUMAN, time.Hour),
		approvalCmd(6, "pending", v1.ApprovalSource_APPROVAL_SOURCE_HUMAN, time.Hour))
	finished, late := mustActivity(t, e), mustActivity(t, e)
	if _, err := e.SignalRun(t.Context(), "owner", &v1.SignalRunRequest{RunId: "r", Name: "wake"}); err != nil {
		t.Fatal(err)
	}
	w := mustPoll(t, e)
	prefix := historyBytes(t, e, "r")
	if _, err := e.SignalRun(t.Context(), "owner", &v1.SignalRunRequest{RunId: "r", Name: "first"}); err != nil {
		t.Fatal(err)
	}
	mustFinishActivity(t, e, finished.TaskToken)
	clock.Advance(time.Second)
	if n, err := e.FireDueTimers(t.Context(), 1); n != 1 || err != nil {
		t.Fatalf("timer: %d %v", n, err)
	}
	if _, err := e.ResolveApproval(t.Context(), "owner", &v1.ResolveApprovalRequest{RunId: "r", ApprovalId: "resolved", Outcome: v1.ApprovalOutcome_APPROVAL_OUTCOME_APPROVED}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.CancelRun(t.Context(), "owner", &v1.CancelRunRequest{RunId: "r", Reason: "cooperative"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.SignalRun(t.Context(), "owner", &v1.SignalRunRequest{RunId: "r", Name: "last"}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(prefix, historyBytes(t, e, "r")) {
		t.Fatal("inbox changed in-flight history")
	}
	if _, err := e.TerminateRun(t.Context(), "owner", &v1.TerminateRunRequest{RunId: "r", Reason: "operator stop"}); err != nil {
		t.Fatal(err)
	}
	assertTerminated(t, e, "r", "operator stop")
	h := history(t, e, "r")
	wantTypes(t, h[len(h)-7:], v1.EventType_EVENT_TYPE_SIGNAL_RECEIVED, v1.EventType_EVENT_TYPE_ACTIVITY_COMPLETED,
		v1.EventType_EVENT_TYPE_TIMER_FIRED, v1.EventType_EVENT_TYPE_APPROVAL_RESOLVED,
		v1.EventType_EVENT_TYPE_RUN_CANCEL_REQUESTED, v1.EventType_EVENT_TYPE_SIGNAL_RECEIVED, v1.EventType_EVENT_TYPE_RUN_FAILED)
	events := historyEvents(t, e, "r")
	if events[len(events)-7].GetSignalReceived().Name != "first" || events[len(events)-2].GetSignalReceived().Name != "last" {
		t.Fatal("inbox arrival order changed")
	}
	if err := s.InTx(t.Context(), func(tx store.Tx) error {
		resolved, err := tx.GetApproval("r", "resolved", false)
		if err != nil {
			return err
		}
		pending, err := tx.GetApproval("r", "pending", false)
		if err != nil {
			return err
		}
		if resolved.Status != store.ApprovalApproved || pending.Status != store.ApprovalExpired {
			t.Fatalf("approval cleanup: %+v %+v", resolved, pending)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	before := historyBytes(t, e, "r")
	if _, err := e.CompleteWorkflowTask(t.Context(), &v1.CompleteWorkflowTaskRequest{TaskToken: w.TaskToken, Commands: []*v1.Command{completeCmd()}}); !errors.Is(err, ErrStaleTask) {
		t.Fatalf("late workflow: %v", err)
	}
	if _, err := e.CompleteActivityTask(t.Context(), &v1.CompleteActivityTaskRequest{TaskToken: late.TaskToken}); !errors.Is(err, ErrStaleTask) {
		t.Fatalf("late activity: %v", err)
	}
	clock.Advance(2 * time.Hour)
	if n, err := e.FireDueTimers(t.Context(), 10); n != 0 || err != nil {
		t.Fatalf("late timers: %d %v", n, err)
	}
	if !bytes.Equal(before, historyBytes(t, e, "r")) {
		t.Fatal("late work changed terminated history")
	}
}

func TestTerminateRunClosesBlockedRunWithDefaultReason(t *testing.T) {
	e, _, _ := newTestEngine(t)
	mustStart(t, e, "r")
	w := mustPoll(t, e)
	if _, err := e.FailWorkflowTask(t.Context(), &v1.FailWorkflowTaskRequest{TaskToken: w.TaskToken, Cause: v1.TaskFailedCause_TASK_FAILED_CAUSE_HISTORY_MISMATCH, Failure: &v1.Failure{Type: "HistoryMismatch", Message: "old failure"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.SignalRun(t.Context(), "owner", &v1.SignalRunRequest{RunId: "r", Name: "kept"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.TerminateRun(t.Context(), "owner", &v1.TerminateRunRequest{RunId: "r"}); err != nil {
		t.Fatal(err)
	}
	assertTerminated(t, e, "r", "terminated")
	wantTypes(t, history(t, e, "r"), v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED,
		v1.EventType_EVENT_TYPE_TASK_STARTED, v1.EventType_EVENT_TYPE_TASK_FAILED, v1.EventType_EVENT_TYPE_RUN_BLOCKED,
		v1.EventType_EVENT_TYPE_SIGNAL_RECEIVED, v1.EventType_EVENT_TYPE_RUN_FAILED)
}

func TestTerminateRunRejectsClosedAndMissingRuns(t *testing.T) {
	e, _, s := newTestEngine(t)
	if _, err := e.TerminateRun(t.Context(), "", &v1.TerminateRunRequest{RunId: "missing"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing run: %v", err)
	}
	for _, status := range []v1.RunStatus{v1.RunStatus_RUN_STATUS_COMPLETED, v1.RunStatus_RUN_STATUS_FAILED, v1.RunStatus_RUN_STATUS_CANCELLED, v1.RunStatus_RUN_STATUS_TIMED_OUT, v1.RunStatus_RUN_STATUS_CONTINUED_AS_NEW} {
		t.Run(status.String(), func(t *testing.T) {
			id := status.String()
			mustStart(t, e, id)
			if err := s.InTx(t.Context(), func(tx store.Tx) error {
				r, err := tx.GetRun(id, true)
				if err != nil {
					return err
				}
				r.Status = status
				return tx.UpdateRun(r)
			}); err != nil {
				t.Fatal(err)
			}
			before := historyBytes(t, e, id)
			if _, err := e.TerminateRun(t.Context(), "", &v1.TerminateRunRequest{RunId: id}); !errors.Is(err, ErrRunClosed) {
				t.Fatalf("closed run: %v", err)
			}
			if !bytes.Equal(before, historyBytes(t, e, id)) {
				t.Fatal("closed history changed")
			}
		})
	}
}

func TestTerminateRunRollsBackInboxAndCleanupOnFailure(t *testing.T) {
	for _, stage := range []string{"terminal_append", "commit"} {
		t.Run(stage, func(t *testing.T) {
			e, _, s := newTestEngine(t)
			mustStart(t, e, "r")
			mustComplete(t, e, mustPoll(t, e).TaskToken, activityCmd(1), timerCmd(2, time.Hour), approvalCmd(3, "a", v1.ApprovalSource_APPROVAL_SOURCE_HUMAN, time.Hour))
			if _, err := e.SignalRun(t.Context(), "", &v1.SignalRunRequest{RunId: "r", Name: "wake"}); err != nil {
				t.Fatal(err)
			}
			mustPoll(t, e)
			if _, err := e.SignalRun(t.Context(), "", &v1.SignalRunRequest{RunId: "r", Name: "keep"}); err != nil {
				t.Fatal(err)
			}
			before := historyBytes(t, e, "r")
			fault := errors.New("injected termination failure")
			if stage == "terminal_append" {
				e.deps.Store = &observedStore{Store: s, failAppend: 2, failure: fault}
			} else {
				e.deps.Store = &failedCommitStore{Store: s, remaining: 1, failure: fault}
			}
			if _, err := e.TerminateRun(t.Context(), "", &v1.TerminateRunRequest{RunId: "r"}); !errors.Is(err, fault) {
				t.Fatalf("termination failure: %v", err)
			}
			e.deps.Store = s
			if !bytes.Equal(before, historyBytes(t, e, "r")) {
				t.Fatal("failed termination changed history")
			}
			if err := s.InTx(t.Context(), func(tx store.Tx) error {
				r, err := tx.GetRun("r", false)
				if err != nil {
					return err
				}
				tasks, err := tx.RunTasks("r")
				if err != nil {
					return err
				}
				timers, err := tx.RunTimers("r")
				if err != nil {
					return err
				}
				a, err := tx.GetApproval("r", "a", false)
				if err != nil {
					return err
				}
				n, err := tx.InboxSize("r")
				if err != nil {
					return err
				}
				if r.Status != v1.RunStatus_RUN_STATUS_RUNNING || !r.InFlight || len(tasks) != 2 || len(timers) != 1 || a.Status != store.ApprovalPending || n != 1 {
					t.Fatalf("rollback lost state: run=%+v tasks=%v timers=%v approval=%+v inbox=%d", r, tasks, timers, a, n)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := e.TerminateRun(t.Context(), "", &v1.TerminateRunRequest{RunId: "r"}); err != nil {
				t.Fatal(err)
			}
			assertTerminated(t, e, "r", "terminated")
		})
	}
}
