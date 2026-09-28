package engine

import (
	"bytes"
	"context"
	"errors"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/types/known/durationpb"
	"testing"
	"time"
)

func TestWorkerMethodsUseOneTransaction(t *testing.T) {
	e, _, s := newTestEngine(t)
	wrapped := &observedStore{Store: s}
	e.deps.Store = wrapped
	mustStart(t, e, "r")
	check := func(name string, fn func() error) {
		t.Helper()
		n := wrapped.transactions
		if err := fn(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if wrapped.transactions-n != 1 {
			t.Fatalf("%s transactions=%d", name, wrapped.transactions-n)
		}
	}
	var p *v1.PollWorkflowTaskResponse
	check("workflow poll", func() error {
		var err error
		p, _, err = e.PollWorkflowTask(context.Background(), &v1.PollWorkflowTaskRequest{TaskQueue: "q"})
		return err
	})
	check("workflow complete", func() error {
		_, err := e.CompleteWorkflowTask(context.Background(), &v1.CompleteWorkflowTaskRequest{TaskToken: p.TaskToken, Commands: []*v1.Command{activityCmd(1), activityCmd(2)}})
		return err
	})
	var a *v1.PollActivityTaskResponse
	check("activity poll", func() error {
		var err error
		a, _, err = e.PollActivityTask(context.Background(), &v1.PollActivityTaskRequest{TaskQueue: "q"})
		return err
	})
	check("heartbeat", func() error {
		_, err := e.HeartbeatActivityTask(context.Background(), &v1.HeartbeatActivityTaskRequest{TaskToken: a.TaskToken})
		return err
	})
	var reservation *v1.ReserveAICallResponse
	check("reserve", func() error {
		var err error
		reservation, err = e.ReserveAICall(context.Background(), &v1.ReserveAICallRequest{TaskToken: a.TaskToken, EstimateUsd: .1})
		return err
	})
	check("finish", func() error {
		_, err := e.FinishAICall(context.Background(), &v1.FinishAICallRequest{ReservationId: reservation.ReservationId, Ok: true})
		return err
	})
	check("activity complete", func() error {
		_, err := e.CompleteActivityTask(context.Background(), &v1.CompleteActivityTaskRequest{TaskToken: a.TaskToken})
		return err
	})
	check("activity poll", func() error {
		var err error
		a, _, err = e.PollActivityTask(context.Background(), &v1.PollActivityTaskRequest{TaskQueue: "q"})
		return err
	})
	check("activity failure", func() error {
		_, err := e.FailActivityTask(context.Background(), &v1.FailActivityTaskRequest{TaskToken: a.TaskToken, Failure: &v1.Failure{NonRetryable: true}})
		return err
	})
	p = mustPoll(t, e)
	check("workflow failure", func() error {
		_, err := e.FailWorkflowTask(context.Background(), &v1.FailWorkflowTaskRequest{TaskToken: p.TaskToken, Cause: v1.TaskFailedCause_TASK_FAILED_CAUSE_HISTORY_MISMATCH})
		return err
	})
	check("resume", func() error {
		_, err := e.ResumeRun(context.Background(), "", &v1.ResumeRunRequest{RunId: "r"})
		return err
	})
	check("next wakeup", func() error { _, _, err := e.NextWakeup(context.Background()); return err })
}

func TestBackgroundMethodsUseOneTransactionPerItem(t *testing.T) {
	for _, kind := range []string{"timers", "tasks", "approvals", "runs"} {
		t.Run(kind, func(t *testing.T) {
			e, c, s := newTestEngine(t)
			for _, id := range []string{"a", "b"} {
				if kind == "runs" {
					if _, err := e.StartRun(context.Background(), "", &v1.StartRunRequest{RunId: id, WorkflowType: "flow", TaskQueue: "q", RunTimeout: durationpb.New(time.Second)}); err != nil {
						t.Fatal(err)
					}
					continue
				}
				mustStart(t, e, id)
				p := mustPoll(t, e)
				switch kind {
				case "tasks":
				case "timers":
					mustComplete(t, e, p.TaskToken, timerCmd(1, time.Second))
				case "approvals":
					mustComplete(t, e, p.TaskToken, &v1.Command{Attributes: &v1.Command_RequestApproval{RequestApproval: &v1.RequestApprovalCommand{Seq: 1, ApprovalId: "a", Source: v1.ApprovalSource_APPROVAL_SOURCE_HUMAN, Timeout: durationpb.New(time.Second)}}})
				}
			}
			c.Advance(10 * time.Second)
			wrapped := &observedStore{Store: s}
			e.deps.Store = wrapped
			var n int
			var err error
			switch kind {
			case "timers":
				n, err = e.FireDueTimers(context.Background(), 2)
			case "tasks":
				n, err = e.ProcessDueTasks(context.Background(), 2)
			case "approvals":
				n, err = e.ProcessDueApprovals(context.Background(), 2)
			case "runs":
				n, err = e.TimeoutRuns(context.Background(), 2)
			}
			if err != nil || n != 2 || wrapped.transactions != 2 {
				t.Fatalf("items=%d tx=%d err=%v", n, wrapped.transactions, err)
			}
		})
	}
}

type observedStore struct {
	store.Store
	transactions int
	failAppend   int
	failure      error
}

func (s *observedStore) InTx(ctx context.Context, fn func(store.Tx) error) error {
	s.transactions++
	return s.Store.InTx(ctx, func(tx store.Tx) error { return fn(&observedTx{Tx: tx, owner: s}) })
}

type observedTx struct {
	store.Tx
	owner *observedStore
}

func (tx *observedTx) AppendEvents(id string, events []*v1.HistoryEvent) error {
	if tx.owner.failAppend > 0 {
		tx.owner.failAppend--
		if tx.owner.failAppend == 0 {
			return tx.owner.failure
		}
	}
	return tx.Tx.AppendEvents(id, events)
}

func TestWorkflowCompletionRollsBackEveryCommandOnStoreError(t *testing.T) {
	e, _, s := newTestEngine(t)
	mustStart(t, e, "r")
	p := mustPoll(t, e)
	before := historyBytes(t, e, "r")
	sentinel := errors.New("injected append failure")
	wrapped := &observedStore{Store: s, failAppend: 3, failure: sentinel}
	e.deps.Store = wrapped
	notify, cancel := s.Subscribe(store.TaskActivity, "q")
	defer cancel()
	_, err := e.CompleteWorkflowTask(context.Background(), &v1.CompleteWorkflowTaskRequest{TaskToken: p.TaskToken, Commands: []*v1.Command{activityCmd(1), markerCmd(2)}})
	if !errors.Is(err, sentinel) {
		t.Fatalf("fault: %v", err)
	}
	if wrapped.transactions != 1 {
		t.Fatalf("completion transactions=%d", wrapped.transactions)
	}
	if !bytes.Equal(before, historyBytes(t, e, "r")) {
		t.Fatal("partial command history committed")
	}
	select {
	case <-notify:
		t.Fatal("rolled back notification escaped")
	default:
	}
	if err := s.InTx(context.Background(), func(tx store.Tx) error {
		tasks, err := tx.RunTasks("r")
		if err != nil {
			return err
		}
		if len(tasks) != 1 || tasks[0].Kind != store.TaskWorkflow {
			t.Fatalf("rolled back tasks %+v", tasks)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	mustComplete(t, e, p.TaskToken, activityCmd(1), markerCmd(2))
}
func TestRunClientMethodsUseOneTransaction(t *testing.T) {
	e, _, s := newTestEngine(t)
	wrapped := &observedStore{Store: s}
	e.deps.Store = wrapped
	check := func(name string, fn func() error) {
		t.Helper()
		before := wrapped.transactions
		if err := fn(); err != nil {
			t.Fatal(err)
		}
		if wrapped.transactions-before != 1 {
			t.Fatalf("%s transactions %d", name, wrapped.transactions-before)
		}
	}
	check("start", func() error {
		_, err := e.StartRun(context.Background(), "", &v1.StartRunRequest{RunId: "r", WorkflowType: "flow", TaskQueue: "q"})
		return err
	})
	check("describe", func() error {
		_, err := e.DescribeRun(context.Background(), &v1.DescribeRunRequest{RunId: "r"})
		return err
	})
	check("list", func() error { _, err := e.ListRuns(context.Background(), &v1.ListRunsRequest{}); return err })
	check("history", func() error {
		_, err := e.GetHistory(context.Background(), &v1.GetHistoryRequest{RunId: "r"})
		return err
	})
	check("signal", func() error {
		_, err := e.SignalRun(context.Background(), "", &v1.SignalRunRequest{RunId: "r", Name: "s"})
		return err
	})
	check("cancel", func() error {
		_, err := e.CancelRun(context.Background(), "", &v1.CancelRunRequest{RunId: "r"})
		return err
	})
}
