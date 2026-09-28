package engine

import (
	"bytes"
	"context"
	"errors"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"testing"
)

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
