package memstore_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"github.com/Aly700/capstan/internal/store/memstore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var epoch = time.Date(2026, 9, 28, 12, 0, 0, 123000, time.UTC)

func newStore(t *testing.T) store.Store {
	t.Helper()
	s := memstore.New()
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func tx(t *testing.T, s store.Store, fn func(store.Tx) error) {
	t.Helper()
	if err := s.InTx(context.Background(), fn); err != nil {
		t.Fatal(err)
	}
}

func run(id string) *store.Run {
	return &store.Run{RunID: id, WorkflowType: "workflow", TaskQueue: "q", Status: capstanv1.RunStatus_RUN_STATUS_RUNNING, TaskTimeout: time.Second, StartedAt: epoch, Input: payload("input")}
}

func payload(s string) *capstanv1.Payload {
	return &capstanv1.Payload{ContentType: "text/plain", Data: []byte(s)}
}

func event(id int64, name string) *capstanv1.HistoryEvent {
	return &capstanv1.HistoryEvent{EventId: id, Type: capstanv1.EventType_EVENT_TYPE_SIGNAL_RECEIVED, Time: timestamppb.New(epoch), Attributes: &capstanv1.HistoryEvent_SignalReceived{SignalReceived: &capstanv1.SignalReceivedAttributes{Name: name, Input: payload(name)}}}
}

func task(runID string, at time.Time) *store.Task {
	return &store.Task{RunID: runID, Kind: store.TaskActivity, TaskQueue: "q", Attempt: 1, VisibleAt: at, ScheduledAt: epoch, Activity: &capstanv1.ActivityScheduledAttributes{Input: payload("activity")}, HeartbeatDetails: payload("heartbeat"), LastFailure: &capstanv1.Failure{Details: payload("failure")}}
}

func TestDeepCopiesOnInputOutputAndRollback(t *testing.T) {
	s := newStore(t)
	r := run("r")
	r.Result = payload("result")
	r.Failure = &capstanv1.Failure{Cause: &capstanv1.Failure{Details: payload("cause")}}
	ev := event(1, "history")
	ev.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	original := proto.Clone(ev)
	taskRow := task("r", epoch)
	tx(t, s, func(tx store.Tx) error {
		if err := tx.InsertRun(r); err != nil {
			return err
		}
		if err := tx.InsertTask(taskRow); err != nil {
			return err
		}
		if err := tx.AppendEvents("r", []*capstanv1.HistoryEvent{ev}); err != nil {
			return err
		}
		return tx.PushInbox("r", event(999, "inbox"))
	})
	r.Input.Data[0] = 'X'
	r.Result.Data[0] = 'X'
	r.Failure.Cause.Details.Data[0] = 'X'
	ev.GetSignalReceived().Input.Data[0] = 'X'
	taskRow.Activity.Input.Data[0] = 'X'
	taskRow.HeartbeatDetails.Data[0] = 'X'
	taskRow.LastFailure.Details.Data[0] = 'X'
	tx(t, s, func(tx store.Tx) error {
		a, _ := tx.GetRun("r", false)
		if string(a.Input.Data) != "input" || string(a.Result.Data) != "result" || string(a.Failure.Cause.Details.Data) != "cause" {
			t.Fatal("run input aliases storage")
		}
		a.Input.Data[0] = 'Y'
		b, _ := tx.GetRun("r", false)
		if string(b.Input.Data) != "input" {
			t.Fatal("run output aliases storage")
		}
		h, _ := tx.ReadHistory("r", 0, 0)
		if !proto.Equal(h[0], original) {
			t.Fatal("history did not round-trip exactly")
		}
		h[0].GetSignalReceived().Input.Data[0] = 'Y'
		h, _ = tx.ReadHistory("r", 0, 0)
		if !proto.Equal(h[0], original) {
			t.Fatal("history output aliases storage")
		}
		c, _ := tx.GetTask(taskRow.ID, false)
		if string(c.Activity.Input.Data) != "activity" || string(c.HeartbeatDetails.Data) != "heartbeat" || string(c.LastFailure.Details.Data) != "failure" {
			t.Fatal("task input aliases storage")
		}
		c.Activity.Input.Data[0] = 'Y'
		d, _ := tx.GetTask(taskRow.ID, false)
		if string(d.Activity.Input.Data) != "activity" {
			t.Fatal("task output aliases storage")
		}
		return nil
	})
	abort := errors.New("abort")
	_ = s.InTx(context.Background(), func(tx store.Tx) error {
		in, _ := tx.DrainInbox("r")
		if len(in) != 1 || in[0].EventId != 0 {
			t.Fatal("inbox must ignore incoming event id")
		}
		in[0].GetSignalReceived().Input.Data[0] = 'Y'
		return abort
	})
	tx(t, s, func(tx store.Tx) error {
		in, _ := tx.DrainInbox("r")
		if string(in[0].GetSignalReceived().Input.Data) != "inbox" {
			t.Fatal("rollback aliases inbox")
		}
		n, _ := tx.InboxSize("r")
		if n != 0 {
			t.Fatal("drain did not empty inbox")
		}
		return nil
	})
}

func TestCancellationRollsBackAndDoesNotWaitForActiveTransaction(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newStore(t)
		entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		go func() {
			finished <- s.InTx(context.Background(), func(tx store.Tx) error { close(entered); <-release; return nil })
		}()
		<-entered
		ctx, cancel := context.WithCancel(context.Background())
		waiting := make(chan error, 1)
		go func() {
			waiting <- s.InTx(ctx, func(store.Tx) error { t.Error("cancelled callback executed"); return nil })
		}()
		synctest.Wait()
		cancel()
		synctest.Wait()
		select {
		case err := <-waiting:
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		default:
			t.Fatal("cancellation waited for active transaction")
		}
		close(release)
		if err := <-finished; err != nil {
			t.Fatal(err)
		}
		ctx, cancel = context.WithCancel(context.Background())
		err := s.InTx(ctx, func(tx store.Tx) error {
			if err := tx.InsertRun(run("cancelled")); err != nil {
				return err
			}
			cancel()
			return nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		tx(t, s, func(tx store.Tx) error {
			_, err := tx.GetRun("cancelled", false)
			if !errors.Is(err, store.ErrNotFound) {
				t.Fatal("cancelled transaction committed")
			}
			return nil
		})
	})
}

func TestPanicRollsBackAndReleasesTransaction(t *testing.T) {
	s := newStore(t)
	func() {
		defer func() {
			if recover() != "boom" {
				t.Fatal("panic changed")
			}
		}()
		_ = s.InTx(context.Background(), func(tx store.Tx) error {
			if err := tx.InsertRun(run("r")); err != nil {
				t.Fatal(err)
			}
			panic("boom")
		})
	}()
	tx(t, s, func(tx store.Tx) error { return tx.InsertRun(run("r")) })
}

func TestCloseIsIdempotentAndStopsTransactions(t *testing.T) {
	s := newStore(t)
	_, cancel := s.Subscribe(store.TaskWorkflow, "q")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.InTx(context.Background(), func(store.Tx) error { t.Fatal("callback after close"); return nil }); err == nil {
		t.Fatal("closed store accepted transaction")
	}
	_, cancel = s.Subscribe(store.TaskWorkflow, "q")
	cancel()
}

// Due queries break time ties the way the PostgreSQL store orders them (check_at,id;
// due_at,run_id,seq; check_at,run_id,approval_id), so a bounded sweep visits the same rows
// on either store and a lab seed replays the same schedule.
func TestDueQueriesBreakTimeTiesLikePostgres(t *testing.T) {
	s := newStore(t)
	due := epoch.Add(time.Second)
	tx(t, s, func(tx store.Tx) error {
		for _, id := range []string{"b", "a"} {
			if err := tx.InsertRun(run(id)); err != nil {
				return err
			}
		}
		for _, id := range []string{"b", "a", "b"} {
			row := task(id, epoch)
			row.CheckAt = due
			if err := tx.InsertTask(row); err != nil {
				return err
			}
		}
		for _, timer := range []store.Timer{{RunID: "b", Seq: 2, DueAt: due}, {RunID: "b", Seq: 1, DueAt: due}, {RunID: "a", Seq: 9, DueAt: due}} {
			if err := tx.InsertTimer(&timer); err != nil {
				return err
			}
		}
		for _, approval := range []store.Approval{{RunID: "b", ApprovalID: "y"}, {RunID: "b", ApprovalID: "x"}, {RunID: "a", ApprovalID: "z"}} {
			approval.Status, approval.Source, approval.RequestedAt, approval.CheckAt = store.ApprovalPending, capstanv1.ApprovalSource_APPROVAL_SOURCE_HUMAN, epoch, due
			if err := tx.InsertApproval(&approval); err != nil {
				return err
			}
		}
		return nil
	})
	tx(t, s, func(tx store.Tx) error {
		tasks, err := tx.DueTasks(due, 10)
		if err != nil {
			return err
		}
		for i, row := range tasks {
			if i > 0 && row.ID <= tasks[i-1].ID {
				t.Fatalf("due tasks with one check time are not in id order: %d after %d", row.ID, tasks[i-1].ID)
			}
		}
		timers, err := tx.DueTimers(due, 10)
		if err != nil {
			return err
		}
		var timerOrder []string
		for _, timer := range timers {
			timerOrder = append(timerOrder, timer.RunID+"/"+string(rune('0'+timer.Seq)))
		}
		if len(tasks) != 3 || len(timerOrder) != 3 || timerOrder[0] != "a/9" || timerOrder[1] != "b/1" || timerOrder[2] != "b/2" {
			t.Fatalf("due order: %d tasks, timers %v", len(tasks), timerOrder)
		}
		approvals, err := tx.DueApprovals(due, 10)
		if err != nil {
			return err
		}
		var approvalOrder []string
		for _, approval := range approvals {
			approvalOrder = append(approvalOrder, approval.RunID+"/"+approval.ApprovalID)
		}
		if len(approvalOrder) != 3 || approvalOrder[0] != "a/z" || approvalOrder[1] != "b/x" || approvalOrder[2] != "b/y" {
			t.Fatalf("due approvals order: %v", approvalOrder)
		}
		return nil
	})
}
