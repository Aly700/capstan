package memstore_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
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

func TestRollbackDiscardsEveryRecordAndPreservesError(t *testing.T) {
	s := newStore(t)
	tx(t, s, func(tx store.Tx) error { return tx.InsertRun(run("r")) })
	wantErr := errors.New("abort")
	err := s.InTx(context.Background(), func(tx store.Tx) error {
		r, _ := tx.GetRun("r", true)
		r.CancelRequested = true
		for _, err := range []error{tx.UpdateRun(r), tx.AppendEvents("r", []*capstanv1.HistoryEvent{event(1, "a")}), tx.PushInbox("r", event(0, "b")), tx.InsertTask(task("r", epoch)), tx.InsertTimer(&store.Timer{RunID: "r", Seq: 1, DueAt: epoch}), tx.InsertApproval(&store.Approval{RunID: "r", ApprovalID: "a", Status: store.ApprovalPending}), tx.RecordSignalRequest("r", "s"), tx.InsertAICall(&store.AICall{RunID: "r", Status: store.AICallReserved, EstimateUSD: 1, At: epoch})} {
			if err != nil {
				t.Fatal(err)
			}
		}
		return wantErr
	})
	if err != wantErr {
		t.Fatalf("error = %v, want unchanged sentinel", err)
	}
	tx(t, s, func(tx store.Tx) error {
		r, _ := tx.GetRun("r", false)
		if r.CancelRequested {
			t.Fatal("run update survived rollback")
		}
		h, _ := tx.ReadHistory("r", 0, 0)
		inbox, _ := tx.InboxSize("r")
		tasks, _ := tx.RunTasks("r")
		timers, _ := tx.RunTimers("r")
		approvals, _ := tx.RunApprovals("r")
		cost, _ := tx.RunCost("r")
		if len(h)+inbox+len(tasks)+len(timers)+len(approvals) != 0 || cost != 0 {
			t.Fatal("records survived rollback")
		}
		return tx.RecordSignalRequest("r", "s")
	})
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

func TestHistoryContiguousAndBatchAtomic(t *testing.T) {
	s := newStore(t)
	tx(t, s, func(tx store.Tx) error {
		if err := tx.InsertRun(run("r")); err != nil {
			return err
		}
		if err := tx.AppendEvents("r", []*capstanv1.HistoryEvent{event(1, "a"), event(2, "b")}); err != nil {
			return err
		}
		for _, ids := range [][]int64{{2}, {4}, {3, 5}} {
			batch := make([]*capstanv1.HistoryEvent, len(ids))
			for i, id := range ids {
				batch[i] = event(id, "bad")
			}
			if err := tx.AppendEvents("r", batch); !errors.Is(err, store.ErrConflict) {
				t.Fatalf("ids %v: %v", ids, err)
			}
		}
		if err := tx.AppendEvents("r", []*capstanv1.HistoryEvent{event(3, "c")}); err != nil {
			return err
		}
		h, _ := tx.ReadHistory("r", 1, 1)
		if len(h) != 1 || h[0].EventId != 2 {
			t.Fatalf("history page: %v", h)
		}
		r, _ := tx.GetRun("r", false)
		if r.LastEventID != 0 {
			t.Fatal("AppendEvents updated run projection")
		}
		return nil
	})
}

func TestRunsFilterDeadlineUpdateAndMissing(t *testing.T) {
	s := newStore(t)
	tx(t, s, func(tx store.Tx) error {
		if _, err := tx.GetRun("missing", true); !errors.Is(err, store.ErrNotFound) {
			t.Fatal(err)
		}
		if err := tx.UpdateRun(run("missing")); !errors.Is(err, store.ErrNotFound) {
			t.Fatal(err)
		}
		for _, id := range []string{"d", "c", "b", "a"} {
			r := run(id)
			r.RunDeadline = epoch
			if id == "a" {
				r.RunDeadline = epoch.Add(-time.Hour)
			}
			if id == "c" {
				r.WorkflowType = "other"
				r.Status = capstanv1.RunStatus_RUN_STATUS_COMPLETED
			}
			if id == "d" {
				r.RunDeadline = time.Time{}
			}
			if err := tx.InsertRun(r); err != nil {
				return err
			}
		}
		if err := tx.InsertRun(run("a")); !errors.Is(err, store.ErrAlreadyExists) {
			t.Fatal(err)
		}
		rows, _ := tx.ListRuns(store.RunFilter{AfterRunID: "a", Limit: 1, WorkflowType: "workflow", Status: capstanv1.RunStatus_RUN_STATUS_RUNNING})
		if len(rows) != 1 || rows[0].RunID != "b" {
			t.Fatalf("list: %v", rows)
		}
		due, _ := tx.RunsPastDeadline(epoch, 1)
		if len(due) != 1 || due[0].RunID != "a" {
			t.Fatalf("deadline: %v", due)
		}
		r, _ := tx.GetRun("b", true)
		r.Status = capstanv1.RunStatus_RUN_STATUS_BLOCKED
		r.LastEventID = 42
		r.WorkflowTaskID = 9
		r.InFlight = true
		r.CancelRequested = true
		r.Result = payload("updated")
		r.Failure = &capstanv1.Failure{Message: "updated"}
		r.ClosedAt = epoch
		r.ContinuedToRunID = "next"
		if err := tx.UpdateRun(r); err != nil {
			return err
		}
		got, _ := tx.GetRun("b", false)
		if !reflect.DeepEqual(r, got) {
			t.Fatalf("update mismatch: %+v", got)
		}
		r.Result.Data[0] = 'X'
		got, _ = tx.GetRun("b", false)
		if string(got.Result.Data) != "updated" {
			t.Fatal("update aliases payload")
		}
		return nil
	})
}

func TestTasksClaimUpdateOrderAndDelete(t *testing.T) {
	s := newStore(t)
	tx(t, s, func(tx store.Tx) error {
		if err := tx.InsertRun(run("r")); err != nil {
			return err
		}
		a, b, c := task("r", epoch), task("r", epoch.Add(-time.Minute)), task("r", epoch.Add(time.Minute))
		for _, v := range []*store.Task{a, b, c} {
			v.CheckAt = v.VisibleAt
			if err := tx.InsertTask(v); err != nil {
				return err
			}
		}
		if !(0 < a.ID && a.ID < b.ID && b.ID < c.ID) {
			t.Fatal("ids not increasing")
		}
		claimed, err := tx.ClaimTask(store.TaskActivity, "q", epoch, time.Minute, "worker")
		if err != nil {
			return err
		}
		if claimed.ID != b.ID || claimed.WorkerID != "worker" || !claimed.StartedAt.Equal(epoch) || !claimed.LeasedUntil.Equal(epoch.Add(time.Minute)) {
			t.Fatalf("claim: %+v", claimed)
		}
		claimed, err = tx.ClaimTask(store.TaskActivity, "q", epoch, time.Minute, "worker2")
		if err != nil {
			return err
		}
		if claimed.ID != a.ID {
			t.Fatal("claim did not skip lease")
		}
		claimed, err = tx.ClaimTask(store.TaskActivity, "q", epoch, time.Minute, "worker3")
		if err != nil || claimed != nil {
			t.Fatalf("claim should be empty: %v %v", claimed, err)
		}
		due, _ := tx.DueTasks(epoch, 1)
		if len(due) != 1 || due[0].ID != b.ID {
			t.Fatal("due order or limit")
		}
		a, _ = tx.GetTask(a.ID, true)
		a.Attempt++
		a.VisibleAt = epoch.Add(time.Hour)
		a.LeasedUntil = time.Time{}
		a.WorkerID = ""
		a.StartedAt = time.Time{}
		a.CheckAt = epoch.Add(time.Hour)
		a.CancelRequested = true
		a.LastHeartbeatAt = epoch
		a.StartedEventID = 99
		a.LastFailure = &capstanv1.Failure{Message: "updated"}
		if err := tx.UpdateTask(a); err != nil {
			return err
		}
		got, _ := tx.GetTask(a.ID, false)
		if !reflect.DeepEqual(a, got) {
			t.Fatal("task update failed")
		}
		rows, _ := tx.RunTasks("r")
		if len(rows) != 3 || rows[0].ID != a.ID || rows[2].ID != c.ID {
			t.Fatal("run task order")
		}
		if err := tx.DeleteTask(a.ID); err != nil {
			return err
		}
		if err := tx.DeleteTask(a.ID); err != nil {
			return err
		}
		if _, err := tx.GetTask(a.ID, false); !errors.Is(err, store.ErrNotFound) {
			t.Fatal(err)
		}
		if err := tx.UpdateTask(a); !errors.Is(err, store.ErrNotFound) {
			t.Fatal(err)
		}
		return nil
	})
}

func TestTimersApprovalsSignalsAndLedger(t *testing.T) {
	s := newStore(t)
	tx(t, s, func(tx store.Tx) error {
		for _, id := range []string{"r", "other"} {
			if err := tx.InsertRun(run(id)); err != nil {
				return err
			}
		}
		for _, seq := range []int64{2, 1, 3} {
			if err := tx.InsertTimer(&store.Timer{RunID: "r", Seq: seq, DueAt: epoch.Add(time.Duration(seq) * time.Minute)}); err != nil {
				return err
			}
		}
		if err := tx.InsertTimer(&store.Timer{RunID: "r", Seq: 1}); !errors.Is(err, store.ErrAlreadyExists) {
			t.Fatal(err)
		}
		due, _ := tx.DueTimers(epoch.Add(2*time.Minute), 1)
		if len(due) != 1 || due[0].Seq != 1 {
			t.Fatal("due timers")
		}
		due[0].Seq = 999
		rows, _ := tx.RunTimers("r")
		if len(rows) != 3 || rows[0].Seq != 1 {
			t.Fatal("timers alias or order")
		}
		yes, _ := tx.DeleteTimer("r", 1)
		no, _ := tx.DeleteTimer("r", 1)
		if !yes || no {
			t.Fatal("timer deletion existence")
		}
		a := &store.Approval{RunID: "r", ApprovalID: "a", Source: capstanv1.ApprovalSource_APPROVAL_SOURCE_HUMAN, Status: store.ApprovalPending, CheckAt: epoch, RequestedAt: epoch}
		if err := tx.InsertApproval(a); err != nil {
			return err
		}
		if err := tx.InsertApproval(a); !errors.Is(err, store.ErrAlreadyExists) {
			t.Fatal(err)
		}
		if _, err := tx.GetApproval("r", "missing", false); !errors.Is(err, store.ErrNotFound) {
			t.Fatal(err)
		}
		if err := tx.UpdateApproval(&store.Approval{RunID: "r", ApprovalID: "missing"}); !errors.Is(err, store.ErrNotFound) {
			t.Fatal(err)
		}
		a.Status = store.ApprovalApproved
		a.CheckAt = epoch.Add(time.Hour)
		a.GatePolls = 2
		a.ResolvedAt = epoch
		a.Resolver = "me"
		a.Choice = "yes"
		a.Note = "done"
		if err := tx.UpdateApproval(a); err != nil {
			return err
		}
		got, _ := tx.GetApproval("r", "a", true)
		if !reflect.DeepEqual(a, got) {
			t.Fatal("approval update")
		}
		got.Note = "aliased"
		as, _ := tx.RunApprovals("r")
		if as[0].Note != "done" {
			t.Fatal("approval alias")
		}
		for _, id := range []string{"b", "c"} {
			b := *a
			b.ApprovalID = id
			b.Status = store.ApprovalPending
			b.CheckAt = epoch
			if id == "c" {
				b.CheckAt = time.Time{}
			}
			if err := tx.InsertApproval(&b); err != nil {
				return err
			}
		}
		pending, _ := tx.DueApprovals(epoch.Add(2*time.Hour), 1)
		if len(pending) != 1 || pending[0].ApprovalID != "b" {
			t.Fatal("due approvals")
		}
		if err := tx.RecordSignalRequest("r", "s"); err != nil {
			return err
		}
		if err := tx.RecordSignalRequest("r", "s"); !errors.Is(err, store.ErrAlreadyExists) {
			t.Fatal(err)
		}
		if err := tx.RecordSignalRequest("other", "s"); err != nil {
			return err
		}
		if err := tx.LockBudget(); err != nil {
			return err
		}
		c := &store.AICall{RunID: "r", Status: store.AICallReserved, EstimateUSD: 2, CostUSD: 99, At: epoch}
		if err := tx.InsertAICall(c); err != nil {
			return err
		}
		d := &store.AICall{RunID: "other", Status: store.AICallFinished, CostUSD: 3, At: epoch.Add(-time.Second)}
		if err := tx.InsertAICall(d); err != nil {
			return err
		}
		if c.ID <= 0 || d.ID <= c.ID {
			t.Fatal("ledger ids")
		}
		sum, _ := tx.SpentSince(epoch)
		cost, _ := tx.RunCost("r")
		if sum != 2 || cost != 2 {
			t.Fatalf("reserved costs %v %v", sum, cost)
		}
		c.Status = store.AICallFinished
		c.CostUSD = 1
		c.InputTokens = 4
		c.OutputTokens = 5
		c.CacheReadTokens = 6
		c.CacheWriteTokens = 7
		c.ErrorCode = "ok"
		c.FinishedAt = epoch
		if err := tx.UpdateAICall(c); err != nil {
			return err
		}
		cg, _ := tx.GetAICall(c.ID, true)
		if !reflect.DeepEqual(c, cg) {
			t.Fatal("ledger update")
		}
		cg.CostUSD = 10
		sum, _ = tx.SpentSince(time.Time{})
		cost, _ = tx.RunCost("r")
		if sum != 4 || cost != 1 {
			t.Fatal("ledger totals or alias")
		}
		if _, err := tx.GetAICall(999, false); !errors.Is(err, store.ErrNotFound) {
			t.Fatal(err)
		}
		if err := tx.UpdateAICall(&store.AICall{ID: 999}); !errors.Is(err, store.ErrNotFound) {
			t.Fatal(err)
		}
		return nil
	})
}

func TestNotificationsCommitRollbackCoalesceAndCancel(t *testing.T) {
	s := newStore(t)
	ch, cancel := s.Subscribe(store.TaskWorkflow, "q")
	other, stop := s.Subscribe(store.TaskActivity, "q")
	defer stop()
	assertEmpty := func(ch <-chan struct{}) {
		t.Helper()
		select {
		case _, ok := <-ch:
			if ok {
				t.Fatal("unexpected notification")
			}
		default:
		}
	}
	tx(t, s, func(tx store.Tx) error { tx.Notify(store.TaskWorkflow, "q"); assertEmpty(ch); return nil })
	select {
	case <-ch:
	default:
		t.Fatal("missing commit notification")
	}
	assertEmpty(other)
	_ = s.InTx(context.Background(), func(tx store.Tx) error { tx.Notify(store.TaskWorkflow, "q"); return errors.New("abort") })
	assertEmpty(ch)
	for range 3 {
		tx(t, s, func(tx store.Tx) error {
			for range 10 {
				tx.Notify(store.TaskWorkflow, "q")
			}
			return nil
		})
	}
	select {
	case <-ch:
	default:
		t.Fatal("missing coalesced notification")
	}
	assertEmpty(ch)
	cancel()
	cancel()
	tx(t, s, func(tx store.Tx) error { tx.Notify(store.TaskWorkflow, "q"); return nil })
	assertEmpty(ch)
}

func TestConcurrentTransactionsSerializeAndClaimOnce(t *testing.T) {
	s := newStore(t)
	const n = 32
	tx(t, s, func(tx store.Tx) error {
		if err := tx.InsertRun(run("r")); err != nil {
			return err
		}
		for range n {
			if err := tx.InsertTask(task("r", epoch)); err != nil {
				return err
			}
		}
		return nil
	})
	var wg sync.WaitGroup
	ids := make(chan int64, n)
	errs := make(chan error, n)
	for range n {
		wg.Go(func() {
			errs <- s.InTx(context.Background(), func(tx store.Tx) error {
				r, err := tx.GetRun("r", true)
				if err != nil {
					return err
				}
				r.LastEventID++
				if err := tx.UpdateRun(r); err != nil {
					return err
				}
				c, err := tx.ClaimTask(store.TaskActivity, "q", epoch, time.Minute, "w")
				if err == nil && c != nil {
					ids <- c.ID
				}
				return err
			})
		})
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	seen := map[int64]bool{}
	for id := range ids {
		if seen[id] {
			t.Fatal("task claimed twice")
		}
		seen[id] = true
	}
	if len(seen) != n {
		t.Fatalf("claims=%d", len(seen))
	}
	tx(t, s, func(tx store.Tx) error {
		r, _ := tx.GetRun("r", false)
		if r.LastEventID != n {
			t.Fatalf("lost updates: %d", r.LastEventID)
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
