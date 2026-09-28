package storetest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
)

func ledgerRows(t *testing.T, s store.Store) {
	t.Helper()
	seed(t, s, "r", "other")
	mustTx(t, s, func(tx store.Tx) error {
		for _, c := range []*store.AICall{
			{RunID: "r", Model: "m", Status: store.AICallReserved, EstimateUSD: .75, CostUSD: 99, At: epoch},
			{RunID: "r", Model: "m", Status: store.AICallFinished, EstimateUSD: 2, CostUSD: .25, At: epoch.Add(time.Hour)},
			{RunID: "other", Model: "m", Status: store.AICallFailed, EstimateUSD: 2, CostUSD: .125, At: epoch},
			{RunID: "r", Model: "m", Status: store.AICallFinished, CostUSD: 4, At: epoch.Add(-time.Microsecond)},
		} {
			if err := tx.InsertAICall(c); err != nil {
				return err
			}
		}
		return nil
	})
}

func ledgerSpent(t *testing.T, s store.Store) {
	ledgerRows(t, s)
	mustTx(t, s, func(tx store.Tx) error {
		v, err := tx.SpentSince(epoch)
		if err != nil {
			return err
		}
		equal(t, v, 1.125)
		v, err = tx.SpentSince(epoch.Add(2 * time.Hour))
		if err != nil {
			return err
		}
		equal(t, v, float64(0))
		v, err = tx.SpentSince(time.Time{})
		if err != nil {
			return err
		}
		equal(t, v, 5.125)
		return nil
	})
}

func ledgerRunCost(t *testing.T, s store.Store) {
	ledgerRows(t, s)
	mustTx(t, s, func(tx store.Tx) error {
		for id, want := range map[string]float64{"r": 5, "other": .125, "missing": 0} {
			v, err := tx.RunCost(id)
			if err != nil {
				return err
			}
			equal(t, v, want)
		}
		return nil
	})
}

func ledgerUpdate(t *testing.T, s store.Store) {
	seed(t, s, "r", "other")
	c := &store.AICall{RunID: "r", ActivitySeq: 7, Model: "m", Status: store.AICallReserved, EstimateUSD: .25, At: epoch}
	mustTx(t, s, func(tx store.Tx) error { return tx.InsertAICall(c) })
	if c.ID <= 0 {
		t.Fatalf("ledger id = %d", c.ID)
	}
	mustTx(t, s, func(tx store.Tx) error {
		got, err := tx.GetAICall(c.ID, true)
		if err != nil {
			return err
		}
		equal(t, got, c)
		return nil
	})
	c.RunID = "other"
	c.ActivitySeq = 8
	c.Model = "new"
	c.Status = store.AICallFailed
	c.EstimateUSD = .5
	c.CostUSD = .125
	c.InputTokens = 100
	c.OutputTokens = 200
	c.CacheReadTokens = 300
	c.CacheWriteTokens = 400
	c.ErrorCode = "FAILED"
	c.At = epoch.Add(-time.Second)
	c.FinishedAt = epoch.Add(time.Second)
	mustTx(t, s, func(tx store.Tx) error { return tx.UpdateAICall(c) })
	mustTx(t, s, func(tx store.Tx) error {
		got, err := tx.GetAICall(c.ID, false)
		if err != nil {
			return err
		}
		equal(t, got, c)
		return nil
	})
	c = &store.AICall{ID: c.ID, RunID: "r", Status: store.AICallReserved, At: epoch}
	mustTx(t, s, func(tx store.Tx) error { return tx.UpdateAICall(c) })
	mustTx(t, s, func(tx store.Tx) error {
		got, err := tx.GetAICall(c.ID, false)
		if err != nil {
			return err
		}
		equal(t, got, c)
		return nil
	})
	isError(t, s.InTx(t.Context(), func(tx store.Tx) error { _, err := tx.GetAICall(c.ID+1000, true); return err }), store.ErrNotFound)
}

func ledgerLock(t *testing.T, s store.Store) {
	seed(t, s, "r")
	concurrent(t, 16, func() error {
		return s.InTx(t.Context(), func(tx store.Tx) error {
			if err := tx.LockBudget(); err != nil {
				return err
			}
			v, err := tx.SpentSince(epoch)
			if err != nil {
				return err
			}
			if v >= 1 {
				return nil
			}
			time.Sleep(time.Millisecond)
			return tx.InsertAICall(&store.AICall{RunID: "r", Model: "m", Status: store.AICallReserved, EstimateUSD: .125, At: epoch})
		})
	})
	mustTx(t, s, func(tx store.Tx) error {
		v, err := tx.SpentSince(epoch)
		if err != nil {
			return err
		}
		equal(t, v, float64(1))
		return nil
	})
}

func txRollback(t *testing.T, s store.Store) {
	seed(t, s, "r")
	task := sampleTask("r")
	timer := &store.Timer{RunID: "r", Seq: 1, DueAt: epoch}
	approval := sampleApproval("r", "a")
	call := &store.AICall{RunID: "r", Status: store.AICallReserved, EstimateUSD: .5, At: epoch}
	mustTx(t, s, func(tx store.Tx) error {
		for _, f := range []func() error{func() error { return tx.InsertTask(task) }, func() error { return tx.InsertTimer(timer) }, func() error { return tx.InsertApproval(approval) }, func() error { return tx.InsertAICall(call) }, func() error { return tx.AppendEvents("r", []*capstanv1.HistoryEvent{event(1)}) }, func() error { return tx.PushInbox("r", event(0)) }} {
			if err := f(); err != nil {
				return err
			}
		}
		return nil
	})
	abort := errors.New("abort everything")
	err := s.InTx(t.Context(), func(tx store.Tx) error {
		if err := tx.InsertRun(sampleRun("new")); err != nil {
			return err
		}
		r, err := tx.GetRun("r", true)
		if err != nil {
			return err
		}
		r.LastEventID = 2
		if err := tx.UpdateRun(r); err != nil {
			return err
		}
		if err := tx.AppendEvents("r", []*capstanv1.HistoryEvent{event(2)}); err != nil {
			return err
		}
		if _, err := tx.DrainInbox("r"); err != nil {
			return err
		}
		if err := tx.PushInbox("r", event(99)); err != nil {
			return err
		}
		if err := tx.DeleteTask(task.ID); err != nil {
			return err
		}
		if err := tx.InsertTask(sampleTask("r")); err != nil {
			return err
		}
		if _, err := tx.DeleteTimer("r", 1); err != nil {
			return err
		}
		if err := tx.InsertTimer(&store.Timer{RunID: "r", Seq: 2, DueAt: epoch}); err != nil {
			return err
		}
		a := *approval
		a.Status = store.ApprovalDenied
		if err := tx.UpdateApproval(&a); err != nil {
			return err
		}
		if err := tx.InsertApproval(sampleApproval("r", "new")); err != nil {
			return err
		}
		c := *call
		c.Status = store.AICallFinished
		c.CostUSD = 99
		if err := tx.UpdateAICall(&c); err != nil {
			return err
		}
		if err := tx.InsertAICall(&store.AICall{RunID: "r", Status: store.AICallReserved, EstimateUSD: 10, At: epoch}); err != nil {
			return err
		}
		if err := tx.RecordSignalRequest("r", "rolled-back"); err != nil {
			return err
		}
		return abort
	})
	equal(t, err, abort)
	isError(t, s.InTx(t.Context(), func(tx store.Tx) error { _, err := tx.GetRun("new", false); return err }), store.ErrNotFound)
	mustTx(t, s, func(tx store.Tx) error {
		r, err := tx.GetRun("r", false)
		if err != nil {
			return err
		}
		equalRun(t, r, sampleRun("r"))
		h, err := tx.ReadHistory("r", 0, 0)
		if err != nil {
			return err
		}
		equal(t, len(h), 1)
		equalProto(t, h[0], event(1))
		inbox, err := tx.DrainInbox("r")
		if err != nil {
			return err
		}
		equal(t, len(inbox), 1)
		equalProto(t, inbox[0], event(0))
		ts, err := tx.RunTasks("r")
		if err != nil {
			return err
		}
		equal(t, len(ts), 1)
		equalTask(t, ts[0], task)
		timers, err := tx.RunTimers("r")
		if err != nil {
			return err
		}
		equal(t, timers, []*store.Timer{timer})
		as, err := tx.RunApprovals("r")
		if err != nil {
			return err
		}
		equal(t, as, []*store.Approval{approval})
		c, err := tx.GetAICall(call.ID, false)
		if err != nil {
			return err
		}
		equal(t, c, call)
		cost, err := tx.RunCost("r")
		if err != nil {
			return err
		}
		equal(t, cost, .5)
		return tx.RecordSignalRequest("r", "rolled-back")
	})
}

func txErrors(t *testing.T, s store.Store) {
	for _, sentinel := range []error{store.ErrNotFound, store.ErrAlreadyExists, store.ErrConflict, context.Canceled, errors.New("callback error")} {
		for _, want := range []error{sentinel, fmt.Errorf("context: %w", sentinel)} {
			got := s.InTx(t.Context(), func(store.Tx) error { return want })
			if got != want {
				t.Fatalf("InTx returned %v, want original error %v", got, want)
			}
			isError(t, got, sentinel)
		}
	}
}

func txHandled(t *testing.T, s store.Store) {
	seed(t, s, "r")
	timer := &store.Timer{RunID: "r", Seq: 1, DueAt: epoch}
	a := sampleApproval("r", "a")
	mustTx(t, s, func(tx store.Tx) error {
		if err := tx.InsertTimer(timer); err != nil {
			return err
		}
		if err := tx.InsertApproval(a); err != nil {
			return err
		}
		if err := tx.RecordSignalRequest("r", "request"); err != nil {
			return err
		}
		return tx.AppendEvents("r", []*capstanv1.HistoryEvent{event(1)})
	})
	mustTx(t, s, func(tx store.Tx) error {
		for _, f := range []func() error{func() error { return tx.InsertRun(sampleRun("r")) }, func() error { return tx.InsertTimer(timer) }, func() error { return tx.InsertApproval(a) }, func() error { return tx.RecordSignalRequest("r", "request") }} {
			isError(t, f(), store.ErrAlreadyExists)
		}
		isError(t, tx.AppendEvents("r", []*capstanv1.HistoryEvent{event(2), event(4)}), store.ErrConflict)
		_, err := tx.GetRun("missing", false)
		isError(t, err, store.ErrNotFound)
		// The engine handles duplicate signals and starts as successful requests.
		if err := tx.AppendEvents("r", []*capstanv1.HistoryEvent{event(2)}); err != nil {
			return err
		}
		return tx.InsertRun(sampleRun("after-errors"))
	})
	mustTx(t, s, func(tx store.Tx) error { _, err := tx.GetRun("after-errors", false); return err })
}
