package storetest

import (
	"testing"
	"time"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
)

func timersDuplicate(t *testing.T, s store.Store) {
	seed(t, s, "r", "other")
	v := &store.Timer{RunID: "r", Seq: 7, StartedEventID: 4, DueAt: epoch}
	mustTx(t, s, func(tx store.Tx) error { return tx.InsertTimer(v) })
	isError(t, s.InTx(t.Context(), func(tx store.Tx) error { return tx.InsertTimer(v) }), store.ErrAlreadyExists)
	mustTx(t, s, func(tx store.Tx) error { other := *v; other.RunID = "other"; return tx.InsertTimer(&other) })
}

func timersDue(t *testing.T, s store.Store) {
	seed(t, s, "r", "other")
	want := []*store.Timer{{RunID: "r", Seq: 3, StartedEventID: 30, DueAt: epoch.Add(-time.Hour)}, {RunID: "r", Seq: 1, StartedEventID: 10, DueAt: epoch.Add(-time.Minute)}, {RunID: "other", Seq: 2, StartedEventID: 20, DueAt: epoch}}
	mustTx(t, s, func(tx store.Tx) error {
		for _, v := range []*store.Timer{want[1], want[2], want[0], {RunID: "r", Seq: 4, DueAt: epoch.Add(time.Hour)}} {
			if err := tx.InsertTimer(v); err != nil {
				return err
			}
		}
		return nil
	})
	for _, limit := range []int{-1, 0, 2, 10} {
		mustTx(t, s, func(tx store.Tx) error {
			got, err := tx.DueTimers(epoch, limit)
			if err != nil {
				return err
			}
			w := want
			w = w[:min(max(limit, 0), len(w))]
			equal(t, len(got), len(w))
			for i := range got {
				equal(t, got[i], w[i])
			}
			return nil
		})
	}
	mustTx(t, s, func(tx store.Tx) error {
		got, err := tx.RunTimers("other")
		if err != nil {
			return err
		}
		equal(t, got, want[2:])
		return nil
	})
}

func timersDelete(t *testing.T, s store.Store) {
	seed(t, s, "r")
	mustTx(t, s, func(tx store.Tx) error {
		if err := tx.InsertTimer(&store.Timer{RunID: "r", Seq: 1, DueAt: epoch}); err != nil {
			return err
		}
		found, err := tx.DeleteTimer("r", 1)
		if err != nil {
			return err
		}
		equal(t, found, true)
		found, err = tx.DeleteTimer("r", 1)
		if err != nil {
			return err
		}
		equal(t, found, false)
		return nil
	})
}

func approvalsUpdate(t *testing.T, s store.Store) {
	seed(t, s, "r", "other")
	a := sampleApproval("r", "a")
	mustTx(t, s, func(tx store.Tx) error {
		if err := tx.InsertApproval(a); err != nil {
			return err
		}
		return tx.InsertApproval(sampleApproval("other", "a"))
	})
	isError(t, s.InTx(t.Context(), func(tx store.Tx) error { return tx.InsertApproval(a) }), store.ErrAlreadyExists)
	mustTx(t, s, func(tx store.Tx) error {
		got, err := tx.GetApproval("r", "a", true)
		if err != nil {
			return err
		}
		equal(t, got, a)
		return nil
	})
	a.Seq = 3
	a.RequestedEventID = 17
	a.Source = capstanv1.ApprovalSource_APPROVAL_SOURCE_GATE
	a.GateDecisionID = "decision"
	a.Status = store.ApprovalApproved
	a.DueAt = epoch.Add(time.Hour)
	a.CheckAt = epoch.Add(time.Minute)
	a.GatePolls = 4
	a.RequestedAt = epoch.Add(-time.Hour)
	a.ResolvedAt = epoch
	a.Resolver = "person"
	a.Choice = "go"
	a.Note = "done"
	mustTx(t, s, func(tx store.Tx) error { return tx.UpdateApproval(a) })
	mustTx(t, s, func(tx store.Tx) error {
		got, err := tx.GetApproval("r", "a", false)
		if err != nil {
			return err
		}
		equal(t, got, a)
		all, err := tx.RunApprovals("r")
		if err != nil {
			return err
		}
		equal(t, all, []*store.Approval{a})
		return nil
	})
	a = sampleApproval("r", "a")
	mustTx(t, s, func(tx store.Tx) error { return tx.UpdateApproval(a) })
	mustTx(t, s, func(tx store.Tx) error {
		got, err := tx.GetApproval("r", "a", false)
		if err != nil {
			return err
		}
		equal(t, got, a)
		return nil
	})
	isError(t, s.InTx(t.Context(), func(tx store.Tx) error { _, err := tx.GetApproval("r", "missing", true); return err }), store.ErrNotFound)
}

func approvalsDue(t *testing.T, s store.Store) {
	seed(t, s, "r")
	mustTx(t, s, func(tx store.Tx) error {
		for _, id := range []string{"early", "boundary", "finished", "future", "unset"} {
			a := sampleApproval("r", id)
			a.CheckAt = epoch
			switch id {
			case "early":
				a.CheckAt = epoch.Add(-time.Hour)
			case "finished":
				a.Status = store.ApprovalDenied
			case "future":
				a.CheckAt = epoch.Add(time.Second)
			case "unset":
				a.CheckAt = time.Time{}
			}
			if err := tx.InsertApproval(a); err != nil {
				return err
			}
		}
		return nil
	})
	for _, limit := range []int{-1, 0, 1, 10} {
		mustTx(t, s, func(tx store.Tx) error {
			as, err := tx.DueApprovals(epoch, limit)
			if err != nil {
				return err
			}
			ids := []string{}
			for _, a := range as {
				ids = append(ids, a.ApprovalID)
			}
			want := []string{"early", "boundary"}
			want = want[:min(max(limit, 0), len(want))]
			equal(t, ids, want)
			return nil
		})
	}
}

func signalsDuplicate(t *testing.T, s store.Store) {
	seed(t, s, "r", "other")
	mustTx(t, s, func(tx store.Tx) error { return tx.RecordSignalRequest("r", "request") })
	isError(t, s.InTx(t.Context(), func(tx store.Tx) error { return tx.RecordSignalRequest("r", "request") }), store.ErrAlreadyExists)
	mustTx(t, s, func(tx store.Tx) error {
		if err := tx.RecordSignalRequest("other", "request"); err != nil {
			return err
		}
		return tx.RecordSignalRequest("r", "another")
	})
}
