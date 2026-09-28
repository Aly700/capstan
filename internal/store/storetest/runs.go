package storetest

import (
	"testing"
	"time"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
)

func runsInsertGetUpdate(t *testing.T, s store.Store) {
	r := sampleRun("r")
	r.Input = payload()
	mustTx(t, s, func(tx store.Tx) error { return tx.InsertRun(r) })
	mustTx(t, s, func(tx store.Tx) error {
		got, err := tx.GetRun("r", true)
		if err != nil {
			return err
		}
		equalRun(t, got, r)
		got.WorkflowType = "changed"
		got.TaskQueue = "other"
		got.Status = capstanv1.RunStatus_RUN_STATUS_BLOCKED
		got.Input = &capstanv1.Payload{}
		got.Result = payload()
		got.Failure = &capstanv1.Failure{Message: "blocked", Details: payload()}
		got.TaskTimeout = 1234 * time.Millisecond
		got.RunTimeout = time.Hour
		got.RunDeadline = epoch.Add(time.Hour)
		got.StartedAt = epoch.Add(-time.Second)
		got.ClosedAt = epoch.Add(time.Minute)
		got.LastEventID = 17
		got.WorkflowTaskID = 23
		got.InFlight = true
		got.CancelRequested = true
		got.ContinuedFromRunID = "before"
		got.ContinuedToRunID = "after"
		got.Identity = "starter"
		r = got
		return tx.UpdateRun(got)
	})
	mustTx(t, s, func(tx store.Tx) error {
		got, err := tx.GetRun("r", false)
		if err != nil {
			return err
		}
		equalRun(t, got, r)
		return nil
	})
	// Returned protobufs must not be aliases of stored values.
	r.Result.Data[0] = 99
	mustTx(t, s, func(tx store.Tx) error {
		got, err := tx.GetRun("r", false)
		if err != nil {
			return err
		}
		equalProto(t, got.Result, payload())
		got.Result.Data[0] = 33
		return nil
	})
	mustTx(t, s, func(tx store.Tx) error {
		got, err := tx.GetRun("r", false)
		if err != nil {
			return err
		}
		equalProto(t, got.Result, payload())
		return nil
	})
}

func runsDuplicate(t *testing.T, s store.Store) {
	seed(t, s, "r")
	r := sampleRun("r")
	r.WorkflowType = "other"
	isError(t, s.InTx(t.Context(), func(tx store.Tx) error { return tx.InsertRun(r) }), store.ErrAlreadyExists)
	mustTx(t, s, func(tx store.Tx) error {
		got, err := tx.GetRun("r", false)
		if err != nil {
			return err
		}
		equalRun(t, got, sampleRun("r"))
		return nil
	})
}

func runsMissing(t *testing.T, s store.Store) {
	for _, lock := range []bool{false, true} {
		isError(t, s.InTx(t.Context(), func(tx store.Tx) error { _, err := tx.GetRun("missing", lock); return err }), store.ErrNotFound)
	}
}

func runsList(t *testing.T, s store.Store) {
	mustTx(t, s, func(tx store.Tx) error {
		for _, id := range []string{"d", "b", "e", "a", "c"} {
			r := sampleRun(id)
			if id == "b" {
				r.WorkflowType = "other"
			}
			if id == "c" {
				r.Status = capstanv1.RunStatus_RUN_STATUS_COMPLETED
			}
			if err := tx.InsertRun(r); err != nil {
				return err
			}
		}
		return nil
	})
	for _, tc := range []struct {
		filter store.RunFilter
		ids    []string
	}{
		{store.RunFilter{Limit: 10}, []string{"a", "b", "c", "d", "e"}},
		{store.RunFilter{Status: capstanv1.RunStatus_RUN_STATUS_RUNNING, WorkflowType: "wf", Limit: 2}, []string{"a", "d"}},
		{store.RunFilter{Status: capstanv1.RunStatus_RUN_STATUS_RUNNING, WorkflowType: "wf", AfterRunID: "d", Limit: 2}, []string{"e"}},
		{store.RunFilter{WorkflowType: "other", Limit: 10}, []string{"b"}},
		{store.RunFilter{Status: capstanv1.RunStatus_RUN_STATUS_COMPLETED, Limit: 10}, []string{"c"}},
		{store.RunFilter{AfterRunID: "z", Limit: 10}, []string{}},
	} {
		mustTx(t, s, func(tx store.Tx) error {
			rs, err := tx.ListRuns(tc.filter)
			if err != nil {
				return err
			}
			ids := []string{}
			for _, r := range rs {
				ids = append(ids, r.RunID)
			}
			equal(t, ids, tc.ids)
			return nil
		})
	}
}

func runsDue(t *testing.T, s store.Store) {
	mustTx(t, s, func(tx store.Tx) error {
		for _, id := range []string{"late", "early", "boundary", "closed", "unset", "future"} {
			r := sampleRun(id)
			r.RunDeadline = epoch
			switch id {
			case "early":
				r.RunDeadline = epoch.Add(-time.Hour)
				r.Status = capstanv1.RunStatus_RUN_STATUS_BLOCKED
			case "late":
				r.RunDeadline = epoch.Add(-time.Minute)
			case "closed":
				r.Status = capstanv1.RunStatus_RUN_STATUS_COMPLETED
			case "unset":
				r.RunDeadline = time.Time{}
			case "future":
				r.RunDeadline = epoch.Add(time.Second)
			}
			if err := tx.InsertRun(r); err != nil {
				return err
			}
		}
		return nil
	})
	for _, limit := range []int{2, 10} {
		mustTx(t, s, func(tx store.Tx) error {
			rs, err := tx.RunsPastDeadline(epoch, limit)
			if err != nil {
				return err
			}
			ids := []string{}
			for _, r := range rs {
				ids = append(ids, r.RunID)
			}
			want := []string{"early", "late", "boundary"}
			if limit == 2 {
				want = want[:2]
			}
			equal(t, ids, want)
			return nil
		})
	}
}
