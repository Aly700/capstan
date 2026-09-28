package storetest

import (
	"testing"
	"time"

	"github.com/Aly700/capstan/internal/store"
)

func timeZero(t *testing.T, s store.Store)      { timeRoundTrip(t, s, false, false) }
func timeUTC(t *testing.T, s store.Store)       { timeRoundTrip(t, s, true, false) }
func timeUpdateUTC(t *testing.T, s store.Store) { timeRoundTrip(t, s, true, true) }

func timeRoundTrip(t *testing.T, s store.Store, nonzero, update bool) {
	t.Helper()
	at := epoch.In(time.FixedZone("caller", -4*60*60))
	r := sampleRun("r")
	r.StartedAt = at
	task := sampleTask("r")
	task.VisibleAt = at
	task.ScheduledAt = at
	timer := &store.Timer{RunID: "r", Seq: 1, DueAt: at}
	a := sampleApproval("r", "a")
	a.RequestedAt = at
	c := &store.AICall{RunID: "r", Status: store.AICallReserved, At: at}
	if update {
		mustTx(t, s, func(tx store.Tx) error {
			initialTask, initialApproval := sampleTask("r"), sampleApproval("r", "a")
			initialCall := &store.AICall{RunID: "r", Status: store.AICallReserved, At: epoch}
			for _, insert := range []func() error{
				func() error { return tx.InsertRun(sampleRun("r")) },
				func() error { return tx.InsertTask(initialTask) },
				func() error { return tx.InsertApproval(initialApproval) },
				func() error { return tx.InsertAICall(initialCall) },
			} {
				if err := insert(); err != nil {
					return err
				}
			}
			task.ID, c.ID = initialTask.ID, initialCall.ID
			return nil
		})
	}
	optional := []*time.Time{&r.RunDeadline, &r.ClosedAt, &task.LeasedUntil, &task.StartedAt, &task.CheckAt, &task.LastHeartbeatAt, &a.DueAt, &a.CheckAt, &a.ResolvedAt, &c.FinishedAt}
	if nonzero {
		for _, p := range optional {
			*p = at
		}
	}
	mustTx(t, s, func(tx store.Tx) error {
		writeRun, writeTask, writeApproval, writeCall := tx.InsertRun, tx.InsertTask, tx.InsertApproval, tx.InsertAICall
		if update {
			writeRun, writeTask, writeApproval, writeCall = tx.UpdateRun, tx.UpdateTask, tx.UpdateApproval, tx.UpdateAICall
		}
		for _, f := range []func() error{func() error { return writeRun(r) }, func() error { return writeTask(task) }, func() error { return tx.InsertTimer(timer) }, func() error { return writeApproval(a) }, func() error { return writeCall(c) }} {
			if err := f(); err != nil {
				return err
			}
		}
		return nil
	})
	// Inserts and updates must not normalize the caller's records in place.
	all := append(optional, &r.StartedAt, &task.VisibleAt, &task.ScheduledAt, &timer.DueAt, &a.RequestedAt, &c.At)
	for _, p := range all {
		if !p.IsZero() {
			equal(t, p.Location(), at.Location())
			*p = p.UTC()
		}
	}
	for _, tc := range []struct {
		name  string
		check func(*testing.T, store.Tx) error
	}{
		{"Run", func(t *testing.T, tx store.Tx) error {
			got, err := tx.GetRun("r", false)
			if err == nil {
				equalRun(t, got, r)
			}
			return err
		}},
		{"Task", func(t *testing.T, tx store.Tx) error {
			got, err := tx.GetTask(task.ID, false)
			if err == nil {
				equalTask(t, got, task)
			}
			return err
		}},
		{"Timer", func(t *testing.T, tx store.Tx) error {
			got, err := tx.RunTimers("r")
			if err == nil {
				equal(t, got, []*store.Timer{timer})
			}
			return err
		}},
		{"Approval", func(t *testing.T, tx store.Tx) error {
			got, err := tx.GetApproval("r", "a", false)
			if err == nil {
				equal(t, got, a)
			}
			return err
		}},
		{"AICall", func(t *testing.T, tx store.Tx) error {
			got, err := tx.GetAICall(c.ID, false)
			if err == nil {
				equal(t, got, c)
			}
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mustTx(t, s, func(tx store.Tx) error { return tc.check(t, tx) })
		})
	}
}

func timeClaimUTC(t *testing.T, s store.Store) {
	seed(t, s, "r")
	want := sampleTask("r")
	mustTx(t, s, func(tx store.Tx) error { return tx.InsertTask(want) })
	now := epoch.In(time.FixedZone("worker", 5*60*60+30*60))
	lease := time.Second + time.Microsecond
	want.StartedAt = now.UTC()
	want.LeasedUntil = now.Add(lease).UTC()
	want.WorkerID = "worker"
	mustTx(t, s, func(tx store.Tx) error {
		got, err := tx.ClaimTask(store.TaskActivity, "q", now, lease, "worker")
		if err != nil {
			return err
		}
		equalTask(t, got, want)
		return nil
	})
	mustTx(t, s, func(tx store.Tx) error {
		got, err := tx.GetTask(want.ID, false)
		if err != nil {
			return err
		}
		equalTask(t, got, want)
		return nil
	})
}
