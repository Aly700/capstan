package storetest

import (
	"testing"
	"time"

	"github.com/Aly700/capstan/internal/store"
)

func timeZero(t *testing.T, s store.Store) { timeRoundTrip(t, s, false) }
func timeUTC(t *testing.T, s store.Store)  { timeRoundTrip(t, s, true) }

func timeRoundTrip(t *testing.T, s store.Store, nonzero bool) {
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
	optional := []*time.Time{&r.RunDeadline, &r.ClosedAt, &task.LeasedUntil, &task.StartedAt, &task.CheckAt, &task.LastHeartbeatAt, &a.DueAt, &a.CheckAt, &a.ResolvedAt, &c.FinishedAt}
	if nonzero {
		for _, p := range optional {
			*p = at
		}
	}
	mustTx(t, s, func(tx store.Tx) error {
		for _, f := range []func() error{func() error { return tx.InsertRun(r) }, func() error { return tx.InsertTask(task) }, func() error { return tx.InsertTimer(timer) }, func() error { return tx.InsertApproval(a) }, func() error { return tx.InsertAICall(c) }} {
			if err := f(); err != nil {
				return err
			}
		}
		return nil
	})
	all := append(optional, &r.StartedAt, &task.VisibleAt, &task.ScheduledAt, &timer.DueAt, &a.RequestedAt, &c.At)
	for _, p := range all {
		if !p.IsZero() {
			*p = p.UTC()
		}
	}
	mustTx(t, s, func(tx store.Tx) error {
		gotR, err := tx.GetRun("r", false)
		if err != nil {
			return err
		}
		equalRun(t, gotR, r)
		gotT, err := tx.GetTask(task.ID, false)
		if err != nil {
			return err
		}
		equalTask(t, gotT, task)
		timers, err := tx.RunTimers("r")
		if err != nil {
			return err
		}
		equal(t, timers, []*store.Timer{timer})
		gotA, err := tx.GetApproval("r", "a", false)
		if err != nil {
			return err
		}
		equal(t, gotA, a)
		gotC, err := tx.GetAICall(c.ID, false)
		if err != nil {
			return err
		}
		equal(t, gotC, c)
		for _, v := range []time.Time{gotR.StartedAt, gotR.RunDeadline, gotR.ClosedAt, gotT.VisibleAt, gotT.ScheduledAt, gotT.LeasedUntil, gotT.StartedAt, gotT.CheckAt, gotT.LastHeartbeatAt, timers[0].DueAt, gotA.RequestedAt, gotA.DueAt, gotA.CheckAt, gotA.ResolvedAt, gotC.At, gotC.FinishedAt} {
			if v.Location() != time.UTC {
				t.Fatalf("time came back in %s: %s", v.Location(), v)
			}
		}
		return nil
	})
}
