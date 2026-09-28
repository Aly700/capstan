package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
)

func runNotifyCommit(t *testing.T, s store.Store) {
	seed(t, s, "r")
	ch, cancel := s.SubscribeRun("r")
	defer cancel()
	mustTx(t, s, func(tx store.Tx) error {
		run, err := tx.GetRun("r", true)
		if err != nil {
			return err
		}
		run.Status = capstanv1.RunStatus_RUN_STATUS_COMPLETED
		run.ClosedAt = epoch.Add(time.Second)
		if err := tx.UpdateRun(run); err != nil {
			return err
		}
		tx.NotifyRunClosed("r")
		noWake(t, ch)
		return nil
	})
	awaitWake(t, ch)
	mustTx(t, s, func(tx store.Tx) error {
		run, err := tx.GetRun("r", false)
		if err != nil {
			return err
		}
		equal(t, run.Status, capstanv1.RunStatus_RUN_STATUS_COMPLETED)
		return nil
	})
}

func runNotifyRollback(t *testing.T, s store.Store) {
	ch, cancel := s.SubscribeRun("r")
	defer cancel()
	abort := errors.New("rollback")
	equal(t, s.InTx(t.Context(), func(tx store.Tx) error {
		tx.NotifyRunClosed("r")
		return abort
	}), abort)
	noWake(t, ch)
}

func runNotifyTopics(t *testing.T, s store.Store) {
	const runID = "r:quotes'雪"
	first, stopFirst := s.SubscribeRun(runID)
	defer stopFirst()
	second, stopSecond := s.SubscribeRun(runID)
	defer stopSecond()
	other, stopOther := s.SubscribeRun("other")
	defer stopOther()
	workflow, stopWorkflow := s.Subscribe(store.TaskWorkflow, runID)
	defer stopWorkflow()
	activity, stopActivity := s.Subscribe(store.TaskActivity, runID)
	defer stopActivity()
	mustTx(t, s, func(tx store.Tx) error {
		tx.NotifyRunClosed(runID)
		return nil
	})
	awaitWake(t, first)
	awaitWake(t, second)
	noWake(t, other)
	noWake(t, workflow)
	noWake(t, activity)
	mustTx(t, s, func(tx store.Tx) error {
		tx.Notify(store.TaskWorkflow, runID)
		tx.Notify(store.TaskActivity, runID)
		return nil
	})
	awaitWake(t, workflow)
	awaitWake(t, activity)
	noWake(t, first)
	noWake(t, second)
}

func runNotifyCoalesced(t *testing.T, s store.Store) {
	slow, stopSlow := s.SubscribeRun("r")
	defer stopSlow()
	fast, stopFast := s.SubscribeRun("r")
	defer stopFast()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	for range 20 {
		must(t, s.InTx(ctx, func(tx store.Tx) error {
			for range 5 {
				tx.NotifyRunClosed("r")
			}
			return nil
		}))
		awaitWake(t, fast)
	}
	equal(t, len(slow), 1)
	awaitWake(t, slow)
}

func runNotifyCancel(t *testing.T, s store.Store) {
	ch, cancel := s.SubscribeRun("r")
	cancel()
	cancel()
	live, stop := s.SubscribeRun("r")
	defer stop()
	mustTx(t, s, func(tx store.Tx) error {
		tx.NotifyRunClosed("r")
		return nil
	})
	awaitWake(t, live)
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("cancelled subscription received a notification")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled subscription was not closed")
	}
}
