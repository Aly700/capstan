package engine

import (
	"context"
	"time"

	"github.com/Aly700/capstan/internal/store"
)

func (e *Engine) NextWakeup(ctx context.Context) (at time.Time, ok bool, err error) {
	latest := time.Date(9999, time.December, 31, 23, 59, 59, 999999000, time.UTC)
	consider := func(candidate time.Time) {
		if !candidate.IsZero() && (!ok || candidate.Before(at)) {
			at, ok = candidate, true
		}
	}
	err = e.deps.Store.InTx(ctx, func(tx store.Tx) error {
		timers, err := tx.DueTimers(latest, 1)
		if err != nil {
			return err
		}
		if len(timers) != 0 {
			consider(timers[0].DueAt)
		}
		tasks, err := tx.DueTasks(latest, 1)
		if err != nil {
			return err
		}
		if len(tasks) != 0 {
			consider(tasks[0].CheckAt)
		}
		approvals, err := tx.DueApprovals(latest, 1)
		if err != nil {
			return err
		}
		if len(approvals) != 0 {
			consider(approvals[0].CheckAt)
		}
		runs, err := tx.RunsPastDeadline(latest, 1)
		if err != nil {
			return err
		}
		if len(runs) != 0 {
			consider(runs[0].RunDeadline)
		}
		return nil
	})
	if err != nil {
		return time.Time{}, false, err
	}
	return at, ok, nil
}
