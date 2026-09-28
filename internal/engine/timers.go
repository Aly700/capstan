package engine

import (
	"context"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
)

func (e *Engine) FireDueTimers(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, Invalid("limit must be positive")
	}
	count := 0
	for count < limit {
		handled := false
		err := e.deps.Store.InTx(ctx, func(tx store.Tx) error {
			timers, err := tx.DueTimers(e.now(), 1)
			if err != nil || len(timers) == 0 {
				return err
			}
			timer := timers[0]
			r, err := tx.GetRun(timer.RunID, true)
			if err != nil {
				return err
			}
			deleted, err := tx.DeleteTimer(timer.RunID, timer.Seq)
			if err != nil || !deleted {
				return err
			}
			handled = true
			if err := e.deliver(tx, r, &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_TIMER_FIRED, Attributes: &v1.HistoryEvent_TimerFired{TimerFired: &v1.TimerFiredAttributes{StartedEventId: timer.StartedEventID, Seq: timer.Seq}}}); err != nil {
				return err
			}
			return tx.UpdateRun(r)
		})
		if err != nil {
			return count, err
		}
		if !handled {
			break
		}
		count++
	}
	return count, nil
}
