package engine

import (
	"context"
	"errors"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
)

func (e *Engine) TimeoutRuns(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, Invalid("limit must be positive")
	}
	processed := 0
	for processed < limit {
		found := false
		err := e.deps.Store.InTx(ctx, func(tx store.Tx) error {
			now := e.now()
			due, err := tx.RunsPastDeadline(now, 1)
			if err != nil || len(due) == 0 {
				return err
			}
			found = true
			r, err := tx.GetRun(due[0].RunID, true)
			if errors.Is(err, store.ErrNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			if !r.Open() || r.RunDeadline.IsZero() || r.RunDeadline.After(now) {
				return nil
			}
			if _, err := e.flush(tx, r); err != nil {
				return err
			}
			if err := e.appendEvents(tx, r, &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_RUN_TIMED_OUT, Attributes: &v1.HistoryEvent_RunTimedOut{RunTimedOut: &v1.RunTimedOutAttributes{}}}); err != nil {
				return err
			}
			if err := e.closeRun(tx, r, v1.RunStatus_RUN_STATUS_TIMED_OUT); err != nil {
				return err
			}
			return tx.UpdateRun(r)
		})
		if err != nil {
			return processed, err
		}
		if !found {
			break
		}
		processed++
	}
	return processed, nil
}
