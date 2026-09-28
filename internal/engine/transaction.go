package engine

import (
	"context"
	"errors"

	"github.com/Aly700/capstan/internal/store"
)

// inTx retries transactions PostgreSQL has explicitly aborted for a deadlock.
// Normal engine paths avoid inverted row locks; keep this bounded retry as a
// safety net for other transactions that PostgreSQL explicitly aborts.
// Callbacks contain database work only and reset any captured result per attempt.
// Connection errors have an unknown commit outcome and must never be retried here.
func (e *Engine) inTx(ctx context.Context, fn func(store.Tx) error) error {
	const attempts = 4
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := e.deps.Store.InTx(ctx, fn)
		var state interface{ SQLState() string }
		if attempt == attempts || !errors.As(err, &state) || state.SQLState() != "40P01" {
			return err
		}
	}
}

// A poll may claim a task whose run is busy. PostgreSQL rejects the run lock
// without waiting; callers check this only after InTx has rolled back the lease.
func isRunLockBusy(err error) bool {
	var state interface{ SQLState() string }
	return errors.As(err, &state) && state.SQLState() == "55P03"
}
