package engine

import (
	"context"
	"errors"

	"github.com/Aly700/capstan/internal/store"
)

// inTx retries transactions PostgreSQL has explicitly aborted for a deadlock.
// Claims and due scans lock a child row before its run; closure locks the run
// before deleting its children. Either participant can lose that lock race.
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
