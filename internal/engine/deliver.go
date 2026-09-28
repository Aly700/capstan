package engine

import (
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// deliver is the only entry point for external events. The caller persists the run
// projection in the same transaction as this delivery.
func (e *Engine) deliver(tx store.Tx, r *store.Run, ev *v1.HistoryEvent) error {
	if !r.Open() {
		return nil
	}
	if r.InFlight {
		ev.Time = timestamppb.New(e.now())
		return tx.PushInbox(r.RunID, ev)
	}
	if err := e.appendEvents(tx, r, ev); err != nil {
		return err
	}
	if r.Status == v1.RunStatus_RUN_STATUS_RUNNING && r.WorkflowTaskID == 0 {
		return e.scheduleWorkflow(tx, r, 1, e.now())
	}
	return nil
}
func (e *Engine) flush(tx store.Tx, r *store.Run) (bool, error) {
	events, err := tx.DrainInbox(r.RunID)
	if err != nil {
		return false, err
	}
	if len(events) == 0 {
		return false, nil
	}
	return true, e.appendEvents(tx, r, events...)
}
