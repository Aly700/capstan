package engine

import (
	"context"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
)

func (e *Engine) TerminateRun(ctx context.Context, identity string, req *v1.TerminateRunRequest) (*v1.TerminateRunResponse, error) {
	err := e.inTx(ctx, func(tx store.Tx) error {
		r, err := tx.GetRun(req.GetRunId(), true)
		if err != nil {
			return runError(err)
		}
		if !r.Open() {
			return ErrRunClosed
		}
		if _, err := e.flush(tx, r); err != nil {
			return err
		}
		reason := req.GetReason()
		if reason == "" {
			reason = "terminated"
		}
		r.Failure = &v1.Failure{Type: "Terminated", Message: reason}
		if err := e.appendEvents(tx, r, &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_RUN_FAILED, Attributes: &v1.HistoryEvent_RunFailed{RunFailed: &v1.RunFailedAttributes{Failure: r.Failure}}}); err != nil {
			return err
		}
		if err := e.closeRun(tx, r, v1.RunStatus_RUN_STATUS_FAILED); err != nil {
			return err
		}
		return tx.UpdateRun(r)
	})
	if err != nil {
		return nil, err
	}
	return &v1.TerminateRunResponse{}, nil
}
