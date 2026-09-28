package engine

import (
	"context"
	"errors"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
)

func (e *Engine) SignalRun(ctx context.Context, identity string, req *v1.SignalRunRequest) (*v1.SignalRunResponse, error) {
	err := e.deps.Store.InTx(ctx, func(tx store.Tx) error {
		r, err := tx.GetRun(req.GetRunId(), true)
		if err != nil {
			return runError(err)
		}
		if !r.Open() {
			return ErrRunClosed
		}
		if req.GetName() == "" {
			return Invalid("signal name is required")
		}
		if req.GetRequestId() != "" {
			err := tx.RecordSignalRequest(r.RunID, req.RequestId)
			if errors.Is(err, store.ErrAlreadyExists) {
				return nil
			}
			if err != nil {
				return err
			}
		}
		ev := &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_SIGNAL_RECEIVED, Attributes: &v1.HistoryEvent_SignalReceived{SignalReceived: &v1.SignalReceivedAttributes{Name: req.Name, Input: req.Input, Identity: identity, RequestId: req.RequestId}}}
		if err := e.deliver(tx, r, ev); err != nil {
			return err
		}
		return tx.UpdateRun(r)
	})
	if err != nil {
		return nil, err
	}
	return &v1.SignalRunResponse{}, nil
}
func (e *Engine) CancelRun(ctx context.Context, identity string, req *v1.CancelRunRequest) (*v1.CancelRunResponse, error) {
	err := e.deps.Store.InTx(ctx, func(tx store.Tx) error {
		r, err := tx.GetRun(req.GetRunId(), true)
		if err != nil {
			return runError(err)
		}
		if !r.Open() {
			return ErrRunClosed
		}
		if r.CancelRequested {
			return nil
		}
		r.CancelRequested = true
		if err := e.deliver(tx, r, &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_RUN_CANCEL_REQUESTED, Attributes: &v1.HistoryEvent_RunCancelRequested{RunCancelRequested: &v1.RunCancelRequestedAttributes{Reason: req.GetReason(), Identity: identity}}}); err != nil {
			return err
		}
		return tx.UpdateRun(r)
	})
	if err != nil {
		return nil, err
	}
	return &v1.CancelRunResponse{}, nil
}
func (e *Engine) ResumeRun(ctx context.Context, identity string, req *v1.ResumeRunRequest) (*v1.ResumeRunResponse, error) {
	err := e.deps.Store.InTx(ctx, func(tx store.Tx) error {
		r, err := tx.GetRun(req.GetRunId(), true)
		if err != nil {
			return runError(err)
		}
		if r.Status != v1.RunStatus_RUN_STATUS_BLOCKED {
			return ErrFailedPrecondition
		}
		if err := e.appendEvents(tx, r, &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_RUN_RESUMED, Attributes: &v1.HistoryEvent_RunResumed{RunResumed: &v1.RunResumedAttributes{Identity: identity, Reason: req.GetReason()}}}); err != nil {
			return err
		}
		r.Status = v1.RunStatus_RUN_STATUS_RUNNING
		r.Failure = nil
		if err := e.scheduleWorkflow(tx, r, 1, e.now()); err != nil {
			return err
		}
		return tx.UpdateRun(r)
	})
	if err != nil {
		return nil, err
	}
	return &v1.ResumeRunResponse{}, nil
}
