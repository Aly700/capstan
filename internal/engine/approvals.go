package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
)

func (e *Engine) ResolveApproval(ctx context.Context, identity string, req *v1.ResolveApprovalRequest) (*v1.ResolveApprovalResponse, error) {
	if req == nil {
		return nil, Invalid("approval resolution is required")
	}
	err := e.inTx(ctx, func(tx store.Tx) error {
		r, err := tx.GetRun(req.RunId, true)
		if errors.Is(err, store.ErrNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		a, err := tx.GetApproval(req.RunId, req.ApprovalId, true)
		if errors.Is(err, store.ErrNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if a.Source != v1.ApprovalSource_APPROVAL_SOURCE_HUMAN {
			return fmt.Errorf("%w: decide it in AgentOps Gate", ErrFailedPrecondition)
		}
		if a.Status != store.ApprovalPending {
			return fmt.Errorf("%w: approval is no longer pending", ErrFailedPrecondition)
		}
		if !a.DueAt.IsZero() && !a.DueAt.After(e.now()) {
			return fmt.Errorf("%w: approval deadline has passed", ErrFailedPrecondition)
		}
		if req.Outcome != v1.ApprovalOutcome_APPROVAL_OUTCOME_APPROVED && req.Outcome != v1.ApprovalOutcome_APPROVAL_OUTCOME_DENIED {
			return Invalid("approval outcome must be APPROVED or DENIED")
		}
		if req.Outcome == v1.ApprovalOutcome_APPROVAL_OUTCOME_APPROVED {
			history, err := tx.ReadHistory(r.RunID, a.RequestedEventID-1, 1)
			if err != nil {
				return err
			}
			if len(history) != 1 || history[0].EventId != a.RequestedEventID || history[0].GetApprovalRequested() == nil {
				return fmt.Errorf("%w: approval request is missing from history", ErrFailedPrecondition)
			}
			options := history[0].GetApprovalRequested().Options
			if len(options) > 0 && !slices.Contains(options, req.Choice) {
				return Invalid("choice must be one of the approval's options")
			}
		}
		resolver := req.Resolver
		if resolver == "" {
			resolver = identity
		}
		return e.resolveApproval(tx, r, a, req.Outcome, req.Choice, resolver, req.Note)
	})
	if err != nil {
		return nil, err
	}
	return &v1.ResolveApprovalResponse{}, nil
}

// resolveApproval persists the decision and routes its event through the run's inbox.
func (e *Engine) resolveApproval(tx store.Tx, r *store.Run, a *store.Approval, outcome v1.ApprovalOutcome, choice, resolver, note string) error {
	switch outcome {
	case v1.ApprovalOutcome_APPROVAL_OUTCOME_APPROVED:
		a.Status = store.ApprovalApproved
	case v1.ApprovalOutcome_APPROVAL_OUTCOME_DENIED:
		a.Status = store.ApprovalDenied
	case v1.ApprovalOutcome_APPROVAL_OUTCOME_EXPIRED:
		a.Status = store.ApprovalExpired
	default:
		return Invalid("invalid approval outcome")
	}
	a.ResolvedAt = e.now()
	a.CheckAt = time.Time{}
	a.Choice, a.Resolver, a.Note = choice, resolver, note
	if err := tx.UpdateApproval(a); err != nil {
		return err
	}
	ev := &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_APPROVAL_RESOLVED, Attributes: &v1.HistoryEvent_ApprovalResolved{ApprovalResolved: &v1.ApprovalResolvedAttributes{
		RequestedEventId: a.RequestedEventID,
		Seq:              a.Seq,
		ApprovalId:       a.ApprovalID,
		Outcome:          outcome,
		Choice:           choice,
		Resolver:         resolver,
		Note:             note,
	}}}
	if err := e.deliver(tx, r, ev); err != nil {
		return err
	}
	return tx.UpdateRun(r)
}

func (e *Engine) ProcessDueApprovals(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, Invalid("limit must be positive")
	}
	processed := 0
	for processed < limit {
		var leased *store.Approval
		found := false
		err := e.inTx(ctx, func(tx store.Tx) error {
			leased, found = nil, false
			now := e.now()
			due, err := tx.DueApprovals(now, 1)
			if err != nil || len(due) == 0 {
				return err
			}
			found = true
			a, err := tx.GetApproval(due[0].RunID, due[0].ApprovalID, true)
			if errors.Is(err, store.ErrNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			if a.Status != store.ApprovalPending || a.CheckAt.IsZero() || a.CheckAt.After(now) {
				return nil
			}
			r, err := tx.GetRun(a.RunID, true)
			if err != nil {
				return err
			}
			if !a.DueAt.IsZero() && !a.DueAt.After(now) {
				return e.resolveApproval(tx, r, a, v1.ApprovalOutcome_APPROVAL_OUTCOME_EXPIRED, "", "timeout", "")
			}
			if a.Source != v1.ApprovalSource_APPROVAL_SOURCE_GATE || e.deps.Gate == nil {
				// Without a Gate client only expiry can make this approval due again.
				a.CheckAt = a.DueAt
				return tx.UpdateApproval(a)
			}
			a.CheckAt = addDeadline(now, time.Minute)
			if a.GatePolls < math.MaxInt32 {
				a.GatePolls++
			}
			if err := tx.UpdateApproval(a); err != nil {
				return err
			}
			leased = a
			return nil
		})
		if err != nil {
			return processed, err
		}
		if !found {
			break
		}
		if leased != nil {
			decision, gateErr := e.deps.Gate.ApprovalStatus(ctx, leased.ApprovalID)
			err = e.inTx(ctx, func(tx store.Tx) error {
				a, err := tx.GetApproval(leased.RunID, leased.ApprovalID, true)
				if errors.Is(err, store.ErrNotFound) {
					return nil
				}
				if err != nil {
					return err
				}
				if a.Status != store.ApprovalPending || a.GatePolls != leased.GatePolls {
					return nil
				}
				r, err := tx.GetRun(a.RunID, true)
				if err != nil {
					return err
				}
				now := e.now()
				if !a.DueAt.IsZero() && !a.DueAt.After(now) {
					return e.resolveApproval(tx, r, a, v1.ApprovalOutcome_APPROVAL_OUTCOME_EXPIRED, "", "timeout", "")
				}
				if gateErr == nil {
					var outcome v1.ApprovalOutcome
					switch decision.Status {
					case "APPROVED":
						outcome = v1.ApprovalOutcome_APPROVAL_OUTCOME_APPROVED
					case "DENIED":
						outcome = v1.ApprovalOutcome_APPROVAL_OUTCOME_DENIED
					case "EXPIRED":
						outcome = v1.ApprovalOutcome_APPROVAL_OUTCOME_EXPIRED
					}
					if outcome != v1.ApprovalOutcome_APPROVAL_OUTCOME_UNSPECIFIED {
						return e.resolveApproval(tx, r, a, outcome, "", decision.DecidedBy, "")
					}
				}
				a.CheckAt = addDeadline(now, e.approvalPollBackoff(a.GatePolls))
				if !a.DueAt.IsZero() && a.DueAt.Before(a.CheckAt) {
					a.CheckAt = a.DueAt
				}
				return tx.UpdateApproval(a)
			})
			if err != nil {
				return processed, err
			}
		}
		processed++
	}
	return processed, nil
}

func (e *Engine) approvalPollBackoff(polls int32) time.Duration {
	delay := min(e.cfg.GatePollInitial, e.cfg.GatePollMax)
	for ; polls > 0 && delay < e.cfg.GatePollMax; polls-- {
		if delay > e.cfg.GatePollMax/2 {
			return e.cfg.GatePollMax
		}
		delay *= 2
	}
	return delay
}
