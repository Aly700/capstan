package lab

import (
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/lab/labworker"
	"time"
)

type scenario struct {
	name     string
	workflow labworker.Workflow
	input    any
	timeout  time.Duration
}

func scenarioCatalog() []scenario {
	effect := func(wf *labworker.Context, value any) (any, error) {
		return wf.Activity("effect", value, labworker.ActivityOptions{StartToCloseTimeout: 20 * time.Millisecond})
	}
	return []scenario{
		{name: "pipeline", workflow: func(wf *labworker.Context, _ any) (any, error) {
			first, err := effect(wf, 1)
			if err != nil {
				return nil, err
			}
			if err := wf.Sleep(3 * time.Millisecond); err != nil {
				return nil, err
			}
			second, err := effect(wf, 2)
			return []any{first, second}, err
		}},
		{name: "fanout", workflow: func(wf *labworker.Context, _ any) (any, error) {
			return wf.All(
				func(child *labworker.Context) (any, error) { return effect(child, 10) },
				func(child *labworker.Context) (any, error) {
					if err := child.Sleep(5 * time.Millisecond); err != nil {
						return nil, err
					}
					return effect(child, 20)
				},
				func(child *labworker.Context) (any, error) { return effect(child, 30) },
			)
		}},
		{name: "signal", workflow: func(wf *labworker.Context, _ any) (any, error) {
			value, err := wf.Signal("go")
			if err != nil {
				return nil, err
			}
			if err := wf.Sleep(2 * time.Millisecond); err != nil {
				return nil, err
			}
			return effect(wf, value)
		}},
		{name: "human", workflow: func(wf *labworker.Context, _ any) (any, error) {
			value, err := wf.Approval(labworker.ApprovalRequest{ApprovalID: "human", Prompt: "Proceed?"})
			if err != nil {
				return nil, err
			}
			return effect(wf, value)
		}},
		{name: "gate", workflow: func(wf *labworker.Context, _ any) (any, error) {
			value, err := wf.Approval(labworker.ApprovalRequest{ApprovalID: "gate", GateDecisionID: "decision", Source: v1.ApprovalSource_APPROVAL_SOURCE_GATE})
			if err != nil {
				return nil, err
			}
			return effect(wf, value)
		}},
		{name: "gate-errors", workflow: func(wf *labworker.Context, _ any) (any, error) {
			value, err := wf.Approval(labworker.ApprovalRequest{ApprovalID: "gate-errors", GateDecisionID: "decision", Source: v1.ApprovalSource_APPROVAL_SOURCE_GATE})
			if err != nil {
				return nil, err
			}
			return effect(wf, value)
		}},
		{name: "gate-late", workflow: func(wf *labworker.Context, _ any) (any, error) {
			value, err := wf.Approval(labworker.ApprovalRequest{ApprovalID: "gate-late", GateDecisionID: "decision", Source: v1.ApprovalSource_APPROVAL_SOURCE_GATE, Timeout: 30 * time.Millisecond})
			if err != nil {
				return nil, err
			}
			return effect(wf, value)
		}},
		{name: "continuation", input: float64(0), workflow: func(wf *labworker.Context, input any) (any, error) {
			n := input.(float64)
			value, err := effect(wf, n)
			if err != nil {
				return nil, err
			}
			if n < 1 {
				wf.ContinueAsNew(n + 1)
			}
			return value, nil
		}},
		{name: "cancel", workflow: func(wf *labworker.Context, _ any) (any, error) { err := wf.Sleep(time.Hour); return "unexpected", err }},
		{name: "timeout", timeout: 30 * time.Millisecond, workflow: func(wf *labworker.Context, _ any) (any, error) { return wf.Signal("never") }},
		{name: "approval-expiry", workflow: func(wf *labworker.Context, _ any) (any, error) {
			value, err := wf.Approval(labworker.ApprovalRequest{ApprovalID: "expire", Timeout: 8 * time.Millisecond})
			if err != nil {
				return nil, err
			}
			return effect(wf, value)
		}},
		{name: "retry", workflow: func(wf *labworker.Context, _ any) (any, error) { return effect(wf, 99) }},
	}
}

// The queue companion preserves two independent effects and distinct results;
// primary scenarios already exercise durable timers and need their own wakeups.
func peerScenario() scenario {
	return scenario{name: "pipeline-peer", input: []any{"peer", "done"}, workflow: func(wf *labworker.Context, input any) (any, error) {
		values := input.([]any)
		first, err := wf.Activity("effect", values[0], labworker.ActivityOptions{StartToCloseTimeout: 20 * time.Millisecond})
		if err != nil {
			return nil, err
		}
		second, err := wf.Activity("effect", values[1], labworker.ActivityOptions{StartToCloseTimeout: 20 * time.Millisecond})
		return []any{first, second}, err
	}}
}
