package lab

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Aly700/capstan/internal/engine"
)

// scriptedGate models the engine-facing Gate client responses. HTTP transport
// mapping is outside this lab: an upstream 503 is represented by its client error.
type scriptedGate struct {
	mu        sync.Mutex
	calls     map[string]int
	responses []string
	advance   func(time.Duration)
}

func (g *scriptedGate) ApprovalStatus(_ context.Context, id string) (engine.GateApproval, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.calls == nil {
		g.calls = make(map[string]int)
	}
	call := g.calls[id]
	g.calls[id]++
	decision := engine.GateApproval{Status: "APPROVED", DecidedBy: "lab-gate"}
	switch id {
	case "gate-errors":
		switch call {
		case 0:
			g.responses = append(g.responses, "pending")
			return engine.GateApproval{Status: "PENDING"}, nil
		case 1:
			g.responses = append(g.responses, "http-503")
			return engine.GateApproval{}, errors.New("Gate HTTP 503 Service Unavailable")
		case 2:
			g.responses = append(g.responses, "timeout")
			return engine.GateApproval{}, context.DeadlineExceeded
		}
	case "gate-late":
		// The lease transaction completes before the external response moves time.
		// The second engine transaction must recheck the approval deadline.
		g.advance(40 * time.Millisecond)
		g.responses = append(g.responses, "approved-after-deadline")
		return decision, nil
	}
	g.responses = append(g.responses, "approved")
	return decision, nil
}
func (g *scriptedGate) observed() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.responses...)
}

func checkGateResponses(scenario string, responses []string) error {
	if scenario != "gate-errors" {
		return nil
	}
	expected := []string{"pending", "http-503", "timeout", "approved"}
	if len(responses) < len(expected) {
		return fmt.Errorf("Gate errors scenario stopped after %v; expected pending, 503 and timeout recovery", responses)
	}
	for i, want := range expected {
		if responses[i] != want {
			return fmt.Errorf("Gate response %d: got %s, want %s", i, responses[i], want)
		}
	}
	return nil
}
