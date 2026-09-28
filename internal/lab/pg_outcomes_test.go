package lab

import (
	"fmt"
	"testing"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
)

func TestPostgresOutcomeChecksTerminalCleanup(t *testing.T) {
	for name, mutate := range map[string]func(*Snapshot){
		"in flight":        func(s *Snapshot) { s.Runs[0].InFlight = true },
		"workflow task id": func(s *Snapshot) { s.Runs[0].WorkflowTaskID = 1 },
		"tasks":            func(s *Snapshot) { s.Tasks[s.Runs[0].RunID] = []*store.Task{{ID: 1}} },
		"timers":           func(s *Snapshot) { s.Timers[s.Runs[0].RunID] = []*store.Timer{{Seq: 1}} },
		"inbox":            func(s *Snapshot) { s.InboxSizes[s.Runs[0].RunID] = 1 },
		"failure":          func(s *Snapshot) { s.Runs[0].Failure = &v1.Failure{Type: "unexpected"} },
		"approval": func(s *Snapshot) {
			s.Approvals[s.Runs[0].RunID] = []*store.Approval{{ApprovalID: "a", Status: store.ApprovalPending}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := Snapshot{Tasks: map[string][]*store.Task{}, Timers: map[string][]*store.Timer{}, Approvals: map[string][]*store.Approval{}, InboxSizes: map[string]int{}}
			for i := 0; i < 2; i++ {
				n := float64(i * 100000)
				result, _ := payload([]any{n, n + 1, n + 2})
				s.Runs = append(s.Runs, &store.Run{RunID: fmt.Sprintf("pg-%06d-r%d", 0, i), Status: v1.RunStatus_RUN_STATUS_COMPLETED, Result: result})
			}
			if err := checkPostgresOutcome(s, 0); err != nil {
				t.Fatalf("valid completed scenario rejected: %v", err)
			}
			mutate(&s)
			if err := checkPostgresOutcome(s, 0); err == nil {
				t.Fatal("retained terminal state went undetected")
			}
		})
	}
}
