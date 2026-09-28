package lab

import (
	"fmt"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
)

func checkPostgresOutcome(snapshot Snapshot, seed int64) error {
	if len(snapshot.Runs) != 2 {
		return fmt.Errorf("scenario: expected two PostgreSQL runs, got %d", len(snapshot.Runs))
	}
	for i, run := range snapshot.Runs {
		id := fmt.Sprintf("pg-%06d-r%d", seed, i)
		if run == nil || run.RunID != id {
			return fmt.Errorf("scenario: expected PostgreSQL run %s", id)
		}
		if run.InFlight || run.WorkflowTaskID != 0 || len(snapshot.Tasks[id]) != 0 || len(snapshot.Timers[id]) != 0 || snapshot.InboxSizes[id] != 0 {
			return fmt.Errorf("scenario: run %s retained live work after closure", id)
		}
		for _, approval := range snapshot.Approvals[id] {
			if approval.Status == store.ApprovalPending {
				return fmt.Errorf("scenario: run %s retained a pending approval after closure", id)
			}
		}
		n := float64(seed*10 + int64(i)*100000)
		expected, _ := payload([]any{n, n + 1, n + 2})
		if run.Status != v1.RunStatus_RUN_STATUS_COMPLETED || run.Failure != nil || !equalPayload(expected, run.Result) {
			return fmt.Errorf("scenario: run %s has wrong status/result", run.RunID)
		}
	}
	return nil
}
