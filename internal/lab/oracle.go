package lab

import (
	"fmt"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
)

type signalExpectation struct{ runID, requestID, name string }

func checkSignals(snapshot Snapshot, expected []signalExpectation) error {
	for _, want := range expected {
		count := 0
		for _, event := range snapshot.Histories[want.runID] {
			signal := event.GetSignalReceived()
			if signal == nil || signal.RequestId != want.requestID {
				continue
			}
			count++
			input, _ := payload(7)
			if signal.Name != want.name || !equalPayload(signal.Input, input) {
				return fmt.Errorf("signal %s/%s was changed", want.runID, want.requestID)
			}
		}
		if count != 1 {
			return fmt.Errorf("signal %s/%s: got %d deliveries, want one", want.runID, want.requestID, count)
		}
	}
	return nil
}
func checkScenario(snapshot Snapshot, name string) error {
	runs, err := snapshotRuns(snapshot)
	if err != nil {
		return err
	}
	for _, run := range snapshot.Runs {
		id := run.RunID
		if run.Open() || run.InFlight || run.WorkflowTaskID != 0 || len(snapshot.Tasks[id]) != 0 || len(snapshot.Timers[id]) != 0 || snapshot.InboxSizes[id] != 0 {
			return fmt.Errorf("scenario %s: run %s retained live work after closure", name, id)
		}
		for _, approval := range snapshot.Approvals[id] {
			if approval.Status == store.ApprovalPending {
				return fmt.Errorf("scenario %s: run %s retained pending approval", name, id)
			}
		}
	}
	count := 1
	if name == "continuation" {
		count = 2
	}
	if len(runs) != count || runs["lab"] == nil {
		return fmt.Errorf("scenario %s: unexpected run chain", name)
	}
	run := runs["lab"]
	status := v1.RunStatus_RUN_STATUS_COMPLETED
	var result any
	switch name {
	case "pipeline":
		result = []any{1, 2}
	case "pipeline-peer":
		result = []any{"peer", "done"}
	case "fanout":
		result = []any{10, 20, 30}
	case "signal":
		result = 7
	case "retry":
		result = 99
	case "human", "gate", "gate-errors", "gate-late", "approval-expiry":
		resolver, outcome := "lab-human", "approved"
		if name == "gate" || name == "gate-errors" {
			resolver = "lab-gate"
		}
		if name == "approval-expiry" || name == "gate-late" {
			resolver, outcome = "timeout", "expired"
		}
		result = map[string]any{"outcome": outcome, "choice": "", "resolver": resolver, "note": ""}
	case "cancel":
		status = v1.RunStatus_RUN_STATUS_CANCELLED
	case "timeout":
		status = v1.RunStatus_RUN_STATUS_TIMED_OUT
	case "continuation":
		next := runs["lab~2"]
		if run.Status != v1.RunStatus_RUN_STATUS_CONTINUED_AS_NEW || run.ContinuedToRunID != "lab~2" || next == nil || next.ContinuedFromRunID != "lab" {
			return fmt.Errorf("scenario continuation: broken chain")
		}
		run, result = next, 1
	default:
		return fmt.Errorf("unknown scenario %q", name)
	}
	if run.Status != status {
		return fmt.Errorf("scenario %s: got status %s, want %s", name, run.Status, status)
	}
	if status == v1.RunStatus_RUN_STATUS_COMPLETED {
		want, _ := payload(result)
		if run.Failure != nil || !equalPayload(run.Result, want) {
			return fmt.Errorf("scenario %s: unexpected result %s or failure %v", name, run.Result, run.Failure)
		}
	}
	return nil
}

// ScenarioNames returns the supported campaign scenarios in stable order.
func ScenarioNames() []string {
	var names []string
	for _, s := range scenarioCatalog() {
		names = append(names, s.name)
	}
	return names
}

func firstActivation(history []*v1.HistoryEvent) bool {
	for _, event := range history {
		if event.Type == v1.EventType_EVENT_TYPE_TASK_COMPLETED {
			return false
		}
	}
	return true
}
