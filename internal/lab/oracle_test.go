package lab

import (
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"testing"
)

func TestSignalOracleRejectsLostAndDuplicateSignals(t *testing.T) {
	expected := []signalExpectation{{"lab", "input", "noise"}}
	event := &v1.HistoryEvent{Attributes: &v1.HistoryEvent_SignalReceived{SignalReceived: &v1.SignalReceivedAttributes{RequestId: "input", Name: "noise", Input: &v1.Payload{ContentType: "application/json", Data: []byte("7")}}}}
	for _, count := range []int{0, 1, 2} {
		snapshot := Snapshot{Histories: map[string][]*v1.HistoryEvent{}}
		for i := 0; i < count; i++ {
			snapshot.Histories["lab"] = append(snapshot.Histories["lab"], event)
		}
		err := checkSignals(snapshot, expected)
		if (err == nil) != (count == 1) {
			t.Errorf("%d deliveries: %v", count, err)
		}
	}
	event.GetSignalReceived().Name = "wrong"
	if err := checkSignals(Snapshot{Histories: map[string][]*v1.HistoryEvent{"lab": {event}}}, expected); err == nil {
		t.Fatal("accepted corrupted signal")
	}
}

func TestScenarioOracleRejectsWrongStatusAndResult(t *testing.T) {
	value, _ := payload([]any{1, 2})
	run := &store.Run{RunID: "lab", Status: v1.RunStatus_RUN_STATUS_COMPLETED, Result: value}
	snapshot := Snapshot{Runs: []*store.Run{run}}
	if err := checkScenario(snapshot, "pipeline"); err != nil {
		t.Fatal(err)
	}
	run.Status = v1.RunStatus_RUN_STATUS_FAILED
	if err := checkScenario(snapshot, "pipeline"); err == nil {
		t.Fatal("accepted systematic failed baseline")
	}
	run.Status = v1.RunStatus_RUN_STATUS_COMPLETED
	run.Result, _ = payload([]any{1, 3})
	if err := checkScenario(snapshot, "pipeline"); err == nil {
		t.Fatal("accepted wrong result")
	}
}

func TestScenarioOracleRejectsWorkRetainedAfterClose(t *testing.T) {
	for _, kind := range []string{"task", "lease", "workflow-id", "timer", "inbox", "approval"} {
		t.Run(kind, func(t *testing.T) {
			run := &store.Run{RunID: "lab", Status: v1.RunStatus_RUN_STATUS_TIMED_OUT}
			snapshot := Snapshot{Runs: []*store.Run{run}}
			switch kind {
			case "task":
				snapshot.Tasks = map[string][]*store.Task{"lab": {{ID: 1}}}
			case "lease":
				run.InFlight = true
			case "workflow-id":
				run.WorkflowTaskID = 1
			case "timer":
				snapshot.Timers = map[string][]*store.Timer{"lab": {{Seq: 1}}}
			case "inbox":
				snapshot.InboxSizes = map[string]int{"lab": 1}
			case "approval":
				snapshot.Approvals = map[string][]*store.Approval{"lab": {{Status: store.ApprovalPending}}}
			}
			if err := checkScenario(snapshot, "timeout"); err == nil {
				t.Fatal("accepted retained " + kind)
			}
		})
	}
}
