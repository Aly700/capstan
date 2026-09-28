package server_test

import (
	"fmt"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/proto"
)

func TestAuditPostgresHumanWaitSevenDays(t *testing.T) {
	at := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	s := newAuditLedgerServer(t, at, 1)
	const runID, queue, approvalID = "seven-day-human", "audit-human", "approval-seven-days"
	if _, err := s.client.StartRun(t.Context(), connect.NewRequest(&v1.StartRunRequest{RunId: runID, WorkflowType: "human-wait", TaskQueue: queue})); err != nil {
		t.Fatal(err)
	}
	first, err := s.worker.PollWorkflowTask(t.Context(), connect.NewRequest(&v1.PollWorkflowTaskRequest{TaskQueue: queue}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.worker.CompleteWorkflowTask(t.Context(), connect.NewRequest(&v1.CompleteWorkflowTaskRequest{TaskToken: first.Msg.TaskToken, Commands: []*v1.Command{{Attributes: &v1.Command_RequestApproval{RequestApproval: &v1.RequestApprovalCommand{Seq: 1, ApprovalId: approvalID, Source: v1.ApprovalSource_APPROVAL_SOURCE_HUMAN, Prompt: "Ship after a week?", Options: []string{"ship", "hold"}}}}}}))
	if err != nil {
		t.Fatal(err)
	}
	assertIdle := func() {
		t.Helper()
		err := s.store.InTx(t.Context(), func(tx store.Tx) error {
			tasks, err := tx.RunTasks(runID)
			if err != nil {
				return err
			}
			run, err := tx.GetRun(runID, false)
			if err != nil {
				return err
			}
			approval, err := tx.GetApproval(runID, approvalID, false)
			if err != nil {
				return err
			}
			if len(tasks) != 0 || run.InFlight || run.WorkflowTaskID != 0 || run.Status != v1.RunStatus_RUN_STATUS_RUNNING || approval.Status != store.ApprovalPending {
				return fmt.Errorf("idle state: tasks=%d in_flight=%v workflow_task=%d status=%v approval=%v", len(tasks), run.InFlight, run.WorkflowTaskID, run.Status, approval.Status)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	assertIdle()
	before, err := s.client.GetHistory(t.Context(), connect.NewRequest(&v1.GetHistoryRequest{RunId: runID}))
	if err != nil {
		t.Fatal(err)
	}
	s.clock.advance(7 * 24 * time.Hour)
	assertIdle()
	_, err = s.client.ResolveApproval(t.Context(), connect.NewRequest(&v1.ResolveApprovalRequest{RunId: runID, ApprovalId: approvalID, Outcome: v1.ApprovalOutcome_APPROVAL_OUTCOME_APPROVED, Choice: "ship", Resolver: "audit-owner"}))
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := s.worker.PollWorkflowTask(t.Context(), connect.NewRequest(&v1.PollWorkflowTaskRequest{TaskQueue: queue}))
	if err != nil {
		t.Fatal(err)
	}
	for i, event := range before.Msg.Events {
		if !proto.Equal(event, resumed.Msg.History[i]) {
			t.Fatalf("history prefix changed at index %d", i)
		}
	}
	resolved := 0
	for _, event := range resumed.Msg.History {
		if a := event.GetApprovalResolved(); a != nil {
			resolved++
			if a.Choice != "ship" || a.Outcome != v1.ApprovalOutcome_APPROVAL_OUTCOME_APPROVED || !event.Time.AsTime().Equal(at.Add(7*24*time.Hour)) {
				t.Fatalf("approval result: %v", event)
			}
		}
	}
	if resolved != 1 {
		t.Fatalf("approval resolved events=%d, want 1", resolved)
	}
	_, err = s.worker.CompleteWorkflowTask(t.Context(), connect.NewRequest(&v1.CompleteWorkflowTaskRequest{TaskToken: resumed.Msg.TaskToken, Commands: []*v1.Command{{Attributes: &v1.Command_CompleteRun{CompleteRun: &v1.CompleteRunCommand{Result: &v1.Payload{ContentType: "application/json", Data: []byte(`"shipped"`)}}}}}}))
	if err != nil {
		t.Fatal(err)
	}
	info, err := s.client.DescribeRun(t.Context(), connect.NewRequest(&v1.DescribeRunRequest{RunId: runID}))
	if err != nil || info.Msg.Run.Status != v1.RunStatus_RUN_STATUS_COMPLETED || string(info.Msg.Run.Result.Data) != `"shipped"` {
		t.Fatalf("completion: %v %v", info, err)
	}
	t.Log("HUMAN approval waited seven engine-clock days in PostgreSQL with zero tasks/leases and no worker; resolution produced one ApprovalResolved, preserved history prefix, and run completed")
}
