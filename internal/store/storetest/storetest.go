package storetest

import (
	"errors"
	"reflect"
	"testing"
	"time"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Run exercises the same contract against each implementation. open must return a new,
// empty store and register cleanup with the supplied subtest, not its parent.
func Run(t *testing.T, open func(t *testing.T) store.Store) {
	t.Helper()
	for _, tc := range []struct {
		name string
		run  func(*testing.T, store.Store)
	}{
		{"Runs/InsertGetUpdate", runsInsertGetUpdate},
		{"Runs/InsertDuplicateIsAlreadyExists", runsDuplicate},
		{"Runs/GetMissingIsNotFound", runsMissing},
		{"Runs/ListFiltersAndPaginates", runsList},
		{"Runs/RunsPastDeadlineOrdersAndLimits", runsDue},
		{"History/AppendContiguous", historyContiguous},
		{"History/AppendGapIsConflict", historyGap},
		{"History/AppendDuplicateIsConflict", historyDuplicate},
		{"History/ReadAfterAndLimit", historyRead},
		{"History/ProtoRoundTripExact", historyProto},
		{"Inbox/ArrivalOrderAndDrainEmpties", inboxDrain},
		{"Inbox/Size", inboxSize},
		{"Tasks/InsertAssignsIncreasingIDs", tasksIDs},
		{"Tasks/ClaimOldestVisibleUnleased", tasksClaimOldest},
		{"Tasks/ClaimRespectsVisibleAt", tasksVisible},
		{"Tasks/ClaimNoneReturnsNil", tasksClaimNone},
		{"Tasks/UpdateAndGet", tasksUpdate},
		{"Tasks/DueTasksOrderAndLimit", tasksDue},
		{"Tasks/DeleteMissingIsNotError", tasksDelete},
		{"Tasks/RunTasks", tasksRun},
		{"Timers/InsertDuplicateIsAlreadyExists", timersDuplicate},
		{"Timers/DueOrderAndLimit", timersDue},
		{"Timers/DeleteReportsExistence", timersDelete},
		{"Approvals/InsertGetUpdate", approvalsUpdate},
		{"Approvals/DueOnlyPending", approvalsDue},
		{"Signals/RecordDuplicateIsAlreadyExists", signalsDuplicate},
		{"Ledger/SpentSinceCountsReservedAtEstimate", ledgerSpent},
		{"Ledger/RunCost", ledgerRunCost},
		{"Ledger/InsertGetUpdate", ledgerUpdate},
		{"Ledger/BoundedRoundTrip", ledgerBounded},
		{"Ledger/LockBudgetSerialises", ledgerLock},
		{"Tx/RollbackDiscardsEverything", txRollback},
		{"Tx/ErrorIsReturnedUnchanged", txErrors},
		{"Tx/HandledSentinelsPermitCommit", txHandled},
		{"Notify/DeliveredOnCommitOnly", notifyCommit},
		{"Notify/CoalescedNonBlocking", notifyCoalesced},
		{"Notify/CancelStopsDelivery", notifyCancel},
		{"Notify/OneWakePerTask", notifyOnePerTask},
		{"Notify/MultipleTasksInOneTransaction", notifyMultipleTasks},
		{"Notify/RoundRobin", notifyRoundRobin},
		{"Notify/SkipsPendingHints", notifySkipsPending},
		{"Notify/CancelPreservesRotation", notifyCancelRotation},
		{"RunNotify/DeliveredOnCommitOnly", runNotifyCommit},
		{"RunNotify/RollbackDiscardsNotification", runNotifyRollback},
		{"RunNotify/IsolatedTopicsBroadcast", runNotifyTopics},
		{"RunNotify/CoalescedNonBlocking", runNotifyCoalesced},
		{"RunNotify/CancelIsIdempotent", runNotifyCancel},
		{"Concurrency/GetRunForUpdateSerialises", runLock},
		{"Concurrency/ClaimSkipsLockedRows", claimLock},
		{"Time/ZeroTimesRoundTrip", timeZero},
		{"Time/UTCPreserved", timeUTC},
		{"Time/UpdateUTCPreserved", timeUpdateUTC},
		{"Time/ClaimUTCPreserved", timeClaimUTC},
	} {
		t.Run(tc.name, func(t *testing.T) { tc.run(t, open(t)) })
	}
}

var epoch = time.Date(2026, 9, 28, 12, 0, 0, 123456000, time.UTC)

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func mustTx(t *testing.T, s store.Store, fn func(store.Tx) error) {
	t.Helper()
	must(t, s.InTx(t.Context(), fn))
}

func equal(t *testing.T, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func isError(t *testing.T, got, want error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Fatalf("got error %v, want %v", got, want)
	}
}

func equalProto(t *testing.T, got, want proto.Message) {
	t.Helper()
	if !proto.Equal(got, want) {
		t.Fatalf("protobuf got %v, want %v", got, want)
	}
}

func payload() *capstanv1.Payload {
	return &capstanv1.Payload{ContentType: "application/octet-stream", Data: []byte{0, 255, 0, 128, 10, 13, 34}}
}

func sampleRun(id string) *store.Run {
	return &store.Run{RunID: id, WorkflowType: "wf", TaskQueue: "q", Status: capstanv1.RunStatus_RUN_STATUS_RUNNING, TaskTimeout: 10 * time.Second, StartedAt: epoch}
}

func seed(t *testing.T, s store.Store, ids ...string) {
	t.Helper()
	mustTx(t, s, func(tx store.Tx) error {
		for _, id := range ids {
			if err := tx.InsertRun(sampleRun(id)); err != nil {
				return err
			}
		}
		return nil
	})
}

func event(id int64) *capstanv1.HistoryEvent {
	return &capstanv1.HistoryEvent{EventId: id, Type: capstanv1.EventType_EVENT_TYPE_SIGNAL_RECEIVED, Time: timestamppb.New(epoch), Attributes: &capstanv1.HistoryEvent_SignalReceived{SignalReceived: &capstanv1.SignalReceivedAttributes{Name: "signal", Input: payload()}}}
}

func sampleTask(runID string) *store.Task {
	return &store.Task{Kind: store.TaskActivity, RunID: runID, TaskQueue: "q", ScheduledEventID: 1, Attempt: 1, VisibleAt: epoch, ScheduledAt: epoch}
}

func sampleApproval(runID, id string) *store.Approval {
	return &store.Approval{RunID: runID, ApprovalID: id, Seq: 1, RequestedEventID: 1, Source: capstanv1.ApprovalSource_APPROVAL_SOURCE_HUMAN, Status: store.ApprovalPending, RequestedAt: epoch}
}

func equalRun(t *testing.T, got, want *store.Run) {
	t.Helper()
	if got == nil || want == nil {
		equal(t, got, want)
		return
	}
	equalProto(t, got.Input, want.Input)
	equalProto(t, got.Result, want.Result)
	equalProto(t, got.Failure, want.Failure)
	g, w := *got, *want
	g.Input, g.Result, g.Failure, w.Input, w.Result, w.Failure = nil, nil, nil, nil, nil, nil
	equal(t, g, w)
}

func equalTask(t *testing.T, got, want *store.Task) {
	t.Helper()
	if got == nil || want == nil {
		equal(t, got, want)
		return
	}
	equalProto(t, got.Activity, want.Activity)
	equalProto(t, got.HeartbeatDetails, want.HeartbeatDetails)
	equalProto(t, got.LastFailure, want.LastFailure)
	g, w := *got, *want
	g.Activity, g.HeartbeatDetails, g.LastFailure, w.Activity, w.HeartbeatDetails, w.LastFailure = nil, nil, nil, nil, nil, nil
	equal(t, g, w)
}
