package memstore_test

import (
	"reflect"
	"testing"
	"time"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func assertRecordEqual(t *testing.T, got, want any) {
	t.Helper()
	g, w := reflect.ValueOf(got).Elem(), reflect.ValueOf(want).Elem()
	for i := range g.NumField() {
		actual, expected := g.Field(i).Interface(), w.Field(i).Interface()
		if message, ok := actual.(proto.Message); ok {
			if !proto.Equal(message, expected.(proto.Message)) {
				t.Fatalf("%s = %v, want %v", g.Type().Field(i).Name, actual, expected)
			}
		} else if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("%s = %v, want %v", g.Type().Field(i).Name, actual, expected)
		}
	}
}

func TestUpdateRunReplacesEveryNonKeyField(t *testing.T) {
	s := newStore(t)
	tx(t, s, func(tx store.Tx) error { return tx.InsertRun(run("r")) })
	want := &store.Run{
		RunID: "r", WorkflowType: "replacement", TaskQueue: "replacement-q",
		Status: capstanv1.RunStatus_RUN_STATUS_BLOCKED, Input: payload("new input"),
		Result: payload("new result"), Failure: &capstanv1.Failure{Cause: &capstanv1.Failure{Details: payload("new cause")}},
		TaskTimeout: 2 * time.Second, RunTimeout: time.Hour, RunDeadline: epoch.Add(time.Hour),
		StartedAt: epoch.Add(time.Minute), ClosedAt: epoch.Add(2 * time.Minute), LastEventID: 42,
		WorkflowTaskID: 7, InFlight: true, CancelRequested: true,
		ContinuedFromRunID: "previous", ContinuedToRunID: "next", Identity: "replacement identity",
	}
	want.Input.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	update := *want
	update.Input = proto.Clone(want.Input).(*capstanv1.Payload)
	update.Result = proto.Clone(want.Result).(*capstanv1.Payload)
	update.Failure = proto.Clone(want.Failure).(*capstanv1.Failure)
	tx(t, s, func(tx store.Tx) error {
		if err := tx.UpdateRun(&update); err != nil {
			return err
		}
		update.Input.Data[0] = 'X'
		update.Input.ProtoReflect().GetUnknown()[2] = 2
		update.Result.Data[0] = 'X'
		update.Failure.Cause.Details.Data[0] = 'X'
		got, err := tx.GetRun("r", false)
		if err != nil {
			return err
		}
		assertRecordEqual(t, got, want)
		got.Input.Data[0] = 'Y'
		got.Input.ProtoReflect().GetUnknown()[2] = 3
		got.Result.Data[0] = 'Y'
		got.Failure.Cause.Details.Data[0] = 'Y'
		return nil
	})
	tx(t, s, func(tx store.Tx) error {
		got, err := tx.GetRun("r", false)
		if err != nil {
			return err
		}
		assertRecordEqual(t, got, want)
		cleared := &store.Run{RunID: "r"}
		if err := tx.UpdateRun(cleared); err != nil {
			return err
		}
		got, err = tx.GetRun("r", false)
		if err != nil {
			return err
		}
		assertRecordEqual(t, got, cleared)
		return nil
	})
}

func TestUpdateTaskReplacesEveryNonKeyField(t *testing.T) {
	s := newStore(t)
	original := &store.Task{RunID: "r", Kind: store.TaskWorkflow, TaskQueue: "q", ScheduledAt: epoch}
	tx(t, s, func(tx store.Tx) error {
		for _, id := range []string{"r", "other"} {
			if err := tx.InsertRun(run(id)); err != nil {
				return err
			}
		}
		return tx.InsertTask(original)
	})
	want := &store.Task{
		ID: original.ID, Kind: store.TaskActivity, RunID: "other", TaskQueue: "new-q", ScheduledEventID: 9, Attempt: 2,
		VisibleAt: epoch.Add(time.Minute), LeasedUntil: epoch.Add(2 * time.Minute), WorkerID: "worker",
		StartedAt: epoch.Add(3 * time.Minute), ScheduledAt: epoch.Add(4 * time.Minute), CheckAt: epoch.Add(5 * time.Minute),
		Activity: &capstanv1.ActivityScheduledAttributes{Seq: 10, ActivityType: "replacement", Input: payload("new activity"),
			StartToCloseTimeout: durationpb.New(time.Second + time.Nanosecond)},
		LastHeartbeatAt: epoch.Add(6 * time.Minute), HeartbeatDetails: payload("new heartbeat"),
		LastFailure:     &capstanv1.Failure{Cause: &capstanv1.Failure{Details: payload("new cause")}},
		CancelRequested: true, StartedEventID: 11,
	}
	want.Activity.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	update := *want
	update.Activity = proto.Clone(want.Activity).(*capstanv1.ActivityScheduledAttributes)
	update.HeartbeatDetails = proto.Clone(want.HeartbeatDetails).(*capstanv1.Payload)
	update.LastFailure = proto.Clone(want.LastFailure).(*capstanv1.Failure)
	tx(t, s, func(tx store.Tx) error {
		if err := tx.UpdateTask(&update); err != nil {
			return err
		}
		update.Activity.Input.Data[0] = 'X'
		update.Activity.StartToCloseTimeout.Nanos = 2
		update.Activity.ProtoReflect().GetUnknown()[2] = 2
		update.HeartbeatDetails.Data[0] = 'X'
		update.LastFailure.Cause.Details.Data[0] = 'X'
		got, err := tx.GetTask(original.ID, false)
		if err != nil {
			return err
		}
		assertRecordEqual(t, got, want)
		if got.Activity != nil {
			got.Activity.Input.Data[0] = 'Y'
			got.Activity.StartToCloseTimeout.Nanos = 3
			got.Activity.ProtoReflect().GetUnknown()[2] = 3
		}
		got.HeartbeatDetails.Data[0] = 'Y'
		got.LastFailure.Cause.Details.Data[0] = 'Y'
		return nil
	})
	tx(t, s, func(tx store.Tx) error {
		got, err := tx.GetTask(original.ID, false)
		if err != nil {
			return err
		}
		assertRecordEqual(t, got, want)
		cleared := &store.Task{ID: original.ID}
		if err := tx.UpdateTask(cleared); err != nil {
			return err
		}
		got, err = tx.GetTask(original.ID, false)
		if err != nil {
			return err
		}
		assertRecordEqual(t, got, cleared)
		return nil
	})
}

func TestUpdateApprovalReplacesEveryNonKeyField(t *testing.T) {
	s := newStore(t)
	tx(t, s, func(tx store.Tx) error {
		if err := tx.InsertRun(run("r")); err != nil {
			return err
		}
		return tx.InsertApproval(&store.Approval{RunID: "r", ApprovalID: "a", Source: capstanv1.ApprovalSource_APPROVAL_SOURCE_HUMAN, Status: store.ApprovalPending})
	})
	want := &store.Approval{
		RunID: "r", ApprovalID: "a", Seq: 4, RequestedEventID: 5, Source: capstanv1.ApprovalSource_APPROVAL_SOURCE_GATE,
		GateDecisionID: "gate", Status: store.ApprovalApproved, DueAt: epoch.Add(time.Hour), CheckAt: epoch.Add(time.Minute),
		GatePolls: 2, RequestedAt: epoch, ResolvedAt: epoch.Add(2 * time.Minute), Resolver: "reviewer", Choice: "yes", Note: "approved",
	}
	tx(t, s, func(tx store.Tx) error { return tx.UpdateApproval(want) })
	tx(t, s, func(tx store.Tx) error {
		got, err := tx.GetApproval("r", "a", false)
		if err != nil {
			return err
		}
		assertRecordEqual(t, got, want)
		cleared := &store.Approval{RunID: "r", ApprovalID: "a"}
		if err := tx.UpdateApproval(cleared); err != nil {
			return err
		}
		got, err = tx.GetApproval("r", "a", false)
		if err != nil {
			return err
		}
		assertRecordEqual(t, got, cleared)
		return nil
	})
}

func TestUpdateAICallReplacesEveryNonKeyField(t *testing.T) {
	s := newStore(t)
	original := &store.AICall{RunID: "r", Status: store.AICallReserved}
	tx(t, s, func(tx store.Tx) error {
		for _, id := range []string{"r", "other"} {
			if err := tx.InsertRun(run(id)); err != nil {
				return err
			}
		}
		return tx.InsertAICall(original)
	})
	want := &store.AICall{
		ID: original.ID, RunID: "other", ActivitySeq: 4, Model: "replacement", Status: store.AICallFinished,
		Bounded: true, EstimateUSD: 0.1, CostUSD: 0.2, InputTokens: 10, OutputTokens: 20, CacheReadTokens: 30, CacheWriteTokens: 40,
		ErrorCode: "replacement", At: epoch, FinishedAt: epoch.Add(time.Second),
	}
	tx(t, s, func(tx store.Tx) error { return tx.UpdateAICall(want) })
	tx(t, s, func(tx store.Tx) error {
		got, err := tx.GetAICall(original.ID, false)
		if err != nil {
			return err
		}
		assertRecordEqual(t, got, want)
		cleared := &store.AICall{ID: original.ID}
		if err := tx.UpdateAICall(cleared); err != nil {
			return err
		}
		got, err = tx.GetAICall(original.ID, false)
		if err != nil {
			return err
		}
		assertRecordEqual(t, got, cleared)
		return nil
	})
}
