package engine

import (
	"bytes"
	"context"
	"errors"
	"math"
	"testing"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestMalformedCommandDetailsAreAtomic(t *testing.T) {
	for _, tc := range []struct {
		name string
		cmds []*v1.Command
	}{
		{"activity_type", []*v1.Command{{Attributes: &v1.Command_ScheduleActivity{ScheduleActivity: &v1.ScheduleActivityCommand{Seq: 1, StartToCloseTimeout: durationpb.New(time.Second)}}}}},
		{"activity_queue", []*v1.Command{activityCmd(1)}},
		{"negative_optional_timeout", []*v1.Command{activityCmd(1)}},
		{"retry_policy", []*v1.Command{activityCmd(1)}},
		{"approval_id", []*v1.Command{{Attributes: &v1.Command_RequestApproval{RequestApproval: &v1.RequestApprovalCommand{Seq: 1, Source: v1.ApprovalSource_APPROVAL_SOURCE_HUMAN}}}}},
		{"gate_decision_id", []*v1.Command{{Attributes: &v1.Command_RequestApproval{RequestApproval: &v1.RequestApprovalCommand{Seq: 1, ApprovalId: "a", Source: v1.ApprovalSource_APPROVAL_SOURCE_GATE}}}}},
		{"approval_source", []*v1.Command{{Attributes: &v1.Command_RequestApproval{RequestApproval: &v1.RequestApprovalCommand{Seq: 1, ApprovalId: "a"}}}}},
		{"duplicate_approval", []*v1.Command{{Attributes: &v1.Command_RequestApproval{RequestApproval: &v1.RequestApprovalCommand{Seq: 1, ApprovalId: "a", Source: v1.ApprovalSource_APPROVAL_SOURCE_HUMAN}}}, {Attributes: &v1.Command_RequestApproval{RequestApproval: &v1.RequestApprovalCommand{Seq: 2, ApprovalId: "a", Source: v1.ApprovalSource_APPROVAL_SOURCE_HUMAN}}}}},
		{"unknown_activity_cancel", []*v1.Command{{Attributes: &v1.Command_RequestActivityCancel{RequestActivityCancel: &v1.RequestActivityCancelCommand{Seq: 1}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			switch tc.name {
			case "activity_queue":
				tc.cmds[0].GetScheduleActivity().TaskQueue = string([]byte{0xff})
			case "negative_optional_timeout":
				tc.cmds[0].GetScheduleActivity().HeartbeatTimeout = durationpb.New(-time.Second)
			case "retry_policy":
				tc.cmds[0].GetScheduleActivity().RetryPolicy = &v1.RetryPolicy{BackoffCoefficient: math.NaN()}
			}
			e, _, _ := newTestEngine(t)
			mustStart(t, e, "r")
			p := mustPoll(t, e)
			before := historyBytes(t, e, "r")
			if _, err := e.CompleteWorkflowTask(context.Background(), &v1.CompleteWorkflowTaskRequest{TaskToken: p.TaskToken, Commands: tc.cmds}); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("validation %v", err)
			}
			if !bytes.Equal(before, historyBytes(t, e, "r")) {
				t.Fatal("invalid command mutated history")
			}
			mustComplete(t, e, p.TaskToken)
		})
	}
}

func TestPagingAndSignalValidation(t *testing.T) {
	e, _, _ := newTestEngine(t)
	mustStart(t, e, "r")
	before := historyBytes(t, e, "r")
	_, a := e.ListRuns(context.Background(), &v1.ListRunsRequest{PageSize: -1})
	_, b := e.ListRuns(context.Background(), &v1.ListRunsRequest{PageToken: "!"})
	_, c := e.GetHistory(context.Background(), &v1.GetHistoryRequest{RunId: "r", AfterEventId: -1})
	_, d := e.GetHistory(context.Background(), &v1.GetHistoryRequest{RunId: "r", PageSize: -1})
	_, f := e.SignalRun(context.Background(), "", &v1.SignalRunRequest{RunId: "r", Name: ""})
	for _, err := range []error{a, b, c, d, f} {
		if !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("validation %v", err)
		}
	}
	if !bytes.Equal(before, historyBytes(t, e, "r")) {
		t.Fatal("read validation changed history")
	}
	for _, tc := range []struct {
		n              int32
		def, max, want int
	}{{0, 50, 500, 50}, {999, 50, 500, 500}, {0, 1000, 5000, 1000}, {9999, 1000, 5000, 5000}} {
		n, err := pageSize(tc.n, tc.def, tc.max)
		if err != nil || n != tc.want {
			t.Fatalf("page size %d %v", n, err)
		}
	}
}

func TestStartRunRejectsUnrepresentableDuration(t *testing.T) {
	e, _, _ := newTestEngine(t)
	_, err := e.StartRun(context.Background(), "", &v1.StartRunRequest{RunId: "r", WorkflowType: "flow", TaskQueue: "q", RunTimeout: &durationpb.Duration{Seconds: 315576000000}})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("overflowing duration %v", err)
	}
}
