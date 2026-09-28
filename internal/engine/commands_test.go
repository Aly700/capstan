package engine

import (
	"bytes"
	"context"
	"errors"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"testing"
	"time"
)

func TestCompleteRejectsInvalidCommands(t *testing.T) {
	for _, tc := range []struct {
		name string
		cmds []*v1.Command
	}{
		{"zero", []*v1.Command{markerCmd(0)}}, {"decreasing", []*v1.Command{markerCmd(2), markerCmd(1)}}, {"duplicate", []*v1.Command{markerCmd(1), markerCmd(1)}},
		{"timeout", []*v1.Command{{Attributes: &v1.Command_ScheduleActivity{ScheduleActivity: &v1.ScheduleActivityCommand{Seq: 1, ActivityType: "a"}}}}},
		{"timer", []*v1.Command{timerCmd(1, 0)}}, {"close_not_last", []*v1.Command{completeCmd(), markerCmd(1)}}, {"two_close", []*v1.Command{completeCmd(), completeCmd()}},
		{"cancel_without_request", []*v1.Command{{Attributes: &v1.Command_CancelRun{CancelRun: &v1.CancelRunCommand{}}}}}, {"empty", []*v1.Command{{}}}, {"nil", []*v1.Command{nil}},
		{"unknown_timer", []*v1.Command{{Attributes: &v1.Command_CancelTimer{CancelTimer: &v1.CancelTimerCommand{Seq: 1}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, _, _ := newTestEngine(t)
			mustStart(t, e, "r")
			p := mustPoll(t, e)
			before := historyBytes(t, e, "r")
			_, err := e.CompleteWorkflowTask(context.Background(), &v1.CompleteWorkflowTaskRequest{TaskToken: p.TaskToken, Commands: tc.cmds})
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("invalid: %v", err)
			}
			if !bytes.Equal(before, historyBytes(t, e, "r")) {
				t.Fatal("invalid changed history")
			}
			mustComplete(t, e, p.TaskToken, markerCmd(1))
		})
	}
}
func TestCompleteRejectsReusedSequence(t *testing.T) {
	e, _, _ := newTestEngine(t)
	mustStart(t, e, "r")
	mustComplete(t, e, mustPoll(t, e).TaskToken, markerCmd(3))
	if _, err := e.SignalRun(context.Background(), "", &v1.SignalRunRequest{RunId: "r", Name: "wake"}); err != nil {
		t.Fatal(err)
	}
	p := mustPoll(t, e)
	before := historyBytes(t, e, "r")
	_, err := e.CompleteWorkflowTask(context.Background(), &v1.CompleteWorkflowTaskRequest{TaskToken: p.TaskToken, Commands: []*v1.Command{markerCmd(2)}})
	if !errors.Is(err, ErrInvalidArgument) || !bytes.Equal(before, historyBytes(t, e, "r")) {
		t.Fatalf("reuse: %v", err)
	}
}
func TestCompleteRunClosesAndCleansUp(t *testing.T) {
	e, _, s := newTestEngine(t)
	mustStart(t, e, "r")
	p := mustPoll(t, e)
	approval := &v1.Command{Attributes: &v1.Command_RequestApproval{RequestApproval: &v1.RequestApprovalCommand{Seq: 3, ApprovalId: "a", Source: v1.ApprovalSource_APPROVAL_SOURCE_HUMAN}}}
	mustComplete(t, e, p.TaskToken, activityCmd(1), timerCmd(2, time.Hour), approval, completeCmd())
	wantTypes(t, history(t, e, "r"), v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED, v1.EventType_EVENT_TYPE_TASK_COMPLETED, v1.EventType_EVENT_TYPE_ACTIVITY_SCHEDULED, v1.EventType_EVENT_TYPE_TIMER_STARTED, v1.EventType_EVENT_TYPE_APPROVAL_REQUESTED, v1.EventType_EVENT_TYPE_RUN_COMPLETED)
	if err := s.InTx(context.Background(), func(tx store.Tx) error {
		r, err := tx.GetRun("r", false)
		if err != nil {
			return err
		}
		tasks, _ := tx.RunTasks("r")
		timers, _ := tx.RunTimers("r")
		a, err := tx.GetApproval("r", "a", false)
		if err != nil {
			return err
		}
		if r.Status != v1.RunStatus_RUN_STATUS_COMPLETED || string(r.Result.Data) != "done" || r.InFlight || r.WorkflowTaskID != 0 || r.ClosedAt.IsZero() || len(tasks) != 0 || len(timers) != 0 || a.Status != store.ApprovalExpired {
			t.Fatalf("cleanup %+v tasks %v timers %v approval %+v", r, tasks, timers, a)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
func TestFailRunAndCancelRun(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(map[bool]string{false: "failure", true: "cancellation"}[cancel], func(t *testing.T) {
			e, _, _ := newTestEngine(t)
			mustStart(t, e, "r")
			if cancel {
				if _, err := e.CancelRun(context.Background(), "client", &v1.CancelRunRequest{RunId: "r"}); err != nil {
					t.Fatal(err)
				}
			}
			p := mustPoll(t, e)
			cmd := &v1.Command{Attributes: &v1.Command_FailRun{FailRun: &v1.FailRunCommand{Failure: &v1.Failure{Type: "ApplicationFailure"}}}}
			want := v1.RunStatus_RUN_STATUS_FAILED
			if cancel {
				cmd = &v1.Command{Attributes: &v1.Command_CancelRun{CancelRun: &v1.CancelRunCommand{}}}
				want = v1.RunStatus_RUN_STATUS_CANCELLED
			}
			mustComplete(t, e, p.TaskToken, cmd)
			r, err := e.DescribeRun(context.Background(), &v1.DescribeRunRequest{RunId: "r"})
			if err != nil || r.Run.Status != want {
				t.Fatalf("close: %v %v", r, err)
			}
		})
	}
}
func TestContinueAsNewCreatesSuccessor(t *testing.T) {
	e, _, _ := newTestEngine(t)
	mustStart(t, e, "job")
	for _, pair := range [][2]string{{"job", "job~2"}, {"job~2", "job~3"}} {
		p := mustPoll(t, e)
		mustComplete(t, e, p.TaskToken, &v1.Command{Attributes: &v1.Command_ContinueAsNew{ContinueAsNew: &v1.ContinueAsNewCommand{Input: &v1.Payload{Data: []byte("next")}}}})
		r, err := e.DescribeRun(context.Background(), &v1.DescribeRunRequest{RunId: pair[0]})
		if err != nil || r.Run.Status != v1.RunStatus_RUN_STATUS_CONTINUED_AS_NEW || r.Run.ContinuedAsNewRunId != pair[1] {
			t.Fatalf("continue %v %v", r, err)
		}
		wantTypes(t, history(t, e, pair[1]), v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED)
		h := historyEvents(t, e, pair[1])
		if h[0].GetRunStarted().ContinuedFromRunId != pair[0] || string(h[0].GetRunStarted().Input.Data) != "next" {
			t.Fatal("continuation linkage")
		}
	}
}
func TestHistoryLimitFailsTheRun(t *testing.T) {
	e, _, _ := newTestEngine(t)
	e.cfg.MaxHistoryEvents = 20
	mustStart(t, e, "r")
	p := mustPoll(t, e)
	var cmds []*v1.Command
	for i := int64(1); i <= 17; i++ {
		cmds = append(cmds, markerCmd(i))
	}
	mustComplete(t, e, p.TaskToken, cmds...)
	r, err := e.DescribeRun(context.Background(), &v1.DescribeRunRequest{RunId: "r"})
	if err != nil || r.Run.Status != v1.RunStatus_RUN_STATUS_FAILED || r.Run.Failure.Type != "HistoryLimitExceeded" {
		t.Fatalf("limit %v %v", r, err)
	}
}
