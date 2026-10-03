package engine

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestCancelReferencesCoexistWithNewAllocations(t *testing.T) {
	for _, fromHistory := range []bool{false, true} {
		t.Run(map[bool]string{false: "same_task", true: "history"}[fromHistory], func(t *testing.T) {
			e, _, _ := newTestEngine(t)
			mustStart(t, e, "r")
			p := mustPoll(t, e)
			cmds := []*v1.Command{timerCmd(1, time.Minute), activityCmd(2)}
			want := []v1.EventType{v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED, v1.EventType_EVENT_TYPE_TASK_COMPLETED, v1.EventType_EVENT_TYPE_TIMER_STARTED, v1.EventType_EVENT_TYPE_ACTIVITY_SCHEDULED}
			if fromHistory {
				mustComplete(t, e, p.TaskToken, cmds...)
				if _, err := e.SignalRun(t.Context(), "", &v1.SignalRunRequest{RunId: "r", Name: "wake"}); err != nil {
					t.Fatal(err)
				}
				p = mustPoll(t, e)
				cmds = nil
				want = append(want, v1.EventType_EVENT_TYPE_SIGNAL_RECEIVED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED, v1.EventType_EVENT_TYPE_TASK_COMPLETED)
			}
			cmds = append(cmds, activityCmd(3), &v1.Command{Attributes: &v1.Command_CancelTimer{CancelTimer: &v1.CancelTimerCommand{Seq: 1}}}, &v1.Command{Attributes: &v1.Command_RequestActivityCancel{RequestActivityCancel: &v1.RequestActivityCancelCommand{Seq: 2}}})
			mustComplete(t, e, p.TaskToken, cmds...)
			want = append(want, v1.EventType_EVENT_TYPE_ACTIVITY_SCHEDULED, v1.EventType_EVENT_TYPE_TIMER_CANCELLED, v1.EventType_EVENT_TYPE_ACTIVITY_CANCEL_REQUESTED, v1.EventType_EVENT_TYPE_ACTIVITY_CANCELLED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED)
			wantTypes(t, history(t, e, "r"), want...)
		})
	}
}

func TestCancelTimerRejectsSettledReference(t *testing.T) {
	for _, settlement := range []string{"fired", "cancelled", "earlier_command"} {
		t.Run(settlement, func(t *testing.T) {
			e, c, _ := newTestEngine(t)
			mustStart(t, e, "r")
			p := mustPoll(t, e)
			cancel := &v1.Command{Attributes: &v1.Command_CancelTimer{CancelTimer: &v1.CancelTimerCommand{Seq: 1}}}
			cmds := []*v1.Command{cancel}
			if settlement == "earlier_command" {
				cmds = []*v1.Command{timerCmd(1, time.Second), cancel, cancel}
			} else {
				first := []*v1.Command{timerCmd(1, time.Second)}
				if settlement == "cancelled" {
					first = append(first, cancel)
				}
				mustComplete(t, e, p.TaskToken, first...)
				if settlement == "fired" {
					c.Advance(time.Second)
					if n, err := e.FireDueTimers(t.Context(), 1); n != 1 || err != nil {
						t.Fatalf("fire %d %v", n, err)
					}
				} else {
					if _, err := e.SignalRun(t.Context(), "", &v1.SignalRunRequest{RunId: "r", Name: "wake"}); err != nil {
						t.Fatal(err)
					}
				}
				p = mustPoll(t, e)
			}
			before := historyBytes(t, e, "r")
			if _, err := e.CompleteWorkflowTask(t.Context(), &v1.CompleteWorkflowTaskRequest{TaskToken: p.TaskToken, Commands: cmds}); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("settled target: %v", err)
			}
			if !bytes.Equal(before, historyBytes(t, e, "r")) {
				t.Fatal("invalid reference changed history")
			}
		})
	}
}

func TestActivityCancelRejectsSettledReference(t *testing.T) {
	for _, kind := range []v1.EventType{v1.EventType_EVENT_TYPE_ACTIVITY_COMPLETED, v1.EventType_EVENT_TYPE_ACTIVITY_FAILED, v1.EventType_EVENT_TYPE_ACTIVITY_TIMED_OUT, v1.EventType_EVENT_TYPE_ACTIVITY_CANCELLED} {
		t.Run(kind.String(), func(t *testing.T) {
			e, c, _ := newTestEngine(t)
			mustStart(t, e, "r")
			cmd := activityCmd(1)
			cmd.GetScheduleActivity().RetryPolicy = &v1.RetryPolicy{MaximumAttempts: 1}
			mustComplete(t, e, mustPoll(t, e).TaskToken, cmd)
			if kind == v1.EventType_EVENT_TYPE_ACTIVITY_CANCELLED {
				if _, err := e.SignalRun(t.Context(), "", &v1.SignalRunRequest{RunId: "r", Name: "wake"}); err != nil {
					t.Fatal(err)
				}
				mustComplete(t, e, mustPoll(t, e).TaskToken, &v1.Command{Attributes: &v1.Command_RequestActivityCancel{RequestActivityCancel: &v1.RequestActivityCancelCommand{Seq: 1}}})
			} else {
				a := mustActivity(t, e)
				switch kind {
				case v1.EventType_EVENT_TYPE_ACTIVITY_COMPLETED:
					mustFinishActivity(t, e, a.TaskToken)
				case v1.EventType_EVENT_TYPE_ACTIVITY_FAILED:
					if _, err := e.FailActivityTask(t.Context(), &v1.FailActivityTaskRequest{TaskToken: a.TaskToken, Failure: &v1.Failure{NonRetryable: true}}); err != nil {
						t.Fatal(err)
					}
				case v1.EventType_EVENT_TYPE_ACTIVITY_TIMED_OUT:
					c.Advance(10 * time.Second)
					if n, err := e.ProcessDueTasks(t.Context(), 1); n != 1 || err != nil {
						t.Fatalf("timeout %d %v", n, err)
					}
				}
			}
			p := mustPoll(t, e)
			before := historyBytes(t, e, "r")
			if _, err := e.CompleteWorkflowTask(t.Context(), &v1.CompleteWorkflowTaskRequest{TaskToken: p.TaskToken, Commands: []*v1.Command{{Attributes: &v1.Command_RequestActivityCancel{RequestActivityCancel: &v1.RequestActivityCancelCommand{Seq: 1}}}}}); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("settled target: %v", err)
			}
			if !bytes.Equal(before, historyBytes(t, e, "r")) {
				t.Fatal("invalid reference changed history")
			}
		})
	}
}

func TestCallerRunIDBoundariesAndContinuation(t *testing.T) {
	e, _, _ := newTestEngine(t)
	for _, id := range []string{strings.Repeat("x", 181), "job~2"} {
		if _, err := e.StartRun(t.Context(), "", &v1.StartRunRequest{RunId: id, WorkflowType: "flow", TaskQueue: "q"}); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("StartRun(%q): %v", id, err)
		}
	}
	base := strings.Repeat("x", 180)
	mustStart(t, e, base)
	mustComplete(t, e, mustPoll(t, e).TaskToken, &v1.Command{Attributes: &v1.Command_ContinueAsNew{ContinueAsNew: &v1.ContinueAsNewCommand{}}})
	r, err := e.DescribeRun(t.Context(), &v1.DescribeRunRequest{RunId: base + "~2"})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Run.RunId) > 200 {
		t.Fatalf("successor too long: %d", len(r.Run.RunId))
	}
	wantTypes(t, history(t, e, base+"~2"), v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED)
}

func TestStartRunRequiresWholeMillisecondTimeouts(t *testing.T) {
	for _, which := range []string{"task", "run"} {
		t.Run(which, func(t *testing.T) {
			e, _, _ := newTestEngine(t)
			req := &v1.StartRunRequest{RunId: "r", WorkflowType: "flow", TaskQueue: "q"}
			d := durationpb.New(1500 * time.Microsecond)
			if which == "task" {
				req.TaskTimeout = d
			} else {
				req.RunTimeout = d
			}
			if _, err := e.StartRun(t.Context(), "", req); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("fractional ms: %v", err)
			}
		})
	}
}

func TestClockReadingsAndComputedDeadlinesUseMicroseconds(t *testing.T) {
	e, c, s := newTestEngine(t)
	c.Advance(987654321 * time.Nanosecond)
	now := c.Now().Truncate(time.Microsecond)
	e.cfg.GatePollInitial = 3*time.Millisecond + 321*time.Nanosecond
	_, err := e.StartRun(t.Context(), "", &v1.StartRunRequest{RunId: "r", WorkflowType: "flow", TaskQueue: "q", RunTimeout: durationpb.New(time.Hour), TaskTimeout: durationpb.New(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	p := mustPoll(t, e)
	if err := s.InTx(t.Context(), func(tx store.Tx) error {
		r, err := tx.GetRun("r", false)
		if err != nil {
			return err
		}
		rows, err := tx.RunTasks("r")
		if err != nil {
			return err
		}
		assertMicroTime(t, r.StartedAt, now)
		assertMicroTime(t, r.RunDeadline, now.Add(time.Hour))
		assertMicroTime(t, rows[0].VisibleAt, now)
		assertMicroTime(t, rows[0].StartedAt, now)
		assertMicroTime(t, rows[0].LeasedUntil, now.Add(time.Second))
		assertMicroTime(t, rows[0].CheckAt, now.Add(time.Second))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	a := activityCmd(1)
	a.GetScheduleActivity().StartToCloseTimeout = durationpb.New(10*time.Millisecond + 789*time.Nanosecond)
	a.GetScheduleActivity().ScheduleToCloseTimeout = durationpb.New(time.Second + 543*time.Nanosecond)
	a.GetScheduleActivity().ScheduleToStartTimeout = durationpb.New(20*time.Millisecond + 654*time.Nanosecond)
	a.GetScheduleActivity().HeartbeatTimeout = durationpb.New(5*time.Millisecond + 432*time.Nanosecond)
	a.GetScheduleActivity().RetryPolicy = &v1.RetryPolicy{InitialInterval: durationpb.New(6*time.Microsecond + 321*time.Nanosecond)}
	mustComplete(t, e, p.TaskToken, a, timerCmd(2, 7*time.Millisecond+765*time.Nanosecond), &v1.Command{Attributes: &v1.Command_RequestApproval{RequestApproval: &v1.RequestApprovalCommand{Seq: 3, ApprovalId: "a", Source: v1.ApprovalSource_APPROVAL_SOURCE_GATE, GateDecisionId: "d", Timeout: durationpb.New(9*time.Millisecond + 987*time.Nanosecond)}}})
	if err := s.InTx(t.Context(), func(tx store.Tx) error {
		rows, err := tx.RunTasks("r")
		if err != nil {
			return err
		}
		assertMicroTime(t, rows[0].CheckAt, now.Add(20*time.Millisecond))
		timers, err := tx.RunTimers("r")
		if err != nil {
			return err
		}
		assertMicroTime(t, timers[0].DueAt, now.Add(7*time.Millisecond))
		approval, err := tx.GetApproval("r", "a", false)
		if err != nil {
			return err
		}
		assertMicroTime(t, approval.RequestedAt, now)
		assertMicroTime(t, approval.DueAt, now.Add(9*time.Millisecond))
		assertMicroTime(t, approval.CheckAt, now.Add(3*time.Millisecond))
		if rows[0].Activity.GetStartToCloseTimeout().AsDuration() != 10*time.Millisecond+789*time.Nanosecond {
			t.Fatal("activity option lost precision")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	act := mustActivity(t, e)
	assertMicroTime(t, act.StartedTime.AsTime(), now)
	assertMicroTime(t, act.ScheduledTime.AsTime(), now)
	if err := s.InTx(t.Context(), func(tx store.Tx) error {
		rows, err := tx.RunTasks("r")
		if err != nil {
			return err
		}
		assertMicroTime(t, rows[0].LeasedUntil, now.Add(10*time.Millisecond))
		assertMicroTime(t, rows[0].CheckAt, now.Add(5*time.Millisecond))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	c.Advance(time.Microsecond)
	now = c.Now().Truncate(time.Microsecond)
	if _, err := e.HeartbeatActivityTask(t.Context(), &v1.HeartbeatActivityTaskRequest{TaskToken: act.TaskToken}); err != nil {
		t.Fatal(err)
	}
	if err := s.InTx(t.Context(), func(tx store.Tx) error {
		rows, err := tx.RunTasks("r")
		if err != nil {
			return err
		}
		assertMicroTime(t, rows[0].LastHeartbeatAt, now)
		assertMicroTime(t, rows[0].CheckAt, now.Add(5*time.Millisecond))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.FailActivityTask(t.Context(), &v1.FailActivityTaskRequest{TaskToken: act.TaskToken, Failure: &v1.Failure{Type: "Retry"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.InTx(t.Context(), func(tx store.Tx) error {
		rows, err := tx.RunTasks("r")
		if err != nil {
			return err
		}
		assertMicroTime(t, rows[0].VisibleAt, now.Add(6*time.Microsecond))
		assertMicroTime(t, rows[0].CheckAt, now.Add(6*time.Microsecond))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, ev := range historyEvents(t, e, "r") {
		if ev.Time.Nanos%1000 != 0 {
			t.Fatalf("event time: %v", ev.Time)
		}
	}
}

func assertMicroTime(t *testing.T, got, want time.Time) {
	t.Helper()
	if got.Nanosecond()%1000 != 0 || !got.Equal(want) {
		t.Fatalf("time got=%v want=%v", got, want)
	}
}

func TestSubmicrosecondRetryRemainsVisibleToSweeper(t *testing.T) {
	e, _, s := newTestEngine(t)
	ch, cancel := s.Subscribe(store.TaskActivity, "q")
	defer cancel()
	mustStart(t, e, "r")
	cmd := activityCmd(1)
	cmd.GetScheduleActivity().RetryPolicy = &v1.RetryPolicy{InitialInterval: durationpb.New(time.Nanosecond)}
	mustComplete(t, e, mustPoll(t, e).TaskToken, cmd)
	awaitNotification(t, ch)
	a := mustActivity(t, e)
	if _, err := e.FailActivityTask(t.Context(), &v1.FailActivityTaskRequest{TaskToken: a.TaskToken, Failure: &v1.Failure{Type: "Retry"}}); err != nil {
		t.Fatal(err)
	}
	if n, err := e.ProcessDueTasks(t.Context(), 1); n != 1 || err != nil {
		t.Fatalf("immediate retry not discoverable: %d %v", n, err)
	}
	awaitNotification(t, ch)
	if a = mustActivity(t, e); a.Attempt != 2 {
		t.Fatalf("attempt=%d", a.Attempt)
	}
}
