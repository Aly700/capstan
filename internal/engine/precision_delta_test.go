package engine

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestPrecisionScheduleToCloseExpiresAtMicrosecondBoundary(t *testing.T) {
	e, clock, s := newTestEngine(t)
	clock.Advance(321 * time.Nanosecond)
	started := clock.Now().Truncate(time.Microsecond)
	scheduleTestActivity(t, e, "r", func(c *v1.ScheduleActivityCommand) {
		c.ScheduleToCloseTimeout = durationpb.New(2*time.Millisecond + 789*time.Nanosecond)
	})
	a := mustActivity(t, e)
	deadline := started.Add(2 * time.Millisecond)
	if err := s.InTx(t.Context(), func(tx store.Tx) error {
		tasks, err := tx.RunTasks("r")
		if err != nil {
			return err
		}
		if len(tasks) != 1 {
			t.Fatalf("activity tasks=%d", len(tasks))
		}
		assertMicroTime(t, tasks[0].CheckAt, deadline)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	before := historyBytes(t, e, "r")
	clock.Advance(deadline.Sub(clock.Now()))
	if _, err := e.CompleteActivityTask(t.Context(), &v1.CompleteActivityTaskRequest{TaskToken: a.TaskToken}); !errors.Is(err, ErrStaleTask) {
		t.Fatalf("completion at truncated deadline: %v", err)
	}
	if !bytes.Equal(before, historyBytes(t, e, "r")) {
		t.Fatal("stale completion changed history")
	}
	if n, err := e.ProcessDueTasks(t.Context(), 1); n != 1 || err != nil {
		t.Fatalf("timeout at truncated deadline: %d %v", n, err)
	}
	assertActivityTimeout(t, e, "r", v1.TimeoutType_TIMEOUT_TYPE_SCHEDULE_TO_CLOSE)
}

func TestPrecisionBufferedEventTimeBeforeFlush(t *testing.T) {
	e, clock, s := newTestEngine(t)
	clock.Advance(987654321 * time.Nanosecond)
	mustStart(t, e, "r")
	w := mustPoll(t, e)
	clock.Advance(1501 * time.Nanosecond)
	delivered := clock.Now().Truncate(time.Microsecond)
	before := historyBytes(t, e, "r")
	if _, err := e.SignalRun(t.Context(), "caller", &v1.SignalRunRequest{RunId: "r", Name: "buffered"}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, historyBytes(t, e, "r")) {
		t.Fatal("buffered event changed history")
	}
	rollback := errors.New("inspect inbox without consuming it")
	if err := s.InTx(t.Context(), func(tx store.Tx) error {
		events, err := tx.DrainInbox("r")
		if err != nil {
			return err
		}
		if len(events) != 1 || events[0].Type != v1.EventType_EVENT_TYPE_SIGNAL_RECEIVED {
			t.Fatalf("inbox events=%v", events)
		}
		assertMicroTime(t, events[0].Time.AsTime(), delivered)
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatalf("inspect inbox: %v", err)
	}
	clock.Advance(2501 * time.Nanosecond)
	mustComplete(t, e, w.TaskToken, markerCmd(1))
	wantTypes(t, history(t, e, "r"),
		v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED,
		v1.EventType_EVENT_TYPE_TASK_STARTED, v1.EventType_EVENT_TYPE_TASK_COMPLETED,
		v1.EventType_EVENT_TYPE_MARKER_RECORDED, v1.EventType_EVENT_TYPE_SIGNAL_RECEIVED,
		v1.EventType_EVENT_TYPE_TASK_SCHEDULED)
}

func TestPrecisionGateLeaseAndBackoff(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "error"}[failed], func(t *testing.T) {
			e, clock, s := newTestEngine(t)
			clock.Advance(987654321 * time.Nanosecond)
			e.cfg.GatePollInitial = 3*time.Millisecond + 789*time.Nanosecond
			pendingApproval(t, e, v1.ApprovalSource_APPROVAL_SOURCE_GATE, time.Hour)
			original := readApproval(t, s)
			before := historyBytes(t, e, "approval-run")
			clock.Advance(3 * time.Millisecond)
			leasedAt := clock.Now().Truncate(time.Microsecond)
			calls := 0
			e.deps.Gate = approvalGateFunc(func(context.Context, string) (GateApproval, error) {
				calls++
				leased := readApproval(t, s)
				assertMicroTime(t, leased.CheckAt, leasedAt.Add(time.Minute))
				if leased.GatePolls != 1 || leased.Status != store.ApprovalPending {
					t.Fatalf("poll lease: %+v", leased)
				}
				clock.Advance(4567 * time.Nanosecond)
				if failed {
					return GateApproval{}, errors.New("Gate unavailable")
				}
				return GateApproval{Status: "PENDING"}, nil
			})
			if n, err := e.ProcessDueApprovals(t.Context(), 1); n != 1 || err != nil {
				t.Fatalf("poll: %d %v", n, err)
			}
			if calls != 1 {
				t.Fatalf("Gate calls=%d", calls)
			}
			a := readApproval(t, s)
			// Doubling retains fractional duration precision before the deadline truncates.
			assertMicroTime(t, a.CheckAt, clock.Now().Truncate(time.Microsecond).Add(6*time.Millisecond+time.Microsecond))
			assertMicroTime(t, a.RequestedAt, original.RequestedAt)
			assertMicroTime(t, a.DueAt, original.DueAt)
			if a.Status != store.ApprovalPending || a.GatePolls != 1 || !bytes.Equal(before, historyBytes(t, e, "approval-run")) {
				t.Fatalf("pending poll changed approval or history: %+v", a)
			}
		})
	}
}

func TestPrecisionWorkflowRetryDeadlines(t *testing.T) {
	for _, timedOut := range []bool{false, true} {
		t.Run(map[bool]string{false: "failure", true: "timeout"}[timedOut], func(t *testing.T) {
			e, clock, s := newTestEngine(t)
			clock.Advance(321 * time.Nanosecond)
			started := clock.Now().Truncate(time.Microsecond)
			e.cfg.TaskRetryInitial = 1501 * time.Nanosecond
			if _, err := e.StartRun(t.Context(), "owner", &v1.StartRunRequest{RunId: "r", WorkflowType: "flow", TaskQueue: "q", TaskTimeout: durationpb.New(time.Millisecond)}); err != nil {
				t.Fatal(err)
			}
			w := mustPoll(t, e)
			kind := v1.EventType_EVENT_TYPE_TASK_FAILED
			if timedOut {
				kind = v1.EventType_EVENT_TYPE_TASK_TIMED_OUT
				clock.Advance(time.Millisecond)
				if n, err := e.ProcessDueTasks(t.Context(), 1); n != 1 || err != nil {
					t.Fatalf("workflow timeout: %d %v", n, err)
				}
			} else {
				clock.Advance(2501 * time.Nanosecond)
				if _, err := e.FailWorkflowTask(t.Context(), &v1.FailWorkflowTaskRequest{TaskToken: w.TaskToken, Cause: v1.TaskFailedCause_TASK_FAILED_CAUSE_SDK_ERROR}); err != nil {
					t.Fatal(err)
				}
			}
			now := clock.Now().Truncate(time.Microsecond)
			visible := now.Add(time.Microsecond)
			if err := s.InTx(t.Context(), func(tx store.Tx) error {
				tasks, err := tx.RunTasks("r")
				if err != nil {
					return err
				}
				if len(tasks) != 1 || tasks[0].Attempt != 2 {
					t.Fatalf("retry tasks=%+v", tasks)
				}
				assertMicroTime(t, tasks[0].ScheduledAt, now)
				assertMicroTime(t, tasks[0].VisibleAt, visible)
				assertMicroTime(t, tasks[0].CheckAt, visible)
				r, err := tx.GetRun("r", false)
				if err == nil {
					assertMicroTime(t, r.StartedAt, started)
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			wantTypes(t, history(t, e, "r"),
				v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED,
				v1.EventType_EVENT_TYPE_TASK_STARTED, kind, v1.EventType_EVENT_TYPE_TASK_SCHEDULED)
			if _, found, err := e.PollWorkflowTask(t.Context(), &v1.PollWorkflowTaskRequest{TaskQueue: "q"}); found || err != nil {
				t.Fatalf("retry before visibility: %v %v", found, err)
			}
			clock.Advance(visible.Sub(clock.Now()))
			if retry := mustPoll(t, e); retry.Attempt != 2 {
				t.Fatalf("retry attempt=%d", retry.Attempt)
			}
		})
	}
}

func TestPrecisionRunClosureAndContinuationTimes(t *testing.T) {
	for _, continued := range []bool{false, true} {
		t.Run(map[bool]string{false: "completed", true: "continued"}[continued], func(t *testing.T) {
			e, clock, s := newTestEngine(t)
			clock.Advance(987654321 * time.Nanosecond)
			started := clock.Now().Truncate(time.Microsecond)
			if _, err := e.StartRun(t.Context(), "owner", &v1.StartRunRequest{RunId: "r", WorkflowType: "flow", TaskQueue: "q", RunTimeout: durationpb.New(time.Hour)}); err != nil {
				t.Fatal(err)
			}
			mustComplete(t, e, mustPoll(t, e).TaskToken, approvalCmd(1, "pending", v1.ApprovalSource_APPROVAL_SOURCE_HUMAN, 2*time.Hour+789*time.Nanosecond))
			if _, err := e.SignalRun(t.Context(), "owner", &v1.SignalRunRequest{RunId: "r", Name: "close"}); err != nil {
				t.Fatal(err)
			}
			w := mustPoll(t, e)
			clock.Advance(time.Millisecond + 876*time.Nanosecond)
			closed := clock.Now().Truncate(time.Microsecond)
			cmd, kind := completeCmd(), v1.EventType_EVENT_TYPE_RUN_COMPLETED
			if continued {
				cmd = &v1.Command{Attributes: &v1.Command_ContinueAsNew{ContinueAsNew: &v1.ContinueAsNewCommand{}}}
				kind = v1.EventType_EVENT_TYPE_RUN_CONTINUED_AS_NEW
			}
			mustComplete(t, e, w.TaskToken, cmd)
			if err := s.InTx(t.Context(), func(tx store.Tx) error {
				r, err := tx.GetRun("r", false)
				if err != nil {
					return err
				}
				assertMicroTime(t, r.StartedAt, started)
				assertMicroTime(t, r.RunDeadline, started.Add(time.Hour))
				assertMicroTime(t, r.ClosedAt, closed)
				a, err := tx.GetApproval("r", "pending", false)
				if err != nil {
					return err
				}
				assertMicroTime(t, a.RequestedAt, started)
				assertMicroTime(t, a.DueAt, started.Add(2*time.Hour))
				assertMicroTime(t, a.ResolvedAt, closed)
				assertMicroTime(t, a.CheckAt, closed)
				if a.Status != store.ApprovalExpired {
					t.Fatalf("pending approval after closure: %+v", a)
				}
				if continued {
					next, err := tx.GetRun("r~2", false)
					if err != nil {
						return err
					}
					assertMicroTime(t, next.StartedAt, closed)
					assertMicroTime(t, next.RunDeadline, closed.Add(time.Hour))
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			response, err := e.DescribeRun(t.Context(), &v1.DescribeRunRequest{RunId: "r"})
			if err != nil {
				t.Fatal(err)
			}
			assertMicroTime(t, response.Run.StartedAt.AsTime(), started)
			assertMicroTime(t, response.Run.ClosedAt.AsTime(), closed)
			wantTypes(t, history(t, e, "r"),
				v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED,
				v1.EventType_EVENT_TYPE_TASK_STARTED, v1.EventType_EVENT_TYPE_TASK_COMPLETED,
				v1.EventType_EVENT_TYPE_APPROVAL_REQUESTED, v1.EventType_EVENT_TYPE_SIGNAL_RECEIVED,
				v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED,
				v1.EventType_EVENT_TYPE_TASK_COMPLETED, kind)
			if continued {
				wantTypes(t, history(t, e, "r~2"), v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED)
			}
		})
	}
}
