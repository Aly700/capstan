package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type approvalGateFunc func(context.Context, string) (GateApproval, error)

func (f approvalGateFunc) ApprovalStatus(ctx context.Context, id string) (GateApproval, error) {
	return f(ctx, id)
}

func approvalCmd(seq int64, id string, source v1.ApprovalSource, timeout time.Duration, options ...string) *v1.Command {
	c := &v1.RequestApprovalCommand{Seq: seq, ApprovalId: id, Source: source, Prompt: "Choose a direction", Options: options}
	if source == v1.ApprovalSource_APPROVAL_SOURCE_GATE {
		c.GateDecisionId = "decision-1"
	}
	if timeout != 0 {
		c.Timeout = durationpb.New(timeout)
	}
	return &v1.Command{Attributes: &v1.Command_RequestApproval{RequestApproval: c}}
}

func pendingApproval(t *testing.T, e *Engine, source v1.ApprovalSource, timeout time.Duration, options ...string) {
	t.Helper()
	mustStart(t, e, "approval-run")
	task := mustPoll(t, e)
	mustComplete(t, e, task.TaskToken, approvalCmd(1, "approval-1", source, timeout, options...))
}

func readApproval(t *testing.T, s store.Store) *store.Approval {
	t.Helper()
	var a *store.Approval
	if err := s.InTx(context.Background(), func(tx store.Tx) error {
		var err error
		a, err = tx.GetApproval("approval-run", "approval-1", false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return a
}

func approvalResolution(t *testing.T, e *Engine) *v1.ApprovalResolvedAttributes {
	t.Helper()
	h, err := e.GetHistory(context.Background(), &v1.GetHistoryRequest{RunId: "approval-run"})
	if err != nil {
		t.Fatal(err)
	}
	var resolution *v1.ApprovalResolvedAttributes
	for _, ev := range h.Events {
		if a := ev.GetApprovalResolved(); a != nil {
			if resolution != nil {
				t.Fatal("approval resolved more than once")
			}
			resolution = a
		}
	}
	if resolution == nil {
		t.Fatal("missing ApprovalResolved")
	}
	return resolution
}

func TestHumanApprovalResolve(t *testing.T) {
	e, clock, s := newTestEngine(t)
	pendingApproval(t, e, v1.ApprovalSource_APPROVAL_SOURCE_HUMAN, time.Hour, "ship", "hold")
	before := historyBytes(t, e, "approval-run")
	req := &v1.ResolveApprovalRequest{RunId: "approval-run", ApprovalId: "approval-1", Outcome: v1.ApprovalOutcome_APPROVAL_OUTCOME_APPROVED, Choice: "invalid", Resolver: "owner", Note: "reviewed"}
	if _, err := e.ResolveApproval(context.Background(), "caller", req); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("invalid choice: %v", err)
	}
	if got := historyBytes(t, e, "approval-run"); string(got) != string(before) {
		t.Fatal("invalid choice changed history")
	}
	req.Choice = "ship"
	clock.Advance(9876 * time.Nanosecond)
	resolved := clock.Now().Truncate(time.Microsecond)
	if _, err := e.ResolveApproval(context.Background(), "caller", req); err != nil {
		t.Fatal(err)
	}
	a := readApproval(t, s)
	assertMicroTime(t, a.ResolvedAt, resolved)
	if a.Status != store.ApprovalApproved || a.Choice != "ship" || a.Resolver != "owner" || !a.CheckAt.IsZero() {
		t.Fatalf("approval: %+v", a)
	}
	r := approvalResolution(t, e)
	if r.ApprovalId != "approval-1" || r.Seq != 1 || r.RequestedEventId != a.RequestedEventID || r.Outcome != req.Outcome || r.Choice != "ship" || r.Resolver != "owner" || r.Note != "reviewed" {
		t.Fatalf("resolution: %v", r)
	}
	if _, found, err := e.PollWorkflowTask(context.Background(), &v1.PollWorkflowTaskRequest{TaskQueue: "q"}); err != nil || !found {
		t.Fatalf("resolution did not schedule task: found=%v err=%v", found, err)
	}
	if _, err := e.ResolveApproval(context.Background(), "caller", req); !errors.Is(err, ErrFailedPrecondition) {
		t.Fatalf("second resolution: %v", err)
	}
}

func TestHumanApprovalDenialDoesNotRequireChoice(t *testing.T) {
	e, _, _ := newTestEngine(t)
	pendingApproval(t, e, v1.ApprovalSource_APPROVAL_SOURCE_HUMAN, 0, "ship", "hold")
	if _, err := e.ResolveApproval(context.Background(), "owner", &v1.ResolveApprovalRequest{RunId: "approval-run", ApprovalId: "approval-1", Outcome: v1.ApprovalOutcome_APPROVAL_OUTCOME_DENIED}); err != nil {
		t.Fatal(err)
	}
	if got := approvalResolution(t, e); got.Outcome != v1.ApprovalOutcome_APPROVAL_OUTCOME_DENIED || got.Resolver != "owner" {
		t.Fatalf("denial: %v", got)
	}
}

func TestResolveApprovalRejectsMissingAndInvalidOutcome(t *testing.T) {
	e, _, _ := newTestEngine(t)
	if _, err := e.ResolveApproval(context.Background(), "owner", &v1.ResolveApprovalRequest{RunId: "missing", ApprovalId: "missing", Outcome: v1.ApprovalOutcome_APPROVAL_OUTCOME_APPROVED}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	pendingApproval(t, e, v1.ApprovalSource_APPROVAL_SOURCE_HUMAN, 0)
	for _, outcome := range []v1.ApprovalOutcome{v1.ApprovalOutcome_APPROVAL_OUTCOME_UNSPECIFIED, v1.ApprovalOutcome_APPROVAL_OUTCOME_EXPIRED, 99} {
		if _, err := e.ResolveApproval(context.Background(), "owner", &v1.ResolveApprovalRequest{RunId: "approval-run", ApprovalId: "approval-1", Outcome: outcome}); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("outcome %v: %v", outcome, err)
		}
	}
}

func TestGateApprovalCannotBeResolvedInCapstan(t *testing.T) {
	e, _, _ := newTestEngine(t)
	pendingApproval(t, e, v1.ApprovalSource_APPROVAL_SOURCE_GATE, 0)
	if _, err := e.ResolveApproval(context.Background(), "owner", &v1.ResolveApprovalRequest{RunId: "approval-run", ApprovalId: "approval-1", Outcome: v1.ApprovalOutcome_APPROVAL_OUTCOME_APPROVED}); !errors.Is(err, ErrFailedPrecondition) {
		t.Fatalf("Gate resolution: %v", err)
	}
}

func TestApprovalTimeoutExpires(t *testing.T) {
	for _, source := range []v1.ApprovalSource{v1.ApprovalSource_APPROVAL_SOURCE_HUMAN, v1.ApprovalSource_APPROVAL_SOURCE_GATE} {
		t.Run(source.String(), func(t *testing.T) {
			e, clock, s := newTestEngine(t)
			e.deps.Gate = approvalGateFunc(func(context.Context, string) (GateApproval, error) {
				t.Fatal("expired approval polled Gate")
				return GateApproval{}, nil
			})
			pendingApproval(t, e, source, time.Second)
			if n, err := e.ProcessDueApprovals(context.Background(), 10); err != nil || n != 0 {
				t.Fatalf("early: n=%d err=%v", n, err)
			}
			clock.Advance(time.Second)
			if n, err := e.ProcessDueApprovals(context.Background(), 10); err != nil || n != 1 {
				t.Fatalf("expiry: n=%d err=%v", n, err)
			}
			if got := approvalResolution(t, e); got.Outcome != v1.ApprovalOutcome_APPROVAL_OUTCOME_EXPIRED || got.Resolver != "timeout" {
				t.Fatalf("expiry event: %v", got)
			}
			if got := readApproval(t, s); got.Status != store.ApprovalExpired || !got.ResolvedAt.Equal(clock.Now()) {
				t.Fatalf("expired approval: %+v", got)
			}
			if n, err := e.ProcessDueApprovals(context.Background(), 10); err != nil || n != 0 {
				t.Fatalf("duplicate: n=%d err=%v", n, err)
			}
		})
	}
}

func TestGateApprovalWatcherResolves(t *testing.T) {
	e, clock, s := newTestEngine(t)
	calls := 0
	e.deps.Gate = approvalGateFunc(func(_ context.Context, id string) (GateApproval, error) {
		calls++
		if id != "approval-1" {
			t.Fatalf("polled id %q", id)
		}
		if calls <= 2 {
			return GateApproval{Status: "PENDING"}, nil
		}
		return GateApproval{Status: "APPROVED", DecidedBy: "gate-owner"}, nil
	})
	pendingApproval(t, e, v1.ApprovalSource_APPROVAL_SOURCE_GATE, time.Hour)
	for i, delay := range []time.Duration{15 * time.Second, 30 * time.Second, 60 * time.Second} {
		if got := readApproval(t, s); !got.CheckAt.Equal(clock.Now().Add(delay)) {
			t.Fatalf("poll %d check=%v want=%v", i, got.CheckAt, clock.Now().Add(delay))
		}
		clock.Advance(delay)
		if n, err := e.ProcessDueApprovals(context.Background(), 10); err != nil || n != 1 {
			t.Fatalf("poll %d: n=%d err=%v", i, n, err)
		}
	}
	if got := approvalResolution(t, e); got.Outcome != v1.ApprovalOutcome_APPROVAL_OUTCOME_APPROVED || got.Resolver != "gate-owner" {
		t.Fatalf("Gate resolution: %v", got)
	}
	if calls != 3 {
		t.Fatalf("calls=%d", calls)
	}
}

func TestGateErrorsBackOff(t *testing.T) {
	e, clock, s := newTestEngine(t)
	e.deps.Gate = approvalGateFunc(func(context.Context, string) (GateApproval, error) {
		return GateApproval{}, errors.New("Gate unavailable")
	})
	pendingApproval(t, e, v1.ApprovalSource_APPROVAL_SOURCE_GATE, time.Hour)
	for _, delay := range []time.Duration{15, 30, 60, 120, 240, 300, 300} {
		clock.Advance(delay * time.Second)
		if n, err := e.ProcessDueApprovals(context.Background(), 1); n != 1 || err != nil {
			t.Fatalf("poll: %d %v", n, err)
		}
		a := readApproval(t, s)
		next := min(delay*2, 300) * time.Second
		if a.Status != store.ApprovalPending || !a.CheckAt.Equal(clock.Now().Add(next)) {
			t.Fatalf("backoff: %+v next=%v", a, next)
		}
	}
}

func TestGateCalledOutsideTransaction(t *testing.T) {
	e, clock, s := newTestEngine(t)
	e.deps.Gate = approvalGateFunc(func(ctx context.Context, _ string) (GateApproval, error) {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		err := s.InTx(ctx, func(tx store.Tx) error {
			a, err := tx.GetApproval("approval-run", "approval-1", true)
			if err != nil {
				return err
			}
			if a.GatePolls != 1 || !a.CheckAt.Equal(clock.Now().Add(time.Minute)) {
				t.Errorf("poll lease not committed: %+v", a)
			}
			return nil
		})
		if err != nil {
			t.Errorf("Gate could not reenter store: %v", err)
		}
		return GateApproval{Status: "DENIED", DecidedBy: "gate-owner"}, err
	})
	pendingApproval(t, e, v1.ApprovalSource_APPROVAL_SOURCE_GATE, time.Hour)
	clock.Advance(15 * time.Second)
	if n, err := e.ProcessDueApprovals(context.Background(), 1); err != nil || n != 1 {
		t.Fatalf("poll: %d %v", n, err)
	}
	if got := approvalResolution(t, e); got.Outcome != v1.ApprovalOutcome_APPROVAL_OUTCOME_DENIED {
		t.Fatalf("resolution: %v", got)
	}
}

func TestGatePollingNilClientDoesNotSpin(t *testing.T) {
	for _, timeout := range []time.Duration{0, time.Hour} {
		t.Run(timeout.String(), func(t *testing.T) {
			e, clock, s := newTestEngine(t)
			pendingApproval(t, e, v1.ApprovalSource_APPROVAL_SOURCE_GATE, timeout)
			clock.Advance(15 * time.Second)
			if n, err := e.ProcessDueApprovals(context.Background(), 100); err != nil || n > 1 {
				t.Fatalf("nil Gate: n=%d err=%v", n, err)
			}
			a := readApproval(t, s)
			if a.GatePolls != 0 || !a.CheckAt.Equal(a.DueAt) {
				t.Fatalf("disabled poll: %+v", a)
			}
			if n, err := e.ProcessDueApprovals(context.Background(), 100); err != nil || n != 0 {
				t.Fatalf("repeated nil Gate: n=%d err=%v", n, err)
			}
		})
	}
}

func TestApprovalResolutionWaitsForInFlightCommands(t *testing.T) {
	e, _, _ := newTestEngine(t)
	pendingApproval(t, e, v1.ApprovalSource_APPROVAL_SOURCE_HUMAN, 0)
	if _, err := e.SignalRun(context.Background(), "caller", &v1.SignalRunRequest{RunId: "approval-run", Name: "wake"}); err != nil {
		t.Fatal(err)
	}
	task := mustPoll(t, e)
	before := historyBytes(t, e, "approval-run")
	if _, err := e.ResolveApproval(context.Background(), "owner", &v1.ResolveApprovalRequest{RunId: "approval-run", ApprovalId: "approval-1", Outcome: v1.ApprovalOutcome_APPROVAL_OUTCOME_APPROVED}); err != nil {
		t.Fatal(err)
	}
	if got := historyBytes(t, e, "approval-run"); string(got) != string(before) {
		t.Fatal("in-flight resolution changed history")
	}
	mustComplete(t, e, task.TaskToken, markerCmd(2))
	wantTypes(t, history(t, e, "approval-run"),
		v1.EventType_EVENT_TYPE_RUN_STARTED,
		v1.EventType_EVENT_TYPE_TASK_SCHEDULED,
		v1.EventType_EVENT_TYPE_TASK_STARTED,
		v1.EventType_EVENT_TYPE_TASK_COMPLETED,
		v1.EventType_EVENT_TYPE_APPROVAL_REQUESTED,
		v1.EventType_EVENT_TYPE_SIGNAL_RECEIVED,
		v1.EventType_EVENT_TYPE_TASK_SCHEDULED,
		v1.EventType_EVENT_TYPE_TASK_STARTED,
		v1.EventType_EVENT_TYPE_TASK_COMPLETED,
		v1.EventType_EVENT_TYPE_MARKER_RECORDED,
		v1.EventType_EVENT_TYPE_APPROVAL_RESOLVED,
		v1.EventType_EVENT_TYPE_TASK_SCHEDULED,
	)
}

// These seeded tests isolate the public transitions from workflow command handling.
func seedApproval(t *testing.T, e *Engine, source v1.ApprovalSource) {
	t.Helper()
	now := e.deps.Clock.Now()
	if err := e.deps.Store.InTx(context.Background(), func(tx store.Tx) error {
		r := &store.Run{RunID: "approval-run", WorkflowType: "wf", TaskQueue: "q", Status: v1.RunStatus_RUN_STATUS_RUNNING, StartedAt: now, TaskTimeout: time.Second}
		if err := tx.InsertRun(r); err != nil {
			return err
		}
		a := &v1.ApprovalRequestedAttributes{Seq: 1, ApprovalId: "approval-1", Source: source, Options: []string{"ship", "hold"}}
		if err := tx.AppendEvents(r.RunID, []*v1.HistoryEvent{{EventId: 1, Time: timestamppb.New(now), Type: v1.EventType_EVENT_TYPE_APPROVAL_REQUESTED, Attributes: &v1.HistoryEvent_ApprovalRequested{ApprovalRequested: a}}}); err != nil {
			return err
		}
		r.LastEventID = 1
		if err := tx.UpdateRun(r); err != nil {
			return err
		}
		return tx.InsertApproval(&store.Approval{RunID: r.RunID, ApprovalID: "approval-1", Seq: 1, RequestedEventID: 1, Source: source, Status: store.ApprovalPending, RequestedAt: now, DueAt: now.Add(time.Hour), CheckAt: now})
	}); err != nil {
		t.Fatal(err)
	}
}

func TestGateReplyAfterDeadlineExpires(t *testing.T) {
	e, clock, s := newTestEngine(t)
	seedApproval(t, e, v1.ApprovalSource_APPROVAL_SOURCE_GATE)
	e.deps.Gate = approvalGateFunc(func(context.Context, string) (GateApproval, error) {
		clock.Advance(time.Hour)
		return GateApproval{Status: "APPROVED", DecidedBy: "gate-owner"}, nil
	})
	if n, err := e.ProcessDueApprovals(context.Background(), 1); n != 1 || err != nil {
		t.Fatalf("poll: %d %v", n, err)
	}
	if got := readApproval(t, s); got.Status != store.ApprovalExpired || got.Resolver != "timeout" {
		t.Fatalf("late reply resolution: %+v", got)
	}
}

func TestGateBackoffStopsAtApprovalDeadline(t *testing.T) {
	e, clock, s := newTestEngine(t)
	e.deps.Gate = approvalGateFunc(func(context.Context, string) (GateApproval, error) { return GateApproval{}, errors.New("offline") })
	pendingApproval(t, e, v1.ApprovalSource_APPROVAL_SOURCE_GATE, 20*time.Second)
	clock.Advance(15 * time.Second)
	if n, err := e.ProcessDueApprovals(context.Background(), 10); err != nil || n != 1 {
		t.Fatalf("poll: %d %v", n, err)
	}
	a := readApproval(t, s)
	if !a.CheckAt.Equal(a.DueAt) {
		t.Fatalf("backoff exceeded expiry: %+v", a)
	}
	clock.Advance(5 * time.Second)
	if n, err := e.ProcessDueApprovals(context.Background(), 10); err != nil || n != 1 {
		t.Fatalf("expiry: %d %v", n, err)
	}
	if got := readApproval(t, s); got.Status != store.ApprovalExpired || got.GatePolls != 1 {
		t.Fatalf("expiry: %+v", got)
	}
}

func TestApprovalResolutionOnBlockedRunDoesNotSchedule(t *testing.T) {
	e, _, s := newTestEngine(t)
	seedApproval(t, e, v1.ApprovalSource_APPROVAL_SOURCE_HUMAN)
	if err := s.InTx(context.Background(), func(tx store.Tx) error {
		r, err := tx.GetRun("approval-run", true)
		if err != nil {
			return err
		}
		r.Status = v1.RunStatus_RUN_STATUS_BLOCKED
		return tx.UpdateRun(r)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ResolveApproval(context.Background(), "owner", &v1.ResolveApprovalRequest{RunId: "approval-run", ApprovalId: "approval-1", Outcome: v1.ApprovalOutcome_APPROVAL_OUTCOME_APPROVED, Choice: "ship"}); err != nil {
		t.Fatal(err)
	}
	if err := s.InTx(context.Background(), func(tx store.Tx) error {
		r, err := tx.GetRun("approval-run", false)
		if err != nil {
			return err
		}
		h, err := tx.ReadHistory(r.RunID, 0, 0)
		if err != nil {
			return err
		}
		tasks, err := tx.RunTasks(r.RunID)
		if err != nil {
			return err
		}
		if r.Status != v1.RunStatus_RUN_STATUS_BLOCKED || len(tasks) != 0 || len(h) != 2 || h[1].Type != v1.EventType_EVENT_TYPE_APPROVAL_RESOLVED {
			t.Errorf("blocked resolution: run=%+v tasks=%v history=%v", r, tasks, h)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestGateTerminalStatuses(t *testing.T) {
	for status, want := range map[string]store.ApprovalStatus{"APPROVED": store.ApprovalApproved, "DENIED": store.ApprovalDenied, "EXPIRED": store.ApprovalExpired} {
		t.Run(status, func(t *testing.T) {
			e, _, s := newTestEngine(t)
			seedApproval(t, e, v1.ApprovalSource_APPROVAL_SOURCE_GATE)
			e.deps.Gate = approvalGateFunc(func(context.Context, string) (GateApproval, error) {
				return GateApproval{Status: status, DecidedBy: "gate-owner"}, nil
			})
			if n, err := e.ProcessDueApprovals(context.Background(), 1); err != nil || n != 1 {
				t.Fatalf("poll: %d %v", n, err)
			}
			if got := readApproval(t, s); got.Status != want || got.Resolver != "gate-owner" {
				t.Fatalf("resolution: %+v", got)
			}
		})
	}
}

func TestHumanApprovalAfterDeadlineIsRejectedUntilExpiry(t *testing.T) {
	e, clock, s := newTestEngine(t)
	pendingApproval(t, e, v1.ApprovalSource_APPROVAL_SOURCE_HUMAN, time.Second)
	clock.Advance(time.Second)
	before := historyBytes(t, e, "approval-run")
	if _, err := e.ResolveApproval(context.Background(), "owner", &v1.ResolveApprovalRequest{RunId: "approval-run", ApprovalId: "approval-1", Outcome: v1.ApprovalOutcome_APPROVAL_OUTCOME_APPROVED}); !errors.Is(err, ErrFailedPrecondition) {
		t.Fatalf("late human decision: %v", err)
	}
	if got := historyBytes(t, e, "approval-run"); string(got) != string(before) {
		t.Fatal("late resolution changed history")
	}
	if got := readApproval(t, s); got.Status != store.ApprovalPending || !got.ResolvedAt.IsZero() {
		t.Fatalf("late resolution changed approval: %+v", got)
	}
	if n, err := e.ProcessDueApprovals(context.Background(), 1); n != 1 || err != nil {
		t.Fatalf("expiry: %d %v", n, err)
	}
	if got := approvalResolution(t, e); got.Outcome != v1.ApprovalOutcome_APPROVAL_OUTCOME_EXPIRED || got.Resolver != "timeout" {
		t.Fatalf("expiry: %v", got)
	}
}

func TestOlderGateReplyCannotOverwriteNewerPoll(t *testing.T) {
	for _, oldStatus := range []string{"PENDING", "APPROVED"} {
		t.Run(oldStatus, func(t *testing.T) {
			e, clock, s := newTestEngine(t)
			seedApproval(t, e, v1.ApprovalSource_APPROVAL_SOURCE_GATE)
			calls := 0
			var newerCheck time.Time
			e.deps.Gate = approvalGateFunc(func(ctx context.Context, _ string) (GateApproval, error) {
				calls++
				if calls == 1 {
					clock.Advance(time.Minute)
					if n, err := e.ProcessDueApprovals(ctx, 1); n != 1 || err != nil {
						t.Fatalf("newer poll: %d %v", n, err)
					}
					newerCheck = readApproval(t, s).CheckAt
					clock.Advance(10 * time.Second)
					return GateApproval{Status: oldStatus, DecidedBy: "old-response"}, nil
				}
				return GateApproval{Status: "PENDING"}, nil
			})
			if n, err := e.ProcessDueApprovals(context.Background(), 1); n != 1 || err != nil {
				t.Fatalf("original poll: %d %v", n, err)
			}
			if got := readApproval(t, s); got.Status != store.ApprovalPending || got.GatePolls != 2 || !got.CheckAt.Equal(newerCheck) {
				t.Fatalf("old response overwrote newer poll: %+v want CheckAt=%v", got, newerCheck)
			}
		})
	}
}

func TestApprovalResolutionRollsBackOnAppendFailure(t *testing.T) {
	e, _, s := newTestEngine(t)
	seedApproval(t, e, v1.ApprovalSource_APPROVAL_SOURCE_HUMAN)
	before := historyBytes(t, e, "approval-run")
	sentinel := errors.New("injected approval append failure")
	observed := &observedStore{Store: s, failAppend: 1, failure: sentinel}
	e.deps.Store = observed
	req := &v1.ResolveApprovalRequest{RunId: "approval-run", ApprovalId: "approval-1", Outcome: v1.ApprovalOutcome_APPROVAL_OUTCOME_APPROVED, Choice: "ship"}
	if _, err := e.ResolveApproval(context.Background(), "owner", req); !errors.Is(err, sentinel) {
		t.Fatalf("resolution fault: %v", err)
	}
	if observed.transactions != 1 {
		t.Fatalf("resolution transactions=%d", observed.transactions)
	}
	if got := readApproval(t, s); got.Status != store.ApprovalPending || !got.ResolvedAt.IsZero() {
		t.Fatalf("decision survived rollback: %+v", got)
	}
	if got := historyBytes(t, e, "approval-run"); string(got) != string(before) {
		t.Fatal("history survived rollback")
	}
	if _, err := e.ResolveApproval(context.Background(), "owner", req); err != nil {
		t.Fatal(err)
	}
}

func TestGateResolutionRollbackPreservesPollLease(t *testing.T) {
	e, clock, s := newTestEngine(t)
	seedApproval(t, e, v1.ApprovalSource_APPROVAL_SOURCE_GATE)
	e.deps.Gate = approvalGateFunc(func(context.Context, string) (GateApproval, error) {
		return GateApproval{Status: "APPROVED", DecidedBy: "gate-owner"}, nil
	})
	sentinel := errors.New("injected Gate resolution append failure")
	observed := &observedStore{Store: s, failAppend: 1, failure: sentinel}
	e.deps.Store = observed
	if n, err := e.ProcessDueApprovals(context.Background(), 1); n != 0 || !errors.Is(err, sentinel) {
		t.Fatalf("resolution fault: %d %v", n, err)
	}
	if observed.transactions != 2 {
		t.Fatalf("Gate transactions=%d", observed.transactions)
	}
	if got := readApproval(t, s); got.Status != store.ApprovalPending || got.GatePolls != 1 || !got.CheckAt.Equal(clock.Now().Add(time.Minute)) {
		t.Fatalf("lease after resolution rollback: %+v", got)
	}
	clock.Advance(time.Minute)
	if n, err := e.ProcessDueApprovals(context.Background(), 1); n != 1 || err != nil {
		t.Fatalf("retry: %d %v", n, err)
	}
	if got := readApproval(t, s); got.Status != store.ApprovalApproved {
		t.Fatalf("retried decision: %+v", got)
	}
}
