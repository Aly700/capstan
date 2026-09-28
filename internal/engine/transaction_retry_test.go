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

type transactionFailure struct{ state string }

func (f transactionFailure) Error() string    { return "injected transaction failure: " + f.state }
func (f transactionFailure) SQLState() string { return f.state }

type failedCommitStore struct {
	store.Store
	failure      error
	remaining    int
	attempts     int
	afterFailure func()
}

func (s *failedCommitStore) InTx(ctx context.Context, fn func(store.Tx) error) error {
	s.attempts++
	err := s.Store.InTx(ctx, func(tx store.Tx) error {
		if err := fn(tx); err != nil {
			return err
		}
		if s.remaining > 0 {
			s.remaining--
			return s.failure
		}
		return nil
	})
	if err != nil && s.afterFailure != nil {
		s.afterFailure()
	}
	return err
}

func TestDeadlockRetryResetsStartResponse(t *testing.T) {
	e, _, s := newTestEngine(t)
	other := *e
	faults := &failedCommitStore{Store: s, failure: transactionFailure{"40P01"}, remaining: 1}
	faults.afterFailure = func() { mustStart(t, &other, "r") }
	e.deps.Store = faults
	if response := mustStart(t, e, "r"); response.Started {
		t.Fatal("rolled-back start was reported as the committed creator")
	}
	wantTypes(t, history(t, e, "r"), v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED)
}

func TestDeadlockRetryDoesNotCountRolledBackBackgroundWork(t *testing.T) {
	for _, kind := range []string{"tasks", "timers", "approvals", "runs"} {
		t.Run(kind, func(t *testing.T) {
			e, clock, s := newTestEngine(t)
			if _, err := e.StartRun(t.Context(), "", &v1.StartRunRequest{RunId: "r", WorkflowType: "flow", TaskQueue: "q", RunTimeout: durationpb.New(time.Second)}); err != nil {
				t.Fatal(err)
			}
			a := activityCmd(1)
			a.GetScheduleActivity().ScheduleToStartTimeout = durationpb.New(time.Second)
			mustComplete(t, e, mustPoll(t, e).TaskToken, a, timerCmd(2, time.Second), approvalCmd(3, "a", v1.ApprovalSource_APPROVAL_SOURCE_HUMAN, time.Second))
			if _, err := e.SignalRun(t.Context(), "", &v1.SignalRunRequest{RunId: "r", Name: "close"}); err != nil {
				t.Fatal(err)
			}
			w := mustPoll(t, e)
			clock.Advance(time.Second)
			other := *e
			faults := &failedCommitStore{Store: s, failure: transactionFailure{"40P01"}, remaining: 1}
			faults.afterFailure = func() { mustComplete(t, &other, w.TaskToken, completeCmd()) }
			e.deps.Store = faults
			process := map[string]func(context.Context, int) (int, error){"tasks": e.ProcessDueTasks, "timers": e.FireDueTimers, "approvals": e.ProcessDueApprovals, "runs": e.TimeoutRuns}[kind]
			if n, err := process(t.Context(), 1); n != 0 || err != nil {
				t.Fatalf("rolled-back %s counted: %d, %v", kind, n, err)
			}
			h := history(t, e, "r")
			if h[len(h)-1] != v1.EventType_EVENT_TYPE_RUN_COMPLETED {
				t.Fatalf("last event = %v", h[len(h)-1])
			}
		})
	}
}

func TestDeadlockRetryHonorsCancellation(t *testing.T) {
	e, _, s := newTestEngine(t)
	mustStart(t, e, "r")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	faults := &failedCommitStore{Store: s, failure: transactionFailure{"40P01"}, remaining: 100, afterFailure: cancel}
	e.deps.Store = faults
	_, err := e.SignalRun(ctx, "", &v1.SignalRunRequest{RunId: "r", Name: "signal"})
	if !errors.Is(err, context.Canceled) || faults.attempts != 1 {
		t.Fatalf("canceled retry: attempts=%d err=%v", faults.attempts, err)
	}
}

func TestDeadlockRetryDoesNotDuplicateSignalOrDedupeKey(t *testing.T) {
	e, _, s := newTestEngine(t)
	mustStart(t, e, "r")
	faults := &failedCommitStore{Store: s, failure: transactionFailure{"40P01"}, remaining: 1}
	e.deps.Store = faults
	request := &v1.SignalRunRequest{RunId: "r", Name: "once", RequestId: "once"}
	for range 2 {
		if _, err := e.SignalRun(t.Context(), "owner", request); err != nil {
			t.Fatal(err)
		}
	}
	wantTypes(t, history(t, e, "r"), v1.EventType_EVENT_TYPE_RUN_STARTED,
		v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_SIGNAL_RECEIVED)
}

func TestTransactionRetryIsBoundedAndOnlyRetriesDeadlocks(t *testing.T) {
	for _, tc := range []struct {
		name    string
		failure error
		retry   bool
	}{
		{"deadlock", transactionFailure{"40P01"}, true},
		{"serialization", transactionFailure{"40001"}, false},
		{"connection", errors.New("connection lost; commit outcome unknown"), false},
		{"conflict", store.ErrConflict, false},
		{"business", ErrRunClosed, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, _, s := newTestEngine(t)
			mustStart(t, e, "r")
			before := historyBytes(t, e, "r")
			faults := &failedCommitStore{Store: s, failure: tc.failure, remaining: 100}
			e.deps.Store = faults
			_, err := e.SignalRun(t.Context(), "", &v1.SignalRunRequest{RunId: "r", Name: "signal"})
			if !errors.Is(err, tc.failure) {
				t.Fatalf("failure = %v, want %v", err, tc.failure)
			}
			if tc.retry {
				if faults.attempts < 2 || faults.attempts > 5 {
					t.Fatalf("retry must be bounded: attempts=%d", faults.attempts)
				}
			} else if faults.attempts != 1 {
				t.Fatalf("unsafe retry: attempts=%d", faults.attempts)
			}
			e.deps.Store = s
			if !bytes.Equal(before, historyBytes(t, e, "r")) {
				t.Fatal("failed transaction changed history")
			}
		})
	}
}
