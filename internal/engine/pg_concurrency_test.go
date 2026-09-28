//go:build pgengine

package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// raceWithRunClosure holds the first transaction's run lock until the other reaches
// GetRun, or the competing scan skips the locked run. Channels control both
// serialization orders; neither path may need PostgreSQL deadlock recovery.
func raceWithRunClosure(t *testing.T, e *Engine, first, second func(context.Context, *Engine) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	locked, reached, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var unlock sync.Once
	defer unlock.Do(func() { close(release) })
	var firstOnce, secondOnce sync.Once
	var deadlocks atomic.Int32
	firstEngine, secondEngine := *e, *e
	firstEngine.deps.Store = &runRaceStore{Store: e.deps.Store, deadlocks: &deadlocks, after: func() error {
		firstOnce.Do(func() { close(locked) })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	secondEngine.deps.Store = &runRaceStore{Store: e.deps.Store, deadlocks: &deadlocks, before: func() {
		secondOnce.Do(func() { close(reached) })
	}}
	firstResult, secondResult := make(chan error, 1), make(chan error, 1)
	go func() { firstResult <- first(ctx, &firstEngine) }()
	select {
	case <-locked:
	case err := <-firstResult:
		t.Fatalf("first operation did not lock the run: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	go func() { secondResult <- second(ctx, &secondEngine) }()
	results := []<-chan error{firstResult}
	select {
	case <-reached:
		results = append(results, secondResult)
	case err := <-secondResult:
		if err != nil {
			t.Fatalf("competing operation: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	unlock.Do(func() { close(release) })
	for _, result := range results {
		select {
		case err := <-result:
			if err != nil {
				t.Error(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if n := deadlocks.Load(); n != 0 {
		t.Fatalf("PostgreSQL deadlock retries = %d, want zero", n)
	}
}

func TestPostgresTerminateRunRaces(t *testing.T) {
	for _, kind := range []string{"workflow_completion", "timer_fire"} {
		for _, first := range []string{"terminate", "work"} {
			t.Run(kind+"/"+first+"_locks_run_first", func(t *testing.T) {
				e, clock, _ := newTestEngine(t)
				mustStart(t, e, "r")
				mustComplete(t, e, mustPoll(t, e).TaskToken, timerCmd(1, time.Second),
					approvalCmd(2, "pending", v1.ApprovalSource_APPROVAL_SOURCE_HUMAN, time.Hour), activityCmd(3))
				if _, err := e.SignalRun(t.Context(), "owner", &v1.SignalRunRequest{RunId: "r", Name: "wake"}); err != nil {
					t.Fatal(err)
				}
				w := mustPoll(t, e)
				if _, err := e.SignalRun(t.Context(), "owner", &v1.SignalRunRequest{RunId: "r", Name: "keep"}); err != nil {
					t.Fatal(err)
				}
				prefix := historyEvents(t, e, "r")
				clock.Advance(time.Second)
				terminate := func(ctx context.Context, e *Engine) error {
					_, err := e.TerminateRun(ctx, "owner", &v1.TerminateRunRequest{RunId: "r", Reason: "race"})
					return err
				}
				work := func(ctx context.Context, e *Engine) error {
					if kind == "timer_fire" {
						n, err := e.FireDueTimers(ctx, 1)
						if err == nil && first == "work" && n != 1 {
							return fmt.Errorf("timer that locked first processed %d items", n)
						}
						return err
					}
					_, err := e.CompleteWorkflowTask(ctx, &v1.CompleteWorkflowTaskRequest{TaskToken: w.TaskToken, Commands: []*v1.Command{markerCmd(4)}})
					if first == "terminate" && errors.Is(err, ErrStaleTask) {
						return nil
					}
					return err
				}
				if first == "terminate" {
					raceWithRunClosure(t, e, terminate, work)
				} else {
					raceWithRunClosure(t, e, work, terminate)
				}
				assertTerminated(t, e, "r", "race")
				h := historyEvents(t, e, "r")
				kept, effects := 0, 0
				for i, event := range h {
					if i < len(prefix) && !proto.Equal(event, prefix[i]) {
						t.Fatal("race changed the committed history prefix")
					}
					if event.GetSignalReceived().GetName() == "keep" {
						kept++
					}
					if kind == "timer_fire" && event.GetTimerFired().GetSeq() == 1 || kind == "workflow_completion" && event.GetMarkerRecorded().GetSeq() == 4 {
						effects++
					}
				}
				if kept != 1 {
					t.Fatalf("inbox signal count after racing termination: %d", kept)
				}
				if effects > 1 || first == "work" && effects != 1 {
					t.Fatalf("racing %s events before termination: %d", kind, effects)
				}
				before := historyBytes(t, e, "r")
				if _, err := e.CompleteWorkflowTask(t.Context(), &v1.CompleteWorkflowTaskRequest{TaskToken: w.TaskToken, Commands: []*v1.Command{completeCmd()}}); !errors.Is(err, ErrStaleTask) {
					t.Fatalf("late completion: %v", err)
				}
				clock.Advance(2 * time.Hour)
				if n, err := e.FireDueTimers(t.Context(), 10); n != 0 || err != nil {
					t.Fatalf("late timer: %d %v", n, err)
				}
				if !bytes.Equal(before, historyBytes(t, e, "r")) {
					t.Fatal("history changed after racing termination")
				}
			})
		}
	}
}

type runRaceStore struct {
	store.Store
	before    func()
	after     func() error
	deadlocks *atomic.Int32
}

func (s *runRaceStore) InTx(ctx context.Context, fn func(store.Tx) error) error {
	err := s.Store.InTx(ctx, func(tx store.Tx) error {
		return fn(&runRaceTx{Tx: tx, owner: s})
	})
	var state interface{ SQLState() string }
	if errors.As(err, &state) && state.SQLState() == "40P01" {
		s.deadlocks.Add(1)
	}
	return err
}

type runRaceTx struct {
	store.Tx
	owner *runRaceStore
}

func (tx *runRaceTx) GetRun(id string, update bool) (*store.Run, error) {
	if update && tx.owner.before != nil {
		tx.owner.before()
	}
	r, err := tx.Tx.GetRun(id, update)
	if err == nil && update && tx.owner.after != nil {
		err = tx.owner.after()
	}
	return r, err
}

func TestPostgresRunTimeoutRaces(t *testing.T) {
	for _, kind := range []string{"workflow_completion", "timer_fire"} {
		t.Run(kind, func(t *testing.T) {
			e, clock, _ := newTestEngine(t)
			if _, err := e.StartRun(t.Context(), "owner", &v1.StartRunRequest{RunId: "r", WorkflowType: "flow", TaskQueue: "q", RunTimeout: durationpb.New(time.Second)}); err != nil {
				t.Fatal(err)
			}
			w := mustPoll(t, e)
			if kind == "timer_fire" {
				mustComplete(t, e, w.TaskToken, timerCmd(1, time.Second))
			}
			clock.Advance(time.Second)
			raceWithRunClosure(t, e, func(ctx context.Context, e *Engine) error {
				_, err := e.TimeoutRuns(ctx, 1)
				return err
			}, func(ctx context.Context, e *Engine) error {
				if kind == "timer_fire" {
					_, err := e.FireDueTimers(ctx, 1)
					return err
				}
				_, err := e.CompleteWorkflowTask(ctx, &v1.CompleteWorkflowTaskRequest{TaskToken: w.TaskToken, Commands: []*v1.Command{markerCmd(1)}})
				if errors.Is(err, ErrStaleTask) {
					return nil
				}
				return err
			})
			// A sweep can SKIP LOCKED past the winning operation. The next sweep
			// must close it once both racing transactions have ended.
			if _, err := e.TimeoutRuns(t.Context(), 1); err != nil {
				t.Fatal(err)
			}
			h := historyEvents(t, e, "r")
			if h[len(h)-1].Type != v1.EventType_EVENT_TYPE_RUN_TIMED_OUT {
				t.Fatalf("last event = %v", h[len(h)-1])
			}
			before := historyBytes(t, e, "r")
			if _, err := e.FireDueTimers(t.Context(), 10); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, historyBytes(t, e, "r")) {
				t.Fatal("history changed after run timeout")
			}
		})
	}
}
