//go:build pgengine

package engine

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/types/known/durationpb"
)

// raceWithRunClosure holds the closing transaction's run lock until the competing
// operation has locked its child row and reached GetRun. This exercises PostgreSQL's
// actual deadlock handling, rather than relying on goroutine timing to find the race.
func raceWithRunClosure(t *testing.T, e *Engine, closeRun, work func(context.Context, *Engine) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	locked, reached, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var unlock sync.Once
	defer unlock.Do(func() { close(release) })
	var closeOnce, workOnce sync.Once
	var deadlocks atomic.Int32
	closing, working := *e, *e
	closing.deps.Store = &runRaceStore{Store: e.deps.Store, deadlocks: &deadlocks, after: func() error {
		closeOnce.Do(func() { close(locked) })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	working.deps.Store = &runRaceStore{Store: e.deps.Store, deadlocks: &deadlocks, before: func() {
		workOnce.Do(func() { close(reached) })
	}}
	closeResult, workResult := make(chan error, 1), make(chan error, 1)
	go func() { closeResult <- closeRun(ctx, &closing) }()
	select {
	case <-locked:
	case err := <-closeResult:
		t.Fatalf("closure did not lock the run: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	go func() { workResult <- work(ctx, &working) }()
	select {
	case <-reached:
	case err := <-workResult:
		t.Fatalf("competitor did not reach the run: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	unlock.Do(func() { close(release) })
	for _, result := range []<-chan error{closeResult, workResult} {
		select {
		case err := <-result:
			if err != nil {
				t.Error(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	t.Logf("PostgreSQL deadlocks encountered: %d", deadlocks.Load())
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
			// A deadlock victim's retry may SKIP LOCKED past the winning operation.
			// The next sweep must close it once both racing transactions have ended.
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
