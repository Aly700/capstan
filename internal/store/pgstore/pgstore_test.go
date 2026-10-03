package pgstore_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"github.com/Aly700/capstan/internal/store/pgstore"
	"github.com/Aly700/capstan/internal/store/storetest"
	"github.com/Aly700/capstan/internal/testpg"
	"github.com/jackc/pgx/v5"
)

var at = time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)

func openStore(t *testing.T) (store.Store, string) {
	t.Helper()
	dsn := testpg.New(t)
	s, err := pgstore.Open(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s, dsn
}

func run(id string) *store.Run {
	return &store.Run{RunID: id, WorkflowType: "wf", TaskQueue: "q", Status: capstanv1.RunStatus_RUN_STATUS_RUNNING, TaskTimeout: time.Second, StartedAt: at}
}

func txOK(t *testing.T, s store.Store, fn func(store.Tx) error) {
	t.Helper()
	if err := s.InTx(t.Context(), fn); err != nil {
		t.Fatal(err)
	}
}

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Store { s, _ := openStore(t); return s })
}

func TestClaimTaskConcurrentWorkersClaimOnce(t *testing.T) {
	s, _ := openStore(t)
	var oldest int64
	txOK(t, s, func(tx store.Tx) error {
		if err := tx.InsertRun(run("r")); err != nil {
			return err
		}
		for range 200 {
			task := &store.Task{Kind: store.TaskActivity, RunID: "r", TaskQueue: "q", Attempt: 1, VisibleAt: at, ScheduledAt: at}
			if err := tx.InsertTask(task); err != nil {
				return err
			}
			if oldest == 0 {
				oldest = task.ID
			}
		}
		return nil
	})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	start, locked, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unlock := func() { once.Do(func() { close(release) }) }
	defer unlock()
	type result struct {
		ids []int64
		err error
	}
	results := make(chan result, 32)
	for worker := range 32 {
		go func() {
			<-start
			r := result{}
			for {
				var claimed *store.Task
				err := s.InTx(ctx, func(tx store.Tx) error {
					var err error
					claimed, err = tx.ClaimTask(store.TaskActivity, "q", at, time.Minute, fmt.Sprintf("worker-%d", worker))
					if err != nil || claimed == nil {
						return err
					}
					if claimed.ID == oldest {
						close(locked)
						select {
						case <-release:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					return nil
				})
				if err != nil {
					r.err = err
					break
				}
				if claimed == nil {
					break
				}
				r.ids = append(r.ids, claimed.ID)
			}
			results <- r
		}()
	}
	close(start)
	select {
	case <-locked:
	case <-ctx.Done():
		t.Fatal("oldest task was never claimed")
	}
	seen := map[int64]int{}
	collect := func() {
		t.Helper()
		select {
		case r := <-results:
			if r.err != nil {
				t.Error(r.err)
			}
			for _, id := range r.ids {
				seen[id]++
			}
		case <-ctx.Done():
			t.Fatal("workers waited on the locked oldest task")
		}
	}
	// The other 31 workers must finish all 199 unlocked rows before releasing the
	// oldest row. This fails if claims use FOR UPDATE without SKIP LOCKED.
	for range 31 {
		collect()
	}
	if len(seen) != 199 {
		t.Errorf("while oldest locked, claimed %d tasks, want 199", len(seen))
	}
	unlock()
	collect()
	if len(seen) != 200 {
		t.Fatalf("claimed %d tasks, want 200", len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("task %d claimed %d times", id, n)
		}
	}
}

func TestSubscribeSurvivesListenerReconnect(t *testing.T) {
	s, dsn := openStore(t)
	ch, cancel := s.Subscribe(store.TaskActivity, "q")
	defer cancel()
	other, stop := s.Subscribe(store.TaskWorkflow, "other")
	defer stop()
	conn, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	var pid int32
	if err := conn.QueryRow(t.Context(), `select pid from pg_stat_activity where datname=current_database() and query='listen capstan_tasks' and pid<>pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	var killed bool
	if err := conn.QueryRow(t.Context(), `select pg_terminate_backend($1)`, pid).Scan(&killed); err != nil {
		t.Fatal(err)
	}
	if !killed {
		t.Fatal("listener was not terminated")
	}
	txOK(t, s, func(tx store.Tx) error { tx.Notify(store.TaskActivity, "q"); return nil })
	for _, sub := range []<-chan struct{}{ch, other} {
		select {
		case <-sub:
		case <-time.After(5 * time.Second):
			t.Fatal("subscriber did not wake after reconnect")
		}
	}
	// A second, ordinary notification proves the replacement LISTEN is active.
	txOK(t, s, func(tx store.Tx) error { tx.Notify(store.TaskWorkflow, "other"); return nil })
	select {
	case <-other:
	case <-time.After(2 * time.Second):
		t.Fatal("replacement listener did not deliver notification")
	}
}
