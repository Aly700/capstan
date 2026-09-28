package pgstore_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"github.com/Aly700/capstan/internal/store/pgstore"
	"github.com/Aly700/capstan/internal/testpg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func holdTransaction(t *testing.T, s store.Store, fn func(store.Tx) error) func() {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	ready, done, release := make(chan error, 1), make(chan error, 1), make(chan struct{})
	var once sync.Once
	unlock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() {
		unlock()
		// Let the released transaction commit before cancelling: cancelling first can interrupt
		// the commit mid-write, which pgx reports as an i/o timeout. The 10 s context bounds the wait.
		err := <-done
		cancel()
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	})
	go func() {
		done <- s.InTx(ctx, func(tx store.Tx) error {
			err := fn(tx)
			ready <- err
			if err != nil {
				return err
			}
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
	return unlock
}

func TestGetForUpdateHoldsRowLocks(t *testing.T) {
	for _, kind := range []string{"run", "task", "approval", "ai_call"} {
		for _, locked := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/forUpdate=%v", kind, locked), func(t *testing.T) {
				s, dsn := openStore(t)
				task := &store.Task{Kind: store.TaskActivity, RunID: "r", TaskQueue: "q", Attempt: 1, VisibleAt: at, ScheduledAt: at}
				a := &store.Approval{RunID: "r", ApprovalID: "a", Source: capstanv1.ApprovalSource_APPROVAL_SOURCE_HUMAN, Status: store.ApprovalPending, RequestedAt: at}
				call := &store.AICall{RunID: "r", Status: store.AICallReserved, At: at}
				txOK(t, s, func(tx store.Tx) error {
					if err := tx.InsertRun(run("r")); err != nil {
						return err
					}
					if err := tx.InsertTask(task); err != nil {
						return err
					}
					if err := tx.InsertApproval(a); err != nil {
						return err
					}
					return tx.InsertAICall(call)
				})
				unlock := holdTransaction(t, s, func(tx store.Tx) error {
					switch kind {
					case "run":
						_, err := tx.GetRun("r", locked)
						return err
					case "task":
						_, err := tx.GetTask(task.ID, locked)
						return err
					case "approval":
						_, err := tx.GetApproval("r", "a", locked)
						return err
					default:
						_, err := tx.GetAICall(call.ID, locked)
						return err
					}
				})
				defer unlock()
				conn, err := pgx.Connect(t.Context(), dsn)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close(context.Background())
				_, err = conn.Exec(t.Context(), "select 1 from "+kind+" for update nowait")
				var pgErr *pgconn.PgError
				if locked {
					if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
						t.Fatalf("competing row lock: %v, want lock_not_available", err)
					}
				} else if err != nil {
					t.Fatalf("read unexpectedly locked row: %v", err)
				}
			})
		}
	}
}

func TestAppendUniqueViolationIsConflictAndRecoverable(t *testing.T) {
	s, dsn := openStore(t)
	txOK(t, s, func(tx store.Tx) error { return tx.InsertRun(run("r")) })
	e := &capstanv1.HistoryEvent{EventId: 1, Type: capstanv1.EventType_EVENT_TYPE_RUN_STARTED, Time: timestamppb.New(at)}
	unlock := holdTransaction(t, s, func(tx store.Tx) error { return tx.AppendEvents("r", []*capstanv1.HistoryEvent{e}) })
	defer unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- s.InTx(ctx, func(tx store.Tx) error {
			err := tx.AppendEvents("r", []*capstanv1.HistoryEvent{e})
			if !errors.Is(err, store.ErrConflict) {
				return fmt.Errorf("append collision: got %v, want ErrConflict", err)
			}
			if _, err := tx.GetRun("r", false); err != nil {
				return fmt.Errorf("transaction unusable after collision: %w", err)
			}
			return tx.InsertRun(run("after-collision"))
		})
	}()
	conn, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		if err := conn.QueryRow(ctx, `select exists(select 1 from pg_stat_activity where datname=current_database() and wait_event_type='Lock' and query like 'insert into event%')`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err := <-result:
			t.Fatalf("append did not reach SQL uniqueness check: %v", err)
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	// Both writers read the same maximum; only now release the winner, forcing
	// PostgreSQL's unique-violation path in the second writer.
	unlock()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	txOK(t, s, func(tx store.Tx) error {
		h, err := tx.ReadHistory("r", 0, 0)
		if err != nil {
			return err
		}
		if len(h) != 1 {
			t.Fatalf("history length=%d", len(h))
		}
		_, err = tx.GetRun("after-collision", false)
		return err
	})
}

func TestZeroTimesAreSQLNull(t *testing.T) {
	s, dsn := openStore(t)
	txOK(t, s, func(tx store.Tx) error {
		if err := tx.InsertRun(run("r")); err != nil {
			return err
		}
		if err := tx.InsertTask(&store.Task{Kind: store.TaskActivity, RunID: "r", TaskQueue: "q", Attempt: 1, VisibleAt: at, ScheduledAt: at}); err != nil {
			return err
		}
		if err := tx.InsertApproval(&store.Approval{RunID: "r", ApprovalID: "a", Source: capstanv1.ApprovalSource_APPROVAL_SOURCE_HUMAN, Status: store.ApprovalPending, RequestedAt: at}); err != nil {
			return err
		}
		return tx.InsertAICall(&store.AICall{RunID: "r", Status: store.AICallReserved, At: at})
	})
	conn, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	var allNull bool
	err = conn.QueryRow(t.Context(), `select r.run_deadline is null and r.closed_at is null and t.leased_until is null and t.started_at is null and t.check_at is null and t.last_heartbeat_at is null and a.due_at is null and a.check_at is null and a.resolved_at is null and c.finished_at is null from run r join task t using(run_id) join approval a using(run_id) join ai_call c using(run_id)`).Scan(&allNull)
	if err != nil {
		t.Fatal(err)
	}
	if !allNull {
		t.Fatal("zero time was stored as a non-NULL timestamp")
	}
}

func TestOpenContextAndCloseLifecycle(t *testing.T) {
	dsn := testpg.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s, err := pgstore.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	cancel()
	ch, stop := s.Subscribe(store.TaskActivity, "q")
	defer stop()
	txOK(t, s, func(tx store.Tx) error { tx.Notify(store.TaskActivity, "q"); return nil })
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("Open context cancellation stopped listener")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	called := false
	if err := s.InTx(t.Context(), func(store.Tx) error { called = true; return nil }); err == nil || called {
		t.Fatalf("transaction after close: err=%v called=%v", err, called)
	}
	conn, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	var n int
	if err := conn.QueryRow(t.Context(), `select count(*) from pg_stat_activity where datname=current_database() and application_name='capstan-listener'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("Close left %d listeners", n)
	}
}
