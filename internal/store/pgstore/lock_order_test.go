package pgstore_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"github.com/jackc/pgx/v5"
)

func TestClaimedRunLockDoesNotWaitAndRollsBackLease(t *testing.T) {
	s, dsn := openStore(t)
	var firstID, secondID int64
	txOK(t, s, func(tx store.Tx) error {
		for _, id := range []string{"first", "second"} {
			if err := tx.InsertRun(run(id)); err != nil {
				return err
			}
			task := &store.Task{Kind: store.TaskActivity, RunID: id, TaskQueue: "q", Attempt: 1, VisibleAt: at, ScheduledAt: at}
			if err := tx.InsertTask(task); err != nil {
				return err
			}
			if id == "first" {
				firstID = task.ID
			} else {
				secondID = task.ID
			}
		}
		return nil
	})
	conn, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	lock, err := conn.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(context.Background())
	if _, err := lock.Exec(t.Context(), `select 1 from run where run_id='first' for update`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err = s.InTx(ctx, func(tx store.Tx) error {
		task, err := tx.ClaimTask(store.TaskActivity, "q", at, time.Minute, "first-worker")
		if err != nil {
			return err
		}
		if task == nil || task.ID != firstID {
			return fmt.Errorf("first claim = %+v", task)
		}
		// A competing claimant must progress on another run while this task is held.
		if err := s.InTx(ctx, func(other store.Tx) error {
			next, err := other.ClaimTask(store.TaskActivity, "q", at, time.Minute, "second-worker")
			if err != nil {
				return err
			}
			if next == nil || next.ID != secondID {
				return fmt.Errorf("other-run claim = %+v", next)
			}
			_, err = other.GetRun(next.RunID, true)
			return err
		}); err != nil {
			return err
		}
		_, err = tx.GetRun(task.RunID, true)
		return err
	})
	var state interface{ SQLState() string }
	if !errors.As(err, &state) || state.SQLState() != "55P03" {
		t.Fatalf("contended claimed run = %v, want NOWAIT lock conflict", err)
	}
	if ctx.Err() != nil {
		t.Fatalf("claim waited for the run lock: %v", ctx.Err())
	}
	txOK(t, s, func(tx store.Tx) error {
		first, err := tx.GetTask(firstID, false)
		if err != nil {
			return err
		}
		if !first.LeasedUntil.IsZero() || !first.StartedAt.IsZero() || first.WorkerID != "" {
			return fmt.Errorf("rolled-back claim persisted: %+v", first)
		}
		second, err := tx.GetTask(secondID, false)
		if err == nil && (second.LeasedUntil.IsZero() || second.WorkerID != "second-worker") {
			return fmt.Errorf("other-run claim did not commit: %+v", second)
		}
		return err
	})
}

func TestDueQueriesSkipLockedRuns(t *testing.T) {
	for _, kind := range []string{"tasks", "timers", "approvals"} {
		t.Run(kind, func(t *testing.T) {
			s, dsn := openStore(t)
			txOK(t, s, func(tx store.Tx) error {
				for _, id := range []string{"first", "second"} {
					if err := tx.InsertRun(run(id)); err != nil {
						return err
					}
					if err := tx.InsertTask(&store.Task{Kind: store.TaskActivity, RunID: id, TaskQueue: "q", Attempt: 1, VisibleAt: at, ScheduledAt: at, CheckAt: at}); err != nil {
						return err
					}
					if err := tx.InsertTimer(&store.Timer{RunID: id, Seq: 1, DueAt: at}); err != nil {
						return err
					}
					if err := tx.InsertApproval(&store.Approval{RunID: id, ApprovalID: "a", Source: capstanv1.ApprovalSource_APPROVAL_SOURCE_HUMAN, Status: store.ApprovalPending, RequestedAt: at, CheckAt: at}); err != nil {
						return err
					}
				}
				return nil
			})
			conn, err := pgx.Connect(t.Context(), dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close(context.Background())
			lock, err := conn.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer lock.Rollback(context.Background())
			if _, err := lock.Exec(t.Context(), `select 1 from run where run_id='first' for update`); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			err = s.InTx(ctx, func(tx store.Tx) error {
				var ids []string
				switch kind {
				case "tasks":
					rows, err := tx.DueTasks(at, 10)
					if err != nil {
						return err
					}
					for _, row := range rows {
						ids = append(ids, row.RunID)
					}
				case "timers":
					rows, err := tx.DueTimers(at, 10)
					if err != nil {
						return err
					}
					for _, row := range rows {
						ids = append(ids, row.RunID)
					}
				case "approvals":
					rows, err := tx.DueApprovals(at, 10)
					if err != nil {
						return err
					}
					for _, row := range rows {
						ids = append(ids, row.RunID)
					}
				}
				if len(ids) != 1 || ids[0] != "second" {
					return fmt.Errorf("due %s = %v, want [second]", kind, ids)
				}
				_, err := tx.GetRun("second", true)
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
