package pgstore_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"github.com/jackc/pgx/v5"
)

func TestDueQueriesSkipLockedRows(t *testing.T) {
	for _, kind := range []string{"runs", "tasks", "timers", "approvals"} {
		t.Run(kind, func(t *testing.T) {
			s, dsn := openStore(t)
			var firstTask int64
			txOK(t, s, func(tx store.Tx) error {
				for _, id := range []string{"first", "second"} {
					r := run(id)
					r.RunDeadline = at
					if err := tx.InsertRun(r); err != nil {
						return err
					}
					task := &store.Task{Kind: store.TaskActivity, RunID: id, TaskQueue: "q", Attempt: 1, VisibleAt: at, ScheduledAt: at, CheckAt: at}
					if err := tx.InsertTask(task); err != nil {
						return err
					}
					if id == "first" {
						firstTask = task.ID
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
			query := map[string]string{"runs": "select 1 from run where run_id='first' for update", "tasks": fmt.Sprintf("select 1 from task where id=%d for update", firstTask), "timers": "select 1 from timer where run_id='first' for update", "approvals": "select 1 from approval where run_id='first' for update"}[kind]
			if _, err := lock.Exec(t.Context(), query); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			err = s.InTx(ctx, func(tx store.Tx) error {
				var ids []string
				switch kind {
				case "runs":
					rows, err := tx.RunsPastDeadline(at, 10)
					if err != nil {
						return err
					}
					for _, r := range rows {
						ids = append(ids, r.RunID)
					}
				case "tasks":
					rows, err := tx.DueTasks(at, 10)
					if err != nil {
						return err
					}
					for _, r := range rows {
						ids = append(ids, r.RunID)
					}
				case "timers":
					rows, err := tx.DueTimers(at, 10)
					if err != nil {
						return err
					}
					for _, r := range rows {
						ids = append(ids, r.RunID)
					}
				case "approvals":
					rows, err := tx.DueApprovals(at, 10)
					if err != nil {
						return err
					}
					for _, r := range rows {
						ids = append(ids, r.RunID)
					}
				}
				if len(ids) != 1 || ids[0] != "second" {
					return fmt.Errorf("due %s returned %v, want [second]", kind, ids)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
