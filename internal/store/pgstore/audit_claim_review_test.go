package pgstore_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Aly700/capstan/internal/store"
	"github.com/jackc/pgx/v5"
)

func TestAuditReviewClaimBusyBacklog(t *testing.T) {
	for _, count := range []int{64, 512, 2048} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			s, dsn := openStore(t)
			txOK(t, s, func(tx store.Tx) error {
				if err := tx.InsertRun(run("busy")); err != nil {
					return err
				}
				if err := tx.InsertRun(run("ready")); err != nil {
					return err
				}
				for i := 0; i < count+1; i++ {
					id := "busy"
					if i == count {
						id = "ready"
					}
					if err := tx.InsertTask(&store.Task{Kind: store.TaskActivity, RunID: id, TaskQueue: "q", Attempt: 1, VisibleAt: at, ScheduledAt: at}); err != nil {
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
			if _, err := lock.Exec(t.Context(), "select 1 from run where run_id='busy' for update"); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			began := time.Now()
			err = s.InTx(ctx, func(tx store.Tx) error {
				task, err := tx.ClaimTask(store.TaskActivity, "q", at, time.Minute, "review")
				if err != nil {
					return err
				}
				if task == nil || task.RunID != "ready" {
					return fmt.Errorf("ready task not claimed: %+v", task)
				}
				return nil
			})
			t.Logf("busy tasks=%d elapsed=%v error=%v", count, time.Since(began), err)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
