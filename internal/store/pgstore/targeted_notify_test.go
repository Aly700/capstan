package pgstore_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Aly700/capstan/internal/store"
	"github.com/Aly700/capstan/internal/store/pgstore"
	"github.com/Aly700/capstan/internal/testpg"
	"github.com/jackc/pgx/v5"
)

func TestQueueNotificationClaimTransactions(t *testing.T) {
	const waiters = 40
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	dsn := testpg.New(t)
	// One pool connection keeps startup and health-check work out of the claim
	// window. All forty subscriber goroutines still compete to claim the task.
	s, err := pgstore.OpenWithMaxConns(ctx, dsn, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	// Observe from postgres so measurement queries never increment the isolated
	// workload database's transaction count.
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	database := cfg.Database
	cfg.Database = "postgres"
	observer, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close(context.Background())
	txOK(t, s, func(tx store.Tx) error { return tx.InsertRun(run("notification-claim-run")) })
	channels := make([]<-chan struct{}, waiters)
	for i := range channels {
		ch, stop := s.Subscribe(store.TaskActivity, "notification-claim-queue")
		defer stop()
		channels[i] = ch
	}
	fence, stopFence := s.SubscribeRun("notification-delivery-fence")
	defer stopFence()
	var attempts, claimed atomic.Int32
	finished := make(chan error, waiters)
	deliveryComplete := make(chan struct{})
	for i, ch := range channels {
		go func() {
			// Inspect only after the listener has completed the whole fanout. This
			// keeps a broadcast baseline measurable without timing or sleeps.
			select {
			case <-ctx.Done():
				finished <- ctx.Err()
				return
			case <-deliveryComplete:
			}
			select {
			case <-ch:
				attempts.Add(1)
				finished <- s.InTx(ctx, func(tx store.Tx) error {
					task, err := tx.ClaimTask(store.TaskActivity, "notification-claim-queue", at, time.Minute, fmt.Sprintf("waiter-%d", i))
					if err == nil && task != nil {
						claimed.Add(1)
					}
					return err
				})
			default:
				finished <- nil
			}
		}()
	}
	txOK(t, s, func(tx store.Tx) error {
		task := &store.Task{Kind: store.TaskActivity, RunID: "notification-claim-run", TaskQueue: "notification-claim-queue", Attempt: 1, VisibleAt: at, ScheduledAt: at}
		if err := tx.InsertTask(task); err != nil {
			return err
		}
		tx.Notify(store.TaskActivity, "notification-claim-queue")
		tx.NotifyRunClosed("notification-delivery-fence")
		return nil
	})
	select {
	case <-fence:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Both the insert and PostgreSQL's internal LISTEN transaction precede the
	// measurement. Claimers remain parked until their buffered hints are delivered
	// and these setup transactions have reached a stable statistics snapshot.
	flushBefore, releaseBefore, err := pgstore.FlushPoolStatsForTest(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseBefore()
	controlBefore := stableDatabaseCommits(t, ctx, observer, database)
	releaseBefore()
	// Measure the flush overhead without releasing any claimers. Use the same
	// query and snapshot protocol as the workload window, rather than assuming
	// every statistics query appears in the database counter.
	flushControl, releaseControl, err := pgstore.FlushPoolStatsForTest(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseControl()
	before := stableDatabaseCommits(t, ctx, observer, database)
	controlDelta := before - controlBefore
	if controlDelta != int64(flushControl) {
		t.Fatalf("no-claim control delta=%d, flush queries=%d", controlDelta, flushControl)
	}
	releaseControl()
	close(deliveryComplete)
	for range waiters {
		if err := <-finished; err != nil {
			t.Error(err)
		}
	}
	flushAfter, releaseAfter, err := pgstore.FlushPoolStatsForTest(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseAfter()
	after := stableDatabaseCommits(t, ctx, observer, database)
	delta := after - before
	claimTransactions := delta - controlDelta
	t.Logf("waiters=%d pool_connections=1 claim_attempts=%d claimed=%d control_before=%d control_after=%d control_delta=%d xact_commit_before=%d xact_commit_after=%d xact_commit_delta=%d flush_before=%d flush_control=%d flush_after=%d setup_before_measurement=true claim_transactions=%d",
		waiters, attempts.Load(), claimed.Load(), controlBefore, before, controlDelta, before, after, delta, flushBefore, flushControl, flushAfter, claimTransactions)
	if attempts.Load() != 1 || claimed.Load() != 1 {
		t.Errorf("one task: attempts=%d claimed=%d, want 1 each", attempts.Load(), claimed.Load())
	}
	if claimTransactions != int64(attempts.Load()) {
		t.Errorf("measured claim transactions=%d, counted attempts=%d", claimTransactions, attempts.Load())
	}
}

func stableDatabaseCommits(t *testing.T, ctx context.Context, observer *pgx.Conn, database string) int64 {
	t.Helper()
	var previous int64 = -1
	var stableSince time.Time
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := observer.Exec(ctx, "select pg_stat_clear_snapshot()"); err != nil {
			t.Fatal(err)
		}
		var commits int64
		if err := observer.QueryRow(ctx, "select xact_commit from pg_stat_database where datname=$1", database).Scan(&commits); err != nil {
			t.Fatal(err)
		}
		if commits != previous {
			previous, stableSince = commits, time.Now()
		} else if time.Since(stableSince) >= time.Second {
			return commits
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
}
