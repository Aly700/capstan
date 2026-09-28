package server

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/Aly700/capstan/internal/store"
	"github.com/Aly700/capstan/internal/store/memstore"
	"google.golang.org/protobuf/proto"
)

func TestLongPollNotificationWakesOneWaitingPoller(t *testing.T) {
	for _, kind := range []store.TaskKind{store.TaskWorkflow, store.TaskActivity} {
		t.Run(kind.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const pollers = 32
				db := memstore.New()
				defer db.Close()
				var claims atomic.Int32
				var available atomic.Bool
				api := &fakeAPI{call: func(context.Context, string, string, proto.Message) (proto.Message, bool, error) {
					claims.Add(1)
					if available.CompareAndSwap(true, false) {
						return taskResponse(kind), true, nil
					}
					return nil, false, nil
				}}
				s := New(testConfig(), api, db, Options{})
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				type result struct {
					token []byte
					err   error
				}
				results := make(chan result, pollers)
				for range pollers {
					go func() {
						token, err := poll(s, ctx, kind)
						results <- result{token: token, err: err}
					}()
				}

				// Wait for every initial claim to finish and every poller to block.
				// Wait does not advance the clock to the fallback polling tick.
				synctest.Wait()
				if got := claims.Load(); got != pollers {
					t.Fatalf("initial claims = %d, want %d", got, pollers)
				}
				if got := len(results); got != 0 {
					t.Fatalf("polls returned before notification = %d, want 0", got)
				}

				available.Store(true)
				if err := db.InTx(ctx, func(tx store.Tx) error {
					tx.Notify(kind, "q")
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				if got := claims.Load(); got != pollers+1 {
					t.Fatalf("claims after one notification = %d, want %d", got, pollers+1)
				}
				if got := len(results); got != 1 {
					t.Fatalf("polls returned after one notification = %d, want 1", got)
				}
				claimed := <-results
				if claimed.err != nil || string(claimed.token) != "claimed" {
					t.Fatalf("notified poll returned token=%q err=%v", claimed.token, claimed.err)
				}

				cancel()
				synctest.Wait()
				if got := len(results); got != pollers-1 {
					t.Fatalf("cancelled polls = %d, want %d", got, pollers-1)
				}
				for range pollers - 1 {
					cancelled := <-results
					if len(cancelled.token) != 0 || !errors.Is(cancelled.err, context.Canceled) {
						t.Errorf("cancelled poll returned token=%q err=%v", cancelled.token, cancelled.err)
					}
				}
				if got := claims.Load(); got != pollers+1 {
					t.Fatalf("claims after cancellation = %d, want %d", got, pollers+1)
				}
			})
		})
	}
}
