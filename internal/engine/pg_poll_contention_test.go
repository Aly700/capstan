//go:build pgengine

package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
)

// A run whose row another transaction holds must not stall the queue. Its whole
// fan-out sits first in visible_at order; concurrent pollers have to skip past it and
// drain every other run's task inside the poll bound, then claim the fan-out itself
// once the lock is released. Reverting either half of the ClaimTask fix (the NOWAIT
// probe, or skipping the busy run for the rest of the poll) makes this time out.
func TestPostgresConcurrentPollsDrainQueueBehindLockedFanOut(t *testing.T) {
	const fanOut, others, pollers = 40, 12, 6
	e, _, s := newTestEngine(t)
	mustStart(t, e, "busy")
	commands := make([]*v1.Command, 0, fanOut)
	for seq := int64(1); seq <= fanOut; seq++ {
		commands = append(commands, activityCmd(seq))
	}
	mustComplete(t, e, mustPoll(t, e).TaskToken, commands...)
	for i := 0; i < others; i++ {
		mustStart(t, e, fmt.Sprintf("run-%02d", i))
		mustComplete(t, e, mustPoll(t, e).TaskToken, activityCmd(1))
	}

	locked, release, lockDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var unlock sync.Once
	defer unlock.Do(func() { close(release) })
	go func() {
		lockDone <- s.InTx(t.Context(), func(tx store.Tx) error {
			if _, err := tx.GetRun("busy", true); err != nil {
				return err
			}
			close(locked)
			<-release
			return nil
		})
	}()
	select {
	case <-locked:
	case err := <-lockDone:
		t.Fatalf("run lock: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var mu sync.Mutex
	claimed := map[string]int{}
	var wg sync.WaitGroup
	began := time.Now()
	for p := 0; p < pollers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				response, found, err := e.PollActivityTask(ctx, &v1.PollActivityTaskRequest{TaskQueue: "q", Identity: fmt.Sprintf("poller-%d", p)})
				if err != nil {
					if ctx.Err() == nil {
						t.Error(err)
					}
					return
				}
				mu.Lock()
				if found {
					claimed[response.RunId]++
				}
				drained := len(claimed) >= others
				mu.Unlock()
				if drained {
					return
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(began)
	if ctx.Err() != nil {
		t.Fatalf("pollers did not drain %d runnable tasks behind the locked run within %v: claimed %v", others, elapsed, claimed)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("draining %d tasks behind the locked run took %v", others, elapsed)
	}
	if _, ok := claimed["busy"]; ok {
		t.Fatalf("claimed a task of the locked run: %v", claimed)
	}
	if len(claimed) != others {
		t.Fatalf("claimed runs = %v, want all %d others", claimed, others)
	}
	for run, n := range claimed {
		if n != 1 {
			t.Fatalf("run %s claimed %d times by concurrent pollers", run, n)
		}
	}

	unlock.Do(func() { close(release) })
	if err := <-lockDone; err != nil {
		t.Fatal(err)
	}
	busy := 0
	for {
		response, found, err := e.PollActivityTask(t.Context(), &v1.PollActivityTaskRequest{TaskQueue: "q", Identity: "late"})
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			break
		}
		if response.RunId != "busy" {
			t.Fatalf("leftover task for %s after the drain", response.RunId)
		}
		busy++
	}
	if busy != fanOut {
		t.Fatalf("claimed %d of the released run's %d tasks", busy, fanOut)
	}
}
