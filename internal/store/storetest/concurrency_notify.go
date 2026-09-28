package storetest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Aly700/capstan/internal/store"
)

func concurrent(t *testing.T, n int, fn func() error) {
	t.Helper()
	start := make(chan struct{})
	errs := make(chan error, n)
	for range n {
		go func() { <-start; errs <- fn() }()
	}
	close(start)
	for range n {
		select {
		case err := <-errs:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("concurrent transactions did not finish")
		}
	}
}

func runLock(t *testing.T, s store.Store) {
	seed(t, s, "r")
	concurrent(t, 16, func() error {
		return s.InTx(t.Context(), func(tx store.Tx) error {
			r, err := tx.GetRun("r", true)
			if err != nil {
				return err
			}
			time.Sleep(time.Millisecond)
			r.LastEventID++
			return tx.UpdateRun(r)
		})
	})
	mustTx(t, s, func(tx store.Tx) error {
		r, err := tx.GetRun("r", false)
		if err != nil {
			return err
		}
		equal(t, r.LastEventID, int64(16))
		return nil
	})
}

func claimLock(t *testing.T, s store.Store) {
	seed(t, s, "r")
	first, second := sampleTask("r"), sampleTask("r")
	mustTx(t, s, func(tx store.Tx) error {
		if err := tx.InsertTask(first); err != nil {
			return err
		}
		return tx.InsertTask(second)
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	locked, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var once sync.Once
	unlock := func() { once.Do(func() { close(release) }) }
	defer unlock()
	go func() {
		done <- s.InTx(ctx, func(tx store.Tx) error {
			v, err := tx.ClaimTask(store.TaskActivity, "q", epoch, time.Minute, "first")
			if err != nil {
				return err
			}
			if v == nil || v.ID != first.ID {
				return fmt.Errorf("first claim = %v", v)
			}
			close(locked)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	select {
	case <-locked:
	case err := <-done:
		t.Fatalf("lock first task: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	entered, result := make(chan struct{}), make(chan error, 1)
	go func() {
		result <- s.InTx(ctx, func(tx store.Tx) error {
			close(entered)
			v, err := tx.ClaimTask(store.TaskActivity, "q", epoch, time.Minute, "second")
			if err != nil {
				return err
			}
			if v == nil || v.ID != second.ID {
				return fmt.Errorf("claim while first is held = %v, want id %d", v, second.ID)
			}
			return nil
		})
	}()
	select {
	case <-entered:
		// Once a transaction has entered, ClaimTask itself must not wait on the row.
		select {
		case err := <-result:
			must(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("ClaimTask waited on a locked row")
		}
		unlock()
	case <-time.After(200 * time.Millisecond):
		// The plan permits memstore to serialize InTx entry with one mutex. Complete
		// the first claim so that implementation can enter its second transaction.
		unlock()
		select {
		case err := <-result:
			must(t, err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	must(t, <-done)
}

func awaitWake(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case _, ok := <-ch:
		if !ok {
			t.Fatal("subscription closed before wake-up")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no committed notification")
	}
}

func noWake(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("unexpected notification")
		}
	case <-time.After(200 * time.Millisecond):
	}
}

func notifyCommit(t *testing.T, s store.Store) {
	ch, cancel := s.Subscribe(store.TaskActivity, "q:quotes'雪")
	defer cancel()
	other, stopOther := s.Subscribe(store.TaskWorkflow, "q:quotes'雪")
	defer stopOther()
	otherQueue, stopQueue := s.Subscribe(store.TaskActivity, "different")
	defer stopQueue()
	abort := errors.New("rollback")
	equal(t, s.InTx(t.Context(), func(tx store.Tx) error { tx.Notify(store.TaskActivity, "q:quotes'雪"); return abort }), abort)
	noWake(t, ch)
	mustTx(t, s, func(tx store.Tx) error { tx.Notify(store.TaskActivity, "q:quotes'雪"); noWake(t, ch); return nil })
	awaitWake(t, ch)
	noWake(t, other)
	noWake(t, otherQueue)
}

func notifyCoalesced(t *testing.T, s store.Store) {
	slow, stopSlow := s.Subscribe(store.TaskActivity, "q")
	defer stopSlow()
	fast, stopFast := s.Subscribe(store.TaskActivity, "q")
	defer stopFast()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	for range 20 {
		must(t, s.InTx(ctx, func(tx store.Tx) error {
			for range 5 {
				tx.Notify(store.TaskActivity, "q")
			}
			return nil
		}))
		awaitWake(t, fast)
	}
	// Earlier fanouts have filled the slow subscriber's slot. The latest fast
	// receipt need not mean that its fanout has finished visiting every subscriber.
	equal(t, len(slow), 1)
	awaitWake(t, slow)
}

func notifyCancel(t *testing.T, s store.Store) {
	ch, cancel := s.Subscribe(store.TaskActivity, "q")
	cancel()
	cancel()
	live, stop := s.Subscribe(store.TaskActivity, "q")
	defer stop()
	mustTx(t, s, func(tx store.Tx) error { tx.Notify(store.TaskActivity, "q"); return nil })
	awaitWake(t, live)
	noWake(t, ch)
}
