package pgstore_test

import (
	"context"
	"errors"
	"github.com/Aly700/capstan/internal/store"
	"github.com/Aly700/capstan/internal/store/pgstore"
	"github.com/Aly700/capstan/internal/testpg"
	"testing"
	"time"
)

func TestExplicitPoolLimit(t *testing.T) {
	dsn := testpg.New(t)
	if err := pgstore.Migrate(t.Context(), dsn); err != nil {
		t.Fatal(err)
	}
	s, err := pgstore.OpenWithMaxConns(t.Context(), dsn, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	held, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() { done <- s.InTx(t.Context(), func(store.Tx) error { close(held); <-release; return nil }) }()
	<-held
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	err = s.InTx(ctx, func(store.Tx) error { t.Error("second transaction exceeded one-connection limit"); return nil })
	close(release)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("waiting for pool: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for _, n := range []int32{0, -1} {
		if _, err := pgstore.OpenWithMaxConns(t.Context(), dsn, n); err == nil {
			t.Fatalf("accepted %d", n)
		}
	}
}
