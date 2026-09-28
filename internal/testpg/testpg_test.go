package testpg_test

import (
	"context"
	"regexp"
	"sync"
	"testing"

	"github.com/Aly700/capstan/internal/store/pgstore"
	"github.com/Aly700/capstan/internal/testpg"
	"github.com/jackc/pgx/v5"
)

func TestNewCreatesMigratedDatabase(t *testing.T) {
	dsn := testpg.New(t)
	conn, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	var n int
	if err := conn.QueryRow(t.Context(), `select count(*) from schema_migration where applied_at is not null`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("migrations applied = %d, want 1", n)
	}
	if !regexp.MustCompile(`^capstan_t_[0-9a-f]{16}$`).MatchString(conn.Config().Database) {
		t.Fatalf("database name = %q", conn.Config().Database)
	}
}

func TestMigrateIsIdempotentAndConcurrencySafe(t *testing.T) {
	dsn := testpg.New(t)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Go(func() { errs <- pgstore.Migrate(t.Context(), dsn) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestNewDropsOnlyItsDatabase(t *testing.T) {
	keeper := testpg.New(t)
	var removed string
	t.Run("child", func(t *testing.T) {
		cfg, err := pgx.ParseConfig(testpg.New(t))
		if err != nil {
			t.Fatal(err)
		}
		removed = cfg.Database
	})
	conn, err := pgx.Connect(t.Context(), keeper)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	var exists bool
	if err := conn.QueryRow(t.Context(), `select exists(select 1 from pg_database where datname=$1)`, removed).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatalf("test database %s survived cleanup", removed)
	}
}
