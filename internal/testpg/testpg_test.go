package testpg_test

import (
	"context"
	"testing"

	"github.com/Aly700/capstan/internal/testpg"
	"github.com/jackc/pgx/v5"
)

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
