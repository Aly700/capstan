package testpg_test

import (
	"context"
	"testing"

	"github.com/Aly700/capstan/internal/store/pgstore"
	"github.com/Aly700/capstan/internal/testpg"
	"github.com/jackc/pgx/v5"
)

func TestBoundedMigrationPreservesLegacyReservations(t *testing.T) {
	dsn := testpg.New(t)
	conn, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	// Reconstruct the v1 schema only in this test's isolated disposable database.
	// Keeping the 0001 entry proves Migrate applies just the pending 0002 file.
	_, err = conn.Exec(t.Context(), `
		alter table ai_call drop column if exists bounded;
		delete from schema_migration where version='migrations/0002_ai_call_bounded.sql';
		insert into run (run_id,workflow_type,task_queue,status,task_timeout_ms,started_at)
		values ('legacy','wf','q',1,1000,'2026-09-28T12:00:00Z');
		insert into ai_call (run_id,activity_seq,model,status,estimate_usd,at)
		values ('legacy',1,'claude-sonnet-5',1,.75,'2026-09-28T12:00:00Z');
	`)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := pgstore.Migrate(t.Context(), dsn); err != nil {
			t.Fatal(err)
		}
	}
	var bounded bool
	var estimate float64
	var status int
	if err := conn.QueryRow(t.Context(), `select bounded,estimate_usd,status from ai_call where run_id='legacy'`).Scan(&bounded, &estimate, &status); err != nil {
		t.Fatal(err)
	}
	if bounded || estimate != .75 || status != 1 {
		t.Fatalf("migrated legacy row: bounded=%v estimate=%v status=%v", bounded, estimate, status)
	}
	// A legacy writer that omits the additive column also receives false.
	if err := conn.QueryRow(t.Context(), `insert into ai_call (run_id,activity_seq,model,status,estimate_usd,at) values ('legacy',2,'claude-sonnet-5',1,.25,'2026-09-28T12:00:00Z') returning bounded`).Scan(&bounded); err != nil {
		t.Fatal(err)
	}
	if bounded {
		t.Fatal("legacy insert defaulted to bounded")
	}
}
