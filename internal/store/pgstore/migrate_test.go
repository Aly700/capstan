package pgstore_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Aly700/capstan/internal/testpg"
	"github.com/jackc/pgx/v5"
)

func TestEventTableIsAppendOnly(t *testing.T) {
	conn, err := pgx.Connect(t.Context(), testpg.New(t))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	_, err = conn.Exec(t.Context(), `insert into run(run_id,workflow_type,task_queue,status,task_timeout_ms,started_at) values ('r','wf','q',1,10000,'2026-01-01Z'); insert into event(run_id,event_id,type,at,data) values ('r',1,1,'2026-01-01Z','\x0801')`)
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{`update event set type=2 where run_id='r'`, `delete from event where run_id='r'`} {
		_, err := conn.Exec(t.Context(), query)
		if err == nil || !strings.Contains(err.Error(), "append-only") {
			t.Fatalf("%s: got %v, want append-only error", query, err)
		}
	}
	var n int
	if err := conn.QueryRow(t.Context(), `select count(*) from event where run_id='r' and event_id=1 and type=1`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("event changed after rejected writes: %d", n)
	}
}
