package pgstore

import (
	"context"
	"sync"

	"github.com/Aly700/capstan/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

// FlushPoolStatsForTest pins every pool connection, then asks each backend to flush
// its transaction counters. Pinning all connections avoids repeatedly flushing
// the same idle connection while another backend still holds unreported claims.
// Touching a catalog relation creates pending relation statistics even on an idle
// backend; PostgreSQL otherwise may return before flushing a bare function call.
// Connections remain pinned until release is called. Keeping them pinned while
// statistics settle prevents the next acquire from sending an idle health ping.
func FlushPoolStatsForTest(ctx context.Context, value store.Store) (count int, release func(), err error) {
	s := value.(*pgStore)
	connections := make([]*pgxpool.Conn, 0, s.pool.Config().MaxConns)
	var once sync.Once
	release = func() {
		once.Do(func() {
			for _, conn := range connections {
				conn.Release()
			}
		})
	}
	for range s.pool.Config().MaxConns {
		conn, err := s.pool.Acquire(ctx)
		if err != nil {
			release()
			return 0, release, err
		}
		connections = append(connections, conn)
	}
	for _, conn := range connections {
		if _, err := conn.Exec(ctx, "select pg_stat_force_next_flush() from pg_catalog.pg_class limit 1"); err != nil {
			release()
			return 0, release, err
		}
	}
	return len(connections), release, nil
}
