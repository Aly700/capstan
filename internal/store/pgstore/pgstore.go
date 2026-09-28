// Package pgstore implements store.Store on PostgreSQL 16 with pgx. Implemented by the
// store lane; must pass the shared conformance suite in store/storetest.
package pgstore

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Aly700/capstan/internal/store"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type pgStore struct {
	pool           *pgxpool.Pool
	listenerConfig *pgx.ConnConfig
	listenCtx      context.Context
	stop           context.CancelFunc
	done           chan struct{}
	mu             sync.Mutex
	subs           map[string]map[chan struct{}]struct{}
	closed         bool
	closeOnce      sync.Once
}

var _ store.Store = (*pgStore)(nil)

// Open connects to PostgreSQL and returns a Store. It does not apply migrations.
func Open(ctx context.Context, dsn string) (store.Store, error) {
	// Forty led the 10/20/40/60 pool comparison on the documented load machine.
	// See docs/evidence/load.md; servers may override it for a different database.
	return OpenWithMaxConns(ctx, dsn, 40)
}

// OpenWithMaxConns uses an explicit pool limit; the listener has one separate connection.
func OpenWithMaxConns(ctx context.Context, dsn string, maxConns int32) (store.Store, error) {
	if maxConns <= 0 {
		return nil, errors.New("pgstore: max connections must be positive")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns
	if cfg.ConnConfig.ConnectTimeout == 0 {
		cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	listenCtx, stop := context.WithCancel(context.Background())
	s := &pgStore{pool: pool, listenerConfig: cfg.ConnConfig.Copy(), listenCtx: listenCtx, stop: stop, done: make(chan struct{}), subs: make(map[string]map[chan struct{}]struct{})}
	s.listenerConfig.RuntimeParams["application_name"] = "capstan-listener"
	conn, err := s.connectListener(ctx)
	if err != nil {
		stop()
		pool.Close()
		return nil, err
	}
	go s.listen(conn)
	return s, nil
}

func (s *pgStore) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		for _, subscribers := range s.subs {
			for ch := range subscribers {
				closeSubscription(ch)
			}
		}
		s.subs = nil
		s.mu.Unlock()
		s.stop()
		<-s.done
		s.pool.Close()
	})
	return nil
}
