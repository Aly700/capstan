// Package pgstore implements store.Store on PostgreSQL 16 with pgx. Implemented by the
// store lane; must pass the shared conformance suite in store/storetest.
package pgstore

import (
	"context"
	"errors"

	"github.com/Aly700/capstan/internal/store"
)

var errNotImplemented = errors.New("pgstore: not implemented yet (store lane)")

// Open connects to PostgreSQL and returns a Store. It does not apply migrations.
func Open(ctx context.Context, dsn string) (store.Store, error) {
	_, _ = ctx, dsn
	return nil, errNotImplemented
}
