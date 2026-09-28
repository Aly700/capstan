package main

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store deliberately exposes no SQL or health primitive. A separate, lazy pool
// with at most one connection proves database reachability without extending it.
func databaseReadiness(ctx context.Context, dsn string) (func(context.Context) error, func(), error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, nil, err
	}
	cfg.MaxConns = 1
	cfg.MinConns = 0
	cfg.MinIdleConns = 0
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	return func(ctx context.Context) error {
		var one int
		return pool.QueryRow(ctx, "select 1").Scan(&one)
	}, pool.Close, nil
}
