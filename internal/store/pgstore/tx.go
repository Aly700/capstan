package pgstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Aly700/capstan/internal/store"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type transaction struct {
	ctx         context.Context
	tx          pgx.Tx
	notifyErr   error
	notifySeq   uint64
	claimedRuns map[string]struct{}
}

var _ store.Tx = (*transaction)(nil)

func (s *pgStore) InTx(ctx context.Context, fn func(store.Tx) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return dbError(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	t := &transaction{ctx: ctx, tx: tx}
	if err := fn(t); err != nil {
		return err
	}
	if t.notifyErr != nil {
		return t.notifyErr
	}
	return dbError(tx.Commit(ctx))
}

func dbError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return store.ErrNotFound
	}
	var e *pgconn.PgError
	if errors.As(err, &e) && e.Code == "23505" {
		if e.TableName == "event" || e.ConstraintName == "event_pkey" {
			return store.ErrConflict
		}
		return store.ErrAlreadyExists
	}
	return err
}

type scanner interface{ Scan(...any) error }

func collect[T any](rows pgx.Rows, err error, scan func(scanner) (T, error)) ([]T, error) {
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	var result []T
	for rows.Next() {
		v, err := scan(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, v)
	}
	return result, dbError(rows.Err())
}

// SQL builders only receive package constants, never caller-controlled identifiers.
func insertSQL(table, columns string) string {
	params := make([]string, len(strings.Split(columns, ",")))
	for i := range params {
		params[i] = fmt.Sprintf("$%d", i+1)
	}
	return "insert into " + table + " (" + columns + ") values (" + strings.Join(params, ",") + ")"
}

func updateSQL(table, columns string, keys int) string {
	cols := strings.Split(columns, ",")
	set, where := []string{}, []string{}
	for i, c := range cols {
		expr := strings.TrimSpace(c) + fmt.Sprintf("=$%d", i+1)
		if i < keys {
			where = append(where, expr)
		} else {
			set = append(set, expr)
		}
	}
	return "update " + table + " set " + strings.Join(set, ",") + " where " + strings.Join(where, " and ")
}

func (t *transaction) update(query string, args ...any) error {
	tag, err := t.tx.Exec(t.ctx, query, args...)
	if err != nil {
		return dbError(err)
	}
	if tag.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	return nil
}

func (t *transaction) insertUnique(query string, args ...any) error {
	tag, err := t.tx.Exec(t.ctx, query, args...)
	if err != nil {
		return dbError(err)
	}
	if tag.RowsAffected() == 0 {
		return store.ErrAlreadyExists
	}
	return nil
}

func lockClause(forUpdate bool) string {
	if forUpdate {
		return " for update"
	}
	return ""
}

func readLimit(limit int) any {
	if limit <= 0 {
		return nil
	}
	return limit
}

// qualifiedColumns receives only package constants, just like the SQL builders.
func qualifiedColumns(alias, columns string) string {
	return alias + "." + strings.ReplaceAll(columns, ",", ","+alias+".")
}
