package pgstore

import (
	"context"
	"embed"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrate applies pending files in lexical order, atomically. The advisory lock also
// protects the first creation of schema_migration when several processes start together.
func Migrate(ctx context.Context, dsn string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, `select pg_advisory_xact_lock(hashtext('capstan-migrate'))`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `create table if not exists schema_migration (version text primary key, applied_at timestamptz not null)`); err != nil {
		return err
	}
	files, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return err
	}
	for _, file := range files {
		var applied bool
		if err := tx.QueryRow(ctx, `select exists(select 1 from schema_migration where version=$1)`, file).Scan(&applied); err != nil {
			return err
		}
		if applied {
			continue
		}
		sql, err := migrations.ReadFile(file)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("migrate %s: %w", file, err)
		}
		// This is migration audit metadata only; store records use caller-supplied time.
		if _, err := tx.Exec(ctx, `insert into schema_migration(version,applied_at) values ($1,transaction_timestamp())`, file); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
