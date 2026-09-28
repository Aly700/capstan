// Package testpg gives each test an isolated, migrated PostgreSQL database.
package testpg

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Aly700/capstan/internal/store/pgstore"
	"github.com/jackc/pgx/v5"
)

// New creates a database and registers its removal with t.Cleanup.
func New(t testing.TB) string {
	t.Helper()
	adminDSN := os.Getenv("CAPSTAN_TEST_DATABASE_URL")
	if adminDSN == "" {
		adminDSN = "postgres://capstan:capstan@127.0.0.1:55432/postgres?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatalf("testpg: connect to shared PostgreSQL: %v", err)
	}
	defer admin.Close(context.Background())
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatal(err)
	}
	name := "capstan_audit_" + hex.EncodeToString(suffix[:])
	quoted := pgx.Identifier{name}.Sanitize()
	if _, err := admin.Exec(ctx, "create database "+quoted); err != nil {
		t.Fatalf("testpg: create database: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		conn, err := pgx.Connect(ctx, adminDSN)
		if err != nil {
			t.Errorf("testpg: connect for cleanup: %v", err)
			return
		}
		defer conn.Close(context.Background())
		if _, err := conn.Exec(ctx, "drop database "+quoted+" with (force)"); err != nil {
			t.Errorf("testpg: drop %s: %v", name, err)
		}
	})
	dsn := adminDSN + " dbname=" + name
	if strings.HasPrefix(adminDSN, "postgres://") || strings.HasPrefix(adminDSN, "postgresql://") {
		u, err := url.Parse(adminDSN)
		if err != nil {
			t.Fatal(err)
		}
		u.Path, u.RawPath = "/"+name, ""
		q := u.Query()
		q.Del("dbname")
		u.RawQuery = q.Encode()
		dsn = u.String()
	}
	if err := pgstore.Migrate(ctx, dsn); err != nil {
		t.Fatalf("testpg: migrate: %v", err)
	}
	return dsn
}
