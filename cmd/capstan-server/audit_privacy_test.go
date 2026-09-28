package main

import (
	"context"
	"io"
	"strings"
	"testing"
)

func TestAuditStartupDatabaseErrorsDoNotRevealCredentials(t *testing.T) {
	const secret = "audit-db-password-canary"
	dsn := "postgres://capstan:" + secret + "@127.0.0.1:55432/capstan_audit_unused?sslmode=invalid"
	for _, command := range []string{"serve", "migrate"} {
		t.Run(command, func(t *testing.T) {
			getenv := func(name string) string {
				if name == "CAPSTAN_DATABASE_URL" {
					return dsn
				}
				return testEnv(name)
			}
			err := run(context.Background(), []string{command}, getenv, strings.NewReader(""), io.Discard, io.Discard, productionDependencies())
			if err == nil {
				t.Fatal("invalid database configuration unexpectedly succeeded")
			}
			if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), dsn) {
				t.Fatalf("database credentials leaked: %v", err)
			}
		})
	}
}
