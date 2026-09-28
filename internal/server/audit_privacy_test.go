package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"connectrpc.com/connect"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	rpc "github.com/Aly700/capstan/gen/capstan/v1/capstanv1connect"
	"github.com/Aly700/capstan/internal/engine"
	"google.golang.org/protobuf/proto"
)

func TestAuditConfiguredCredentialsStayOutOfRPCErrorsAndLogs(t *testing.T) {
	cfg := testConfig()
	cfg.DatabaseURL = "postgres://owner:audit-db-canary@127.0.0.1:55432/capstan_audit_private"
	cfg.GateAPIKey, cfg.APIKey, cfg.AnthropicAPIKey = "audit-gate-canary", "audit-api-canary", "audit-provider-canary"
	secrets := []string{cfg.DatabaseURL, "audit-db-canary", cfg.GateAPIKey, cfg.APIKey, cfg.AnthropicAPIKey, "secret"}
	for _, mapped := range []bool{false, true} {
		t.Run(fmt.Sprint(mapped), func(t *testing.T) {
			var logs bytes.Buffer
			f := &fakeAPI{call: func(context.Context, string, string, proto.Message) (proto.Message, bool, error) {
				err := errors.New("upstream rejected credentials " + strings.Join(secrets, " "))
				if mapped {
					err = fmt.Errorf("%s: %w", err, engine.ErrInvalidArgument)
				}
				return nil, false, err
			}}
			hc, url := rpcHTTP(t, New(cfg, f, nil, Options{Logger: slog.New(slog.NewJSONHandler(&logs, nil))}))
			req := connect.NewRequest(&v1.DescribeRunRequest{RunId: "r"})
			req.Header().Set("Authorization", "Bearer secret")
			_, err := rpc.NewClientServiceClient(hc, url).DescribeRun(t.Context(), req)
			if err == nil {
				t.Fatal("expected dependency error")
			}
			for _, secret := range secrets {
				if strings.Contains(err.Error(), secret) || strings.Contains(logs.String(), secret) {
					t.Errorf("credential %q leaked in error/log", secret)
				}
			}
		})
	}
}

func TestAuditConfiguredCredentialsStayOutOfMaintenanceLogs(t *testing.T) {
	cfg := testConfig()
	cfg.GateAPIKey = "audit-maintenance-gate-canary"
	var logs bytes.Buffer
	s := New(cfg, &fakeAPI{}, nil, Options{Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls := 0
	s.runLoop(ctx, "approvals", func(context.Context, int) (int, error) {
		calls++
		if calls > 1 {
			cancel()
		}
		return 0, errors.New("Gate transport: " + cfg.GateAPIKey)
	})
	if strings.Contains(logs.String(), cfg.GateAPIKey) || !strings.Contains(logs.String(), "[REDACTED]") {
		t.Fatalf("unsafe maintenance log: %s", logs.String())
	}
}
