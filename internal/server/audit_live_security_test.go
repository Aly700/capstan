package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	rpc "github.com/Aly700/capstan/gen/capstan/v1/capstanv1connect"
	"github.com/Aly700/capstan/internal/config"
	"github.com/Aly700/capstan/internal/engine"
	"github.com/Aly700/capstan/internal/store/pgstore"
	"github.com/Aly700/capstan/internal/testpg"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestAuditLiveAuthenticationAndCredentialPrivacy(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	dsn := testpg.New(t)
	st, err := pgstore.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	api, err := engine.New(engine.Deps{Store: st}, engine.Config{})
	if err != nil {
		t.Fatal(err)
	}
	const key = "audit-api-private-canary"
	cfg := config.Config{DatabaseURL: dsn, APIKeyHashes: map[string][32]byte{"audit": sha256.Sum256([]byte(key))}, GateAPIKey: "audit-gate-private-canary", AnthropicAPIKey: "audit-provider-private-canary", PollTimeout: 100 * time.Millisecond}
	var logs bytes.Buffer
	s := New(cfg, api, st, Options{Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
	listener, err := net.Listen("tcp", "127.0.0.1:7630")
	if err != nil {
		t.Fatal(err)
	}
	serveCtx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(serveCtx, listener) }()
	defer func() {
		stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	address := "http://" + listener.Addr().String()
	hc := &http.Client{Timeout: 5 * time.Second}
	auth := http.Header{"Authorization": {"Bearer " + key}}
	cases := allRPCs(hc, address)
	methods := 0
	services := v1.File_capstan_v1_capstan_proto.Services()
	for i := 0; i < services.Len(); i++ {
		methods += services.Get(i).Methods().Len()
	}
	if len(cases) != methods {
		t.Fatalf("auth matrix covers %d of %d RPCs", len(cases), methods)
	}
	for _, tc := range cases {
		for _, headers := range []http.Header{{}, {"Authorization": {"Bearer unknown"}}, {"Authorization": {"Bearer unknown:" + key}}, {"Authorization": {"Basic " + key}}, {"Authorization": {"Bearer  " + key}}, {"Authorization": {"Bearer " + key, "Bearer " + key}}} {
			if _, err := tc.invoke(ctx, headers); connect.CodeOf(err) != connect.CodeUnauthenticated {
				t.Errorf("%s header %q accepted: %v", tc.name, headers, err)
			}
		}
	}
	var output strings.Builder
	client := rpc.NewClientServiceClient(hc, address)
	start := connect.NewRequest(&v1.StartRunRequest{RunId: "audit-private-run", WorkflowType: "audit", TaskQueue: "audit-private"})
	start.Header().Set("Authorization", auth.Get("Authorization"))
	if _, err := client.StartRun(ctx, start); err != nil {
		t.Fatal(err)
	}
	describe := connect.NewRequest(&v1.DescribeRunRequest{RunId: "audit-private-run"})
	describe.Header().Set("Authorization", auth.Get("Authorization"))
	description, err := client.DescribeRun(ctx, describe)
	if err != nil {
		t.Fatal(err)
	}
	output.WriteString(protojson.Format(description.Msg))
	history := connect.NewRequest(&v1.GetHistoryRequest{RunId: "audit-private-run"})
	history.Header().Set("Authorization", auth.Get("Authorization"))
	events, err := client.GetHistory(ctx, history)
	if err != nil {
		t.Fatal(err)
	}
	output.WriteString(protojson.Format(events.Msg))
	for _, path := range []string{"/metrics", "/ui/", "/ui/app.js", "/ui/style.css"} {
		for _, authorized := range []bool{false, true} {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, address+path, nil)
			if err != nil {
				t.Fatal(err)
			}
			if authorized {
				req.Header = auth.Clone()
			}
			response, err := hc.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			want := http.StatusOK
			if path == "/metrics" && !authorized {
				want = http.StatusUnauthorized
			}
			if response.StatusCode != want {
				t.Fatalf("%s authorized=%v status=%d", path, authorized, response.StatusCode)
			}
			output.Write(body)
		}
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, command := range [][]string{{"describe", "audit-private-run"}, {"history", "audit-private-run", "--json"}, {"list"}} {
		args := append([]string{filepath.Join(root, "sdk/bin/capstan.mjs")}, command...)
		cmd := exec.CommandContext(ctx, "node", args...)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "CAPSTAN_ADDRESS="+address, "CAPSTAN_API_KEY="+key, "CAPSTAN_GATE_API_KEY="+cfg.GateAPIKey, "ANTHROPIC_API_KEY="+cfg.AnthropicAPIKey, "CAPSTAN_DATABASE_URL="+dsn)
		body, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("CLI %s: %v\n%s", command[0], err, body)
		}
		output.Write(body)
	}
	// Stop all writers before examining the server log buffer under the race detector.
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	done <- nil
	output.WriteString(logs.String())
	for _, secret := range []string{key, cfg.GateAPIKey, cfg.AnthropicAPIKey, dsn} {
		if strings.Contains(output.String(), secret) {
			t.Errorf("configured credential appeared in runtime surfaces")
		}
	}
	t.Logf("%d RPCs × 6 invalid-header cases rejected; metrics authenticated; viewer data RPCs guarded; history, logs, UI, metrics and three CLI commands contained no canaries", len(cases))
}
