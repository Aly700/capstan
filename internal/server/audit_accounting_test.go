package server_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	rpc "github.com/Aly700/capstan/gen/capstan/v1/capstanv1connect"
	"github.com/Aly700/capstan/internal/config"
	"github.com/Aly700/capstan/internal/engine"
	"github.com/Aly700/capstan/internal/server"
	"github.com/Aly700/capstan/internal/store"
	"github.com/Aly700/capstan/internal/store/pgstore"
	"github.com/Aly700/capstan/internal/testpg"
	"google.golang.org/protobuf/types/known/durationpb"
)

type auditLedgerClock struct {
	mu sync.Mutex
	at time.Time
}

func (c *auditLedgerClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *auditLedgerClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(d)
}

type auditLedgerServer struct {
	worker rpc.WorkerServiceClient
	client rpc.ClientServiceClient
	store  store.Store
	clock  *auditLedgerClock
	url    string
}

func newAuditLedgerServer(t *testing.T, at time.Time, capUSD float64) *auditLedgerServer {
	t.Helper()
	s, err := pgstore.OpenWithMaxConns(t.Context(), testpg.New(t), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	clock := &auditLedgerClock{at: at}
	e, err := engine.New(engine.Deps{Store: s, Clock: clock}, engine.Config{DailyCapUSD: capUSD})
	if err != nil {
		t.Fatal(err)
	}
	h := server.New(config.Config{APIKeyHashes: map[string][32]byte{"audit": sha256.Sum256([]byte("audit-ledger-fixture"))}, PollTimeout: time.Second}, e, s, server.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	listener, err := net.Listen("tcp", "127.0.0.1:7640")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(30 * time.Second):
			t.Error("audit server did not stop")
		}
	})
	url := "http://" + listener.Addr().String()
	opts := []connect.ClientOption{connect.WithInterceptors(auditLedgerAuth())}
	return &auditLedgerServer{worker: rpc.NewWorkerServiceClient(http.DefaultClient, url, opts...), client: rpc.NewClientServiceClient(http.DefaultClient, url, opts...), store: s, clock: clock, url: url}
}

func auditLedgerAuth() connect.Interceptor {
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			req.Header().Set("Authorization", "Bearer audit-ledger-fixture")
			return next(ctx, req)
		}
	})
}

func (s *auditLedgerServer) activity(t *testing.T, runID string) []byte {
	t.Helper()
	_, err := s.client.StartRun(t.Context(), connect.NewRequest(&v1.StartRunRequest{RunId: runID, WorkflowType: "audit-accounting", TaskQueue: "audit-accounting"}))
	if err != nil {
		t.Fatal(err)
	}
	w, err := s.worker.PollWorkflowTask(t.Context(), connect.NewRequest(&v1.PollWorkflowTaskRequest{TaskQueue: "audit-accounting", Identity: "audit"}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.worker.CompleteWorkflowTask(t.Context(), connect.NewRequest(&v1.CompleteWorkflowTaskRequest{TaskToken: w.Msg.TaskToken, Commands: []*v1.Command{{Attributes: &v1.Command_ScheduleActivity{ScheduleActivity: &v1.ScheduleActivityCommand{Seq: 1, ActivityType: "capstan.model", StartToCloseTimeout: durationpb.New(time.Hour)}}}}}))
	if err != nil {
		t.Fatal(err)
	}
	a, err := s.worker.PollActivityTask(t.Context(), connect.NewRequest(&v1.PollActivityTaskRequest{TaskQueue: "audit-accounting", Identity: "audit"}))
	if err != nil || len(a.Msg.TaskToken) == 0 {
		t.Fatalf("poll activity: %v", err)
	}
	return a.Msg.TaskToken
}

func (s *auditLedgerServer) reserve(t *testing.T, token []byte, model string, estimate float64) *v1.ReserveAICallResponse {
	t.Helper()
	r, err := s.worker.ReserveAICall(t.Context(), connect.NewRequest(&v1.ReserveAICallRequest{TaskToken: token, Model: model, EstimateUsd: estimate}))
	if err != nil {
		t.Fatal(err)
	}
	return r.Msg
}

func TestAuditPostgresConcurrentAIReservations(t *testing.T) {
	s := newAuditLedgerServer(t, time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC), 1)
	var tokens [][]byte
	for i := range 16 {
		tokens = append(tokens, s.activity(t, fmt.Sprintf("concurrent-%d", i)))
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(tokens))
	for _, token := range tokens {
		wg.Go(func() {
			_, err := s.worker.ReserveAICall(t.Context(), connect.NewRequest(&v1.ReserveAICallRequest{TaskToken: token, Model: "claude-sonnet-5", EstimateUsd: .6}))
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	accepted, rejected := 0, 0
	for err := range errs {
		if err == nil {
			accepted++
		} else if connect.CodeOf(err) == connect.CodeResourceExhausted {
			rejected++
		} else {
			t.Fatal(err)
		}
	}
	if accepted != 1 || rejected != 15 {
		t.Fatalf("accepted=%d rejected=%d, want 1/15", accepted, rejected)
	}
	t.Logf("16 independent activity reservations at $0.60 against $1 cap: accepted=%d rejected=%d", accepted, rejected)
}

func TestAuditPostgresAIInvalidEstimates(t *testing.T) {
	s := newAuditLedgerServer(t, time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC), 1)
	token := s.activity(t, "invalid-estimates")
	for _, estimate := range []float64{-1, -.0000001, math.NaN(), math.Inf(1), math.Inf(-1)} {
		_, err := s.worker.ReserveAICall(t.Context(), connect.NewRequest(&v1.ReserveAICallRequest{TaskToken: token, EstimateUsd: estimate}))
		if connect.CodeOf(err) != connect.CodeResourceExhausted {
			t.Fatalf("estimate=%v err=%v", estimate, err)
		}
	}
	r := s.reserve(t, token, "unpriced", 1)
	if r.SpentTodayUsd != 1 {
		t.Fatalf("invalid inputs changed spend: %v", r)
	}
	t.Log("negative, sub-micro negative, NaN and both infinities rejected over RPC; ledger unchanged")
}

func TestAuditPostgresAIPricingAndRounding(t *testing.T) {
	s := newAuditLedgerServer(t, time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC), 100)
	token := s.activity(t, "pricing")
	for _, tc := range []struct {
		model    string
		estimate float64
		input    int64
		read     int64
		ok       bool
		unknown  bool
		want     float64
	}{
		{"unpriced", .1234565, 100, 0, true, false, .123457},
		{"claude-sonnet-5", 1, 0, 1, true, false, 0},
		{"claude-sonnet-5", 1, 0, 3, true, false, .000001},
		{"claude-fable-5-1", 1, 0, 2, true, false, .000001},
		{"claude-haiku-4-5-20251001", 1, 1_000_000, 0, true, false, 1},
		{"claude-opus-50", .2, 1_000_000, 0, true, false, .2},
		{"claude-sonnet-5", .7000005, 0, 0, false, true, .700001},
		{"claude-sonnet-5", .7, 1_000_000, 0, false, true, 2},
		{"claude-sonnet-5", .7, 0, 0, false, false, 0},
	} {
		r := s.reserve(t, token, tc.model, tc.estimate)
		request := &v1.FinishAICallRequest{ReservationId: r.ReservationId, InputTokens: tc.input, CacheReadTokens: tc.read, Ok: tc.ok, UsageUnknown: tc.unknown}
		for range 2 {
			finished, err := s.worker.FinishAICall(t.Context(), connect.NewRequest(request))
			if err != nil || finished.Msg.CostUsd != tc.want {
				t.Fatalf("%s: cost=%v err=%v want=%.6f", tc.model, finished, err, tc.want)
			}
		}
		t.Logf("model=%s estimate=%.7f input=%d read=%d ok=%v usage_unknown=%v cost=%.6f (repeat identical)", tc.model, tc.estimate, tc.input, tc.read, tc.ok, tc.unknown, tc.want)
	}
}

type auditLostReserveAck struct{ http.RoundTripper }

func (a auditLostReserveAck) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := a.RoundTripper.RoundTrip(req)
	if err == nil && strings.HasSuffix(req.URL.Path, "/ReserveAICall") {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return nil, errors.New("audit: reservation acknowledgement lost after commit")
	}
	return resp, err
}

func TestAuditPostgresLostReserveAckAndTorontoMidnight(t *testing.T) {
	for _, when := range []string{"2026-09-29T03:59:59Z", "2026-12-01T04:59:59Z", "2026-03-08T04:59:59Z", "2026-11-01T03:59:59Z"} {
		t.Run(when, func(t *testing.T) {
			at, err := time.Parse(time.RFC3339, when)
			if err != nil {
				t.Fatal(err)
			}
			s := newAuditLedgerServer(t, at, 1)
			token := s.activity(t, "lost-reserve")
			client := rpc.NewWorkerServiceClient(&http.Client{Transport: auditLostReserveAck{http.DefaultTransport}}, s.url, connect.WithInterceptors(auditLedgerAuth()))
			_, err = client.ReserveAICall(t.Context(), connect.NewRequest(&v1.ReserveAICallRequest{TaskToken: token, Model: "claude-sonnet-5", EstimateUsd: 1}))
			if err == nil {
				t.Fatal("lost acknowledgement appeared successful")
			}
			_, err = s.worker.ReserveAICall(t.Context(), connect.NewRequest(&v1.ReserveAICallRequest{TaskToken: token, EstimateUsd: .000001}))
			if connect.CodeOf(err) != connect.CodeResourceExhausted {
				t.Fatalf("orphan did not consume cap: %v", err)
			}
			s.clock.advance(time.Second)
			if r := s.reserve(t, token, "claude-sonnet-5", 1); r.SpentTodayUsd != 1 {
				t.Fatalf("new Toronto day did not reset cap: %v", r)
			}
			var total float64
			if err := s.store.InTx(t.Context(), func(tx store.Tx) error {
				var err error
				total, err = tx.RunCost("lost-reserve")
				return err
			}); err != nil || total != 2 {
				t.Fatalf("old reservation lost: total=%v err=%v", total, err)
			}
			t.Logf("%s: dropped post-commit ack consumes $1 until Toronto midnight; next day's $1 accepted; durable total=$2", when)
		})
	}
}

func TestAuditPostgresAIUnderestimateObservation(t *testing.T) {
	s := newAuditLedgerServer(t, time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC), 1)
	token := s.activity(t, "underestimate")
	r := s.reserve(t, token, "claude-sonnet-5", 0)
	finished, err := s.worker.FinishAICall(t.Context(), connect.NewRequest(&v1.FinishAICallRequest{ReservationId: r.ReservationId, Ok: true, InputTokens: 1_000_000}))
	if err != nil || finished.Msg.CostUsd != 2 {
		t.Fatalf("server must retain actual billed cost: %v %v", finished, err)
	}
	info, err := s.client.DescribeRun(t.Context(), connect.NewRequest(&v1.DescribeRunRequest{RunId: "underestimate"}))
	if err != nil || info.Msg.Run.CostUsd != 2 {
		t.Fatalf("actual run cost missing: %v %v", info, err)
	}
	t.Log("LIMITATION: $1 cap accepted a $0 reservation then recorded $2 of actual usage; reservation cap is not an unconditional provider-spend cap")
}
