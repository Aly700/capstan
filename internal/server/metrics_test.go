package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	rpc "github.com/Aly700/capstan/gen/capstan/v1/capstanv1connect"
	"github.com/Aly700/capstan/internal/engine"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/proto"
)

func scrape(t *testing.T, s *Server) string {
	t.Helper()
	r := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	req.Header.Set("Authorization", "Bearer secret")
	s.ServeHTTP(r, req)
	if r.Code != 200 || r.Header().Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatalf("metrics status=%d content-type=%q", r.Code, r.Header().Get("Content-Type"))
	}
	return r.Body.String()
}
func TestRPCMetricsIncludeErrorsAndKeepLabelsBounded(t *testing.T) {
	f := &fakeAPI{call: func(_ context.Context, _, _ string, req proto.Message) (proto.Message, bool, error) {
		if req.(*v1.DescribeRunRequest).RunId == "missing" {
			return nil, false, engine.ErrNotFound
		}
		return &v1.DescribeRunResponse{Run: closedRun()}, true, nil
	}}
	s := New(testConfig(), f, nil, Options{})
	hc, url := rpcHTTP(t, s)
	c := rpc.NewClientServiceClient(hc, url)
	for _, id := range []string{"r", "missing", "unauthenticated"} {
		req := connect.NewRequest(&v1.DescribeRunRequest{RunId: id})
		if id != "unauthenticated" {
			req.Header().Set("Authorization", "Bearer secret")
		}
		_, _ = c.DescribeRun(context.Background(), req)
	}
	for _, path := range []string{"/capstan.v1.ClientService/user-controlled-1", "/capstan.v1.ClientService/user-controlled-2", "/unknown"} {
		r := httptest.NewRecorder()
		s.ServeHTTP(r, httptest.NewRequest("GET", path, nil))
	}
	body := scrape(t, s)
	for _, code := range []string{"ok", "not_found", "unauthenticated"} {
		want := `capstan_rpc_total{procedure="` + rpc.ClientServiceDescribeRunProcedure + `",code="` + code + `"} 1`
		if !strings.Contains(body, want) {
			t.Errorf("missing %s", want)
		}
	}
	for _, want := range []string{`capstan_rpc_seconds_count{procedure="` + rpc.ClientServiceDescribeRunProcedure + `"} 3`, `capstan_rpc_seconds_sum{procedure="` + rpc.ClientServiceDescribeRunProcedure + `"}`, "# TYPE capstan_rpc_total counter", "# TYPE capstan_long_poll_waiting gauge"} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %s", want)
		}
	}
	for _, bad := range []string{"user-controlled", "secret", "run_id=", "queue=", "identity="} {
		if strings.Contains(body, bad) {
			t.Errorf("unbounded/private label: %s", bad)
		}
	}
	// Concurrent scrapes and requests also run under the race detector.
	done := make(chan struct{}, 10)
	for range 10 {
		go func() {
			defer func() { done <- struct{}{} }()
			req, _ := http.NewRequest(http.MethodGet, url+"/metrics", nil)
			req.Header.Set("Authorization", "Bearer secret")
			r, err := hc.Do(req)
			if err == nil {
				r.Body.Close()
			}
		}()
	}
	for range 10 {
		<-done
	}
}
func TestLoopAndWaitingMetrics(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := &loopAPI{wake: func(context.Context) (time.Time, bool, error) { return time.Time{}, false, nil }}
		s := New(testConfig(), f, nil, Options{})
		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		s.runLoop(ctx, "timers", func(context.Context, int) (int, error) {
			calls++
			if calls == 1 {
				return 100, nil
			}
			cancel()
			return 7, nil
		})
		if body := scrape(t, s); !strings.Contains(body, `capstan_loop_items_total{loop="timers"} 107`) {
			t.Fatal(body)
		}
		for _, kind := range []store.TaskKind{store.TaskWorkflow, store.TaskActivity} {
			api := &fakeAPI{call: func(context.Context, string, string, proto.Message) (proto.Message, bool, error) {
				return nil, false, nil
			}}
			s := New(testConfig(), api, nil, Options{})
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { defer close(done); _, _ = poll(s, ctx, kind) }()
			synctest.Wait()
			if body := scrape(t, s); !strings.Contains(body, `capstan_long_poll_waiting{kind="`+kind.String()+`"} 1`) {
				t.Fatal(body)
			}
			cancel()
			<-done
			if body := scrape(t, s); !strings.Contains(body, `capstan_long_poll_waiting{kind="`+kind.String()+`"} 0`) {
				t.Fatal(body)
			}
		}
	})
}

var _ http.Handler = (*Server)(nil)
