package server_test

import (
	"context"
	"crypto/sha256"
	"net"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	rpc "github.com/Aly700/capstan/gen/capstan/v1/capstanv1connect"
	"github.com/Aly700/capstan/internal/config"
	"github.com/Aly700/capstan/internal/engine"
	"github.com/Aly700/capstan/internal/server"
	"github.com/Aly700/capstan/internal/store/pgstore"
	"github.com/Aly700/capstan/internal/testpg"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Exercise the real HTTP poll against PostgreSQL while the oldest run is busy.
// An available task behind it must be claimable without waiting for that lock.
func TestAuditPollProgressPastBusyRun(t *testing.T) {
	for _, activity := range []bool{false, true} {
		name := "workflow"
		if activity {
			name = "activity"
		}
		t.Run(name, func(t *testing.T) {
			dsn := testpg.New(t)
			s, err := pgstore.Open(t.Context(), dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			e, err := engine.New(engine.Deps{Store: s}, engine.Config{})
			if err != nil {
				t.Fatal(err)
			}
			const key = "audit-test-key"
			h := server.New(config.Config{APIKeyHashes: map[string][32]byte{"audit": sha256.Sum256([]byte(key))}, PollTimeout: 2 * time.Second}, e, s, server.Options{})
			listener, err := net.Listen("tcp", "127.0.0.1:7600")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- h.Serve(ctx, listener) }()
			defer func() {
				cancel()
				if err := <-done; err != nil {
					t.Error(err)
				}
			}()
			auth := connect.WithInterceptors(connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
				return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
					req.Header().Set("Authorization", "Bearer "+key)
					return next(ctx, req)
				}
			}))
			address := "http://" + listener.Addr().String()
			client := rpc.NewClientServiceClient(http.DefaultClient, address, auth)
			worker := rpc.NewWorkerServiceClient(http.DefaultClient, address, auth)
			for _, id := range []string{"busy", "available"} {
				if _, err := client.StartRun(t.Context(), connect.NewRequest(&v1.StartRunRequest{RunId: id, WorkflowType: "audit", TaskQueue: name})); err != nil {
					t.Fatal(err)
				}
				if activity {
					w, err := worker.PollWorkflowTask(t.Context(), connect.NewRequest(&v1.PollWorkflowTaskRequest{TaskQueue: name}))
					if err != nil {
						t.Fatal(err)
					}
					_, err = worker.CompleteWorkflowTask(t.Context(), connect.NewRequest(&v1.CompleteWorkflowTaskRequest{TaskToken: w.Msg.TaskToken, Commands: []*v1.Command{{Attributes: &v1.Command_ScheduleActivity{ScheduleActivity: &v1.ScheduleActivityCommand{Seq: 1, ActivityType: "effect", StartToCloseTimeout: durationpb.New(time.Minute)}}}}}))
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			conn, err := pgx.Connect(t.Context(), dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close(context.Background())
			lock, err := conn.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer lock.Rollback(context.Background())
			if _, err := lock.Exec(t.Context(), "select 1 from run where run_id='busy' for update"); err != nil {
				t.Fatal(err)
			}
			poll := func() string {
				t.Helper()
				if activity {
					r, err := worker.PollActivityTask(t.Context(), connect.NewRequest(&v1.PollActivityTaskRequest{TaskQueue: name}))
					if err != nil {
						t.Fatal(err)
					}
					return r.Msg.RunId
				}
				r, err := worker.PollWorkflowTask(t.Context(), connect.NewRequest(&v1.PollWorkflowTaskRequest{TaskQueue: name}))
				if err != nil {
					t.Fatal(err)
				}
				return r.Msg.RunId
			}
			began := time.Now()
			got := poll()
			elapsed := time.Since(began)
			if got != "available" {
				t.Fatalf("poll returned %q after %v while available task waited behind busy run", got, elapsed)
			}
			if elapsed > time.Second {
				t.Fatalf("available task waited %v", elapsed)
			}
			if err := lock.Rollback(t.Context()); err != nil {
				t.Fatal(err)
			}
			if got := poll(); got != "busy" {
				t.Fatalf("unlocked task starved: %q", got)
			}
			t.Logf("available task claimed in %v; formerly busy task claimed after release", elapsed)
		})
	}
}
