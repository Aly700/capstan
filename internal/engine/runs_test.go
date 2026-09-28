package engine

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestStartRunWritesStartedAndSchedulesFirstTask(t *testing.T) {
	e, c, s := newTestEngine(t)
	r := mustStart(t, e, "r")
	if !r.Started || r.Run.Status != v1.RunStatus_RUN_STATUS_RUNNING || !r.Run.StartedAt.AsTime().Equal(c.Now()) {
		t.Fatalf("start: %v", r)
	}
	wantTypes(t, history(t, e, "r"), v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED)
	err := s.InTx(context.Background(), func(tx store.Tx) error {
		tasks, err := tx.RunTasks("r")
		if err != nil {
			return err
		}
		if len(tasks) != 1 || tasks[0].Attempt != 1 || !tasks[0].VisibleAt.Equal(c.Now()) {
			t.Fatalf("tasks: %+v", tasks)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
func TestStartRunIsIdempotent(t *testing.T) {
	e, _, _ := newTestEngine(t)
	var wg sync.WaitGroup
	results := make(chan bool, 32)
	for range 32 {
		wg.Go(func() {
			r, err := e.StartRun(context.Background(), "client", &v1.StartRunRequest{RunId: "r", WorkflowType: "flow", TaskQueue: "q"})
			if err != nil {
				t.Error(err)
				return
			}
			results <- r.Started
		})
	}
	wg.Wait()
	close(results)
	n := 0
	for started := range results {
		if started {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("created %d", n)
	}
	before := historyBytes(t, e, "r")
	if mustStart(t, e, "r").Started {
		t.Fatal("duplicate created")
	}
	_, err := e.StartRun(context.Background(), "", &v1.StartRunRequest{RunId: "r", WorkflowType: "other", TaskQueue: "q"})
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("different type: %v", err)
	}
	if !bytes.Equal(before, historyBytes(t, e, "r")) {
		t.Fatal("duplicate changed history")
	}
}
func TestStartRunValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*v1.StartRunRequest)
	}{
		{"id", func(r *v1.StartRunRequest) { r.RunId = "bad/id" }}, {"empty_id", func(r *v1.StartRunRequest) { r.RunId = "" }}, {"long_id", func(r *v1.StartRunRequest) { r.RunId = strings.Repeat("r", 201) }},
		{"type", func(r *v1.StartRunRequest) { r.WorkflowType = "" }}, {"queue", func(r *v1.StartRunRequest) { r.TaskQueue = "" }}, {"negative", func(r *v1.StartRunRequest) { r.RunTimeout = durationpb.New(-time.Second) }}, {"negative_task", func(r *v1.StartRunRequest) { r.TaskTimeout = durationpb.New(-time.Second) }}, {"large", func(r *v1.StartRunRequest) { r.TaskTimeout = durationpb.New(11 * time.Minute) }}, {"malformed", func(r *v1.StartRunRequest) { r.RunTimeout = &durationpb.Duration{Seconds: 1, Nanos: -1} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, _, _ := newTestEngine(t)
			req := &v1.StartRunRequest{RunId: "r", WorkflowType: "flow", TaskQueue: "q"}
			tc.edit(req)
			if _, err := e.StartRun(context.Background(), "", req); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("validation: %v", err)
			}
		})
	}
}
func TestDescribeRunCountsPendingWork(t *testing.T) {
	e, c, s := newTestEngine(t)
	mustStart(t, e, "r")
	err := s.InTx(context.Background(), func(tx store.Tx) error {
		if err := tx.InsertTask(&store.Task{RunID: "r", Kind: store.TaskActivity, TaskQueue: "q", Attempt: 1, ScheduledAt: c.Now(), VisibleAt: c.Now()}); err != nil {
			return err
		}
		if err := tx.InsertApproval(&store.Approval{RunID: "r", ApprovalID: "a", Source: v1.ApprovalSource_APPROVAL_SOURCE_HUMAN, Status: store.ApprovalPending, RequestedAt: c.Now()}); err != nil {
			return err
		}
		return tx.InsertAICall(&store.AICall{RunID: "r", Status: store.AICallReserved, EstimateUSD: .4, At: c.Now()})
	})
	if err != nil {
		t.Fatal(err)
	}
	r, err := e.DescribeRun(context.Background(), &v1.DescribeRunRequest{RunId: "r"})
	if err != nil {
		t.Fatal(err)
	}
	if r.Run.PendingActivities != 1 || r.Run.PendingApprovals != 1 || r.Run.CostUsd != .4 {
		t.Fatalf("describe: %v", r)
	}
	if _, err := e.DescribeRun(context.Background(), &v1.DescribeRunRequest{RunId: "missing"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
}
func TestListRunsFiltersAndPages(t *testing.T) {
	e, _, s := newTestEngine(t)
	for _, id := range []string{"c", "a", "b"} {
		mustStart(t, e, id)
	}
	if err := s.InTx(context.Background(), func(tx store.Tx) error {
		r, err := tx.GetRun("c", true)
		if err != nil {
			return err
		}
		r.Status = v1.RunStatus_RUN_STATUS_BLOCKED
		return tx.UpdateRun(r)
	}); err != nil {
		t.Fatal(err)
	}
	r, err := e.ListRuns(context.Background(), &v1.ListRunsRequest{PageSize: 1, Status: v1.RunStatus_RUN_STATUS_RUNNING, WorkflowType: "flow"})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Runs) != 1 || r.Runs[0].RunId != "a" || r.NextPageToken == "" {
		t.Fatalf("page1: %v", r)
	}
	r, err = e.ListRuns(context.Background(), &v1.ListRunsRequest{PageSize: 1, Status: v1.RunStatus_RUN_STATUS_RUNNING, WorkflowType: "flow", PageToken: r.NextPageToken})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Runs) != 1 || r.Runs[0].RunId != "b" || r.NextPageToken != "" {
		t.Fatalf("page2: %v", r)
	}
	r, err = e.ListRuns(context.Background(), &v1.ListRunsRequest{WorkflowType: "absent"})
	if err != nil || len(r.Runs) != 0 {
		t.Fatalf("filter: %v %v", r, err)
	}
}
func TestGetHistoryPages(t *testing.T) {
	e, _, _ := newTestEngine(t)
	mustStart(t, e, "r")
	r, err := e.GetHistory(context.Background(), &v1.GetHistoryRequest{RunId: "r", PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Events) != 1 || !r.More || r.Events[0].EventId != 1 {
		t.Fatalf("page1: %v", r)
	}
	r, err = e.GetHistory(context.Background(), &v1.GetHistoryRequest{RunId: "r", AfterEventId: 1, PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Events) != 1 || r.More || r.Events[0].EventId != 2 {
		t.Fatalf("page2: %v", r)
	}
	if _, err := e.GetHistory(context.Background(), &v1.GetHistoryRequest{RunId: "missing"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
}
