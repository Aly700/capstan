package engine

import (
	"bytes"
	"context"
	"errors"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/proto"
	"math"
	"testing"
	"time"
)

func TestWorkflowRetryAttemptSaturates(t *testing.T) {
	e, _, s := newTestEngine(t)
	mustStart(t, e, "r")
	p := mustPoll(t, e)
	tok, _ := DecodeToken(p.TaskToken)
	if err := s.InTx(context.Background(), func(tx store.Tx) error {
		task, err := tx.GetTask(tok.TaskId, true)
		if err != nil {
			return err
		}
		task.Attempt = math.MaxInt32
		return tx.UpdateTask(task)
	}); err != nil {
		t.Fatal(err)
	}
	tok.Attempt = math.MaxInt32
	if _, err := e.FailWorkflowTask(context.Background(), &v1.FailWorkflowTaskRequest{TaskToken: EncodeToken(tok), Cause: v1.TaskFailedCause_TASK_FAILED_CAUSE_SDK_ERROR}); err != nil {
		t.Fatal(err)
	}
	h := historyEvents(t, e, "r")
	if h[len(h)-1].GetTaskScheduled().Attempt != math.MaxInt32 {
		t.Fatalf("retry attempt overflow: %v", h[len(h)-1])
	}
}

func TestPollReturnsHistoryAndMarksInFlight(t *testing.T) {
	e, c, s := newTestEngine(t)
	mustStart(t, e, "r")
	p := mustPoll(t, e)
	wantTypes(t, history(t, e, "r"), v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED)
	tok, err := DecodeToken(p.TaskToken)
	if err != nil {
		t.Fatal(err)
	}
	if tok.StartedEventId != 3 || tok.ScheduledEventId != 2 || tok.Attempt != 1 || tok.Kind != v1.TaskKind_TASK_KIND_WORKFLOW || len(p.History) != 3 {
		t.Fatalf("poll %v token %v", p, tok)
	}
	if err := s.InTx(context.Background(), func(tx store.Tx) error {
		r, err := tx.GetRun("r", false)
		if err != nil {
			return err
		}
		task, err := tx.GetTask(tok.TaskId, false)
		if err != nil {
			return err
		}
		if !r.InFlight || r.WorkflowTaskID != task.ID || !task.LeasedUntil.Equal(c.Now().Add(10*time.Second)) || !task.CheckAt.Equal(task.LeasedUntil) {
			t.Fatalf("run %+v task %+v", r, task)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := e.PollWorkflowTask(context.Background(), &v1.PollWorkflowTaskRequest{TaskQueue: "q"}); err != nil || found {
		t.Fatalf("second poll %v %v", found, err)
	}
}
func TestPollSkipsTasksOfClosedRuns(t *testing.T) {
	e, _, s := newTestEngine(t)
	mustStart(t, e, "r")
	if err := s.InTx(context.Background(), func(tx store.Tx) error {
		r, err := tx.GetRun("r", true)
		if err != nil {
			return err
		}
		r.Status = v1.RunStatus_RUN_STATUS_COMPLETED
		return tx.UpdateRun(r)
	}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := e.PollWorkflowTask(context.Background(), &v1.PollWorkflowTaskRequest{TaskQueue: "q"}); err != nil || found {
		t.Fatalf("poll %v %v", found, err)
	}
	wantTypes(t, history(t, e, "r"), v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED)
}
func TestCompleteAppendsCommandEventsInOrder(t *testing.T) {
	e, _, _ := newTestEngine(t)
	mustStart(t, e, "r")
	p := mustPoll(t, e)
	mustComplete(t, e, p.TaskToken, activityCmd(1), timerCmd(2, time.Minute), markerCmd(3))
	wantTypes(t, history(t, e, "r"), v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED, v1.EventType_EVENT_TYPE_TASK_COMPLETED, v1.EventType_EVENT_TYPE_ACTIVITY_SCHEDULED, v1.EventType_EVENT_TYPE_TIMER_STARTED, v1.EventType_EVENT_TYPE_MARKER_RECORDED)
	h := historyEvents(t, e, "r")
	if h[4].GetActivityScheduled().TaskCompletedEventId != 4 || h[5].GetTimerStarted().TaskCompletedEventId != 4 || h[6].GetMarkerRecorded().TaskCompletedEventId != 4 || h[3].GetTaskCompleted().BuildId != "build" {
		t.Fatal("command linkage")
	}
}
func TestCompleteWithoutNewInformationLeavesNoTask(t *testing.T) {
	e, _, s := newTestEngine(t)
	mustStart(t, e, "r")
	mustComplete(t, e, mustPoll(t, e).TaskToken, activityCmd(1))
	if err := s.InTx(context.Background(), func(tx store.Tx) error {
		r, err := tx.GetRun("r", false)
		if err != nil {
			return err
		}
		if r.WorkflowTaskID != 0 || r.InFlight {
			t.Fatalf("run %+v", r)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
func TestWorkflowCompletionAfterLeaseExpiryIsStale(t *testing.T) {
	for _, reap := range []bool{false, true} {
		t.Run(map[bool]string{false: "before_reaper", true: "after_reaper"}[reap], func(t *testing.T) {
			e, c, _ := newTestEngine(t)
			mustStart(t, e, "r")
			p := mustPoll(t, e)
			c.Advance(10 * time.Second)
			if reap {
				if n, err := e.ProcessDueTasks(context.Background(), 1); err != nil || n != 1 {
					t.Fatalf("reap %d %v", n, err)
				}
			}
			before := historyBytes(t, e, "r")
			_, err := e.CompleteWorkflowTask(context.Background(), &v1.CompleteWorkflowTaskRequest{TaskToken: p.TaskToken})
			if !errors.Is(err, ErrStaleTask) {
				t.Fatalf("late completion: %v", err)
			}
			if !bytes.Equal(before, historyBytes(t, e, "r")) {
				t.Fatal("stale changed history")
			}
		})
	}
}
func TestWorkflowTokenValidatesEveryField(t *testing.T) {
	for _, field := range []string{"kind", "run", "task", "attempt", "scheduled", "started", "seq"} {
		t.Run(field, func(t *testing.T) {
			e, _, _ := newTestEngine(t)
			mustStart(t, e, "r")
			p := mustPoll(t, e)
			tok, _ := DecodeToken(p.TaskToken)
			switch field {
			case "kind":
				tok.Kind = v1.TaskKind_TASK_KIND_ACTIVITY
			case "run":
				tok.RunId = "other"
			case "task":
				tok.TaskId++
			case "attempt":
				tok.Attempt++
			case "scheduled":
				tok.ScheduledEventId++
			case "started":
				tok.StartedEventId++
			case "seq":
				tok.Seq++
			}
			before := historyBytes(t, e, "r")
			for _, fail := range []bool{false, true} {
				var err error
				if fail {
					_, err = e.FailWorkflowTask(context.Background(), &v1.FailWorkflowTaskRequest{TaskToken: EncodeToken(tok), Cause: v1.TaskFailedCause_TASK_FAILED_CAUSE_SDK_ERROR})
				} else {
					_, err = e.CompleteWorkflowTask(context.Background(), &v1.CompleteWorkflowTaskRequest{TaskToken: EncodeToken(tok)})
				}
				if !errors.Is(err, ErrStaleTask) {
					t.Fatalf("stale: %v", err)
				}
				if !bytes.Equal(before, historyBytes(t, e, "r")) {
					t.Fatal("stale changed history")
				}
			}
		})
	}
}
func TestFailWorkflowTaskSdkErrorRetriesWithBackoff(t *testing.T) {
	e, c, _ := newTestEngine(t)
	mustStart(t, e, "r")
	p := mustPoll(t, e)
	_, err := e.FailWorkflowTask(context.Background(), &v1.FailWorkflowTaskRequest{TaskToken: p.TaskToken, Cause: v1.TaskFailedCause_TASK_FAILED_CAUSE_SDK_ERROR, Failure: &v1.Failure{Type: "SDK"}})
	if err != nil {
		t.Fatal(err)
	}
	wantTypes(t, history(t, e, "r"), v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED, v1.EventType_EVENT_TYPE_TASK_FAILED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED)
	if _, found, err := e.PollWorkflowTask(context.Background(), &v1.PollWorkflowTaskRequest{TaskQueue: "q"}); err != nil || found {
		t.Fatalf("early %v %v", found, err)
	}
	c.Advance(time.Second)
	if p := mustPoll(t, e); p.Attempt != 2 {
		t.Fatalf("attempt %d", p.Attempt)
	}
}
func TestFailWorkflowTaskMismatchBlocksRun(t *testing.T) {
	e, _, _ := newTestEngine(t)
	mustStart(t, e, "r")
	p := mustPoll(t, e)
	f := &v1.Failure{Type: "HistoryMismatch", Message: "changed"}
	_, err := e.FailWorkflowTask(context.Background(), &v1.FailWorkflowTaskRequest{TaskToken: p.TaskToken, Cause: v1.TaskFailedCause_TASK_FAILED_CAUSE_HISTORY_MISMATCH, Failure: f})
	if err != nil {
		t.Fatal(err)
	}
	wantTypes(t, history(t, e, "r"), v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED, v1.EventType_EVENT_TYPE_TASK_FAILED, v1.EventType_EVENT_TYPE_RUN_BLOCKED)
	r, err := e.DescribeRun(context.Background(), &v1.DescribeRunRequest{RunId: "r"})
	if err != nil || r.Run.Status != v1.RunStatus_RUN_STATUS_BLOCKED || !proto.Equal(r.Run.Failure, f) {
		t.Fatalf("blocked %v %v", r, err)
	}
}
