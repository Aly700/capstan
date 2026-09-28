package labworker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type workerServiceFake struct {
	poll     func(context.Context, *capstanv1.PollWorkflowTaskRequest) (*capstanv1.PollWorkflowTaskResponse, bool, error)
	complete func(context.Context, *capstanv1.CompleteWorkflowTaskRequest) (*capstanv1.CompleteWorkflowTaskResponse, error)
	fail     func(context.Context, *capstanv1.FailWorkflowTaskRequest) (*capstanv1.FailWorkflowTaskResponse, error)
}

func (f workerServiceFake) PollWorkflowTask(ctx context.Context, req *capstanv1.PollWorkflowTaskRequest) (*capstanv1.PollWorkflowTaskResponse, bool, error) {
	return f.poll(ctx, req)
}

func (f workerServiceFake) CompleteWorkflowTask(ctx context.Context, req *capstanv1.CompleteWorkflowTaskRequest) (*capstanv1.CompleteWorkflowTaskResponse, error) {
	return f.complete(ctx, req)
}

func (f workerServiceFake) FailWorkflowTask(ctx context.Context, req *capstanv1.FailWorkflowTaskRequest) (*capstanv1.FailWorkflowTaskResponse, error) {
	return f.fail(ctx, req)
}

func TestWorkerPollForwardsConfigurationAndResult(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	task := workerTask("example", `{"value":7}`)
	pollErr := errors.New("poll interrupted")
	for _, test := range []struct {
		name  string
		task  *capstanv1.PollWorkflowTaskResponse
		found bool
		err   error
	}{
		{name: "task", task: task, found: true},
		{name: "empty"},
		{name: "error", err: pollErr},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			service := workerServiceFake{poll: func(gotCtx context.Context, req *capstanv1.PollWorkflowTaskRequest) (*capstanv1.PollWorkflowTaskResponse, bool, error) {
				calls++
				if gotCtx != ctx {
					t.Fatal("poll context changed")
				}
				want := &capstanv1.PollWorkflowTaskRequest{TaskQueue: "lab", Identity: "worker-2", BuildId: "build-a"}
				if !proto.Equal(req, want) {
					t.Fatalf("poll request = %v, want %v", req, want)
				}
				return test.task, test.found, test.err
			}}
			worker := NewWorker(service, "lab", "worker-2", "build-a", nil)
			got, found, err := worker.Poll(ctx)
			if got != test.task || found != test.found || err != test.err || calls != 1 {
				t.Fatalf("Poll() = (%v, %v, %v), calls = %d", got, found, err, calls)
			}
		})
	}
}

func TestWorkerExecuteReplaysFromHistoryWithoutResponding(t *testing.T) {
	starts := 0
	workflows := map[string]Workflow{"example": func(_ *Context, input any) (any, error) {
		starts++
		return input, nil
	}}
	worker := NewWorker(workerServiceFake{}, "lab", "worker-2", "build-a", workflows)
	delete(workflows, "example")
	task := workerTask("example", `{"value":7}`)
	original := proto.Clone(task)
	for i := 1; i <= 2; i++ {
		result := worker.Execute(task)
		if result.Failure != nil || result.Cause != capstanv1.TaskFailedCause_TASK_FAILED_CAUSE_UNSPECIFIED {
			t.Fatalf("Execute() failed: %+v", result)
		}
		if starts != i {
			t.Fatalf("scenario starts = %d, want %d", starts, i)
		}
		want := &capstanv1.Command{Attributes: &capstanv1.Command_CompleteRun{CompleteRun: &capstanv1.CompleteRunCommand{Result: &capstanv1.Payload{ContentType: "application/json", Data: []byte(`{"value":7}`)}}}}
		if len(result.Commands) != 1 || !proto.Equal(result.Commands[0], want) {
			t.Fatalf("commands = %v, want [%v]", result.Commands, want)
		}
		if !proto.Equal(task, original) {
			t.Fatal("Execute mutated the task history")
		}
	}
}

func TestWorkerExecuteClassifiesTaskFailures(t *testing.T) {
	for _, test := range []struct {
		name    string
		task    *capstanv1.PollWorkflowTaskResponse
		cause   capstanv1.TaskFailedCause
		message string
	}{
		{name: "unknown workflow", task: workerTask("absent", `null`), cause: capstanv1.TaskFailedCause_TASK_FAILED_CAUSE_SDK_ERROR, message: "absent"},
		{name: "nil task", cause: capstanv1.TaskFailedCause_TASK_FAILED_CAUSE_SDK_ERROR, message: "task"},
		{name: "invalid history", task: &capstanv1.PollWorkflowTaskResponse{RunId: "run-1", WorkflowType: "example", TaskToken: []byte("token")}, cause: capstanv1.TaskFailedCause_TASK_FAILED_CAUSE_SDK_ERROR, message: "history"},
		{name: "history mismatch", task: workerMismatchTask(), cause: capstanv1.TaskFailedCause_TASK_FAILED_CAUSE_HISTORY_MISMATCH, message: "5"},
	} {
		t.Run(test.name, func(t *testing.T) {
			worker := NewWorker(workerServiceFake{}, "lab", "worker-2", "build-a", map[string]Workflow{"example": func(*Context, any) (any, error) { return "done", nil }})
			result := worker.Execute(test.task)
			if result.Cause != test.cause || result.Failure == nil || len(result.Commands) != 0 {
				t.Fatalf("Execute() = %+v, want cause %v without commands", result, test.cause)
			}
			if !strings.Contains(strings.ToLower(result.Failure.Message), test.message) {
				t.Fatalf("failure message = %q, want %q", result.Failure.Message, test.message)
			}
			if test.cause == capstanv1.TaskFailedCause_TASK_FAILED_CAUSE_HISTORY_MISMATCH && result.Failure.Type != "HistoryMismatchError" {
				t.Fatalf("failure type = %q", result.Failure.Type)
			}
		})
	}
}

func TestWorkerExecuteScenarioErrorBecomesFailRun(t *testing.T) {
	worker := NewWorker(workerServiceFake{}, "lab", "worker-2", "build-a", map[string]Workflow{"example": func(*Context, any) (any, error) { return nil, errors.New("scenario failed") }})
	result := worker.Execute(workerTask("example", `null`))
	if result.Failure != nil || result.Cause != capstanv1.TaskFailedCause_TASK_FAILED_CAUSE_UNSPECIFIED || len(result.Commands) != 1 {
		t.Fatalf("Execute() = %+v, want one FailRun command", result)
	}
	if failure := result.Commands[0].GetFailRun().GetFailure(); failure == nil || failure.Message != "scenario failed" {
		t.Fatalf("FailRun failure = %v", failure)
	}
}

func TestWorkerRespondCompletesAndAllowsDuplicateSubmission(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	task := workerTask("example", `null`)
	command := &capstanv1.Command{Attributes: &capstanv1.Command_CompleteRun{CompleteRun: &capstanv1.CompleteRunCommand{}}}
	result := Result{Commands: []*capstanv1.Command{command}}
	lateErr := errors.New("stale task token")
	calls := 0
	service := workerServiceFake{complete: func(gotCtx context.Context, req *capstanv1.CompleteWorkflowTaskRequest) (*capstanv1.CompleteWorkflowTaskResponse, error) {
		calls++
		if gotCtx != ctx {
			t.Fatal("completion context changed")
		}
		want := &capstanv1.CompleteWorkflowTaskRequest{TaskToken: task.TaskToken, Commands: result.Commands, Identity: "worker-2", BuildId: "build-a"}
		if !proto.Equal(req, want) {
			t.Fatalf("completion request = %v, want %v", req, want)
		}
		if calls > 1 {
			return nil, lateErr
		}
		return &capstanv1.CompleteWorkflowTaskResponse{}, nil
	}}
	worker := NewWorker(service, "lab", "worker-2", "build-a", nil)
	if err := worker.Respond(ctx, task, result); err != nil {
		t.Fatal(err)
	}
	if err := worker.Respond(ctx, task, result); err != lateErr {
		t.Fatalf("duplicate completion error = %v, want %v", err, lateErr)
	}
	if calls != 2 {
		t.Fatalf("completion calls = %d, want 2", calls)
	}
}

func TestWorkerRespondFailureForwardsCauseAndServiceError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	task := workerTask("example", `null`)
	for _, cause := range []capstanv1.TaskFailedCause{capstanv1.TaskFailedCause_TASK_FAILED_CAUSE_HISTORY_MISMATCH, capstanv1.TaskFailedCause_TASK_FAILED_CAUSE_SDK_ERROR} {
		t.Run(cause.String(), func(t *testing.T) {
			result := Result{Cause: cause, Failure: &capstanv1.Failure{Type: "test failure", Message: "at event 5"}}
			serviceErr := errors.New("failure response lost")
			calls := 0
			worker := NewWorker(workerServiceFake{fail: func(gotCtx context.Context, req *capstanv1.FailWorkflowTaskRequest) (*capstanv1.FailWorkflowTaskResponse, error) {
				calls++
				if gotCtx != ctx {
					t.Fatal("failure context changed")
				}
				want := &capstanv1.FailWorkflowTaskRequest{TaskToken: task.TaskToken, Cause: cause, Failure: result.Failure, Identity: "worker-2"}
				if !proto.Equal(req, want) {
					t.Fatalf("failure request = %v, want %v", req, want)
				}
				return nil, serviceErr
			}}, "lab", "worker-2", "build-a", nil)
			if err := worker.Respond(ctx, task, result); err != serviceErr || calls != 1 {
				t.Fatalf("Respond() = %v, calls = %d", err, calls)
			}
		})
	}
}

func workerTask(workflowType, input string) *capstanv1.PollWorkflowTaskResponse {
	task := &capstanv1.PollWorkflowTaskResponse{
		TaskToken: []byte("opaque-task-token"), RunId: "run-1", WorkflowType: workflowType, Attempt: 1,
		History: []*capstanv1.HistoryEvent{
			{EventId: 1, Type: capstanv1.EventType_EVENT_TYPE_RUN_STARTED, Attributes: &capstanv1.HistoryEvent_RunStarted{RunStarted: &capstanv1.RunStartedAttributes{WorkflowType: workflowType, TaskQueue: "lab", Input: &capstanv1.Payload{ContentType: "application/json", Data: []byte(input)}}}},
			{EventId: 2, Type: capstanv1.EventType_EVENT_TYPE_TASK_SCHEDULED, Attributes: &capstanv1.HistoryEvent_TaskScheduled{TaskScheduled: &capstanv1.TaskScheduledAttributes{TaskQueue: "lab", Attempt: 1}}},
			{EventId: 3, Type: capstanv1.EventType_EVENT_TYPE_TASK_STARTED, Attributes: &capstanv1.HistoryEvent_TaskStarted{TaskStarted: &capstanv1.TaskStartedAttributes{ScheduledEventId: 2, Identity: "worker-2"}}},
		},
	}
	for _, event := range task.History {
		event.Time = timestamppb.New(time.Unix(1, 0))
	}
	return task
}

func workerMismatchTask() *capstanv1.PollWorkflowTaskResponse {
	task := workerTask("example", `null`)
	task.History = append(task.History,
		&capstanv1.HistoryEvent{EventId: 4, Type: capstanv1.EventType_EVENT_TYPE_TASK_COMPLETED, Attributes: &capstanv1.HistoryEvent_TaskCompleted{TaskCompleted: &capstanv1.TaskCompletedAttributes{ScheduledEventId: 2, StartedEventId: 3}}},
		&capstanv1.HistoryEvent{EventId: 5, Type: capstanv1.EventType_EVENT_TYPE_ACTIVITY_SCHEDULED, Attributes: &capstanv1.HistoryEvent_ActivityScheduled{ActivityScheduled: &capstanv1.ActivityScheduledAttributes{Seq: 1, ActivityType: "recorded-step", TaskCompletedEventId: 4}}},
		&capstanv1.HistoryEvent{EventId: 6, Type: capstanv1.EventType_EVENT_TYPE_TASK_SCHEDULED, Attributes: &capstanv1.HistoryEvent_TaskScheduled{TaskScheduled: &capstanv1.TaskScheduledAttributes{TaskQueue: "lab", Attempt: 1}}},
		&capstanv1.HistoryEvent{EventId: 7, Type: capstanv1.EventType_EVENT_TYPE_TASK_STARTED, Attributes: &capstanv1.HistoryEvent_TaskStarted{TaskStarted: &capstanv1.TaskStartedAttributes{ScheduledEventId: 6, Identity: "worker-2"}}},
	)
	for _, event := range task.History {
		event.Time = timestamppb.New(time.Unix(1, 0))
	}
	return task
}
