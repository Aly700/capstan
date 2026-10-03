package labworker

import (
	"strings"
	"testing"
	"time"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
			worker := NewWorker(nil, "lab", "worker-2", "build-a", map[string]Workflow{"example": func(*Context, any) (any, error) { return "done", nil }})
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
