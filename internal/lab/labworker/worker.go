package labworker

import (
	"context"
	"errors"
	"fmt"
	"maps"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
)

// WorkflowService is the engine API needed by the workflow driver. Polling claims
// at most one task and never blocks waiting for a task to become available.
type WorkflowService interface {
	PollWorkflowTask(context.Context, *capstanv1.PollWorkflowTaskRequest) (*capstanv1.PollWorkflowTaskResponse, bool, error)
	CompleteWorkflowTask(context.Context, *capstanv1.CompleteWorkflowTaskRequest) (*capstanv1.CompleteWorkflowTaskResponse, error)
	FailWorkflowTask(context.Context, *capstanv1.FailWorkflowTaskRequest) (*capstanv1.FailWorkflowTaskResponse, error)
}

// Worker drives a task one phase at a time. It holds only configuration: the lab
// owns polled tasks and results, so it can discard or repeat any phase after a fault.
type Worker struct {
	service   WorkflowService
	taskQueue string
	identity  string
	buildID   string
	workflows map[string]Workflow
}

// Result contains either commands to complete a task or a task failure. An empty
// command list with no failure completes a task whose workflow is still waiting.
type Result struct {
	Commands []*capstanv1.Command
	Cause    capstanv1.TaskFailedCause
	Failure  *capstanv1.Failure
}

// NewWorker snapshots the registry. Scenario functions must keep their execution
// state local so a fresh worker can reconstruct the run from history alone.
func NewWorker(service WorkflowService, taskQueue, identity, buildID string, workflows map[string]Workflow) *Worker {
	return &Worker{
		service: service, taskQueue: taskQueue, identity: identity, buildID: buildID,
		workflows: maps.Clone(workflows),
	}
}

// Poll claims a task without executing workflow code or responding to the engine.
func (w *Worker) Poll(ctx context.Context) (*capstanv1.PollWorkflowTaskResponse, bool, error) {
	return w.service.PollWorkflowTask(ctx, &capstanv1.PollWorkflowTaskRequest{
		TaskQueue: w.taskQueue, Identity: w.identity, BuildId: w.buildID,
	})
}

// Execute replays a task from its history without making a service call. Replay
// turns scenario errors into FailRun commands; only replay errors fail the task.
func (w *Worker) Execute(task *capstanv1.PollWorkflowTaskResponse) Result {
	if task == nil {
		return taskFailure(errors.New("cannot execute a nil workflow task"))
	}
	workflow := w.workflows[task.WorkflowType]
	if workflow == nil {
		return taskFailure(fmt.Errorf("unknown workflow %q", task.WorkflowType))
	}
	commands, err := Replay(task.RunId, task.History, workflow)
	if err != nil {
		return taskFailure(err)
	}
	return Result{Commands: commands}
}

// Respond sends one service request for a previously produced result. It does not retry
// or remember tokens: duplicate and late submissions reach the engine unchanged.
func (w *Worker) Respond(ctx context.Context, task *capstanv1.PollWorkflowTaskResponse, result Result) error {
	if result.Failure != nil {
		_, err := w.service.FailWorkflowTask(ctx, &capstanv1.FailWorkflowTaskRequest{
			TaskToken: task.GetTaskToken(), Cause: result.Cause, Failure: result.Failure, Identity: w.identity,
		})
		return err
	}
	_, err := w.service.CompleteWorkflowTask(ctx, &capstanv1.CompleteWorkflowTaskRequest{
		TaskToken: task.GetTaskToken(), Commands: result.Commands, Identity: w.identity, BuildId: w.buildID,
	})
	return err
}

func taskFailure(err error) Result {
	cause := capstanv1.TaskFailedCause_TASK_FAILED_CAUSE_SDK_ERROR
	failureType := "SDKError"
	var mismatch *HistoryMismatchError
	if errors.As(err, &mismatch) {
		cause = capstanv1.TaskFailedCause_TASK_FAILED_CAUSE_HISTORY_MISMATCH
		failureType = "HistoryMismatchError"
	}
	return Result{Cause: cause, Failure: &capstanv1.Failure{Type: failureType, Message: err.Error()}}
}
