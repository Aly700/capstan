//go:build pgengine

package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestPostgresPollLockedRunRollsBackClaim(t *testing.T) {
	for _, kind := range []string{"workflow", "activity"} {
		t.Run(kind, func(t *testing.T) {
			e, _, s := newTestEngine(t)
			mustStart(t, e, "r")
			if kind == "activity" {
				mustComplete(t, e, mustPoll(t, e).TaskToken, activityCmd(1))
			}
			before := historyBytes(t, e, "r")
			locked, release := make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			var once sync.Once
			unlock := func() { once.Do(func() { close(release) }) }
			defer unlock()
			go func() {
				done <- s.InTx(t.Context(), func(tx store.Tx) error {
					if _, err := tx.GetRun("r", true); err != nil {
						return err
					}
					close(locked)
					select {
					case <-release:
						return nil
					case <-t.Context().Done():
						return t.Context().Err()
					}
				})
			}()
			select {
			case <-locked:
			case err := <-done:
				t.Fatalf("run lock: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("run lock was not acquired")
			}
			poll := func(ctx context.Context) (bool, error) {
				if kind == "workflow" {
					response, found, err := e.PollWorkflowTask(ctx, &v1.PollWorkflowTaskRequest{TaskQueue: "q"})
					if err == nil && !found && (response == nil || len(response.TaskToken) != 0) {
						return false, fmt.Errorf("busy workflow response = %v", response)
					}
					return found, err
				}
				response, found, err := e.PollActivityTask(ctx, &v1.PollActivityTaskRequest{TaskQueue: "q"})
				if err == nil && !found && (response == nil || len(response.TaskToken) != 0) {
					return false, fmt.Errorf("busy activity response = %v", response)
				}
				return found, err
			}
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			if found, err := poll(ctx); found || err != nil {
				t.Fatalf("locked-run poll: found=%v err=%v", found, err)
			}
			if ctx.Err() != nil {
				t.Fatalf("poll waited for a run lock: %v", ctx.Err())
			}
			if !bytes.Equal(before, historyBytes(t, e, "r")) {
				t.Fatal("contended poll changed history")
			}
			if err := s.InTx(t.Context(), func(tx store.Tx) error {
				tasks, err := tx.RunTasks("r")
				if err != nil {
					return err
				}
				if len(tasks) != 1 || !tasks[0].LeasedUntil.IsZero() || !tasks[0].StartedAt.IsZero() || tasks[0].WorkerID != "" {
					return fmt.Errorf("contended claim persisted: %+v", tasks)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			unlock()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if found, err := poll(t.Context()); !found || err != nil {
				t.Fatalf("poll after run unlocked: found=%v err=%v", found, err)
			}
		})
	}
}

func TestPostgresTerminateAgainstTaskOperations(t *testing.T) {
	for _, kind := range []string{"workflow_poll", "activity_poll", "activity_completion", "activity_failure", "activity_heartbeat", "activity_reservation", "task_timeout", "approval_timeout"} {
		for _, first := range []string{"terminate", "work"} {
			t.Run(kind+"/"+first+"_first", func(t *testing.T) {
				e, clock, _ := newTestEngine(t)
				mustStart(t, e, "r")
				activity := activityCmd(1)
				if kind == "task_timeout" {
					activity.GetScheduleActivity().ScheduleToStartTimeout = durationpb.New(time.Second)
				}
				mustComplete(t, e, mustPoll(t, e).TaskToken, activity,
					approvalCmd(2, "pending", v1.ApprovalSource_APPROVAL_SOURCE_HUMAN, time.Second))
				var token []byte
				switch kind {
				case "workflow_poll":
					if _, err := e.SignalRun(t.Context(), "owner", &v1.SignalRunRequest{RunId: "r", Name: "wake"}); err != nil {
						t.Fatal(err)
					}
				case "activity_completion", "activity_failure", "activity_heartbeat", "activity_reservation":
					token = mustActivity(t, e).TaskToken
				case "task_timeout", "approval_timeout":
					clock.Advance(time.Second)
				}
				terminate := func(ctx context.Context, current *Engine) error {
					_, err := current.TerminateRun(ctx, "owner", &v1.TerminateRunRequest{RunId: "r", Reason: "race"})
					return err
				}
				work := func(ctx context.Context, current *Engine) error {
					var err error
					switch kind {
					case "workflow_poll":
						_, _, err = current.PollWorkflowTask(ctx, &v1.PollWorkflowTaskRequest{TaskQueue: "q"})
					case "activity_poll":
						_, _, err = current.PollActivityTask(ctx, &v1.PollActivityTaskRequest{TaskQueue: "q"})
					case "activity_completion":
						_, err = current.CompleteActivityTask(ctx, &v1.CompleteActivityTaskRequest{TaskToken: token})
					case "activity_failure":
						_, err = current.FailActivityTask(ctx, &v1.FailActivityTaskRequest{TaskToken: token, Failure: &v1.Failure{NonRetryable: true}})
					case "activity_heartbeat":
						_, err = current.HeartbeatActivityTask(ctx, &v1.HeartbeatActivityTaskRequest{TaskToken: token})
					case "activity_reservation":
						_, err = current.ReserveAICall(ctx, &v1.ReserveAICallRequest{TaskToken: token, EstimateUsd: .01})
					case "task_timeout":
						_, err = current.ProcessDueTasks(ctx, 1)
					case "approval_timeout":
						_, err = current.ProcessDueApprovals(ctx, 1)
					}
					if first == "terminate" && errors.Is(err, ErrStaleTask) {
						return nil
					}
					return err
				}
				if first == "terminate" {
					raceWithRunClosure(t, e, terminate, work)
				} else {
					raceWithRunClosure(t, e, work, terminate)
				}
				assertTerminated(t, e, "r", "race")
			})
		}
	}
}
