package server_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// The external row lock makes all three lifecycle RPCs overlap. The fourth
// operation sweeps a proven-due timer while those RPCs wait, then again as the
// lock is released. SKIP LOCKED may leave the timer for terminal cleanup.
func TestAuditPostgresFourWayTerminalRace(t *testing.T) {
	s := newAuditLedgerServer(t, time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC), 1)
	conn, err := pgx.Connect(t.Context(), s.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	completed, terminated, fired, discarded := 0, 0, 0, 0
	for iteration := range 20 {
		t.Run(fmt.Sprint(iteration), func(t *testing.T) {
			// Slow CI runners need longer to show all three RPCs waiting on the lock.
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			runID := fmt.Sprintf("four-way-%d", iteration)
			// One queue per iteration: a failed iteration must not leave tasks or timers
			// that a later iteration then polls and misattributes.
			queue := fmt.Sprintf("audit-four-way-%d", iteration)
			if _, err := s.client.StartRun(ctx, connect.NewRequest(&v1.StartRunRequest{RunId: runID, WorkflowType: "four-way", TaskQueue: queue})); err != nil {
				t.Fatal(err)
			}
			first, err := s.worker.PollWorkflowTask(ctx, connect.NewRequest(&v1.PollWorkflowTaskRequest{TaskQueue: queue}))
			if err != nil {
				t.Fatal(err)
			}
			_, err = s.worker.CompleteWorkflowTask(ctx, connect.NewRequest(&v1.CompleteWorkflowTaskRequest{TaskToken: first.Msg.TaskToken, Commands: []*v1.Command{{Attributes: &v1.Command_StartTimer{StartTimer: &v1.StartTimerCommand{Seq: 1, FireAfter: durationpb.New(time.Second)}}}}}))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.client.SignalRun(ctx, connect.NewRequest(&v1.SignalRunRequest{RunId: runID, Name: "activate-race"})); err != nil {
				t.Fatal(err)
			}
			inFlight, err := s.worker.PollWorkflowTask(ctx, connect.NewRequest(&v1.PollWorkflowTaskRequest{TaskQueue: queue}))
			if err != nil {
				t.Fatal(err)
			}

			lock, err := conn.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer lock.Rollback(context.Background())
			if _, err := lock.Exec(ctx, "select 1 from run where run_id=$1 for update", runID); err != nil {
				t.Fatal(err)
			}
			s.clock.advance(2 * time.Second)
			var due int
			if err := lock.QueryRow(ctx, "select count(*) from timer where run_id=$1 and due_at<=$2", runID, s.clock.Now()).Scan(&due); err != nil || due != 1 {
				t.Fatalf("timer not proven due before race: due=%d err=%v", due, err)
			}

			type outcome struct {
				name string
				err  error
			}
			start := make(chan struct{})
			outcomes := make(chan outcome, 3)
			for _, operation := range []struct {
				name string
				call func() error
			}{
				{"cancel", func() error {
					_, err := s.client.CancelRun(ctx, connect.NewRequest(&v1.CancelRunRequest{RunId: runID, Reason: "audit cancellation race"}))
					return err
				}},
				{"terminate", func() error {
					_, err := s.client.TerminateRun(ctx, connect.NewRequest(&v1.TerminateRunRequest{RunId: runID, Reason: "audit termination race"}))
					return err
				}},
				{"complete", func() error {
					_, err := s.worker.CompleteWorkflowTask(ctx, connect.NewRequest(&v1.CompleteWorkflowTaskRequest{TaskToken: inFlight.Msg.TaskToken, Commands: []*v1.Command{{Attributes: &v1.Command_CompleteRun{CompleteRun: &v1.CompleteRunCommand{}}}}}))
					return err
				}},
			} {
				go func() { <-start; outcomes <- outcome{operation.name, operation.call()} }()
			}
			blocked := make(chan struct{})
			release := make(chan struct{})
			skipped := make(chan outcome, 1)
			swept := make(chan outcome, 1)
			go func() {
				<-start
				select {
				case <-blocked:
				case <-ctx.Done():
					return
				}
				n, err := s.engine.FireDueTimers(ctx, 1)
				if err == nil && n != 0 {
					err = fmt.Errorf("timer processed while parent externally locked: %d", n)
				}
				skipped <- outcome{"timer-held-lock", err}
				select {
				case <-release:
				case <-ctx.Done():
					return
				}
				_, err = s.engine.FireDueTimers(ctx, 1)
				swept <- outcome{"timer-release-race", err}
			}()
			close(start)

			// Observe actual database waits, rather than assuming goroutine starts
			// mean all RPCs reached their run-lock acquisition.
			for {
				if _, err := lock.Exec(ctx, "select pg_stat_clear_snapshot()"); err != nil {
					t.Fatal(err)
				}
				var waiting int
				err := lock.QueryRow(ctx, "select count(*) from pg_stat_activity where datname=current_database() and pid<>pg_backend_pid() and state='active' and wait_event_type='Lock'").Scan(&waiting)
				if err != nil {
					t.Fatal(err)
				}
				if waiting >= 3 {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("three lifecycle RPCs did not overlap at PostgreSQL lock")
				case <-time.After(time.Millisecond):
				}
			}
			close(blocked)
			select {
			case result := <-skipped:
				if result.err != nil {
					t.Fatal(result.err)
				}
			case <-ctx.Done():
				t.Fatal("timer sweep did not finish during blocked lifecycle RPCs")
			}
			unlock := make(chan error, 1)
			go func() { <-release; unlock <- lock.Rollback(ctx) }()
			close(release)
			if err := <-unlock; err != nil {
				t.Fatal(err)
			}
			for range 3 {
				select {
				case result := <-outcomes:
					if result.err != nil && connect.CodeOf(result.err) != connect.CodeFailedPrecondition {
						t.Fatalf("%s unexpected error: %v", result.name, result.err)
					}
				case <-ctx.Done():
					t.Fatal("lifecycle RPC race did not finish")
				}
			}
			select {
			case result := <-swept:
				if result.err != nil {
					t.Fatal(result.err)
				}
			case <-ctx.Done():
				t.Fatal("release timer sweep did not finish")
			}

			history, err := s.client.GetHistory(ctx, connect.NewRequest(&v1.GetHistoryRequest{RunId: runID}))
			if err != nil {
				t.Fatal(err)
			}
			terminal, timerFired := 0, 0
			for index, event := range history.Msg.Events {
				if event.EventId != int64(index+1) {
					t.Fatalf("history gap at %d: event=%d", index, event.EventId)
				}
				if index < len(inFlight.Msg.History) && !proto.Equal(event, inFlight.Msg.History[index]) {
					t.Fatalf("history prefix changed at %d", index)
				}
				switch event.Type {
				case v1.EventType_EVENT_TYPE_RUN_COMPLETED:
					terminal++
					completed++
				case v1.EventType_EVENT_TYPE_RUN_FAILED:
					terminal++
					terminated++
				case v1.EventType_EVENT_TYPE_TIMER_FIRED:
					timerFired++
				}
			}
			if terminal != 1 || timerFired > 1 {
				t.Fatalf("terminal=%d timer_fired=%d", terminal, timerFired)
			}
			fired += timerFired
			discarded += 1 - timerFired
			var lastEventID int64
			var status, taskCount, timerCount, inboxCount, pendingApprovals int
			var inFlightDB bool
			var taskID int64
			err = conn.QueryRow(ctx, `select last_event_id,status,in_flight,coalesce(workflow_task_id,0),
				(select count(*) from task where run_id=$1),
				(select count(*) from timer where run_id=$1),
				(select count(*) from inbox where run_id=$1),
				(select count(*) from approval where run_id=$1 and status=1)
				from run where run_id=$1`, runID).Scan(&lastEventID, &status, &inFlightDB, &taskID, &taskCount, &timerCount, &inboxCount, &pendingApprovals)
			if err != nil {
				t.Fatal(err)
			}
			if lastEventID != int64(len(history.Msg.Events)) || (status != int(v1.RunStatus_RUN_STATUS_COMPLETED) && status != int(v1.RunStatus_RUN_STATUS_FAILED)) || inFlightDB || taskID != 0 || taskCount+timerCount+inboxCount+pendingApprovals != 0 {
				t.Fatalf("unclean terminal state: last=%d status=%d in_flight=%v task_id=%d tasks=%d timers=%d inbox=%d approvals=%d", lastEventID, status, inFlightDB, taskID, taskCount, timerCount, inboxCount, pendingApprovals)
			}
			t.Logf("three observed DB lock waiters; one timer proven due; held-lock sweep skipped; terminal=%d timer_fired=%d tasks/timers/inbox/approvals=0", terminal, timerFired)
		})
	}
	t.Logf("20 four-way races: completed=%d terminated=%d timer_fired=%d timer_removed_at_close=%d; no duplicate terminal/timer events or remaining resources", completed, terminated, fired, discarded)
}
