package engine

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func mustActivity(t *testing.T, e *Engine) *v1.PollActivityTaskResponse {
	t.Helper()
	r, found, err := e.PollActivityTask(context.Background(), &v1.PollActivityTaskRequest{TaskQueue: "q", Identity: "activity-worker"})
	if err != nil || !found {
		t.Fatalf("poll activity: found=%v, err=%v", found, err)
	}
	return r
}

func scheduleTestActivity(t *testing.T, e *Engine, id string, edit func(*v1.ScheduleActivityCommand)) {
	t.Helper()
	mustStart(t, e, id)
	w := mustPoll(t, e)
	c := activityCmd(1)
	if edit != nil {
		edit(c.GetScheduleActivity())
	}
	mustComplete(t, e, w.TaskToken, c)
}

func mustFinishActivity(t *testing.T, e *Engine, token []byte) {
	t.Helper()
	if _, err := e.CompleteActivityTask(context.Background(), &v1.CompleteActivityTaskRequest{TaskToken: token, Result: &v1.Payload{Data: []byte("done")}, Identity: "worker"}); err != nil {
		t.Fatal(err)
	}
}

func taskForActivity(t *testing.T, s store.Store, token []byte) *store.Task {
	t.Helper()
	decoded, err := DecodeToken(token)
	if err != nil {
		t.Fatal(err)
	}
	var task *store.Task
	if err := s.InTx(context.Background(), func(tx store.Tx) error { var err error; task, err = tx.GetTask(decoded.TaskId, false); return err }); err != nil {
		t.Fatal(err)
	}
	return task
}

func TestActivityRoundTrip(t *testing.T) {
	e, _, s := newTestEngine(t)
	scheduleTestActivity(t, e, "activity", nil)
	before := historyBytes(t, e, "activity")
	a := mustActivity(t, e)
	if a.RunId != "activity" || a.Seq != 1 || a.ActivityType != "act" || a.Attempt != 1 || a.IdempotencyKey != "activity/1" {
		t.Fatalf("bad activity response: %v", a)
	}
	if !bytes.Equal(before, historyBytes(t, e, "activity")) {
		t.Fatal("poll appended activity history")
	}
	row := taskForActivity(t, s, a.TaskToken)
	if !row.LeasedUntil.Equal(row.StartedAt.Add(10 * time.Second)) {
		t.Fatalf("lease=%v, started=%v", row.LeasedUntil, row.StartedAt)
	}
	mustFinishActivity(t, e, a.TaskToken)
	h := history(t, e, "activity")
	wantTypes(t, h,
		v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED,
		v1.EventType_EVENT_TYPE_TASK_COMPLETED, v1.EventType_EVENT_TYPE_ACTIVITY_SCHEDULED,
		v1.EventType_EVENT_TYPE_ACTIVITY_COMPLETED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED)
	w := mustPoll(t, e)
	completed := w.History[len(w.History)-3].GetActivityCompleted()
	if completed == nil || completed.Attempt != 1 || completed.Seq != 1 || string(completed.Result.Data) != "done" {
		t.Fatalf("completion: %v", completed)
	}
}

func TestExternalEventsDuringInFlightTaskFlushAfterCommands(t *testing.T) {
	e, clock, _ := newTestEngine(t)
	mustStart(t, e, "mixed")
	w := mustPoll(t, e)
	mustComplete(t, e, w.TaskToken, activityCmd(1), timerCmd(2, time.Second))
	a := mustActivity(t, e)
	if _, err := e.SignalRun(context.Background(), "client", &v1.SignalRunRequest{RunId: "mixed", Name: "wake"}); err != nil {
		t.Fatal(err)
	}
	w = mustPoll(t, e)
	before := historyBytes(t, e, "mixed")
	mustFinishActivity(t, e, a.TaskToken)
	clock.Advance(time.Second)
	if n, err := e.FireDueTimers(context.Background(), 10); n != 1 || err != nil {
		t.Fatalf("fire: %d, %v", n, err)
	}
	if _, err := e.SignalRun(context.Background(), "client", &v1.SignalRunRequest{RunId: "mixed", Name: "during"}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, historyBytes(t, e, "mixed")) {
		t.Fatal("external events changed in-flight history")
	}
	mustComplete(t, e, w.TaskToken, activityCmd(3))
	h := history(t, e, "mixed")
	wantTypes(t, h,
		v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED,
		v1.EventType_EVENT_TYPE_TASK_COMPLETED, v1.EventType_EVENT_TYPE_ACTIVITY_SCHEDULED, v1.EventType_EVENT_TYPE_TIMER_STARTED,
		v1.EventType_EVENT_TYPE_SIGNAL_RECEIVED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED,
		v1.EventType_EVENT_TYPE_TASK_COMPLETED, v1.EventType_EVENT_TYPE_ACTIVITY_SCHEDULED,
		v1.EventType_EVENT_TYPE_ACTIVITY_COMPLETED, v1.EventType_EVENT_TYPE_TIMER_FIRED, v1.EventType_EVENT_TYPE_SIGNAL_RECEIVED,
		v1.EventType_EVENT_TYPE_TASK_SCHEDULED)
	mustPoll(t, e)
}

func TestLateActivityCompletionIsStale(t *testing.T) {
	e, clock, _ := newTestEngine(t)
	scheduleTestActivity(t, e, "late", nil)
	a := mustActivity(t, e)
	clock.Advance(10 * time.Second)
	if _, err := e.ProcessDueTasks(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	b := mustActivity(t, e)
	if b.Attempt != 2 || b.IdempotencyKey != a.IdempotencyKey {
		t.Fatalf("retry: %v", b)
	}
	before := historyBytes(t, e, "late")
	_, err := e.CompleteActivityTask(context.Background(), &v1.CompleteActivityTaskRequest{TaskToken: a.TaskToken})
	if !errors.Is(err, ErrStaleTask) {
		t.Fatalf("late result: %v", err)
	}
	if !bytes.Equal(before, historyBytes(t, e, "late")) {
		t.Fatal("late result changed history")
	}
	mustFinishActivity(t, e, b.TaskToken)
	wantTypes(t, history(t, e, "late"),
		v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED,
		v1.EventType_EVENT_TYPE_TASK_COMPLETED, v1.EventType_EVENT_TYPE_ACTIVITY_SCHEDULED,
		v1.EventType_EVENT_TYPE_ACTIVITY_COMPLETED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED)
}

func TestCompletionAfterRunClosedIsDropped(t *testing.T) {
	e, _, _ := newTestEngine(t)
	scheduleTestActivity(t, e, "closed", nil)
	a := mustActivity(t, e)
	if _, err := e.SignalRun(context.Background(), "client", &v1.SignalRunRequest{RunId: "closed", Name: "finish"}); err != nil {
		t.Fatal(err)
	}
	w := mustPoll(t, e)
	mustComplete(t, e, w.TaskToken, completeCmd())
	before := historyBytes(t, e, "closed")
	_, err := e.CompleteActivityTask(context.Background(), &v1.CompleteActivityTaskRequest{TaskToken: a.TaskToken})
	if !errors.Is(err, ErrStaleTask) {
		t.Fatalf("closed completion: %v", err)
	}
	if !bytes.Equal(before, historyBytes(t, e, "closed")) {
		t.Fatal("closed completion changed history")
	}
}

func TestActivityTokenValidatesEveryField(t *testing.T) {
	changes := map[string]func(*v1.TaskToken){
		"kind":      func(t *v1.TaskToken) { t.Kind = v1.TaskKind_TASK_KIND_WORKFLOW },
		"run":       func(t *v1.TaskToken) { t.RunId = "different" },
		"task":      func(t *v1.TaskToken) { t.TaskId++ },
		"attempt":   func(t *v1.TaskToken) { t.Attempt++ },
		"scheduled": func(t *v1.TaskToken) { t.ScheduledEventId++ },
		"started":   func(t *v1.TaskToken) { t.StartedEventId++ },
		"seq":       func(t *v1.TaskToken) { t.Seq++ },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			e, _, _ := newTestEngine(t)
			scheduleTestActivity(t, e, "token", nil)
			a := mustActivity(t, e)
			token, _ := DecodeToken(a.TaskToken)
			change(token)
			before := historyBytes(t, e, "token")
			_, err := e.CompleteActivityTask(context.Background(), &v1.CompleteActivityTaskRequest{TaskToken: EncodeToken(token)})
			if !errors.Is(err, ErrStaleTask) {
				t.Fatalf("bad token accepted: %v", err)
			}
			if !bytes.Equal(before, historyBytes(t, e, "token")) {
				t.Fatal("bad token changed history")
			}
			mustFinishActivity(t, e, a.TaskToken)
		})
	}
}

func TestExpiredActivityTokenIsStaleBeforeReaper(t *testing.T) {
	for _, mode := range []string{"complete", "fail", "heartbeat"} {
		t.Run(mode, func(t *testing.T) {
			e, clock, _ := newTestEngine(t)
			scheduleTestActivity(t, e, "expired", nil)
			a := mustActivity(t, e)
			clock.Advance(10 * time.Second)
			before := historyBytes(t, e, "expired")
			var err error
			switch mode {
			case "complete":
				_, err = e.CompleteActivityTask(context.Background(), &v1.CompleteActivityTaskRequest{TaskToken: a.TaskToken})
			case "fail":
				_, err = e.FailActivityTask(context.Background(), &v1.FailActivityTaskRequest{TaskToken: a.TaskToken, Failure: &v1.Failure{Type: "Error"}})
			case "heartbeat":
				_, err = e.HeartbeatActivityTask(context.Background(), &v1.HeartbeatActivityTaskRequest{TaskToken: a.TaskToken})
			}
			if !errors.Is(err, ErrStaleTask) {
				t.Fatalf("expired token: %v", err)
			}
			if !bytes.Equal(before, historyBytes(t, e, "expired")) {
				t.Fatal("expired token changed history")
			}
		})
	}
}

func TestActivityCancelUnstartedIsImmediate(t *testing.T) {
	e, _, _ := newTestEngine(t)
	scheduleTestActivity(t, e, "cancel-idle", nil)
	if _, err := e.SignalRun(context.Background(), "client", &v1.SignalRunRequest{RunId: "cancel-idle", Name: "cancel"}); err != nil {
		t.Fatal(err)
	}
	w := mustPoll(t, e)
	mustComplete(t, e, w.TaskToken, &v1.Command{Attributes: &v1.Command_RequestActivityCancel{RequestActivityCancel: &v1.RequestActivityCancelCommand{Seq: 1}}})
	h := history(t, e, "cancel-idle")
	wantTypes(t, h,
		v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED,
		v1.EventType_EVENT_TYPE_TASK_COMPLETED, v1.EventType_EVENT_TYPE_ACTIVITY_SCHEDULED,
		v1.EventType_EVENT_TYPE_SIGNAL_RECEIVED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED,
		v1.EventType_EVENT_TYPE_TASK_COMPLETED, v1.EventType_EVENT_TYPE_ACTIVITY_CANCEL_REQUESTED,
		v1.EventType_EVENT_TYPE_ACTIVITY_CANCELLED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED)
	if _, found, err := e.PollActivityTask(context.Background(), &v1.PollActivityTaskRequest{TaskQueue: "q"}); found || err != nil {
		t.Fatalf("cancelled task found=%v err=%v", found, err)
	}
}

func TestActivityCancelStartedViaHeartbeat(t *testing.T) {
	e, _, _ := newTestEngine(t)
	scheduleTestActivity(t, e, "cancel-active", nil)
	a := mustActivity(t, e)
	if _, err := e.SignalRun(context.Background(), "client", &v1.SignalRunRequest{RunId: "cancel-active", Name: "cancel"}); err != nil {
		t.Fatal(err)
	}
	w := mustPoll(t, e)
	mustComplete(t, e, w.TaskToken, &v1.Command{Attributes: &v1.Command_RequestActivityCancel{RequestActivityCancel: &v1.RequestActivityCancelCommand{Seq: 1}}})
	r, err := e.HeartbeatActivityTask(context.Background(), &v1.HeartbeatActivityTaskRequest{TaskToken: a.TaskToken})
	if err != nil || !r.CancelRequested {
		t.Fatalf("heartbeat: %v, %v", r, err)
	}
	if _, err := e.FailActivityTask(context.Background(), &v1.FailActivityTaskRequest{TaskToken: a.TaskToken, Failure: &v1.Failure{Type: "CancelledFailure", Details: &v1.Payload{Data: []byte("cancel")}}}); err != nil {
		t.Fatal(err)
	}
	h := history(t, e, "cancel-active")
	wantTypes(t, h,
		v1.EventType_EVENT_TYPE_RUN_STARTED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED,
		v1.EventType_EVENT_TYPE_TASK_COMPLETED, v1.EventType_EVENT_TYPE_ACTIVITY_SCHEDULED,
		v1.EventType_EVENT_TYPE_SIGNAL_RECEIVED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED, v1.EventType_EVENT_TYPE_TASK_STARTED,
		v1.EventType_EVENT_TYPE_TASK_COMPLETED, v1.EventType_EVENT_TYPE_ACTIVITY_CANCEL_REQUESTED,
		v1.EventType_EVENT_TYPE_ACTIVITY_CANCELLED, v1.EventType_EVENT_TYPE_TASK_SCHEDULED)
}

func TestHeartbeatExtendsDeadline(t *testing.T) {
	e, clock, s := newTestEngine(t)
	scheduleTestActivity(t, e, "heartbeat", func(c *v1.ScheduleActivityCommand) { c.HeartbeatTimeout = durationpb.New(4 * time.Second) })
	a := mustActivity(t, e)
	clock.Advance(3 * time.Second)
	details := &v1.Payload{Data: []byte("checkpoint")}
	if _, err := e.HeartbeatActivityTask(context.Background(), &v1.HeartbeatActivityTaskRequest{TaskToken: a.TaskToken, Details: details}); err != nil {
		t.Fatal(err)
	}
	row := taskForActivity(t, s, a.TaskToken)
	if !row.CheckAt.Equal(clock.Now().Add(4*time.Second)) || !proto.Equal(row.HeartbeatDetails, details) {
		t.Fatalf("heartbeat row: %+v", row)
	}
	clock.Advance(2 * time.Second)
	if n, err := e.ProcessDueTasks(context.Background(), 10); n != 0 || err != nil {
		t.Fatalf("premature timeout: %d, %v", n, err)
	}
	mustFinishActivity(t, e, a.TaskToken)
}

// seedActivity isolates activity methods from the workflow command implementation.
func seedActivity(t *testing.T, e *Engine, leased bool) []byte {
	t.Helper()
	now := e.deps.Clock.Now()
	a := &v1.ActivityScheduledAttributes{Seq: 1, ActivityType: "act", TaskQueue: "q", StartToCloseTimeout: durationpb.New(10 * time.Second)}
	r := &store.Run{RunID: "seeded", WorkflowType: "flow", TaskQueue: "q", Status: v1.RunStatus_RUN_STATUS_RUNNING, TaskTimeout: 10 * time.Second, StartedAt: now, LastEventID: 1}
	task := &store.Task{RunID: r.RunID, Kind: store.TaskActivity, TaskQueue: "q", Activity: a, Attempt: 1, ScheduledEventID: 1, ScheduledAt: now, VisibleAt: now}
	if leased {
		task.LeasedUntil = now.Add(10 * time.Second)
		task.StartedAt = now
		task.CheckAt = task.LeasedUntil
	}
	if err := e.deps.Store.InTx(context.Background(), func(tx store.Tx) error {
		if err := tx.InsertRun(r); err != nil {
			return err
		}
		if err := tx.AppendEvents(r.RunID, []*v1.HistoryEvent{{EventId: 1, Type: v1.EventType_EVENT_TYPE_ACTIVITY_SCHEDULED, Time: timestamppb.New(now), Attributes: &v1.HistoryEvent_ActivityScheduled{ActivityScheduled: a}}}); err != nil {
			return err
		}
		return tx.InsertTask(task)
	}); err != nil {
		t.Fatal(err)
	}
	return EncodeToken(&v1.TaskToken{Kind: v1.TaskKind_TASK_KIND_ACTIVITY, RunId: r.RunID, TaskId: task.ID, Attempt: 1, ScheduledEventId: 1, Seq: 1})
}

func TestActivityMethodsFromStoredTask(t *testing.T) {
	for _, method := range []string{"poll", "complete", "fail", "heartbeat"} {
		t.Run(method, func(t *testing.T) {
			e, _, _ := newTestEngine(t)
			token := seedActivity(t, e, method != "poll")
			var err error
			switch method {
			case "poll":
				var found bool
				_, found, err = e.PollActivityTask(context.Background(), &v1.PollActivityTaskRequest{TaskQueue: "q"})
				if err == nil && !found {
					t.Fatal("stored task not claimable")
				}
			case "complete":
				_, err = e.CompleteActivityTask(context.Background(), &v1.CompleteActivityTaskRequest{TaskToken: token})
			case "fail":
				_, err = e.FailActivityTask(context.Background(), &v1.FailActivityTaskRequest{TaskToken: token, Failure: &v1.Failure{Type: "Error"}})
			case "heartbeat":
				_, err = e.HeartbeatActivityTask(context.Background(), &v1.HeartbeatActivityTaskRequest{TaskToken: token})
			}
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPollDoesNotStartExpiredActivity(t *testing.T) {
	for _, timeout := range []v1.TimeoutType{v1.TimeoutType_TIMEOUT_TYPE_SCHEDULE_TO_START, v1.TimeoutType_TIMEOUT_TYPE_SCHEDULE_TO_CLOSE} {
		t.Run(timeout.String(), func(t *testing.T) {
			e, clock, _ := newTestEngine(t)
			scheduleTestActivity(t, e, "expired-before-poll", func(c *v1.ScheduleActivityCommand) {
				if timeout == v1.TimeoutType_TIMEOUT_TYPE_SCHEDULE_TO_START {
					c.ScheduleToStartTimeout = durationpb.New(time.Second)
				} else {
					c.ScheduleToCloseTimeout = durationpb.New(time.Second)
				}
			})
			clock.Advance(time.Second)
			_, found, err := e.PollActivityTask(context.Background(), &v1.PollActivityTaskRequest{TaskQueue: "q"})
			if found || err != nil {
				t.Fatalf("expired activity: found=%v err=%v", found, err)
			}
			assertActivityTimeout(t, e, "expired-before-poll", timeout)
		})
	}
}

func TestActivityPollAcceptsUnicodeQueue(t *testing.T) {
	e, _, _ := newTestEngine(t)
	queue := strings.Repeat("é", 101)
	if _, err := e.StartRun(context.Background(), "client", &v1.StartRunRequest{RunId: "unicode-queue", WorkflowType: "flow", TaskQueue: queue}); err != nil {
		t.Fatal(err)
	}
	w, found, err := e.PollWorkflowTask(context.Background(), &v1.PollWorkflowTaskRequest{TaskQueue: queue})
	if err != nil || !found {
		t.Fatalf("workflow poll: found=%v err=%v", found, err)
	}
	mustComplete(t, e, w.TaskToken, activityCmd(1))
	if _, found, err := e.PollActivityTask(context.Background(), &v1.PollActivityTaskRequest{TaskQueue: queue}); !found || err != nil {
		t.Fatalf("activity poll: found=%v err=%v", found, err)
	}
}
