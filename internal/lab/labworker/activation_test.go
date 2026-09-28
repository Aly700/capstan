package labworker

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func activationHistory(t *testing.T, attributes ...string) []*capstanv1.HistoryEvent {
	t.Helper()
	history := make([]*capstanv1.HistoryEvent, len(attributes))
	for i, attrs := range attributes {
		event := new(capstanv1.HistoryEvent)
		if err := protojson.Unmarshal([]byte("{"+attrs+"}"), event); err != nil {
			t.Fatal(err)
		}
		event.EventId = int64(i + 1)
		event.Time = timestamppb.New(time.Unix(event.EventId, 0))
		history[i] = event
	}
	return history
}

func activationIDs(events []*capstanv1.HistoryEvent) []int64 {
	ids := make([]int64, len(events))
	for i, event := range events {
		ids[i] = event.GetEventId()
	}
	return ids
}

func TestActivationsGroupsCommandsAndExternalEvents(t *testing.T) {
	history := activationHistory(t,
		`"runStarted": {"workflowType": "example"}`,
		`"taskScheduled": {"attempt": 2}`,
		`"signalReceived": {"name": "before"}`,
		`"taskStarted": {"scheduledEventId": "2"}`,
		`"taskCompleted": {"startedEventId": "4"}`,
		`"activityScheduled": {"seq": "1", "activityType": "double"}`,
		`"timerStarted": {"seq": "2"}`,
		`"activityCompleted": {"seq": "1"}`,
		`"timerFired": {"seq": "2"}`,
		`"taskScheduled": {"attempt": 1}`,
		`"taskStarted": {"scheduledEventId": "10"}`,
	)
	got, err := activations(history)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d activations, want 2", len(got))
	}
	for i, want := range []struct {
		started, completed int64
		attempt            int32
		external, recorded []int64
	}{
		{4, 5, 2, []int64{3}, []int64{6, 7}},
		{11, 0, 1, []int64{8, 9}, []int64{}},
	} {
		a := got[i]
		if a.started.GetEventId() != want.started || a.completed.GetEventId() != want.completed || a.attempt != want.attempt {
			t.Errorf("activation %d: started=%d completed=%d attempt=%d", i, a.started.GetEventId(), a.completed.GetEventId(), a.attempt)
		}
		if !reflect.DeepEqual(activationIDs(a.external), want.external) || !reflect.DeepEqual(activationIDs(a.recorded), want.recorded) {
			t.Errorf("activation %d: external=%v recorded=%v", i, activationIDs(a.external), activationIDs(a.recorded))
		}
		if a.started != history[want.started-1] {
			t.Errorf("activation %d did not retain its TaskStarted event and time", i)
		}
	}
}

func TestActivationsDiscardedTasksPreserveBufferedEvents(t *testing.T) {
	history := activationHistory(t,
		`"runStarted": {}`,
		`"taskScheduled": {"attempt": 1}`,
		`"signalReceived": {"name": "before"}`,
		`"taskStarted": {"scheduledEventId": "2"}`,
		`"taskTimedOut": {"startedEventId": "4"}`,
		`"activityCompleted": {"seq": "1"}`,
		`"taskScheduled": {"attempt": 2}`,
		`"taskStarted": {"scheduledEventId": "7"}`,
		`"taskFailed": {"startedEventId": "8", "cause": "TASK_FAILED_CAUSE_HISTORY_MISMATCH"}`,
		`"runBlocked": {"taskFailedEventId": "9"}`,
		`"signalReceived": {"name": "while-blocked"}`,
		`"runResumed": {}`,
		`"taskScheduled": {"attempt": 3}`,
		`"taskStarted": {"scheduledEventId": "13"}`,
	)
	got, err := activations(history)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d activations, want only resumed activation", len(got))
	}
	if got[0].started != history[13] || got[0].attempt != 3 || got[0].completed != nil || len(got[0].recorded) != 0 {
		t.Fatalf("unexpected resumed activation: %+v", got[0])
	}
	if ids := activationIDs(got[0].external); !reflect.DeepEqual(ids, []int64{3, 6, 11}) {
		t.Fatalf("buffered external event ids=%v, want [3 6 11]", ids)
	}
}

func TestActivationsClassifiesEveryExternalKind(t *testing.T) {
	for _, kind := range []string{
		"activityCompleted", "activityFailed", "activityTimedOut", "activityCancelled",
		"timerFired", "signalReceived", "approvalResolved", "runCancelRequested",
	} {
		t.Run(kind, func(t *testing.T) {
			history := activationHistory(t, fmt.Sprintf(`%q: {}`, kind), `"taskStarted": {}`)
			got, err := activations(history)
			if err != nil || len(got) != 1 {
				t.Fatalf("activations=%v error=%v", got, err)
			}
			if len(got[0].external) != 1 || got[0].external[0] != history[0] {
				t.Fatalf("%s was not delivered as an external event", kind)
			}
		})
	}
}

func TestActivationsClassifiesEveryCommandKind(t *testing.T) {
	for _, kind := range []string{
		"activityScheduled", "activityCancelRequested", "timerStarted", "timerCancelled",
		"markerRecorded", "approvalRequested", "runCompleted", "runFailed", "runCancelled", "runContinuedAsNew",
	} {
		t.Run(kind, func(t *testing.T) {
			history := activationHistory(t,
				`"taskStarted": {}`, `"taskCompleted": {}`, fmt.Sprintf(`%q: {}`, kind),
			)
			got, err := activations(history)
			if err != nil || len(got) != 1 {
				t.Fatalf("activations=%v error=%v", got, err)
			}
			if len(got[0].recorded) != 1 || got[0].recorded[0] != history[2] {
				t.Fatalf("%s was not collected as a command event", kind)
			}
		})
	}
}

func TestActivationsAttemptUsesReferencedSchedule(t *testing.T) {
	history := activationHistory(t,
		`"taskScheduled": {"attempt": 7}`, `"taskScheduled": {"attempt": 2}`,
		`"taskStarted": {"scheduledEventId": "1"}`, `"taskCompleted": {}`,
		`"taskStarted": {"scheduledEventId": "99"}`,
	)
	got, err := activations(history)
	if err != nil || len(got) != 2 {
		t.Fatalf("activations=%v error=%v", got, err)
	}
	if got[0].attempt != 7 || got[1].attempt != 1 {
		t.Fatalf("attempts=%d,%d, want 7,1", got[0].attempt, got[1].attempt)
	}
}

func TestActivationsEmptyAndDiscardedHistory(t *testing.T) {
	for name, history := range map[string][]*capstanv1.HistoryEvent{
		"empty":     nil,
		"no task":   activationHistory(t, `"runStarted": {}`, `"taskScheduled": {"attempt": 1}`),
		"failed":    activationHistory(t, `"taskStarted": {}`, `"taskFailed": {}`),
		"timed out": activationHistory(t, `"taskStarted": {}`, `"taskTimedOut": {}`),
	} {
		t.Run(name, func(t *testing.T) {
			got, err := activations(history)
			if err != nil || len(got) != 0 {
				t.Fatalf("activations=%v error=%v, want no activations", got, err)
			}
		})
	}
}

func TestActivationsRejectMalformedTaskBoundary(t *testing.T) {
	for _, kind := range []string{"signalReceived", "activityScheduled", "taskStarted", "taskScheduled", "runBlocked", "runTimedOut"} {
		t.Run(kind, func(t *testing.T) {
			_, err := activations(activationHistory(t, `"taskStarted": {}`, fmt.Sprintf(`%q: {}`, kind)))
			if err == nil || !strings.Contains(err.Error(), "TaskStarted event 1") {
				t.Fatalf("error=%v, want malformed boundary naming TaskStarted event 1", err)
			}
		})
	}
}

func TestActivationsRejectNilHistoryEvent(t *testing.T) {
	for _, history := range [][]*capstanv1.HistoryEvent{
		{nil}, append(activationHistory(t, `"taskStarted": {}`), nil),
	} {
		if _, err := activations(history); err == nil {
			t.Fatal("nil history event was accepted")
		}
	}
}
