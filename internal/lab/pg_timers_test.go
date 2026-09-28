package lab

import (
	"testing"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPostgresTimerClockWindow(t *testing.T) {
	now := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)
	delay := time.Millisecond
	state := Snapshot{
		Runs:      []*store.Run{{RunID: "r", Status: v1.RunStatus_RUN_STATUS_RUNNING}},
		Histories: map[string][]*v1.HistoryEvent{"r": {{EventId: 1, Type: v1.EventType_EVENT_TYPE_TIMER_STARTED, Time: timestamppb.New(now.Add(2 * time.Microsecond)), Attributes: &v1.HistoryEvent_TimerStarted{TimerStarted: &v1.TimerStartedAttributes{Seq: 1, FireAfter: durationpb.New(delay)}}}}},
		Timers:    map[string][]*store.Timer{"r": {{RunID: "r", Seq: 1, StartedEventID: 1, DueAt: now.Add(delay)}}},
	}
	checker := newPostgresTimerChecker()
	checker.command("r", 1, delay, now, now.Add(5*time.Microsecond))
	if err := checker.check(state); err != nil {
		t.Fatalf("separate legal clock reads rejected: %v", err)
	}
	if !state.Histories["r"][0].Time.AsTime().Equal(now.Add(2 * time.Microsecond)) {
		t.Fatal("timer check changed the captured history")
	}
	state.Timers["r"][0].DueAt = now.Add(delay - time.Microsecond)
	if err := checker.check(state); err == nil {
		t.Fatal("accepted a deadline before the command's clock window")
	}
	state.Timers["r"][0].DueAt = now.Add(delay)
	state.Histories["r"] = append(state.Histories["r"], &v1.HistoryEvent{EventId: 2, Type: v1.EventType_EVENT_TYPE_TIMER_FIRED, Time: timestamppb.New(now.Add(delay - time.Microsecond)), Attributes: &v1.HistoryEvent_TimerFired{TimerFired: &v1.TimerFiredAttributes{Seq: 1, StartedEventId: 1}}})
	state.Timers["r"] = nil
	if err := checker.check(state); err == nil {
		t.Fatal("accepted fire before the validated persisted deadline")
	}
}
