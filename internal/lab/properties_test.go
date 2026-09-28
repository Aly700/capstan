package lab

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"github.com/Aly700/capstan/internal/store/memstore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var propertyTime = time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)

func propertyPayload(data string) *v1.Payload {
	return &v1.Payload{ContentType: "application/json", Data: []byte(data)}
}

func propertySnapshot() Snapshot {
	return Snapshot{
		Runs:      []*store.Run{{RunID: "r", Status: v1.RunStatus_RUN_STATUS_RUNNING, LastEventID: 1}},
		Histories: map[string][]*v1.HistoryEvent{"r": {{EventId: 1, Type: v1.EventType_EVENT_TYPE_RUN_STARTED, Time: timestamppb.New(propertyTime), Attributes: &v1.HistoryEvent_RunStarted{RunStarted: &v1.RunStartedAttributes{WorkflowType: "test"}}}}},
		Tasks:     map[string][]*store.Task{}, Timers: map[string][]*store.Timer{},
		Approvals: map[string][]*store.Approval{}, InboxSizes: map[string]int{},
	}
}

func TestCaptureReadsRealStoreWithoutDrainingInbox(t *testing.T) {
	s := memstore.New()
	t.Cleanup(func() { _ = s.Close() })
	if err := s.InTx(t.Context(), func(tx store.Tx) error {
		for _, id := range []string{"z", "a"} {
			if err := tx.InsertRun(&store.Run{RunID: id, Status: v1.RunStatus_RUN_STATUS_RUNNING, LastEventID: 1}); err != nil {
				return err
			}
			if err := tx.AppendEvents(id, []*v1.HistoryEvent{{EventId: 1, Type: v1.EventType_EVENT_TYPE_RUN_STARTED}}); err != nil {
				return err
			}
		}
		if err := tx.InsertTask(&store.Task{RunID: "a", Kind: store.TaskWorkflow}); err != nil {
			return err
		}
		if err := tx.InsertTimer(&store.Timer{RunID: "a", Seq: 4, DueAt: propertyTime}); err != nil {
			return err
		}
		if err := tx.InsertApproval(&store.Approval{RunID: "a", ApprovalID: "review", Status: store.ApprovalPending}); err != nil {
			return err
		}
		return tx.PushInbox("a", &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_SIGNAL_RECEIVED})
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := Capture(t.Context(), s)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Runs) != 2 || snapshot.Runs[0].RunID != "a" || snapshot.Runs[1].RunID != "z" || len(snapshot.Histories["a"]) != 1 || len(snapshot.Tasks["a"]) != 1 || len(snapshot.Timers["a"]) != 1 || len(snapshot.Approvals["a"]) != 1 || snapshot.InboxSizes["a"] != 1 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	snapshot.Runs[0].RunID = "mutated"
	snapshot.Histories["a"][0].EventId = 99
	again, err := Capture(t.Context(), s)
	if err != nil || again.Runs[0].RunID != "a" || again.Histories["a"][0].EventId != 1 || again.InboxSizes["a"] != 1 {
		t.Fatalf("capture mutated storage or drained inbox: %#v, %v", again, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := Capture(ctx, s); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled capture error = %v", err)
	}
}

func TestCheckHistoryAcceptsPrefixExtension(t *testing.T) {
	previous, current := propertySnapshot(), propertySnapshot()
	current.Histories["r"] = append(current.Histories["r"], &v1.HistoryEvent{EventId: 2, Type: v1.EventType_EVENT_TYPE_TASK_SCHEDULED})
	current.Runs[0].LastEventID = 2
	if err := CheckHistory(previous, current); err != nil {
		t.Fatal(err)
	}
}

func TestCheckHistoryRejectsCorruption(t *testing.T) {
	cases := map[string]func(*Snapshot){
		"gap":                  func(s *Snapshot) { s.Histories["r"][0].EventId = 2 },
		"wrong last event":     func(s *Snapshot) { s.Runs[0].LastEventID = 2 },
		"rewritten attributes": func(s *Snapshot) { s.Histories["r"][0].GetRunStarted().WorkflowType = "changed" },
		"rewritten time":       func(s *Snapshot) { s.Histories["r"][0].Time = timestamppb.New(propertyTime.Add(time.Second)) },
		"truncated":            func(s *Snapshot) { s.Histories["r"] = nil; s.Runs[0].LastEventID = 0 },
		"removed run":          func(s *Snapshot) { s.Runs = nil; delete(s.Histories, "r") },
		"duplicate run":        func(s *Snapshot) { s.Runs = append(s.Runs, s.Runs[0]) },
		"orphan history":       func(s *Snapshot) { s.Histories["orphan"] = s.Histories["r"] },
		"nil event":            func(s *Snapshot) { s.Histories["r"][0] = nil },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			previous, current := propertySnapshot(), propertySnapshot()
			mutate(&current)
			if err := CheckHistory(previous, current); err == nil {
				t.Fatal("history corruption went undetected")
			}
		})
	}
}

func propertyTimerSnapshot() Snapshot {
	s := propertySnapshot()
	s.Histories["r"] = append(s.Histories["r"], &v1.HistoryEvent{EventId: 2, Type: v1.EventType_EVENT_TYPE_TIMER_STARTED, Time: timestamppb.New(propertyTime), Attributes: &v1.HistoryEvent_TimerStarted{TimerStarted: &v1.TimerStartedAttributes{Seq: 1, FireAfter: durationpb.New(time.Second)}}})
	s.Runs[0].LastEventID = 2
	s.Timers["r"] = []*store.Timer{{RunID: "r", Seq: 1, StartedEventID: 2, DueAt: propertyTime.Add(time.Second)}}
	return s
}

func propertyFire(s *Snapshot, at time.Time) {
	s.Histories["r"] = append(s.Histories["r"], &v1.HistoryEvent{EventId: int64(len(s.Histories["r"]) + 1), Type: v1.EventType_EVENT_TYPE_TIMER_FIRED, Time: timestamppb.New(at), Attributes: &v1.HistoryEvent_TimerFired{TimerFired: &v1.TimerFiredAttributes{Seq: 1, StartedEventId: 2}}})
	s.Runs[0].LastEventID++
	s.Timers["r"] = nil
}

func propertyCancel(s *Snapshot) {
	s.Histories["r"] = append(s.Histories["r"], &v1.HistoryEvent{EventId: int64(len(s.Histories["r"]) + 1), Type: v1.EventType_EVENT_TYPE_TIMER_CANCELLED, Time: timestamppb.New(propertyTime), Attributes: &v1.HistoryEvent_TimerCancelled{TimerCancelled: &v1.TimerCancelledAttributes{Seq: 1, StartedEventId: 2}}})
	s.Runs[0].LastEventID++
	s.Timers["r"] = nil
}

func TestCheckTimersAcceptsLegitimateStates(t *testing.T) {
	cases := map[string]func(*Snapshot){
		"pending":                                func(*Snapshot) {},
		"fired":                                  func(s *Snapshot) { propertyFire(s, propertyTime.Add(time.Second)) },
		"cancelled":                              propertyCancel,
		"buffered fire followed by cancellation": func(s *Snapshot) { propertyCancel(s); propertyFire(s, propertyTime.Add(time.Second)) },
		"buffered":                               func(s *Snapshot) { s.Timers["r"] = nil; s.Runs[0].InFlight = true; s.InboxSizes["r"] = 1 },
		"closed cleanup":                         func(s *Snapshot) { s.Timers["r"] = nil; s.Runs[0].Status = v1.RunStatus_RUN_STATUS_COMPLETED },
		"submicrosecond truncation": func(s *Snapshot) {
			s.Histories["r"][1].GetTimerStarted().FireAfter = durationpb.New(time.Second + time.Nanosecond)
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s := propertyTimerSnapshot()
			mutate(&s)
			if err := CheckTimers(s); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCheckTimersRejectsCorruption(t *testing.T) {
	cases := map[string]func(*Snapshot){
		"lost":  func(s *Snapshot) { s.Timers["r"] = nil },
		"early": func(s *Snapshot) { propertyFire(s, propertyTime.Add(time.Second-time.Microsecond)) },
		"double fire": func(s *Snapshot) {
			propertyFire(s, propertyTime.Add(time.Second))
			propertyFire(s, propertyTime.Add(2*time.Second))
		},
		"double cancel": func(s *Snapshot) { propertyCancel(s); propertyCancel(s) },
		"wrong reference": func(s *Snapshot) {
			propertyFire(s, propertyTime.Add(time.Second))
			s.Histories["r"][2].GetTimerFired().StartedEventId = 1
		},
		"unknown fire": func(s *Snapshot) {
			propertyFire(s, propertyTime.Add(time.Second))
			s.Histories["r"][2].GetTimerFired().Seq = 9
		},
		"wrong due time": func(s *Snapshot) { s.Timers["r"][0].DueAt = propertyTime },
		"orphan row":     func(s *Snapshot) { s.Timers["r"][0].Seq = 9 },
		"duplicate row":  func(s *Snapshot) { s.Timers["r"] = append(s.Timers["r"], s.Timers["r"][0]) },
		"fired row retained": func(s *Snapshot) {
			row := s.Timers["r"][0]
			propertyFire(s, propertyTime.Add(time.Second))
			s.Timers["r"] = []*store.Timer{row}
		},
		"closed row retained": func(s *Snapshot) { s.Runs[0].Status = v1.RunStatus_RUN_STATUS_FAILED },
		"inbox without task":  func(s *Snapshot) { s.Timers["r"] = nil; s.InboxSizes["r"] = 1 },
		"inbox too small":     func(s *Snapshot) { s.Timers["r"] = nil; s.Runs[0].InFlight = true },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s := propertyTimerSnapshot()
			mutate(&s)
			if err := CheckTimers(s); err == nil {
				t.Fatal("timer corruption went undetected")
			}
		})
	}
}

func propertyEffectSnapshot() Snapshot {
	s := propertySnapshot()
	s.Runs[0].Status = v1.RunStatus_RUN_STATUS_COMPLETED
	s.Runs[0].LastEventID = 2
	s.Histories["r"] = append(s.Histories["r"], &v1.HistoryEvent{EventId: 2, Type: v1.EventType_EVENT_TYPE_ACTIVITY_SCHEDULED, Attributes: &v1.HistoryEvent_ActivityScheduled{ActivityScheduled: &v1.ActivityScheduledAttributes{Seq: 1, ActivityType: "effect"}}})
	return s
}

func TestCheckEffectsUsesAppliedCountsNotAttempts(t *testing.T) {
	s, sink := propertyEffectSnapshot(), NewEffectSink()
	for range 3 {
		if _, err := sink.Apply("r/1", "result"); err != nil {
			t.Fatal(err)
		}
	}
	if err := CheckEffects(s, sink); err != nil {
		t.Fatal(err)
	}
	live := propertyEffectSnapshot()
	live.Runs[0].Status = v1.RunStatus_RUN_STATUS_RUNNING
	if err := CheckEffects(live, NewEffectSink()); err != nil {
		t.Fatalf("live activity may not have executed yet: %v", err)
	}
}

func TestCheckEffectsRejectsMissingAndUnknownKeys(t *testing.T) {
	s := propertyEffectSnapshot()
	if err := CheckEffects(s, NewEffectSink()); err == nil {
		t.Fatal("missing effect went undetected")
	}
	sink := NewEffectSink()
	if _, err := sink.Apply("r/1/attempt-2", "result"); err != nil {
		t.Fatal(err)
	}
	if err := CheckEffects(s, sink); err == nil {
		t.Fatal("attempt-dependent effect key went undetected")
	}
}

func TestCheckEffectsRejectsCorruptDestinationLedger(t *testing.T) {
	for _, count := range []int{0, 2} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			sink := NewEffectSink()
			if _, err := sink.Apply("r/1", "result"); err != nil {
				t.Fatal(err)
			}
			// A broken destination can forget or duplicate an effect independently
			// of the engine history; the property must inspect its ledger.
			record := sink.effects["r/1"]
			record.Count = count
			sink.effects["r/1"] = record
			if err := CheckEffects(propertyEffectSnapshot(), sink); err == nil {
				t.Fatalf("applied count %d went undetected", count)
			}
		})
	}
}

func TestCheckTimersInboxAllowanceIsBoundedByEventCount(t *testing.T) {
	s := propertyTimerSnapshot()
	second := proto.Clone(s.Histories["r"][1]).(*v1.HistoryEvent)
	second.EventId = 3
	second.GetTimerStarted().Seq = 2
	s.Histories["r"] = append(s.Histories["r"], second)
	s.Timers["r"] = nil
	s.Runs[0].InFlight = true
	s.Runs[0].LastEventID = 3
	s.InboxSizes["r"] = 1
	if err := CheckTimers(s); err == nil {
		t.Fatal("one buffered event concealed two lost timers")
	}
	s.InboxSizes["r"] = 2
	if err := CheckTimers(s); err != nil {
		t.Fatalf("two timer fires may be buffered: %v", err)
	}
}

func TestCompareOutcomeIgnoresRetriesAndJSONFormatting(t *testing.T) {
	a, b := propertySnapshot(), propertySnapshot()
	a.Runs[0].Status, b.Runs[0].Status = v1.RunStatus_RUN_STATUS_COMPLETED, v1.RunStatus_RUN_STATUS_COMPLETED
	a.Runs[0].Result = propertyPayload(`{"answer":1,"nested":[true,null]}`)
	b.Runs[0].Result = propertyPayload(`{ "nested": [true, null], "answer": 1.0 }`)
	b.Runs[0].LastEventID = 99
	b.Runs[0].ClosedAt = propertyTime.Add(time.Hour)
	if err := CompareOutcome(a, b); err != nil {
		t.Fatal(err)
	}
	a.Runs[0].Result, b.Runs[0].Result = nil, nil
	a.Runs[0].Status, b.Runs[0].Status = v1.RunStatus_RUN_STATUS_FAILED, v1.RunStatus_RUN_STATUS_FAILED
	a.Runs[0].Failure = &v1.Failure{Type: "Failed", Message: "same", Details: propertyPayload(`{"n":1}`)}
	b.Runs[0].Failure = proto.Clone(a.Runs[0].Failure).(*v1.Failure)
	b.Runs[0].Failure.Details = propertyPayload(`{"n":1e0}`)
	if err := CompareOutcome(a, b); err != nil {
		t.Fatal(err)
	}
}

func TestCompareOutcomeRejectsDifferentEndState(t *testing.T) {
	cases := map[string]func(*Snapshot){
		"missing run":  func(s *Snapshot) { s.Runs = nil },
		"status":       func(s *Snapshot) { s.Runs[0].Status = v1.RunStatus_RUN_STATUS_FAILED },
		"result":       func(s *Snapshot) { s.Runs[0].Result = propertyPayload(`{"n":9007199254740993}`) },
		"continuation": func(s *Snapshot) { s.Runs[0].ContinuedToRunID = "r~2" },
		"failure":      func(s *Snapshot) { s.Runs[0].Failure = &v1.Failure{Type: "Oops"} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			a, b := propertySnapshot(), propertySnapshot()
			a.Runs[0].Status, b.Runs[0].Status = v1.RunStatus_RUN_STATUS_COMPLETED, v1.RunStatus_RUN_STATUS_COMPLETED
			a.Runs[0].Result, b.Runs[0].Result = propertyPayload(`{"n":9007199254740992}`), propertyPayload(`{"n":9007199254740992}`)
			mutate(&b)
			if err := CompareOutcome(a, b); err == nil {
				t.Fatal("different outcome went undetected")
			}
		})
	}
}

func TestCompareOutcomeKeepsPayloadAndFailureMeaning(t *testing.T) {
	cases := []struct {
		name  string
		left  *v1.Payload
		right *v1.Payload
		equal bool
	}{
		{"same binary", &v1.Payload{ContentType: "application/octet-stream", Data: []byte{0, 255}}, &v1.Payload{ContentType: "application/octet-stream", Data: []byte{0, 255}}, true},
		{"different binary", &v1.Payload{ContentType: "application/octet-stream", Data: []byte{0}}, &v1.Payload{ContentType: "application/octet-stream", Data: []byte{1}}, false},
		{"nil versus JSON null", nil, propertyPayload("null"), false},
		{"numeric string", propertyPayload(`"1"`), propertyPayload(`1`), false},
		{"array order", propertyPayload(`[1,2]`), propertyPayload(`[2,1]`), false},
		{"trailing JSON", propertyPayload(`1 2`), propertyPayload(`1 2`), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, b := propertySnapshot(), propertySnapshot()
			a.Runs[0].Status, b.Runs[0].Status = v1.RunStatus_RUN_STATUS_FAILED, v1.RunStatus_RUN_STATUS_FAILED
			a.Runs[0].Failure = &v1.Failure{Type: "outer", Cause: &v1.Failure{Type: "inner", Details: tc.left}}
			b.Runs[0].Failure = &v1.Failure{Type: "outer", Cause: &v1.Failure{Type: "inner", Details: tc.right}}
			if err := CompareOutcome(a, b); (err == nil) != tc.equal {
				t.Fatalf("CompareOutcome error = %v, want equal=%v", err, tc.equal)
			}
		})
	}
}
