package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"github.com/Aly700/capstan/internal/store/memstore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type manualClock struct {
	mu sync.Mutex
	at time.Time
}

func (c *manualClock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.at }
func (c *manualClock) Advance(d time.Duration) { c.mu.Lock(); defer c.mu.Unlock(); c.at = c.at.Add(d) }

func newTestEngine(t *testing.T) (*Engine, *manualClock, store.Store) {
	t.Helper()
	s := memstore.New()
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	c := &manualClock{at: time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)}
	e, err := New(Deps{Store: s, Clock: c}, Config{})
	if err != nil {
		t.Fatal(err)
	}
	return e, c, s
}
func mustStart(t *testing.T, e *Engine, id string) *v1.StartRunResponse {
	t.Helper()
	r, err := e.StartRun(context.Background(), "client", &v1.StartRunRequest{RunId: id, WorkflowType: "flow", TaskQueue: "q"})
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func mustPoll(t *testing.T, e *Engine) *v1.PollWorkflowTaskResponse {
	t.Helper()
	r, found, err := e.PollWorkflowTask(context.Background(), &v1.PollWorkflowTaskRequest{TaskQueue: "q", Identity: "worker"})
	if err != nil || !found {
		t.Fatalf("poll: found=%v err=%v", found, err)
	}
	return r
}
func mustComplete(t *testing.T, e *Engine, token []byte, cmds ...*v1.Command) {
	t.Helper()
	_, err := e.CompleteWorkflowTask(context.Background(), &v1.CompleteWorkflowTaskRequest{TaskToken: token, Commands: cmds, Identity: "worker", BuildId: "build"})
	if err != nil {
		t.Fatal(err)
	}
}
func historyEvents(t *testing.T, e *Engine, id string) []*v1.HistoryEvent {
	t.Helper()
	var out []*v1.HistoryEvent
	err := e.deps.Store.InTx(context.Background(), func(tx store.Tx) error { var err error; out, err = tx.ReadHistory(id, 0, 0); return err })
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func history(t *testing.T, e *Engine, id string) []v1.EventType {
	t.Helper()
	events := historyEvents(t, e, id)
	types := make([]v1.EventType, len(events))
	for i, ev := range events {
		if ev.EventId != int64(i+1) {
			t.Fatalf("history gap at %d: %d", i, ev.EventId)
		}
		types[i] = ev.Type
	}
	return types
}
func wantTypes(t *testing.T, got []v1.EventType, want ...v1.EventType) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("history types\n got %v\nwant %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("history types\n got %v\nwant %v", got, want)
		}
	}
}
func historyBytes(t *testing.T, e *Engine, id string) []byte {
	t.Helper()
	b, err := proto.Marshal(&v1.GetHistoryResponse{Events: historyEvents(t, e, id)})
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func activityCmd(seq int64) *v1.Command {
	return &v1.Command{Attributes: &v1.Command_ScheduleActivity{ScheduleActivity: &v1.ScheduleActivityCommand{Seq: seq, ActivityType: "act", StartToCloseTimeout: durationpb.New(10 * time.Second)}}}
}
func timerCmd(seq int64, d time.Duration) *v1.Command {
	return &v1.Command{Attributes: &v1.Command_StartTimer{StartTimer: &v1.StartTimerCommand{Seq: seq, FireAfter: durationpb.New(d)}}}
}
func markerCmd(seq int64) *v1.Command {
	return &v1.Command{Attributes: &v1.Command_RecordMarker{RecordMarker: &v1.RecordMarkerCommand{Seq: seq, Name: "side_effect"}}}
}
func completeCmd() *v1.Command {
	return &v1.Command{Attributes: &v1.Command_CompleteRun{CompleteRun: &v1.CompleteRunCommand{Result: &v1.Payload{Data: []byte("done")}}}}
}
