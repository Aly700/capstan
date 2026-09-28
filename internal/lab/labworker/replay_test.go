package labworker

import (
	"errors"
	"reflect"
	"testing"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func history(t *testing.T, attrs ...string) []*v1.HistoryEvent {
	t.Helper()
	result := make([]*v1.HistoryEvent, len(attrs))
	for i, attr := range attrs {
		e := new(v1.HistoryEvent)
		if err := protojson.Unmarshal([]byte(attr), e); err != nil {
			t.Fatal(err)
		}
		e.EventId = int64(i + 1)
		result[i] = e
	}
	return result
}

func initial(t *testing.T) []*v1.HistoryEvent {
	return history(t, `{"runStarted":{"workflowType":"test","taskQueue":"q"}}`,
		`{"taskScheduled":{"attempt":1}}`, `{"time":"2026-09-28T12:00:01Z","taskStarted":{"scheduledEventId":"2"}}`)
}

func activityHistory(t *testing.T, external ...string) []*v1.HistoryEvent {
	h := initial(t)
	tail := []string{`{"taskCompleted":{"startedEventId":"3"}}`, `{"activityScheduled":{"seq":"1","activityType":"effect"}}`}
	tail = append(tail, external...)
	tail = append(tail, `{"taskScheduled":{"attempt":1}}`, `{"time":"2026-09-28T13:00:00Z","taskStarted":{}}`)
	h = append(h, history(t, tail...)...)
	for i, e := range h {
		e.EventId = int64(i + 1)
	}
	return h
}

func TestReplayRestartsFromHistory(t *testing.T) {
	starts, finishes := 0, 0
	workflow := func(wf *Context, _ any) (any, error) {
		starts++
		first := wf.Now()
		value, err := wf.Activity("effect", 21)
		if err != nil {
			return nil, err
		}
		finishes++
		return []any{first, wf.Now(), value}, nil
	}
	for range 2 {
		commands, err := Replay("restart", initial(t), workflow)
		if err != nil {
			t.Fatal(err)
		}
		if len(commands) != 1 || commands[0].GetScheduleActivity().GetSeq() != 1 {
			t.Fatalf("commands: %v", commands)
		}
	}
	if starts != 2 || finishes != 0 {
		t.Fatalf("starts=%d finishes=%d", starts, finishes)
	}
	h := activityHistory(t, `{"activityCompleted":{"seq":"1","result":{"contentType":"application/json","data":"NDI="}}}`)
	for range 2 {
		commands, err := Replay("restart", h, workflow)
		if err != nil {
			t.Fatal(err)
		}
		if len(commands) != 1 || commands[0].GetCompleteRun() == nil {
			t.Fatalf("commands: %v", commands)
		}
		if got := string(commands[0].GetCompleteRun().GetResult().GetData()); got != "[1790596801000,1790600400000,42]" {
			t.Fatal(got)
		}
	}
	if starts != 4 || finishes != 2 {
		t.Fatalf("starts=%d finishes=%d", starts, finishes)
	}
}

func TestReplayRejectsUnknownAndDuplicateResults(t *testing.T) {
	for _, events := range [][]string{
		{`{"activityCompleted":{"seq":"9"}}`},
		{`{"activityCompleted":{"seq":"1"}}`, `{"activityCompleted":{"seq":"1"}}`},
		{`{"timerFired":{"seq":"1"}}`},
	} {
		_, err := Replay("invalid", activityHistory(t, events...), func(wf *Context, _ any) (any, error) { return wf.Activity("effect", nil) })
		var mismatch *HistoryMismatchError
		if err == nil || errors.As(err, &mismatch) {
			t.Fatalf("wanted SDK error, got %v", err)
		}
	}
}

func TestActivityCancelIsReferenceAndLateResultWins(t *testing.T) {
	workflow := func(wf *Context, _ any) (any, error) {
		return wf.All(
			func(child *Context) (any, error) { return child.Activity("effect", nil) },
			func(child *Context) (any, error) {
				if err := child.CancelActivity(1); err != nil {
					return nil, err
				}
				err := child.Sleep(time.Second)
				return "slept", err
			},
		)
	}
	commands, err := Replay("cancel", initial(t), workflow)
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 3 || commands[0].GetScheduleActivity().GetSeq() != 1 || commands[1].GetRequestActivityCancel().GetSeq() != 1 || commands[2].GetStartTimer().GetSeq() != 2 {
		t.Fatalf("commands=%v", commands)
	}
	h := initial(t)
	h = append(h, history(t,
		`{"taskCompleted":{"startedEventId":"3"}}`,
		`{"activityScheduled":{"seq":"1","activityType":"effect"}}`,
		`{"activityCancelRequested":{"seq":"1"}}`,
		`{"timerStarted":{"seq":"2"}}`,
		`{"timerFired":{"seq":"2"}}`,
		`{"activityCompleted":{"seq":"1","result":{"contentType":"application/json","data":"NDI="}}}`,
		`{"taskScheduled":{"attempt":1}}`,
		`{"time":"2026-09-28T13:00:00Z","taskStarted":{}}`,
	)...)
	for i, e := range h {
		e.EventId = int64(i + 1)
	}
	commands, err = Replay("cancel", h, workflow)
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || string(commands[0].GetCompleteRun().GetResult().GetData()) != `[42,"slept"]` {
		t.Fatalf("commands=%v", commands)
	}
}

func TestAllUsesCallOrderAndDisposesBlockedBranches(t *testing.T) {
	for range 20 {
		order := []int{}
		commands, err := Replay("all", initial(t), func(wf *Context, _ any) (any, error) {
			return wf.All(func(child *Context) (any, error) { order = append(order, 1); return child.Activity("first", 1) }, func(child *Context) (any, error) { order = append(order, 2); return child.Activity("second", 2) })
		})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(order, []int{1, 2}) || len(commands) != 2 || commands[0].GetScheduleActivity().GetActivityType() != "first" || commands[1].GetScheduleActivity().GetSeq() != 2 {
			t.Fatalf("order=%v commands=%v", order, commands)
		}
	}
}

func TestSideEffectFailureCannotBeCaught(t *testing.T) {
	for _, callback := range []func(*Context) any{
		func(*Context) any { panic("bad side effect") },
		func(*Context) any { return make(chan int) },
		func(wf *Context) any { _ = wf.Sleep(time.Second); return 7 },
	} {
		_, err := Replay("marker", initial(t), func(wf *Context, _ any) (any, error) {
			func() { defer func() { _ = recover() }(); wf.SideEffect(func() any { return callback(wf) }) }()
			return "caught", nil
		})
		if err == nil {
			t.Fatal("invalid side effect escaped as success")
		}
	}
}

func TestWorkflowFailureIsACommand(t *testing.T) {
	commands, err := Replay("fail", initial(t), func(*Context, any) (any, error) { return nil, errors.New("workflow failed") })
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || commands[0].GetFailRun().GetFailure().GetMessage() != "workflow failed" {
		t.Fatalf("commands=%v", commands)
	}
}

func TestNowPreservesRecordedMicroseconds(t *testing.T) {
	h := initial(t)
	h[2].Time = timestamppb.New(time.Unix(1790596801, 123000))
	commands, err := Replay("clock", h, func(wf *Context, _ any) (any, error) { return wf.Now(), nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || string(commands[0].GetCompleteRun().GetResult().GetData()) != "1790596801000.123" {
		t.Fatalf("commands = %v", commands)
	}
}

type panickingJSON struct{}

func (panickingJSON) MarshalJSON() ([]byte, error) { panic("cannot serialize") }

func TestSideEffectEncodingPanicCannotBeCaught(t *testing.T) {
	_, err := Replay("encoding", initial(t), func(wf *Context, _ any) (any, error) {
		func() { defer func() { _ = recover() }(); wf.SideEffect(func() any { return panickingJSON{} }) }()
		return "caught", nil
	})
	if err == nil {
		t.Fatal("unrecordable marker must fail the task even when caught")
	}
}

func TestReplayDisposesWorkflowDefers(t *testing.T) {
	for _, wait := range []bool{false, true} {
		t.Run(map[bool]string{false: "marker", true: "sleep"}[wait], func(t *testing.T) {
			h := initial(t)
			done := make(chan error, 1)
			calls := 0
			go func() {
				_, err := Replay("dispose", h, func(wf *Context, _ any) (any, error) {
					defer func() {
						if wait {
							_ = wf.Sleep(time.Second)
						} else {
							wf.SideEffect(func() any { calls++; return 1 })
						}
					}()
					return wf.Activity("effect", nil)
				})
				done <- err
			}()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
				if calls != 0 {
					t.Fatalf("stack disposal invoked %d side effects", calls)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("replay hung disposing a workflow defer")
			}
		})
	}
}

func TestReplaySerializesDisposalDefers(t *testing.T) {
	for range 10 {
		finished := 0
		_, err := Replay("cleanup", initial(t), func(wf *Context, _ any) (any, error) {
			branches := make([]Branch, 20)
			for i := range branches {
				branches[i] = func(child *Context) (any, error) {
					defer func() { finished++ }()
					return child.Activity("effect", nil)
				}
			}
			return wf.All(branches...)
		})
		if err != nil {
			t.Fatal(err)
		}
		if finished != 20 {
			t.Fatalf("disposal completed %d defers, want 20", finished)
		}
	}
}
