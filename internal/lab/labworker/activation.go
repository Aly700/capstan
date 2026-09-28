package labworker

import (
	"fmt"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
)

type activation struct {
	started            *capstanv1.HistoryEvent
	external, recorded []*capstanv1.HistoryEvent
	completed          *capstanv1.HistoryEvent
	attempt            int32
}

func activations(history []*capstanv1.HistoryEvent) ([]activation, error) {
	var result []activation
	var external []*capstanv1.HistoryEvent
	attempts := make(map[int64]int32)
	for i := 0; i < len(history); i++ {
		event := history[i]
		if event == nil {
			return nil, fmt.Errorf("invalid history: nil event at index %d", i)
		}
		if scheduled := event.GetTaskScheduled(); scheduled != nil {
			attempts[event.EventId] = scheduled.Attempt
		}
		switch event.GetAttributes().(type) {
		case *capstanv1.HistoryEvent_ActivityCompleted,
			*capstanv1.HistoryEvent_ActivityFailed,
			*capstanv1.HistoryEvent_ActivityTimedOut,
			*capstanv1.HistoryEvent_ActivityCancelled,
			*capstanv1.HistoryEvent_TimerFired,
			*capstanv1.HistoryEvent_SignalReceived,
			*capstanv1.HistoryEvent_ApprovalResolved,
			*capstanv1.HistoryEvent_RunCancelRequested:
			external = append(external, event)
		}
		started := event.GetTaskStarted()
		if started == nil {
			continue
		}

		var next *capstanv1.HistoryEvent
		if i+1 < len(history) {
			next = history[i+1]
			if next == nil {
				return nil, fmt.Errorf("invalid history after TaskStarted event %d: nil event", event.EventId)
			}
		}
		// A discarded task consumes neither workflow time nor buffered results.
		if next.GetTaskFailed() != nil || next.GetTaskTimedOut() != nil {
			continue
		}
		a := activation{started: event, external: external, attempt: 1}
		if attempt, ok := attempts[started.ScheduledEventId]; ok {
			a.attempt = attempt
		}
		if next.GetTaskCompleted() != nil {
			a.completed = next
			i++
			for i+1 < len(history) {
				if _, ok := recordedCommandKey(history[i+1]); !ok {
					break
				}
				i++
				a.recorded = append(a.recorded, history[i])
			}
		} else if next != nil {
			return nil, fmt.Errorf("invalid history after TaskStarted event %d", event.EventId)
		}
		result = append(result, a)
		external = nil
	}
	return result, nil
}
