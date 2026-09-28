package labworker

import (
	"fmt"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// HistoryMismatchError identifies the first recorded step the workflow did not reproduce.
type HistoryMismatchError struct {
	EventID          int64
	Expected, Actual string
}

func (e *HistoryMismatchError) Error() string {
	return fmt.Sprintf("history mismatch at event %d: history has %s, code emitted %s", e.EventID, e.Expected, e.Actual)
}

func matchCommands(commands []*capstanv1.Command, events []*capstanv1.HistoryEvent, completedID int64) error {
	for i := 0; i < max(len(commands), len(events)); i++ {
		var command *capstanv1.Command
		var event *capstanv1.HistoryEvent
		if i < len(commands) {
			command = commands[i]
		}
		if i < len(events) {
			event = events[i]
		}
		actual, commandOK := emittedCommandKey(command)
		expected, eventOK := recordedCommandKey(event)
		if !commandOK || !eventOK || actual != expected {
			// Extra commands have no recorded event; D14 anchors them to the task.
			eventID := completedID
			if event != nil {
				eventID = event.EventId
			}
			return &HistoryMismatchError{
				EventID: eventID, Expected: describeReplayStep(event), Actual: describeReplayStep(command),
			}
		}
	}
	return nil
}

// The key contains exactly the identifying fields in conformance/README.md.
// Execution options and payload values are deliberately absent.
type replayCommandKey struct {
	kind     capstanv1.EventType
	seq      int64
	name, id string
	source   capstanv1.ApprovalSource
}

func emittedCommandKey(command *capstanv1.Command) (replayCommandKey, bool) {
	switch {
	case command.GetScheduleActivity() != nil:
		a := command.GetScheduleActivity()
		return replayCommandKey{kind: capstanv1.EventType_EVENT_TYPE_ACTIVITY_SCHEDULED, seq: a.Seq, name: a.ActivityType}, true
	case command.GetRequestActivityCancel() != nil:
		return replayCommandKey{kind: capstanv1.EventType_EVENT_TYPE_ACTIVITY_CANCEL_REQUESTED, seq: command.GetRequestActivityCancel().Seq}, true
	case command.GetStartTimer() != nil:
		return replayCommandKey{kind: capstanv1.EventType_EVENT_TYPE_TIMER_STARTED, seq: command.GetStartTimer().Seq}, true
	case command.GetCancelTimer() != nil:
		return replayCommandKey{kind: capstanv1.EventType_EVENT_TYPE_TIMER_CANCELLED, seq: command.GetCancelTimer().Seq}, true
	case command.GetRecordMarker() != nil:
		a := command.GetRecordMarker()
		return replayCommandKey{kind: capstanv1.EventType_EVENT_TYPE_MARKER_RECORDED, seq: a.Seq, name: a.Name, id: a.MarkerId}, true
	case command.GetRequestApproval() != nil:
		a := command.GetRequestApproval()
		return replayCommandKey{kind: capstanv1.EventType_EVENT_TYPE_APPROVAL_REQUESTED, seq: a.Seq, id: a.ApprovalId, source: a.Source}, true
	case command.GetCompleteRun() != nil:
		return replayCommandKey{kind: capstanv1.EventType_EVENT_TYPE_RUN_COMPLETED}, true
	case command.GetFailRun() != nil:
		return replayCommandKey{kind: capstanv1.EventType_EVENT_TYPE_RUN_FAILED}, true
	case command.GetCancelRun() != nil:
		return replayCommandKey{kind: capstanv1.EventType_EVENT_TYPE_RUN_CANCELLED}, true
	case command.GetContinueAsNew() != nil:
		return replayCommandKey{kind: capstanv1.EventType_EVENT_TYPE_RUN_CONTINUED_AS_NEW}, true
	default:
		return replayCommandKey{}, false
	}
}

func recordedCommandKey(event *capstanv1.HistoryEvent) (replayCommandKey, bool) {
	switch {
	case event.GetActivityScheduled() != nil:
		a := event.GetActivityScheduled()
		return replayCommandKey{kind: capstanv1.EventType_EVENT_TYPE_ACTIVITY_SCHEDULED, seq: a.Seq, name: a.ActivityType}, true
	case event.GetActivityCancelRequested() != nil:
		return replayCommandKey{kind: capstanv1.EventType_EVENT_TYPE_ACTIVITY_CANCEL_REQUESTED, seq: event.GetActivityCancelRequested().Seq}, true
	case event.GetTimerStarted() != nil:
		return replayCommandKey{kind: capstanv1.EventType_EVENT_TYPE_TIMER_STARTED, seq: event.GetTimerStarted().Seq}, true
	case event.GetTimerCancelled() != nil:
		return replayCommandKey{kind: capstanv1.EventType_EVENT_TYPE_TIMER_CANCELLED, seq: event.GetTimerCancelled().Seq}, true
	case event.GetMarkerRecorded() != nil:
		a := event.GetMarkerRecorded()
		return replayCommandKey{kind: capstanv1.EventType_EVENT_TYPE_MARKER_RECORDED, seq: a.Seq, name: a.Name, id: a.MarkerId}, true
	case event.GetApprovalRequested() != nil:
		a := event.GetApprovalRequested()
		return replayCommandKey{kind: capstanv1.EventType_EVENT_TYPE_APPROVAL_REQUESTED, seq: a.Seq, id: a.ApprovalId, source: a.Source}, true
	case event.GetRunCompleted() != nil:
		return replayCommandKey{kind: capstanv1.EventType_EVENT_TYPE_RUN_COMPLETED}, true
	case event.GetRunFailed() != nil:
		return replayCommandKey{kind: capstanv1.EventType_EVENT_TYPE_RUN_FAILED}, true
	case event.GetRunCancelled() != nil:
		return replayCommandKey{kind: capstanv1.EventType_EVENT_TYPE_RUN_CANCELLED}, true
	case event.GetRunContinuedAsNew() != nil:
		return replayCommandKey{kind: capstanv1.EventType_EVENT_TYPE_RUN_CONTINUED_AS_NEW}, true
	default:
		return replayCommandKey{}, false
	}
}

func describeReplayStep(message proto.Message) string {
	value := message.ProtoReflect()
	field := value.WhichOneof(value.Descriptor().Oneofs().ByName("attributes"))
	if field == nil {
		return "<none>"
	}
	attributes := value.Get(field).Message().Interface()
	data, err := protojson.Marshal(attributes)
	if err != nil {
		return fmt.Sprintf("%s %v", field.JSONName(), attributes)
	}
	return fmt.Sprintf("%s %s", field.JSONName(), data)
}
