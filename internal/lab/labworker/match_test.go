package labworker

import (
	"encoding/json"
	"errors"
	"testing"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

type matchingPair struct {
	name, command, event string
	identifying          map[string]any
}

var matchingPairs = []matchingPair{
	{
		"schedule activity",
		`{"scheduleActivity":{"seq":"7","activityType":"double","taskQueue":"new","input":{"data":"Mg=="},"scheduleToCloseTimeout":"20s","scheduleToStartTimeout":"5s","startToCloseTimeout":"3s","heartbeatTimeout":"1s","retryPolicy":{"maximumAttempts":3}}}`,
		`{"eventId":"41","activityScheduled":{"seq":"7","activityType":"double","taskQueue":"old","input":{"data":"MQ=="},"scheduleToCloseTimeout":"90s","scheduleToStartTimeout":"9s","startToCloseTimeout":"8s","heartbeatTimeout":"4s","retryPolicy":{"maximumAttempts":9},"taskCompletedEventId":"40"}}`,
		map[string]any{"seq": "8", "activityType": "triple"},
	},
	{
		"request activity cancel",
		`{"requestActivityCancel":{"seq":"7"}}`,
		`{"eventId":"41","activityCancelRequested":{"seq":"7","scheduledEventId":"14","taskCompletedEventId":"40"}}`,
		map[string]any{"seq": "8"},
	},
	{
		"start timer",
		`{"startTimer":{"seq":"7","fireAfter":"3s"}}`,
		`{"eventId":"41","timerStarted":{"seq":"7","fireAfter":"90s","taskCompletedEventId":"40"}}`,
		map[string]any{"seq": "8"},
	},
	{
		"cancel timer",
		`{"cancelTimer":{"seq":"7"}}`,
		`{"eventId":"41","timerCancelled":{"seq":"7","startedEventId":"14","taskCompletedEventId":"40"}}`,
		map[string]any{"seq": "8"},
	},
	{
		"record marker",
		`{"recordMarker":{"seq":"7","name":"patch","markerId":"v2","details":{"data":"Mg=="}}}`,
		`{"eventId":"41","markerRecorded":{"seq":"7","name":"patch","markerId":"v2","details":{"data":"MQ=="},"taskCompletedEventId":"40"}}`,
		map[string]any{"seq": "8", "name": "side_effect", "markerId": "v3"},
	},
	{
		"request approval",
		`{"requestApproval":{"seq":"7","approvalId":"approve-7","source":"APPROVAL_SOURCE_HUMAN","gateDecisionId":"new-decision","tool":"new-tool","arguments":{"data":"Mg=="},"prompt":"new prompt","options":["a","b"],"timeout":"3s"}}`,
		`{"eventId":"41","approvalRequested":{"seq":"7","approvalId":"approve-7","source":"APPROVAL_SOURCE_HUMAN","gateDecisionId":"old-decision","tool":"old-tool","arguments":{"data":"MQ=="},"prompt":"old prompt","options":["c"],"timeout":"90s","taskCompletedEventId":"40"}}`,
		map[string]any{"seq": "8", "approvalId": "approve-8", "source": "APPROVAL_SOURCE_GATE"},
	},
	{
		"complete run",
		`{"completeRun":{"result":{"data":"Mg=="}}}`,
		`{"eventId":"41","runCompleted":{"result":{"data":"MQ=="},"taskCompletedEventId":"40"}}`,
		nil,
	},
	{
		"fail run",
		`{"failRun":{"failure":{"type":"NewError","message":"new","nonRetryable":true,"details":{"data":"Mg=="}}}}`,
		`{"eventId":"41","runFailed":{"failure":{"type":"OldError","message":"old","details":{"data":"MQ=="}},"taskCompletedEventId":"40"}}`,
		nil,
	},
	{
		"cancel run",
		`{"cancelRun":{"details":{"data":"Mg=="}}}`,
		`{"eventId":"41","runCancelled":{"details":{"data":"MQ=="},"taskCompletedEventId":"40"}}`,
		nil,
	},
	{
		"continue as new",
		`{"continueAsNew":{"workflowType":"new","taskQueue":"new-q","input":{"data":"Mg=="}}}`,
		`{"eventId":"41","runContinuedAsNew":{"newRunId":"child","workflowType":"old","taskQueue":"old-q","input":{"data":"MQ=="},"taskCompletedEventId":"40"}}`,
		nil,
	},
}

func matchingCommand(t *testing.T, raw string) *capstanv1.Command {
	t.Helper()
	command := new(capstanv1.Command)
	if err := protojson.Unmarshal([]byte(raw), command); err != nil {
		t.Fatal(err)
	}
	return command
}

func matchingEvent(t *testing.T, raw string) *capstanv1.HistoryEvent {
	t.Helper()
	event := new(capstanv1.HistoryEvent)
	if err := protojson.Unmarshal([]byte(raw), event); err != nil {
		t.Fatal(err)
	}
	return event
}

func assertHistoryMismatch(t *testing.T, err error, eventID int64) *HistoryMismatchError {
	t.Helper()
	var mismatch *HistoryMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("error=%v, want HistoryMismatchError", err)
	}
	if mismatch.EventID != eventID {
		t.Fatalf("mismatch event=%d, want %d", mismatch.EventID, eventID)
	}
	if mismatch.Expected == "" || mismatch.Actual == "" {
		t.Fatalf("mismatch must describe both sides: %+v", mismatch)
	}
	return mismatch
}

func TestMatchCommandsIgnoresExecutionOptionsAndPayloads(t *testing.T) {
	for _, pair := range matchingPairs {
		t.Run(pair.name, func(t *testing.T) {
			err := matchCommands([]*capstanv1.Command{matchingCommand(t, pair.command)}, []*capstanv1.HistoryEvent{matchingEvent(t, pair.event)}, 40)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMatchCommandsComparesEveryIdentifyingField(t *testing.T) {
	for _, pair := range matchingPairs {
		for field, value := range pair.identifying {
			t.Run(pair.name+"/"+field, func(t *testing.T) {
				var object map[string]map[string]any
				if err := json.Unmarshal([]byte(pair.command), &object); err != nil {
					t.Fatal(err)
				}
				for _, attributes := range object {
					attributes[field] = value
				}
				raw, err := json.Marshal(object)
				if err != nil {
					t.Fatal(err)
				}
				err = matchCommands([]*capstanv1.Command{matchingCommand(t, string(raw))}, []*capstanv1.HistoryEvent{matchingEvent(t, pair.event)}, 40)
				assertHistoryMismatch(t, err, 41)
			})
		}
	}
}

func TestMatchCommandsComparesEveryCommandType(t *testing.T) {
	for i, commandPair := range matchingPairs {
		for j, eventPair := range matchingPairs {
			if i == j {
				continue
			}
			t.Run(commandPair.name+"/"+eventPair.name, func(t *testing.T) {
				err := matchCommands([]*capstanv1.Command{matchingCommand(t, commandPair.command)}, []*capstanv1.HistoryEvent{matchingEvent(t, eventPair.event)}, 40)
				assertHistoryMismatch(t, err, 41)
			})
		}
	}
}

func TestMatchCommandsReportsFirstMismatchAndCompletedAnchor(t *testing.T) {
	activity := matchingCommand(t, matchingPairs[0].command)
	timer := matchingCommand(t, matchingPairs[2].command)
	activityEvent := matchingEvent(t, matchingPairs[0].event)
	timerEvent := matchingEvent(t, matchingPairs[2].event)
	timerEvent.EventId = 42
	for _, test := range []struct {
		name     string
		commands []*capstanv1.Command
		events   []*capstanv1.HistoryEvent
		wantID   int64
	}{
		{"missing first", nil, []*capstanv1.HistoryEvent{activityEvent, timerEvent}, 41},
		{"missing second", []*capstanv1.Command{activity}, []*capstanv1.HistoryEvent{activityEvent, timerEvent}, 42},
		{"extra first", []*capstanv1.Command{activity}, nil, 40},
		{"extra second", []*capstanv1.Command{activity, timer}, []*capstanv1.HistoryEvent{activityEvent}, 40},
		{"reordered", []*capstanv1.Command{timer, activity}, []*capstanv1.HistoryEvent{activityEvent, timerEvent}, 41},
		{"wrong second", []*capstanv1.Command{activity, activity}, []*capstanv1.HistoryEvent{activityEvent, timerEvent}, 42},
		{"nil command", []*capstanv1.Command{nil}, []*capstanv1.HistoryEvent{activityEvent}, 41},
		{"empty command", []*capstanv1.Command{{}}, []*capstanv1.HistoryEvent{activityEvent}, 41},
		{"nil event", []*capstanv1.Command{activity}, []*capstanv1.HistoryEvent{nil}, 40},
		{"noncommand event", []*capstanv1.Command{activity}, []*capstanv1.HistoryEvent{matchingEvent(t, `{"eventId":"44","signalReceived":{}}`)}, 44},
	} {
		t.Run(test.name, func(t *testing.T) {
			assertHistoryMismatch(t, matchCommands(test.commands, test.events, 40), test.wantID)
		})
	}
	if err := matchCommands(nil, nil, 40); err != nil {
		t.Fatalf("empty activation mismatch: %v", err)
	}
	if err := matchCommands([]*capstanv1.Command{activity, timer}, []*capstanv1.HistoryEvent{activityEvent, timerEvent}, 40); err != nil {
		t.Fatalf("matching activation mismatch: %v", err)
	}
}
