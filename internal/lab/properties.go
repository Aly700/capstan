package lab

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/proto"
)

// Snapshot contains the store state visible through its frozen read API. InboxSizes
// deliberately counts buffered events without draining or otherwise changing them.
type Snapshot struct {
	Runs       []*store.Run
	Histories  map[string][]*v1.HistoryEvent
	Tasks      map[string][]*store.Task
	Timers     map[string][]*store.Timer
	Approvals  map[string][]*store.Approval
	InboxSizes map[string]int
}

// Capture reads one transaction's state. The lab calls it between scheduled actions.
func Capture(ctx context.Context, s store.Store) (Snapshot, error) {
	snapshot := Snapshot{
		Histories: make(map[string][]*v1.HistoryEvent), Tasks: make(map[string][]*store.Task),
		Timers: make(map[string][]*store.Timer), Approvals: make(map[string][]*store.Approval),
		InboxSizes: make(map[string]int),
	}
	err := s.InTx(ctx, func(tx store.Tx) error {
		var err error
		snapshot.Runs, err = tx.ListRuns(store.RunFilter{})
		if err != nil {
			return fmt.Errorf("capture runs: %w", err)
		}
		for _, run := range snapshot.Runs {
			id := run.RunID
			if snapshot.Histories[id], err = tx.ReadHistory(id, 0, 0); err != nil {
				return fmt.Errorf("capture %q history: %w", id, err)
			}
			if snapshot.Tasks[id], err = tx.RunTasks(id); err != nil {
				return fmt.Errorf("capture %q tasks: %w", id, err)
			}
			if snapshot.Timers[id], err = tx.RunTimers(id); err != nil {
				return fmt.Errorf("capture %q timers: %w", id, err)
			}
			if snapshot.Approvals[id], err = tx.RunApprovals(id); err != nil {
				return fmt.Errorf("capture %q approvals: %w", id, err)
			}
			if snapshot.InboxSizes[id], err = tx.InboxSize(id); err != nil {
				return fmt.Errorf("capture %q inbox: %w", id, err)
			}
			slices.SortFunc(snapshot.Timers[id], func(a, b *store.Timer) int { return compareInt64(a.Seq, b.Seq) })
			slices.SortFunc(snapshot.Approvals[id], func(a, b *store.Approval) int { return strings.Compare(a.ApprovalID, b.ApprovalID) })
		}
		return nil
	})
	if err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func compareInt64(a, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func snapshotRuns(snapshot Snapshot) (map[string]*store.Run, error) {
	runs := make(map[string]*store.Run, len(snapshot.Runs))
	for _, run := range snapshot.Runs {
		if run == nil || run.RunID == "" {
			return nil, fmt.Errorf("snapshot contains a nil or unnamed run")
		}
		if runs[run.RunID] != nil {
			return nil, fmt.Errorf("snapshot duplicates run %q", run.RunID)
		}
		runs[run.RunID] = run
	}
	return runs, nil
}

// CheckHistory checks P4 and the append-only part of P2 against the preceding capture.
func CheckHistory(previous, current Snapshot) error {
	if err := checkHistoryShape(previous); err != nil {
		return fmt.Errorf("previous snapshot: %w", err)
	}
	if err := checkHistoryShape(current); err != nil {
		return err
	}
	runs, _ := snapshotRuns(current)
	for _, run := range previous.Runs {
		if runs[run.RunID] == nil {
			return fmt.Errorf("P2/P4: run %q disappeared", run.RunID)
		}
		before, after := previous.Histories[run.RunID], current.Histories[run.RunID]
		if len(after) < len(before) {
			return fmt.Errorf("P2/P4: run %q history shrank from %d to %d", run.RunID, len(before), len(after))
		}
		for i, event := range before {
			if !proto.Equal(event, after[i]) {
				return fmt.Errorf("P2/P4: run %q event %d was rewritten", run.RunID, event.EventId)
			}
		}
	}
	return nil
}

func checkHistoryShape(snapshot Snapshot) error {
	runs, err := snapshotRuns(snapshot)
	if err != nil {
		return fmt.Errorf("P4: %w", err)
	}
	for id := range snapshot.Histories {
		if runs[id] == nil {
			return fmt.Errorf("P4: history belongs to missing run %q", id)
		}
	}
	for _, run := range snapshot.Runs {
		history := snapshot.Histories[run.RunID]
		for i, event := range history {
			if event == nil || event.EventId != int64(i+1) {
				return fmt.Errorf("P4: run %q history is not gap-free at event %d", run.RunID, i+1)
			}
		}
		if run.LastEventID != int64(len(history)) {
			return fmt.Errorf("P4: run %q last_event_id=%d, history maximum=%d", run.RunID, run.LastEventID, len(history))
		}
	}
	return nil
}

type timerState struct {
	start            *v1.HistoryEvent
	due              time.Time
	fired, cancelled bool
}

// CheckTimers checks P3, including recorded fire times and pending timer rows. A
// closed run implicitly discards its remaining timers. During an in-flight task,
// absent rows can represent buffered TimerFired events: the store only exposes an
// inbox count, so those timers are checked again after the inbox reaches history.
func CheckTimers(snapshot Snapshot) error {
	runs, err := snapshotRuns(snapshot)
	if err != nil {
		return fmt.Errorf("P3: %w", err)
	}
	for id, rows := range snapshot.Timers {
		if len(rows) > 0 && runs[id] == nil {
			return fmt.Errorf("P3: timers belong to missing run %q", id)
		}
	}
	for _, run := range snapshot.Runs {
		if err := checkRunTimers(snapshot, run); err != nil {
			return fmt.Errorf("P3: run %q: %w", run.RunID, err)
		}
	}
	return nil
}

func checkRunTimers(snapshot Snapshot, run *store.Run) error {
	states := make(map[int64]*timerState)
	for _, event := range snapshot.Histories[run.RunID] {
		if event == nil {
			return fmt.Errorf("nil history event")
		}
		switch event.Type {
		case v1.EventType_EVENT_TYPE_TIMER_STARTED:
			a := event.GetTimerStarted()
			if a == nil || a.Seq <= 0 || a.FireAfter == nil || a.FireAfter.CheckValid() != nil || a.FireAfter.AsDuration() <= 0 || event.Time == nil || event.Time.CheckValid() != nil {
				return fmt.Errorf("invalid timer start at event %d", event.EventId)
			}
			if states[a.Seq] != nil {
				return fmt.Errorf("timer %d started twice", a.Seq)
			}
			states[a.Seq] = &timerState{start: event, due: event.Time.AsTime().Add(a.FireAfter.AsDuration()).Truncate(time.Microsecond)}
		case v1.EventType_EVENT_TYPE_TIMER_FIRED:
			a := event.GetTimerFired()
			if a == nil || states[a.Seq] == nil || states[a.Seq].start.EventId != a.StartedEventId {
				return fmt.Errorf("fire at event %d references an unknown timer", event.EventId)
			}
			state := states[a.Seq]
			if state.fired {
				return fmt.Errorf("timer %d fired twice", a.Seq)
			}
			if event.Time == nil || event.Time.CheckValid() != nil || event.Time.AsTime().Before(state.due) {
				return fmt.Errorf("timer %d fired before its deadline %s", a.Seq, state.due.Format(time.RFC3339Nano))
			}
			// A fire already in the inbox may appear after a cancellation command.
			state.fired = true
		case v1.EventType_EVENT_TYPE_TIMER_CANCELLED:
			a := event.GetTimerCancelled()
			if a == nil || states[a.Seq] == nil || states[a.Seq].start.EventId != a.StartedEventId {
				return fmt.Errorf("cancellation at event %d references an unknown timer", event.EventId)
			}
			state := states[a.Seq]
			if state.cancelled || state.fired {
				return fmt.Errorf("timer %d cancelled after settling", a.Seq)
			}
			state.cancelled = true
		}
	}
	rows := make(map[int64]bool)
	for _, row := range snapshot.Timers[run.RunID] {
		if row == nil {
			return fmt.Errorf("nil timer row")
		}
		state := states[row.Seq]
		if row.RunID != run.RunID || state == nil || row.StartedEventID != state.start.EventId {
			return fmt.Errorf("timer row %d references an unknown start", row.Seq)
		}
		if rows[row.Seq] || state.fired || state.cancelled || !run.Open() {
			return fmt.Errorf("timer row %d is duplicated or retained after settling", row.Seq)
		}
		if !row.DueAt.Equal(state.due) {
			return fmt.Errorf("timer row %d has wrong deadline", row.Seq)
		}
		rows[row.Seq] = true
	}
	missing := 0
	for seq, state := range states {
		if run.Open() && !state.fired && !state.cancelled && !rows[seq] {
			missing++
		}
	}
	if missing > 0 && (!run.InFlight || missing > snapshot.InboxSizes[run.RunID]) {
		return fmt.Errorf("%d timer(s) lost: no row, terminal event, or sufficient inbox entries", missing)
	}
	return nil
}

// CheckEffects checks P1 against the destination's ledger, never an activity's
// claimed result. Retry attempts are allowed, but applying any key twice is not.
func CheckEffects(snapshot Snapshot, sink *EffectSink) error {
	if _, err := snapshotRuns(snapshot); err != nil {
		return fmt.Errorf("P1: %w", err)
	}
	if sink == nil {
		return fmt.Errorf("P1: effect sink is nil")
	}
	expected := make(map[string]bool)
	for _, run := range snapshot.Runs {
		for _, event := range snapshot.Histories[run.RunID] {
			a := event.GetActivityScheduled()
			if a == nil || a.ActivityType != "effect" {
				continue
			}
			key := run.RunID + "/" + strconv.FormatInt(a.Seq, 10)
			if _, exists := expected[key]; exists {
				return fmt.Errorf("P1: effect %q scheduled twice", key)
			}
			expected[key] = run.Status == v1.RunStatus_RUN_STATUS_COMPLETED
		}
	}
	for _, effect := range sink.Entries() {
		if _, exists := expected[effect.Key]; !exists {
			return fmt.Errorf("P1: unknown effect key %q was applied", effect.Key)
		}
		if effect.Count != 1 || effect.Attempts < effect.Count {
			return fmt.Errorf("P1: effect %q applied %d times in %d attempts", effect.Key, effect.Count, effect.Attempts)
		}
		delete(expected, effect.Key)
	}
	for key, required := range expected {
		if required {
			return fmt.Errorf("P1: completed run did not apply effect %q", key)
		}
	}
	return nil
}

// CompareOutcome checks P2's final state, ignoring retry history and wall time.
// JSON payloads are compared by value, preserving arbitrary-precision numbers.
func CompareOutcome(baseline, faulted Snapshot) error {
	before, err := snapshotRuns(baseline)
	if err != nil {
		return fmt.Errorf("P2: baseline: %w", err)
	}
	after, err := snapshotRuns(faulted)
	if err != nil {
		return fmt.Errorf("P2: faulted: %w", err)
	}
	if len(before) != len(after) {
		return fmt.Errorf("P2: run count changed from %d to %d", len(before), len(after))
	}
	for _, run := range baseline.Runs {
		other := after[run.RunID]
		if other == nil {
			return fmt.Errorf("P2: run %q is missing", run.RunID)
		}
		if run.Status != other.Status || run.ContinuedFromRunID != other.ContinuedFromRunID || run.ContinuedToRunID != other.ContinuedToRunID {
			return fmt.Errorf("P2: run %q changed status or continuation links", run.RunID)
		}
		if !equalPayload(run.Result, other.Result) || !equalFailure(run.Failure, other.Failure) {
			return fmt.Errorf("P2: run %q changed result or failure", run.RunID)
		}
	}
	return nil
}

func equalFailure(a, b *v1.Failure) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Type == b.Type && a.Message == b.Message && a.NonRetryable == b.NonRetryable && a.Stack == b.Stack && equalPayload(a.Details, b.Details) && equalFailure(a.Cause, b.Cause)
}

func equalPayload(a, b *v1.Payload) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.ContentType != b.ContentType {
		return false
	}
	if a.ContentType != "application/json" {
		return bytes.Equal(a.Data, b.Data)
	}
	left, leftErr := jsonValue(a.Data)
	right, rightErr := jsonValue(b.Data)
	return leftErr == nil && rightErr == nil && reflect.DeepEqual(left, right)
}

type jsonNumberValue string

func jsonValue(data []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("trailing JSON data")
	}
	return normalizeJSON(value), nil
}

func normalizeJSON(value any) any {
	switch v := value.(type) {
	case json.Number:
		number, ok := new(big.Rat).SetString(string(v))
		if ok {
			return jsonNumberValue(number.RatString())
		}
		return v
	case []any:
		for i, item := range v {
			v[i] = normalizeJSON(item)
		}
	case map[string]any:
		for key, item := range v {
			v[key] = normalizeJSON(item)
		}
	}
	return value
}
