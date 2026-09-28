package storetest

import (
	"testing"
	"time"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func historyContiguous(t *testing.T, s store.Store) {
	seed(t, s, "r", "other")
	mustTx(t, s, func(tx store.Tx) error {
		if err := tx.AppendEvents("r", []*capstanv1.HistoryEvent{event(1), event(2)}); err != nil {
			return err
		}
		return tx.AppendEvents("r", []*capstanv1.HistoryEvent{event(3)})
	})
	// LastEventID is deliberately stale: the stored maximum controls contiguity.
	mustTx(t, s, func(tx store.Tx) error {
		r, err := tx.GetRun("r", false)
		if err != nil {
			return err
		}
		equal(t, r.LastEventID, int64(0))
		r.LastEventID = 99
		if err := tx.UpdateRun(r); err != nil {
			return err
		}
		return tx.AppendEvents("r", []*capstanv1.HistoryEvent{event(4)})
	})
	mustTx(t, s, func(tx store.Tx) error {
		if err := tx.AppendEvents("r", nil); err != nil {
			return err
		}
		if err := tx.AppendEvents("other", []*capstanv1.HistoryEvent{event(1)}); err != nil {
			return err
		}
		h, err := tx.ReadHistory("r", 0, 0)
		if err != nil {
			return err
		}
		equal(t, len(h), 4)
		for i, e := range h {
			equalProto(t, e, event(int64(i+1)))
		}
		r, err := tx.GetRun("r", false)
		if err != nil {
			return err
		}
		equal(t, r.LastEventID, int64(99))
		return nil
	})
}

func historyGap(t *testing.T, s store.Store) {
	seed(t, s, "r")
	for _, events := range [][]*capstanv1.HistoryEvent{{event(2)}, {event(1), event(3)}, {event(0)}} {
		isError(t, s.InTx(t.Context(), func(tx store.Tx) error { return tx.AppendEvents("r", events) }), store.ErrConflict)
		mustTx(t, s, func(tx store.Tx) error {
			h, err := tx.ReadHistory("r", 0, 0)
			if err != nil {
				return err
			}
			equal(t, len(h), 0)
			return nil
		})
	}
	mustTx(t, s, func(tx store.Tx) error { return tx.AppendEvents("r", []*capstanv1.HistoryEvent{event(1)}) })
	isError(t, s.InTx(t.Context(), func(tx store.Tx) error { return tx.AppendEvents("r", []*capstanv1.HistoryEvent{event(3)}) }), store.ErrConflict)
}

func historyDuplicate(t *testing.T, s store.Store) {
	seed(t, s, "r")
	mustTx(t, s, func(tx store.Tx) error { return tx.AppendEvents("r", []*capstanv1.HistoryEvent{event(1)}) })
	for _, events := range [][]*capstanv1.HistoryEvent{{event(1)}, {event(2), event(2)}} {
		isError(t, s.InTx(t.Context(), func(tx store.Tx) error { return tx.AppendEvents("r", events) }), store.ErrConflict)
	}
	mustTx(t, s, func(tx store.Tx) error {
		h, err := tx.ReadHistory("r", 0, 0)
		if err != nil {
			return err
		}
		equal(t, len(h), 1)
		equalProto(t, h[0], event(1))
		return nil
	})
}

func historyRead(t *testing.T, s store.Store) {
	seed(t, s, "r")
	mustTx(t, s, func(tx store.Tx) error {
		return tx.AppendEvents("r", []*capstanv1.HistoryEvent{event(1), event(2), event(3), event(4)})
	})
	for _, tc := range []struct {
		after int64
		limit int
		ids   []int64
	}{{0, 2, []int64{1, 2}}, {2, 1, []int64{3}}, {1, 0, []int64{2, 3, 4}}, {2, -1, []int64{3, 4}}, {4, 3, []int64{}}} {
		mustTx(t, s, func(tx store.Tx) error {
			h, err := tx.ReadHistory("r", tc.after, tc.limit)
			if err != nil {
				return err
			}
			ids := []int64{}
			for _, e := range h {
				ids = append(ids, e.EventId)
			}
			equal(t, ids, tc.ids)
			return nil
		})
	}
}

func historyProto(t *testing.T, s store.Store) {
	seed(t, s, "r")
	events := []*capstanv1.HistoryEvent{
		{EventId: 1, Type: capstanv1.EventType_EVENT_TYPE_ACTIVITY_SCHEDULED, Time: timestamppb.New(epoch.Add(789 * time.Nanosecond)), Attributes: &capstanv1.HistoryEvent_ActivityScheduled{ActivityScheduled: &capstanv1.ActivityScheduledAttributes{Seq: 9007199254740993, ActivityType: "binary", TaskQueue: "q", Input: payload(), ScheduleToCloseTimeout: durationpb.New(time.Hour), StartToCloseTimeout: durationpb.New(time.Second + 123*time.Nanosecond), RetryPolicy: &capstanv1.RetryPolicy{InitialInterval: durationpb.New(time.Second), BackoffCoefficient: 1.25, MaximumAttempts: 7, NonRetryableErrorTypes: []string{"A", "B"}}, TaskCompletedEventId: 42}}},
		{EventId: 2, Type: capstanv1.EventType_EVENT_TYPE_ACTIVITY_FAILED, Time: timestamppb.New(epoch), Attributes: &capstanv1.HistoryEvent_ActivityFailed{ActivityFailed: &capstanv1.ActivityFailedAttributes{Seq: 7, Attempt: 3, Failure: &capstanv1.Failure{Message: "failure", Details: payload(), Cause: &capstanv1.Failure{Type: "nested", NonRetryable: true}}}}},
		event(3),
	}
	// Unknown protobuf fields must survive storage, including fields added by a newer worker.
	events[0].ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x7b})
	events[0].GetActivityScheduled().Input.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x09})
	mustTx(t, s, func(tx store.Tx) error { return tx.AppendEvents("r", events) })
	mustTx(t, s, func(tx store.Tx) error {
		h, err := tx.ReadHistory("r", 0, 0)
		if err != nil {
			return err
		}
		equal(t, len(h), len(events))
		for i, e := range h {
			equalProto(t, e, events[i])
		}
		h[0].GetActivityScheduled().Input.Data[0] = 77
		return nil
	})
	mustTx(t, s, func(tx store.Tx) error {
		h, err := tx.ReadHistory("r", 0, 0)
		if err != nil {
			return err
		}
		equalProto(t, h[0], events[0])
		return nil
	})
}

func inboxDrain(t *testing.T, s store.Store) {
	seed(t, s, "r", "other")
	var want []*capstanv1.HistoryEvent
	mustTx(t, s, func(tx store.Tx) error {
		for i, id := range []int64{99, 2, 11} {
			e := event(id)
			e.Time = timestamppb.New(epoch.Add(-time.Duration(i) * time.Hour))
			e.GetSignalReceived().Name = string(rune('a' + i))
			if err := tx.PushInbox("r", e); err != nil {
				return err
			}
			equal(t, e.EventId, id)
			w := proto.Clone(e).(*capstanv1.HistoryEvent)
			w.EventId = 0
			want = append(want, w)
		}
		return tx.PushInbox("other", event(8))
	})
	mustTx(t, s, func(tx store.Tx) error {
		h, err := tx.DrainInbox("r")
		if err != nil {
			return err
		}
		equal(t, len(h), len(want))
		for i, e := range h {
			equalProto(t, e, want[i])
		}
		return nil
	})
	mustTx(t, s, func(tx store.Tx) error {
		h, err := tx.DrainInbox("r")
		if err != nil {
			return err
		}
		equal(t, len(h), 0)
		h, err = tx.DrainInbox("other")
		if err != nil {
			return err
		}
		equal(t, len(h), 1)
		return nil
	})
}

func inboxSize(t *testing.T, s store.Store) {
	seed(t, s, "r", "other")
	mustTx(t, s, func(tx store.Tx) error {
		n, err := tx.InboxSize("r")
		if err != nil {
			return err
		}
		equal(t, n, 0)
		for range 3 {
			if err := tx.PushInbox("r", event(0)); err != nil {
				return err
			}
		}
		if err := tx.PushInbox("other", event(0)); err != nil {
			return err
		}
		n, err = tx.InboxSize("r")
		if err != nil {
			return err
		}
		equal(t, n, 3)
		return nil
	})
}
