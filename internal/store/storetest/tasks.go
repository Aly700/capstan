package storetest

import (
	"testing"
	"time"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/types/known/durationpb"
)

func tasksIDs(t *testing.T, s store.Store) {
	seed(t, s, "r", "other")
	var last int64
	for _, runID := range []string{"r", "other", "r"} {
		task := sampleTask(runID)
		mustTx(t, s, func(tx store.Tx) error { return tx.InsertTask(task) })
		if task.ID <= last {
			t.Fatalf("id %d did not increase after %d", task.ID, last)
		}
		last = task.ID
	}
}

func tasksClaimOldest(t *testing.T, s store.Store) {
	seed(t, s, "r")
	late, early, tie, leased, kind, queue := sampleTask("r"), sampleTask("r"), sampleTask("r"), sampleTask("r"), sampleTask("r"), sampleTask("r")
	early.VisibleAt = epoch.Add(-time.Hour)
	tie.VisibleAt = early.VisibleAt
	leased.VisibleAt = epoch.Add(-2 * time.Hour)
	leased.LeasedUntil = epoch.Add(-time.Second)
	kind.Kind = store.TaskWorkflow
	kind.VisibleAt = leased.VisibleAt
	queue.TaskQueue = "other"
	queue.VisibleAt = leased.VisibleAt
	mustTx(t, s, func(tx store.Tx) error {
		for _, v := range []*store.Task{late, early, tie, leased, kind, queue} {
			if err := tx.InsertTask(v); err != nil {
				return err
			}
		}
		return nil
	})
	for _, want := range []*store.Task{early, tie, late} {
		mustTx(t, s, func(tx store.Tx) error {
			got, err := tx.ClaimTask(store.TaskActivity, "q", epoch, 17*time.Second, "worker")
			if err != nil {
				return err
			}
			expected := *want
			expected.LeasedUntil = epoch.Add(17 * time.Second)
			expected.WorkerID = "worker"
			expected.StartedAt = epoch
			equalTask(t, got, &expected)
			persisted, err := tx.GetTask(got.ID, false)
			if err != nil {
				return err
			}
			equalTask(t, persisted, &expected)
			return nil
		})
	}
	mustTx(t, s, func(tx store.Tx) error {
		got, err := tx.ClaimTask(store.TaskActivity, "q", epoch, time.Second, "worker")
		if err != nil {
			return err
		}
		if got != nil {
			t.Fatalf("claimed leased/wrong queue/wrong kind task: %+v", got)
		}
		return nil
	})
}

func tasksVisible(t *testing.T, s store.Store) {
	seed(t, s, "r")
	v := sampleTask("r")
	v.VisibleAt = epoch.Add(time.Second)
	mustTx(t, s, func(tx store.Tx) error { return tx.InsertTask(v) })
	mustTx(t, s, func(tx store.Tx) error {
		got, err := tx.ClaimTask(store.TaskActivity, "q", epoch, time.Second, "w")
		if err != nil {
			return err
		}
		if got != nil {
			t.Fatal("claimed future task")
		}
		return nil
	})
	mustTx(t, s, func(tx store.Tx) error {
		got, err := tx.ClaimTask(store.TaskActivity, "q", v.VisibleAt, time.Second, "w")
		if err != nil {
			return err
		}
		if got == nil {
			t.Fatal("did not claim at visibility boundary")
		}
		equal(t, got.ID, v.ID)
		return nil
	})
}

func tasksClaimNone(t *testing.T, s store.Store) {
	mustTx(t, s, func(tx store.Tx) error {
		got, err := tx.ClaimTask(store.TaskWorkflow, "missing", epoch, time.Second, "w")
		if got != nil {
			t.Fatalf("claim from empty store: %+v", got)
		}
		return err
	})
}

func tasksUpdate(t *testing.T, s store.Store) {
	seed(t, s, "r", "other")
	v := sampleTask("r")
	v.Activity = &capstanv1.ActivityScheduledAttributes{ActivityType: "act", Input: payload(), StartToCloseTimeout: durationpb.New(3*time.Second + time.Nanosecond)}
	mustTx(t, s, func(tx store.Tx) error { return tx.InsertTask(v) })
	mustTx(t, s, func(tx store.Tx) error {
		got, err := tx.GetTask(v.ID, true)
		if err != nil {
			return err
		}
		equalTask(t, got, v)
		return nil
	})
	v.Kind = store.TaskWorkflow
	v.RunID = "other"
	v.TaskQueue = "changed"
	v.ScheduledEventID = 33
	v.Attempt = 4
	v.VisibleAt = epoch.Add(time.Minute)
	v.LeasedUntil = epoch.Add(2 * time.Minute)
	v.WorkerID = "w"
	v.StartedAt = epoch.Add(time.Second)
	v.ScheduledAt = epoch.Add(-time.Hour)
	v.CheckAt = epoch.Add(30 * time.Second)
	v.Activity = &capstanv1.ActivityScheduledAttributes{
		ActivityType: "new", Input: &capstanv1.Payload{},
		ScheduleToStartTimeout: durationpb.New(time.Second + time.Nanosecond),
		StartToCloseTimeout:    durationpb.New(2*time.Second + 123*time.Nanosecond),
		ScheduleToCloseTimeout: durationpb.New(3*time.Second + 456*time.Nanosecond),
		HeartbeatTimeout:       durationpb.New(time.Millisecond + 789*time.Nanosecond),
		RetryPolicy: &capstanv1.RetryPolicy{
			InitialInterval: durationpb.New(time.Second + time.Nanosecond),
			MaximumInterval: durationpb.New(time.Minute + 123*time.Nanosecond),
			MaximumAttempts: 9, NonRetryableErrorTypes: []string{"stop"},
		},
	}
	v.LastHeartbeatAt = epoch.Add(5 * time.Second)
	v.HeartbeatDetails = payload()
	v.LastFailure = &capstanv1.Failure{Type: "retry", Cause: &capstanv1.Failure{Details: payload()}}
	v.CancelRequested = true
	v.StartedEventID = 34
	mustTx(t, s, func(tx store.Tx) error { return tx.UpdateTask(v) })
	mustTx(t, s, func(tx store.Tx) error {
		got, err := tx.GetTask(v.ID, false)
		if err != nil {
			return err
		}
		equalTask(t, got, v)
		return nil
	})
	// D23: replace every non-key field, including clearing optional values.
	id := v.ID
	v = sampleTask("r")
	v.ID = id
	mustTx(t, s, func(tx store.Tx) error { return tx.UpdateTask(v) })
	mustTx(t, s, func(tx store.Tx) error {
		got, err := tx.GetTask(v.ID, true)
		if err != nil {
			return err
		}
		equalTask(t, got, v)
		return nil
	})
	isError(t, s.InTx(t.Context(), func(tx store.Tx) error { _, err := tx.GetTask(v.ID+1000, true); return err }), store.ErrNotFound)
}

func tasksDue(t *testing.T, s store.Store) {
	seed(t, s, "r")
	var expected []int64
	mustTx(t, s, func(tx store.Tx) error {
		for _, d := range []time.Duration{-2 * time.Hour, -time.Hour, 0, time.Hour} {
			v := sampleTask("r")
			v.CheckAt = epoch.Add(d)
			if err := tx.InsertTask(v); err != nil {
				return err
			}
			if d <= 0 {
				expected = append(expected, v.ID)
			}
		}
		return tx.InsertTask(sampleTask("r"))
	})
	for _, limit := range []int{-1, 0, 2, 10} {
		mustTx(t, s, func(tx store.Tx) error {
			vs, err := tx.DueTasks(epoch, limit)
			if err != nil {
				return err
			}
			ids := []int64{}
			for _, v := range vs {
				ids = append(ids, v.ID)
			}
			want := expected
			want = want[:min(max(limit, 0), len(want))]
			equal(t, ids, want)
			return nil
		})
	}
}

func tasksDelete(t *testing.T, s store.Store) {
	seed(t, s, "r")
	v := sampleTask("r")
	mustTx(t, s, func(tx store.Tx) error {
		if err := tx.InsertTask(v); err != nil {
			return err
		}
		if err := tx.DeleteTask(v.ID); err != nil {
			return err
		}
		return tx.DeleteTask(v.ID)
	})
	isError(t, s.InTx(t.Context(), func(tx store.Tx) error { _, err := tx.GetTask(v.ID, false); return err }), store.ErrNotFound)
}

func tasksRun(t *testing.T, s store.Store) {
	seed(t, s, "r", "other")
	var want []*store.Task
	mustTx(t, s, func(tx store.Tx) error {
		for _, id := range []string{"r", "other", "r"} {
			v := sampleTask(id)
			if len(want) > 0 {
				v.Kind = store.TaskWorkflow
			}
			if err := tx.InsertTask(v); err != nil {
				return err
			}
			if id == "r" {
				want = append(want, v)
			}
		}
		return nil
	})
	mustTx(t, s, func(tx store.Tx) error {
		got, err := tx.RunTasks("r")
		if err != nil {
			return err
		}
		equal(t, len(got), len(want))
		for i, v := range got {
			equalTask(t, v, want[i])
		}
		return nil
	})
}
