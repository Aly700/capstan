package engine

import (
	"context"
	"testing"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestNextWakeupIsEarliestDeadline(t *testing.T) {
	for _, first := range []string{"timer", "task", "approval", "run"} {
		t.Run(first, func(t *testing.T) {
			e, clock, s := newTestEngine(t)
			now := clock.Now()
			at := map[string]time.Time{"timer": now.Add(time.Hour), "task": now.Add(2 * time.Hour), "approval": now.Add(3 * time.Hour), "run": now.Add(4 * time.Hour)}
			at[first] = now.Add(time.Second)
			if _, err := e.StartRun(context.Background(), "owner", &v1.StartRunRequest{RunId: "wakeup", WorkflowType: "wf", TaskQueue: "q", RunTimeout: durationpb.New(at["run"].Sub(now))}); err != nil {
				t.Fatal(err)
			}
			if err := s.InTx(context.Background(), func(tx store.Tx) error {
				r, err := tx.GetRun("wakeup", true)
				if err != nil {
					return err
				}
				if err := tx.InsertTimer(&store.Timer{RunID: r.RunID, Seq: 1, StartedEventID: 1, DueAt: at["timer"]}); err != nil {
					return err
				}
				if err := tx.InsertTask(&store.Task{Kind: store.TaskActivity, RunID: r.RunID, TaskQueue: "q", Attempt: 1, ScheduledEventID: 1, ScheduledAt: now, VisibleAt: now, CheckAt: at["task"]}); err != nil {
					return err
				}
				return tx.InsertApproval(&store.Approval{RunID: r.RunID, ApprovalID: "wakeup", Seq: 2, RequestedEventID: 1, Source: v1.ApprovalSource_APPROVAL_SOURCE_HUMAN, Status: store.ApprovalPending, RequestedAt: now, DueAt: at["approval"], CheckAt: at["approval"]})
			}); err != nil {
				t.Fatal(err)
			}
			got, ok, err := e.NextWakeup(context.Background())
			if err != nil || !ok || !got.Equal(at[first]) {
				t.Fatalf("next=%v ok=%v err=%v want=%v", got, ok, err, at[first])
			}
		})
	}
}

func TestNextWakeupEmptyAndOverdue(t *testing.T) {
	e, clock, s := newTestEngine(t)
	if at, ok, err := e.NextWakeup(context.Background()); err != nil || ok || !at.IsZero() {
		t.Fatalf("empty: %v %v %v", at, ok, err)
	}
	mustStart(t, e, "wakeup")
	due := clock.Now().Add(-time.Hour)
	if err := s.InTx(context.Background(), func(tx store.Tx) error { return tx.InsertTimer(&store.Timer{RunID: "wakeup", Seq: 1, DueAt: due}) }); err != nil {
		t.Fatal(err)
	}
	if at, ok, err := e.NextWakeup(context.Background()); err != nil || !ok || !at.Equal(due) {
		t.Fatalf("overdue: %v %v %v", at, ok, err)
	}
}
