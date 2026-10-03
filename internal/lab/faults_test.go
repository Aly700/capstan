package lab

import (
	"errors"
	"fmt"
	"testing"
	"time"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"github.com/Aly700/capstan/internal/store/memstore"
)

var faultTestTime = time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)

func faultTestStore(t *testing.T) *FaultStore {
	t.Helper()
	s := NewFaultStore(memstore.New())
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func faultTestTx(t *testing.T, s store.Store, fn func(store.Tx) error) {
	t.Helper()
	if err := s.InTx(t.Context(), fn); err != nil {
		t.Fatal(err)
	}
}

func faultTestRecover(fn func() error) (err error, recovered any) {
	defer func() { recovered = recover() }()
	err = fn()
	return
}

func TestFaultStoreRollsBackAtEachOperation(t *testing.T) {
	for _, crash := range []bool{false, true} {
		for after := 1; after <= 5; after++ {
			t.Run(fmt.Sprintf("crash=%v/after=%d", crash, after), func(t *testing.T) {
				s := faultTestStore(t)
				run := &store.Run{RunID: "r", LastEventID: 1}
				faultTestTx(t, s, func(tx store.Tx) error {
					if err := tx.InsertRun(run); err != nil {
						return err
					}
					return tx.AppendEvents("r", []*capstanv1.HistoryEvent{{EventId: 1}})
				})
				wake, cancel := s.Subscribe(store.TaskWorkflow, "q")
				defer cancel()
				var task *store.Task
				write := func(tx store.Tx) error {
					if err := tx.AppendEvents("r", []*capstanv1.HistoryEvent{{EventId: 2}}); err != nil {
						return err
					}
					if err := tx.UpdateRun(&store.Run{RunID: "r", LastEventID: 2}); err != nil {
						return err
					}
					task = &store.Task{RunID: "r", Kind: store.TaskWorkflow, TaskQueue: "q"}
					if err := tx.InsertTask(task); err != nil {
						return err
					}
					if err := tx.InsertTimer(&store.Timer{RunID: "r", Seq: 1, DueAt: faultTestTime}); err != nil {
						return err
					}
					tx.Notify(store.TaskWorkflow, "q")
					return nil
				}
				s.Arm(after, crash)
				err, recovered := faultTestRecover(func() error { return s.InTx(t.Context(), write) })
				if crash {
					if recovered != ErrServerCrash {
						t.Fatalf("panic = %v, want ErrServerCrash", recovered)
					}
				} else if after == 5 {
					if recovered != ErrInjected {
						t.Fatalf("Notify panic = %v, want ErrInjected", recovered)
					}
				} else if recovered != nil || !errors.Is(err, ErrInjected) {
					t.Fatalf("error = %v, panic = %v, want ErrInjected", err, recovered)
				}
				if s.Hits() != 1 {
					t.Fatalf("hits = %d, want 1", s.Hits())
				}
				if after >= 3 && (task == nil || task.ID == 0) {
					t.Fatal("fault fired before InsertTask assigned its ID")
				}
				faultTestTx(t, s, func(tx store.Tx) error {
					r, err := tx.GetRun("r", false)
					if err != nil {
						return err
					}
					history, err := tx.ReadHistory("r", 0, 0)
					if err != nil {
						return err
					}
					tasks, err := tx.RunTasks("r")
					if err != nil {
						return err
					}
					timers, err := tx.RunTimers("r")
					if err != nil {
						return err
					}
					if r.LastEventID != 1 || len(history) != 1 || history[0].EventId != 1 || len(tasks) != 0 || len(timers) != 0 {
						t.Fatalf("rollback failed: last=%d history=%v tasks=%v timers=%v", r.LastEventID, history, tasks, timers)
					}
					return nil
				})
				select {
				case <-wake:
					t.Fatal("rolled-back transaction delivered a notification")
				default:
				}
				faultTestTx(t, s, write)
				select {
				case <-wake:
				default:
					t.Fatal("retry did not commit its notification")
				}
				if s.Hits() != 1 {
					t.Fatalf("one arm injected %d faults", s.Hits())
				}
			})
		}
	}
}

func TestFaultStoreInterceptsEveryTxMethod(t *testing.T) {
	operations := []struct {
		name string
		call func(store.Tx) error
	}{
		{"InsertRun", func(tx store.Tx) error { return tx.InsertRun(&store.Run{RunID: "new"}) }},
		{"GetRun", func(tx store.Tx) error { _, err := tx.GetRun("r", true); return err }},
		{"UpdateRun", func(tx store.Tx) error { return tx.UpdateRun(&store.Run{RunID: "r"}) }},
		{"ListRuns", func(tx store.Tx) error { _, err := tx.ListRuns(store.RunFilter{}); return err }},
		{"RunsPastDeadline", func(tx store.Tx) error { _, err := tx.RunsPastDeadline(faultTestTime, 1); return err }},
		{"AppendEvents", func(tx store.Tx) error { return tx.AppendEvents("r", []*capstanv1.HistoryEvent{{EventId: 1}}) }},
		{"ReadHistory", func(tx store.Tx) error { _, err := tx.ReadHistory("r", 0, 0); return err }},
		{"PushInbox", func(tx store.Tx) error { return tx.PushInbox("r", &capstanv1.HistoryEvent{}) }},
		{"DrainInbox", func(tx store.Tx) error { _, err := tx.DrainInbox("r"); return err }},
		{"InboxSize", func(tx store.Tx) error { _, err := tx.InboxSize("r"); return err }},
		{"InsertTask", func(tx store.Tx) error { return tx.InsertTask(&store.Task{RunID: "r"}) }},
		{"ClaimTask", func(tx store.Tx) error {
			_, err := tx.ClaimTask(store.TaskWorkflow, "q", faultTestTime, time.Second, "worker")
			return err
		}},
		{"GetTask", func(tx store.Tx) error { _, err := tx.GetTask(1, true); return err }},
		{"UpdateTask", func(tx store.Tx) error { return tx.UpdateTask(&store.Task{ID: 1, RunID: "r"}) }},
		{"DeleteTask", func(tx store.Tx) error { return tx.DeleteTask(1) }},
		{"RunTasks", func(tx store.Tx) error { _, err := tx.RunTasks("r"); return err }},
		{"DueTasks", func(tx store.Tx) error { _, err := tx.DueTasks(faultTestTime, 1); return err }},
		{"InsertTimer", func(tx store.Tx) error { return tx.InsertTimer(&store.Timer{RunID: "r", Seq: 2}) }},
		{"DeleteTimer", func(tx store.Tx) error { _, err := tx.DeleteTimer("r", 1); return err }},
		{"DueTimers", func(tx store.Tx) error { _, err := tx.DueTimers(faultTestTime, 1); return err }},
		{"RunTimers", func(tx store.Tx) error { _, err := tx.RunTimers("r"); return err }},
		{"InsertApproval", func(tx store.Tx) error { return tx.InsertApproval(&store.Approval{RunID: "r", ApprovalID: "new"}) }},
		{"GetApproval", func(tx store.Tx) error { _, err := tx.GetApproval("r", "a", true); return err }},
		{"UpdateApproval", func(tx store.Tx) error { return tx.UpdateApproval(&store.Approval{RunID: "r", ApprovalID: "a"}) }},
		{"RunApprovals", func(tx store.Tx) error { _, err := tx.RunApprovals("r"); return err }},
		{"DueApprovals", func(tx store.Tx) error { _, err := tx.DueApprovals(faultTestTime, 1); return err }},
		{"RecordSignalRequest", func(tx store.Tx) error { return tx.RecordSignalRequest("r", "request") }},
		{"LockBudget", func(tx store.Tx) error { return tx.LockBudget() }},
		{"InsertAICall", func(tx store.Tx) error { return tx.InsertAICall(&store.AICall{RunID: "r"}) }},
		{"GetAICall", func(tx store.Tx) error { _, err := tx.GetAICall(1, true); return err }},
		{"UpdateAICall", func(tx store.Tx) error { return tx.UpdateAICall(&store.AICall{ID: 1, RunID: "r"}) }},
		{"SpentSince", func(tx store.Tx) error { _, err := tx.SpentSince(faultTestTime); return err }},
		{"RunCost", func(tx store.Tx) error { _, err := tx.RunCost("r"); return err }},
		{"Notify", func(tx store.Tx) error { tx.Notify(store.TaskWorkflow, "q"); return nil }},
	}
	for _, op := range operations {
		t.Run(op.name, func(t *testing.T) {
			s := faultTestStore(t)
			faultTestTx(t, s, func(tx store.Tx) error {
				steps := []func() error{
					func() error { return tx.InsertRun(&store.Run{RunID: "r"}) },
					func() error { return tx.InsertTask(&store.Task{RunID: "r", Kind: store.TaskWorkflow, TaskQueue: "q"}) },
					func() error { return tx.InsertTimer(&store.Timer{RunID: "r", Seq: 1}) },
					func() error { return tx.InsertApproval(&store.Approval{RunID: "r", ApprovalID: "a"}) },
					func() error { return tx.InsertAICall(&store.AICall{RunID: "r"}) },
				}
				for _, step := range steps {
					if err := step(); err != nil {
						return err
					}
				}
				return nil
			})
			s.Arm(1, false)
			err, recovered := faultTestRecover(func() error {
				return s.InTx(t.Context(), func(tx store.Tx) error {
					err := op.call(tx)
					if op.name == "Notify" || !errors.Is(err, ErrInjected) {
						t.Errorf("%s returned %v before callback finished; want an immediate fault", op.name, err)
					}
					return err
				})
			})
			if op.name == "Notify" {
				if recovered != ErrInjected {
					t.Fatalf("Notify panic = %v", recovered)
				}
			} else if recovered != nil || !errors.Is(err, ErrInjected) {
				t.Fatalf("error=%v panic=%v", err, recovered)
			}
			if s.Hits() != 1 {
				t.Fatalf("hits=%d", s.Hits())
			}
		})
	}
}

func TestFaultStorePreservesRealErrorsAndCountsOnlySuccess(t *testing.T) {
	s := faultTestStore(t)
	s.Arm(1, false)
	err := s.InTx(t.Context(), func(tx store.Tx) error {
		if _, err := tx.GetRun("missing", false); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("real error replaced: %v", err)
		}
		if s.Hits() != 0 {
			t.Fatal("failed operation counted toward injection")
		}
		return tx.InsertRun(&store.Run{RunID: "r"})
	})
	if !errors.Is(err, ErrInjected) || s.Hits() != 1 {
		t.Fatalf("error=%v hits=%d", err, s.Hits())
	}
	failure := errors.New("callback failed")
	s.Arm(10, true)
	if err := s.InTx(t.Context(), func(store.Tx) error { return failure }); err != failure {
		t.Fatalf("callback error=%v, want original", err)
	}
	faultTestTx(t, s, func(store.Tx) error { return nil })
	if s.Hits() != 1 {
		t.Fatalf("fault escaped failed callback into next transaction: hits=%d", s.Hits())
	}
}

func TestFaultStoreIgnoredInjectionStillRollsBack(t *testing.T) {
	s := faultTestStore(t)
	s.Arm(1, false)
	err := s.InTx(t.Context(), func(tx store.Tx) error {
		_ = tx.InsertRun(&store.Run{RunID: "r"})
		return nil
	})
	if !errors.Is(err, ErrInjected) {
		t.Fatalf("ignored fault allowed commit: %v", err)
	}
	faultTestTx(t, s, func(tx store.Tx) error {
		_, err := tx.GetRun("r", false)
		if !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("failed mutation committed: %v", err)
		}
		return nil
	})
}

func TestFaultStoreSelectsLaterTransaction(t *testing.T) {
	for _, crash := range []bool{false, true} {
		t.Run(fmt.Sprintf("crash=%v", crash), func(t *testing.T) {
			s := faultTestStore(t)
			s.ArmTransaction(2, 1, crash)
			faultTestTx(t, s, func(tx store.Tx) error {
				return tx.InsertRun(&store.Run{RunID: "r", WorkflowType: "first committed"})
			})
			if s.Hits() != 0 {
				t.Fatal("fault fired before the selected transaction")
			}
			err, recovered := faultTestRecover(func() error {
				return s.InTx(t.Context(), func(tx store.Tx) error {
					return tx.UpdateRun(&store.Run{RunID: "r", WorkflowType: "second rolled back"})
				})
			})
			if crash {
				if recovered != ErrServerCrash {
					t.Fatalf("panic=%v, want ErrServerCrash", recovered)
				}
			} else if recovered != nil || !errors.Is(err, ErrInjected) {
				t.Fatalf("error=%v panic=%v, want ErrInjected", err, recovered)
			}
			faultTestTx(t, s, func(tx store.Tx) error {
				run, err := tx.GetRun("r", false)
				if err != nil {
					return err
				}
				if run.WorkflowType != "first committed" {
					t.Fatalf("selected transaction escaped rollback: workflow_type=%q", run.WorkflowType)
				}
				run.WorkflowType = "third committed"
				return tx.UpdateRun(run)
			})
			if s.Hits() != 1 {
				t.Fatalf("one arm injected %d faults", s.Hits())
			}
		})
	}
}
