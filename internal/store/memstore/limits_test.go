package memstore_test

import (
	"fmt"
	"testing"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
)

func TestQueryLimits(t *testing.T) {
	s := newStore(t)
	tx(t, s, func(tx store.Tx) error {
		for _, id := range []string{"a", "b"} {
			r := run(id)
			r.RunDeadline = epoch
			if err := tx.InsertRun(r); err != nil {
				return err
			}
			v := task(id, epoch)
			v.CheckAt = epoch
			if err := tx.InsertTask(v); err != nil {
				return err
			}
			if err := tx.InsertTimer(&store.Timer{RunID: id, Seq: 1, DueAt: epoch}); err != nil {
				return err
			}
			if err := tx.InsertApproval(&store.Approval{RunID: id, ApprovalID: "approval", Status: store.ApprovalPending, CheckAt: epoch}); err != nil {
				return err
			}
		}
		return tx.AppendEvents("a", []*capstanv1.HistoryEvent{event(1, "one"), event(2, "two")})
	})
	queries := []struct {
		name                string
		nonpositiveMeansAll bool
		count               func(store.Tx, int) (int, error)
	}{
		{name: "DueTasks", count: func(tx store.Tx, limit int) (int, error) {
			rows, err := tx.DueTasks(epoch, limit)
			return len(rows), err
		}},
		{name: "DueTimers", count: func(tx store.Tx, limit int) (int, error) {
			rows, err := tx.DueTimers(epoch, limit)
			return len(rows), err
		}},
		{name: "DueApprovals", count: func(tx store.Tx, limit int) (int, error) {
			rows, err := tx.DueApprovals(epoch, limit)
			return len(rows), err
		}},
		{name: "RunsPastDeadline", count: func(tx store.Tx, limit int) (int, error) {
			rows, err := tx.RunsPastDeadline(epoch, limit)
			return len(rows), err
		}},
		{name: "ReadHistory", nonpositiveMeansAll: true, count: func(tx store.Tx, limit int) (int, error) {
			rows, err := tx.ReadHistory("a", 0, limit)
			return len(rows), err
		}},
		{name: "ListRuns", nonpositiveMeansAll: true, count: func(tx store.Tx, limit int) (int, error) {
			rows, err := tx.ListRuns(store.RunFilter{Limit: limit})
			return len(rows), err
		}},
	}
	for _, query := range queries {
		t.Run(query.name, func(t *testing.T) {
			for _, limit := range []int{-1, 0, 1, 2, 3} {
				t.Run(fmt.Sprintf("limit=%d", limit), func(t *testing.T) {
					want := min(max(limit, 0), 2)
					if limit <= 0 && query.nonpositiveMeansAll {
						want = 2
					}
					tx(t, s, func(tx store.Tx) error {
						got, err := query.count(tx, limit)
						if err != nil {
							return err
						}
						if got != want {
							t.Fatalf("rows = %d, want %d", got, want)
						}
						return nil
					})
				})
			}
		})
	}
}
