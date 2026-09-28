package lab

import (
	"context"
	"github.com/Aly700/capstan/internal/store"
	"github.com/Aly700/capstan/internal/store/memstore"
	"math/rand"
	"reflect"
	"testing"
)

func TestTransactionSchedulerInterleavesAPITransactions(t *testing.T) {
	s := &scheduledStore{Store: memstore.New()}
	var order []int
	actions := []func(context.Context) error{}
	for actor := range 2 {
		actions = append(actions, func(ctx context.Context) error {
			for range 2 {
				if err := s.InTx(ctx, func(store.Tx) error { order = append(order, actor); return nil }); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err := s.interleave(context.Background(), rand.New(rand.NewSource(3)), actions, nil); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(order, []int{0, 1, 0, 1}) {
		t.Fatalf("transaction order %v; expected overlapping actor calls", order)
	}
}

func TestConcurrentRunsShareQueueAndKeepSeparateOutcomes(t *testing.T) {
	result, err := Run(context.Background(), 1, Options{NoFaults: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.RootRuns != 2 {
		t.Fatalf("got %d roots, want two sharing the queue", result.RootRuns)
	}
	if result.TransactionSteps == 0 {
		t.Fatal("no transaction-level interleaving")
	}
}

func TestGateFaultResponsesAreExercised(t *testing.T) {
	for _, name := range []string{"gate-errors", "gate-late"} {
		result, err := Run(context.Background(), 1, Options{NoFaults: true, Scenario: name})
		if err != nil {
			t.Fatal(err)
		}
		expected := []string{"pending", "http-503", "timeout", "approved"}
		if name == "gate-late" {
			expected = []string{"approved-after-deadline"}
		}
		if !reflect.DeepEqual(result.GateResponses, expected) {
			t.Fatalf("%s Gate responses=%v, want %v", name, result.GateResponses, expected)
		}
	}
}

func TestGateOracleRequiresRecoveryFromEveryClientResponse(t *testing.T) {
	for _, responses := range [][]string{{"pending"}, {"pending", "approved"}, {"pending", "http-503", "approved"}} {
		if err := checkGateResponses("gate-errors", responses); err == nil {
			t.Fatalf("accepted premature resolution after %v", responses)
		}
	}
}

func TestCompanionKeepsTwoEffectsWithoutRedundantTimer(t *testing.T) {
	_, snapshot, err := runScenario(context.Background(), 0, scenarioCatalog()[0], Options{NoFaults: true, Workers: 3, MaxSteps: 1000})
	if err != nil {
		t.Fatal(err)
	}
	timers := map[string]int{}
	activities := map[string]int{}
	for id, history := range snapshot.Histories {
		for _, event := range history {
			if event.GetTimerStarted() != nil {
				timers[id]++
			}
			if event.GetActivityScheduled() != nil {
				activities[id]++
			}
		}
	}
	if timers["peer"] != 0 {
		t.Fatalf("companion scheduled %d redundant timers", timers["peer"])
	}
	if timers["lab"] != 1 || activities["lab"] != 2 || activities["peer"] != 2 {
		t.Fatalf("primary timers=%d, activity counts=%v", timers["lab"], activities)
	}
}
