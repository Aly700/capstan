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
