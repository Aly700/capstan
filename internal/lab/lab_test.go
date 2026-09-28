package lab

import (
	"context"
	"flag"
	"fmt"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/engine"
	"github.com/Aly700/capstan/internal/lab/labworker"
)

var seedFlag = flag.Int64("seed", -1, "replay exactly one fault-lab seed")
var seedsFlag = flag.Int("seeds", 2000, "number of seeded scenarios")
var knownFlag = flag.Bool("lab.known", true, "recognize only the documented known defect list")

func TestLab(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first, count := int64(0), *seedsFlag
		if *seedFlag >= 0 {
			first, count = *seedFlag, 1
		}
		faults := map[FaultKind]int{}
		if count < 1 {
			t.Fatal("seeds must be positive")
		}
		for offset := 0; offset < count; offset++ {
			seed := first + int64(offset)
			result, err := Run(context.Background(), seed, Options{Clock: engine.SystemClock{}, Advance: time.Sleep})
			if err != nil {
				if *knownFlag && KnownFailure(seed, err) {
					t.Logf("known defect: %v", err)
					continue
				}
				t.Fatalf("%v\nreproduce: go test ./internal/lab -run TestLab -seed %d -lab.known=false", err, seed)
			}
			for _, fault := range result.Faults {
				faults[fault.Kind]++
			}
		}
		t.Logf("validated %d seeds; injected faults: %v", count, faults)
	})
}

func TestSeedReproduces(t *testing.T) {
	first, err := Run(context.Background(), 17, Options{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := Run(context.Background(), 17, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first.Trace, second.Trace) || !reflect.DeepEqual(first.Faults, second.Faults) {
		t.Fatal("seed did not reproduce the same actions")
	}
}

func TestEveryFaultIsExercised(t *testing.T) {
	seen := map[FaultKind]bool{}
	for seed := int64(0); seed < 100; seed++ {
		result, err := Run(context.Background(), seed, Options{})
		if err != nil {
			t.Fatal(err)
		}
		for _, fault := range result.Faults {
			seen[fault.Kind] = true
		}
	}
	for _, kind := range AllFaults() {
		if !seen[kind] {
			t.Errorf("fault %s never exercised", kind)
		}
	}
}

func TestFaultSelection(t *testing.T) {
	for _, kind := range AllFaults() {
		t.Run(string(kind), func(t *testing.T) {
			seen := false
			for seed := int64(0); seed < 20 && !seen; seed++ {
				result, err := Run(context.Background(), seed, Options{Faults: []FaultKind{kind}})
				if err != nil {
					t.Fatal(err)
				}
				for _, fault := range result.Faults {
					if fault.Kind != kind {
						t.Fatal(fmt.Sprintf("selected %s but injected %s", kind, fault.Kind))
					}
					seen = true
				}
			}
			if !seen {
				t.Fatal("selected fault never injected")
			}
		})
	}
}

func TestWorkerCrashDropsBothPollers(t *testing.T) {
	w := &workerState{workflow: &v1.PollWorkflowTaskResponse{}, commands: &labworker.Result{}, activity: &v1.PollActivityTaskResponse{}, effect: &v1.Payload{}}
	w.kill()
	if w.workflow != nil || w.commands != nil || w.activity != nil || w.effect != nil {
		t.Fatal("worker crash retained an in-flight poller task")
	}
}

func TestSeedReproducesAcrossClockDrivers(t *testing.T) {
	for _, seed := range []int64{0, 14, 17, 437} {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			logical, err := Run(context.Background(), seed, Options{})
			if err != nil {
				t.Fatal(err)
			}
			synctest.Test(t, func(t *testing.T) {
				virtual, err := Run(context.Background(), seed, Options{Clock: engine.SystemClock{}, Advance: time.Sleep})
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(logical, virtual) {
					t.Fatal("CLI and synctest did not execute the same steps")
				}
			})
		})
	}
}
