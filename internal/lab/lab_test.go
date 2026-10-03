package lab

import (
	"context"
	"flag"
	"fmt"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Aly700/capstan/internal/engine"
)

var seedFlag = flag.Int64("seed", -1, "replay exactly one fault-lab seed")
var seedsFlag = flag.Int("seeds", 2000, "number of seeded scenarios")
var traceSeedsFlag = flag.Bool("lab.trace-seeds", false, "log each seed before running it for mutation triage")
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
			if *traceSeedsFlag {
				t.Logf("mutation seed %d", seed)
			}
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
