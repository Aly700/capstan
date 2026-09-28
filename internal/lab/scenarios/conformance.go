// Package scenarios contains Go twins of the shared conformance workflows.
package scenarios

import (
	"errors"
	"time"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/lab/labworker"
)

// Registry maps the TypeScript export names to their Go workflows.
func Registry() map[string]labworker.Workflow {
	return map[string]labworker.Workflow{
		"singleActivity":         singleActivity,
		"parallelActivities":     parallelActivities,
		"retryInWorkflow":        retryInWorkflow,
		"sleeping":               sleeping,
		"raceActivityAndSleep":   raceActivityAndSleep,
		"immediateCondition":     immediateCondition,
		"signalledCondition":     signalledCondition,
		"bufferedHandler":        bufferedHandler,
		"orderedHandler":         orderedHandler,
		"twoSignals":             twoSignals,
		"recordedSideEffect":     recordedSideEffect,
		"recordedUuid":           recordedUUID,
		"patchBranch":            patchBranch,
		"deprecatedPatch":        deprecatedPatch,
		"stableRandom":           stableRandom,
		"activationTime":         activationTime,
		"continueCounting":       continueCounting,
		"conditionThenSignal":    conditionThenSignal,
		"renamedActivity":        renamedActivity,
		"removedActivity":        removedActivity,
		"markerChange":           markerChange,
		"approval":               approval,
		"observeActivityFailure": observeActivityFailure,
		"asyncSignalHandler":     asyncSignalHandler,
		"nestedContinuations":    nestedContinuations,
		"waitingOnly":            waitingOnly,
		"newSideEffect":          newSideEffect,
		"signalHandlerClock":     signalHandlerClock,
		"repeatedOldPatch":       repeatedOldPatch,
	}
}

func singleActivity(wf *labworker.Context, input any) (any, error) {
	doubled, err := wf.Activity("double", input.(map[string]any)["n"])
	if err != nil {
		return nil, err
	}
	return map[string]any{"doubled": doubled}, nil
}

func parallelActivities(wf *labworker.Context, _ any) (any, error) {
	return wf.All(
		func(wf *labworker.Context) (any, error) { return wf.Activity("double", 1) },
		func(wf *labworker.Context) (any, error) { return wf.Activity("double", 2) },
	)
}

func retryInWorkflow(wf *labworker.Context, _ any) (any, error) {
	value, err := wf.Activity("double", 1)
	if err != nil {
		return wf.Activity("double", 2)
	}
	return value, nil
}

func sleeping(wf *labworker.Context, _ any) (any, error) {
	if err := wf.Sleep(time.Second); err != nil {
		return nil, err
	}
	return "awake", nil
}

func raceActivityAndSleep(wf *labworker.Context, _ any) (any, error) {
	return wf.Race(
		func(wf *labworker.Context) (any, error) { return wf.Activity("double", 21) },
		func(wf *labworker.Context) (any, error) {
			if err := wf.Sleep(time.Second); err != nil {
				return nil, err
			}
			return "timeout", nil
		},
	)
}

func immediateCondition(wf *labworker.Context, _ any) (any, error) {
	return wf.Condition(func() bool { return true })
}

func signalledCondition(wf *labworker.Context, input any) (any, error) {
	ready := false
	wf.SetHandler("ready", func(value any) { ready = value.(bool) })
	if timeout, ok := input.(map[string]any)["timeout"]; ok {
		duration, err := time.ParseDuration(timeout.(string))
		if err != nil {
			return nil, err
		}
		return wf.Condition(func() bool { return ready }, duration)
	}
	return wf.Condition(func() bool { return ready })
}

func bufferedHandler(wf *labworker.Context, _ any) (any, error) {
	if _, err := wf.Activity("double", 1); err != nil {
		return nil, err
	}
	values := []any{}
	wf.SetHandler("number", func(value any) { values = append(values, value) })
	return values, nil
}

func orderedHandler(wf *labworker.Context, _ any) (any, error) {
	values := []any{}
	wf.SetHandler("number", func(value any) { values = append(values, value) })
	if _, err := wf.Condition(func() bool { return len(values) == 2 }); err != nil {
		return nil, err
	}
	return values, nil
}

func twoSignals(wf *labworker.Context, _ any) (any, error) {
	first, err := wf.Signal("number")
	if err != nil {
		return nil, err
	}
	second, err := wf.Signal("number")
	if err != nil {
		return nil, err
	}
	return []any{first, second}, nil
}

func recordedSideEffect(wf *labworker.Context, _ any) (any, error) {
	value := wf.SideEffect(func() any { panic("must use recorded side effect") })
	if _, err := wf.Activity("double", 1); err != nil {
		return nil, err
	}
	return value, nil
}

func recordedUUID(wf *labworker.Context, _ any) (any, error) {
	value := wf.UUID()
	if _, err := wf.Activity("double", 1); err != nil {
		return nil, err
	}
	return value, nil
}

func patchBranch(wf *labworker.Context, _ any) (any, error) {
	name := "double"
	if wf.Patched("v2") {
		name = "newDouble"
	}
	return wf.Activity(name, 21)
}

func deprecatedPatch(wf *labworker.Context, _ any) (any, error) {
	wf.DeprecatePatch("v2")
	return wf.Activity("newDouble", 21)
}

func stableRandom(wf *labworker.Context, _ any) (any, error) {
	first := wf.Random()
	if _, err := wf.Activity("double", 1); err != nil {
		return nil, err
	}
	return []float64{first, wf.Random()}, nil
}

func activationTime(wf *labworker.Context, _ any) (any, error) {
	first := wf.Now()
	if _, err := wf.Activity("double", 1); err != nil {
		return nil, err
	}
	return []float64{first, wf.Now()}, nil
}

func continueCounting(wf *labworker.Context, input any) (any, error) {
	wf.ContinueAsNew(map[string]any{"n": input.(map[string]any)["n"].(float64) + 1})
	panic("ContinueAsNew returned")
}

func conditionThenSignal(wf *labworker.Context, _ any) (any, error) {
	ready := false
	wf.SetHandler("ready", func(value any) { ready = value.(bool) })
	if _, err := wf.Condition(func() bool { return ready }, time.Second); err != nil {
		return nil, err
	}
	return wf.Signal("done")
}

func renamedActivity(wf *labworker.Context, _ any) (any, error) {
	return wf.Activity("triple", 21)
}

func removedActivity(_ *labworker.Context, _ any) (any, error) {
	return "removed", nil
}

func markerChange(wf *labworker.Context, _ any) (any, error) {
	wf.SideEffect(func() any { return 1 })
	return wf.Signal("done")
}

func approval(wf *labworker.Context, _ any) (any, error) {
	return wf.Approval(labworker.ApprovalRequest{
		ApprovalID: "approval-1", Source: capstanv1.ApprovalSource_APPROVAL_SOURCE_HUMAN, Prompt: "Ship?",
	})
}

func observeActivityFailure(wf *labworker.Context, _ any) (any, error) {
	if _, err := wf.Activity("double", 1); err != nil {
		var failure *labworker.Failure
		if !errors.As(err, &failure) {
			return nil, err
		}
		result := map[string]any{"type": failure.Type}
		if failure.TimeoutType != "" {
			result["timeoutType"] = failure.TimeoutType
		}
		if failure.Cause != nil {
			result["cause"] = failure.Cause.Type
		}
		return result, nil
	}
	return "unexpected", nil
}

func asyncSignalHandler(wf *labworker.Context, _ any) (any, error) {
	value := float64(0)
	// The shared fixture observes the drained handler's value; V8 microtask
	// scheduling itself is outside the Go reference worker's contract (D20).
	wf.SetHandler("number", func(input any) { value = input.(float64) })
	if _, err := wf.Condition(func() bool { return value != 0 }); err != nil {
		return nil, err
	}
	return value, nil
}

func nestedContinuations(wf *labworker.Context, _ any) (any, error) {
	values, err := wf.All(func(wf *labworker.Context) (any, error) {
		value, err := wf.Activity("double", 21)
		if err != nil {
			return nil, err
		}
		return value.(float64) + 1, nil
	})
	if err != nil {
		return nil, err
	}
	return values[0].(float64) * 2, nil
}

func waitingOnly(wf *labworker.Context, _ any) (any, error) {
	return wf.Signal("done")
}

func newSideEffect(wf *labworker.Context, _ any) (any, error) {
	return wf.SideEffect(func() any { return map[string]any{"saved": 7} }), nil
}

func signalHandlerClock(wf *labworker.Context, _ any) (any, error) {
	var observed float64
	hasObserved := false
	wf.SetHandler("go", func(any) {
		observed = wf.Now()
		hasObserved = true
	})
	if _, err := wf.Condition(func() bool { return hasObserved }); err != nil {
		return nil, err
	}
	return []float64{observed, wf.Now()}, nil
}

func repeatedOldPatch(wf *labworker.Context, _ any) (any, error) {
	first := wf.Patched("v2")
	if _, err := wf.Activity("double", 21); err != nil {
		return nil, err
	}
	return []bool{first, wf.Patched("v2")}, nil
}
