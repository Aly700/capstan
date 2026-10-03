package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Aly700/capstan/internal/lab"
)

func readReport(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestCLIBudgetStopsQueuingAndDrainsStartedSeeds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "budget.md")
	synctest.Test(t, func(t *testing.T) {
		var output bytes.Buffer
		var calls atomic.Int64
		code := runCLI([]string{"-seeds", "100", "-parallel", "3", "-duration", "10ms", "-out", path}, &output, func(ctx context.Context, seed int64, _ lab.Options) (lab.Result, error) {
			calls.Add(1)
			time.Sleep(time.Second)
			if ctx.Err() != nil {
				t.Errorf("scheduling budget canceled seed %d: %v", seed, ctx.Err())
			}
			return lab.Result{Seed: seed, Steps: 4, RootRuns: 2, TransactionSteps: 7, GateResponses: []string{"pending", "http-503", "timeout", "approved-after-deadline", "approved"}}, nil
		})
		if code != 0 || calls.Load() != 3 {
			t.Fatalf("code=%d calls=%d output=%s", code, calls.Load(), &output)
		}
		if !strings.Contains(output.String(), "Runs: 3; passed: 3") {
			t.Fatalf("requested limit reported as actual runs: %s", &output)
		}
		report := readReport(t, path)
		for _, want := range []string{"Runs: 3", "Requested seed limit: 100", "Time budget: 10ms", "Scheduling stopped: time budget reached", "Seed range: 0..2", "Root runs: 6", "Store transaction steps: 21", "| pending | 3 |", "| http-503 | 3 |", "| timeout | 3 |", "| approved-after-deadline | 3 |", "| approved | 3 |", "-seeds 100 -parallel 3 -duration 10ms", "-seeds 3 -parallel 3"} {
			if !strings.Contains(report, want) {
				t.Errorf("missing %q: %s", want, report)
			}
		}
	})
}

func TestCLIZeroCompletedSeedsCannotClaimSuccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.md")
	var output bytes.Buffer
	var calls atomic.Int64
	code := runCLI([]string{"-duration", "1ns", "-out", path}, &output, func(context.Context, int64, lab.Options) (lab.Result, error) { calls.Add(1); return lab.Result{}, nil })
	if code != 1 || calls.Load() != 0 {
		t.Fatalf("code=%d calls=%d output=%s", code, calls.Load(), &output)
	}
	report := readReport(t, path)
	if strings.Contains(report, "Confirmed defects found: 0") || !strings.Contains(report, "No defect estimate produced") || !strings.Contains(report, "Seed range: none") {
		t.Fatalf("empty campaign report: %s", report)
	}
}
