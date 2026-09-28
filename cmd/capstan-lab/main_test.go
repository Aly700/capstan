package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Aly700/capstan/internal/lab"
)

func TestCLIRejectsInvalidOptionsBeforeRunning(t *testing.T) {
	for _, args := range [][]string{
		{"-seeds", "0"}, {"-seeds", "-2"}, {"-parallel", "0"},
		{"-workers", "0"}, {"-max-steps", "0"}, {"-seed", "-1"},
		{"-faults", "unknown"}, {"-faults", "kill-workflow,,late-ack"},
		{"-faults", "kill-workflow,kill-workflow"}, {"-faults", "none,late-ack"},
		{"-faults", ""}, {"-out", ""}, {"unexpected"}, {"-missing"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var output bytes.Buffer
			called := false
			code := runCLI(args, &output, func(context.Context, int64, lab.Options) (lab.Result, error) {
				called = true
				return lab.Result{}, nil
			})
			if code != 2 || called || output.Len() == 0 {
				t.Fatalf("code=%d called=%t output=%q", code, called, output.String())
			}
		})
	}
}

func TestCLIRejectsUnknownScenarioBeforeRunning(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid-scenario.md")
	var output bytes.Buffer
	var calls atomic.Int64
	code := runCLI([]string{"-seeds", "2", "-scenario", "no-such-scenario", "-out", path}, &output, func(context.Context, int64, lab.Options) (lab.Result, error) {
		calls.Add(1)
		return lab.Result{}, nil
	})
	if code != 2 || calls.Load() != 0 || !strings.Contains(output.String(), `unknown scenario "no-such-scenario"`) {
		t.Fatalf("code=%d calls=%d output=%q", code, calls.Load(), output.String())
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid scenario created a campaign report: %v", err)
	}
	if strings.Contains(output.String(), "Runs:") {
		t.Fatalf("invalid scenario was reported as a campaign: %s", &output)
	}
}

func TestCLIWritesCampaignSummaryAndPassesOptions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "campaign.md")
	var output bytes.Buffer
	var mu sync.Mutex
	seen := map[int64]bool{}
	code := runCLI([]string{"-seeds", "3", "-parallel", "2", "-workers", "5", "-max-steps", "80", "-scenario", "pipeline", "-faults", "late-ack, kill-workflow", "-out", path}, &output,
		func(ctx context.Context, seed int64, opts lab.Options) (lab.Result, error) {
			if ctx == nil || opts.Workers != 5 || opts.MaxSteps != 80 || opts.Scenario != "pipeline" || opts.NoFaults || !reflect.DeepEqual(opts.Faults, []lab.FaultKind{lab.KillWorkflow, lab.LateAck}) {
				t.Errorf("unexpected options: %+v", opts)
			}
			mu.Lock()
			seen[seed] = true
			mu.Unlock()
			return lab.Result{Seed: seed, Scenario: "pipeline", Steps: int(seed) + 1, Faults: []lab.FaultRecord{{Kind: lab.LateAck}, {Kind: lab.KillWorkflow}}}, nil
		})
	if code != 0 {
		t.Fatalf("code=%d output=%s", code, &output)
	}
	if !reflect.DeepEqual(seen, map[int64]bool{0: true, 1: true, 2: true}) {
		t.Fatalf("seeds=%v", seen)
	}
	report := readReport(t, path)
	for _, want := range []string{"Runs: 3", "Passed: 3", "Failing seeds: 0", "Confirmed defects found: 0", "Seed range: 0..2", "Workers per seed: 5", "Parallelism: 2", "Maximum steps per seed: 80", "Scenario: pipeline", "Fault selection: kill-workflow,late-ack", "Total steps: 6", "Elapsed:", "| kill-workflow | 3 |", "| late-ack | 3 |"} {
		if !strings.Contains(report, want) {
			t.Errorf("report missing %q:\n%s", want, report)
		}
	}
	if !strings.Contains(output.String(), path) {
		t.Errorf("output does not name report: %s", &output)
	}
}

func TestCLISingleSeedAllowsZeroAndDisablesFaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "single.md")
	var output bytes.Buffer
	calls := 0
	code := runCLI([]string{"-seed", "0", "-seeds", "10", "-faults", "none", "-out", path}, &output, func(_ context.Context, seed int64, opts lab.Options) (lab.Result, error) {
		calls++
		if seed != 0 || !opts.NoFaults || len(opts.Faults) != 0 {
			t.Errorf("seed=%d options=%+v", seed, opts)
		}
		return lab.Result{Seed: seed}, nil
	})
	if code != 0 || calls != 1 {
		t.Fatalf("code=%d calls=%d output=%s", code, calls, &output)
	}
	if report := readReport(t, path); !strings.Contains(report, "Seed range: 0..0") || !strings.Contains(report, "Fault selection: none") {
		t.Fatal(report)
	}
}

func TestCLIFailuresAreSortedAndReproducible(t *testing.T) {
	path := filepath.Join(t.TempDir(), "failed.md")
	var output bytes.Buffer
	released := make(chan struct{})
	code := runCLI([]string{"-seeds", "3", "-parallel", "3", "-workers", "5", "-max-steps", "80", "-scenario", "pipeline", "-faults", "late-ack", "-out", path}, &output, func(_ context.Context, seed int64, _ lab.Options) (lab.Result, error) {
		if seed == 0 {
			<-released
		}
		if seed == 2 {
			close(released)
		}
		result := lab.Result{Seed: seed, Scenario: "pipeline", Steps: 9, Trace: []string{fmt.Sprintf("step for seed %d", seed)}, Faults: []lab.FaultRecord{{Step: 2, Kind: lab.LateAck, Detail: "ack after lease"}}}
		if seed != 1 {
			return result, fmt.Errorf("invariant failed at seed %d", seed)
		}
		return result, nil
	})
	if code != 1 {
		t.Fatalf("code=%d output=%s", code, &output)
	}
	report := readReport(t, path)
	for _, want := range []string{"Runs: 3", "Passed: 1", "Failing seeds: 2", "Unrecognized failures: 2", "pending triage", "go test ./internal/lab -run TestLab -seed 0 -lab.known=false", "go test ./internal/lab -run TestLab -seed 2 -lab.known=false", "step for seed 0", "step for seed 2", "ack after lease", "-seed 0 -workers 5 -max-steps 80 -scenario pipeline -faults late-ack"} {
		if !strings.Contains(report, want) {
			t.Errorf("report missing %q:\n%s", want, report)
		}
	}
	first, last := strings.Index(report, "### Seed 0"), strings.Index(report, "### Seed 2")
	if first < 0 || last < first {
		t.Errorf("failure reports are not in seed order:\n%s", report)
	}
	if strings.Contains(report, "Confirmed defects found: 2") {
		t.Error("failing seeds are not confirmed defects")
	}
	if !strings.Contains(output.String(), "go test ./internal/lab -run TestLab -seed 0 -lab.known=false") {
		t.Errorf("missing reproduction on output: %s", &output)
	}
}

func TestCLIBoundsParallelRuns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bounded.md")
	var output bytes.Buffer
	var active, maxActive, calls atomic.Int64
	started := make(chan struct{}, 20)
	release := make(chan struct{})
	done := make(chan int, 1)
	go func() {
		done <- runCLI([]string{"-seeds", "20", "-parallel", "3", "-out", path}, &output, func(_ context.Context, seed int64, _ lab.Options) (lab.Result, error) {
			n := active.Add(1)
			for old := maxActive.Load(); n > old && !maxActive.CompareAndSwap(old, n); old = maxActive.Load() {
			}
			started <- struct{}{}
			<-release
			active.Add(-1)
			calls.Add(1)
			return lab.Result{Seed: seed}, nil
		})
	}()
	for range 3 {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			close(release)
			t.Fatal("pool did not start three runs")
		}
	}
	close(release)
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("code=%d output=%s", code, &output)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("campaign did not finish")
	}
	if got := maxActive.Load(); got != 3 {
		t.Fatalf("maximum active runs=%d, want 3", got)
	}
	if got := calls.Load(); got != 20 {
		t.Fatalf("calls=%d, want 20", got)
	}
}

func TestCLIReportsOutputWriteFailure(t *testing.T) {
	var output bytes.Buffer
	code := runCLI([]string{"-seed", "17", "-out", t.TempDir()}, &output, func(context.Context, int64, lab.Options) (lab.Result, error) { return lab.Result{}, nil })
	if code != 1 || !strings.Contains(output.String(), "write report") {
		t.Fatalf("code=%d output=%s", code, &output)
	}
}

func TestCLIPreservesFailureDiagnosticsWhenReportCannotBeWritten(t *testing.T) {
	for _, failure := range []string{"create directory", "write file"} {
		t.Run(failure, func(t *testing.T) {
			path := t.TempDir()
			if failure == "create directory" {
				blocker := filepath.Join(path, "file")
				if err := os.WriteFile(blocker, []byte("not a directory"), 0o644); err != nil {
					t.Fatal(err)
				}
				path = filepath.Join(blocker, "report.md")
			}
			var output bytes.Buffer
			code := runCLI([]string{"-seed", "17", "-workers", "5", "-max-steps", "80", "-scenario", "timeout", "-faults", "late-ack", "-out", path}, &output, func(context.Context, int64, lab.Options) (lab.Result, error) {
				return lab.Result{Seed: 17, Scenario: "timeout", Steps: 1}, errors.New("scenario did not finish")
			})
			if code != 1 {
				t.Fatalf("code=%d output=%s", code, &output)
			}
			for _, want := range []string{
				"Runs: 1; passed: 0; failing seeds: 1",
				"Seed 17 (pending triage): scenario did not finish",
				"reproduce: go test ./internal/lab -run TestLab -seed 17 -lab.known=false",
				"campaign options: go run ./cmd/capstan-lab -seed 17 -workers 5 -max-steps 80 -scenario timeout -faults late-ack",
				"write report:",
			} {
				if !strings.Contains(output.String(), want) {
					t.Errorf("output missing %q: %s", want, &output)
				}
			}
			if strings.Contains(output.String(), "Report: ") {
				t.Errorf("output claims a report was written: %s", &output)
			}
		})
	}
}

func TestCLIHelpDoesNotRun(t *testing.T) {
	var output bytes.Buffer
	code := runCLI([]string{"-h"}, &output, func(context.Context, int64, lab.Options) (lab.Result, error) {
		t.Error("help ran campaign")
		return lab.Result{}, nil
	})
	if code != 0 || !strings.Contains(output.String(), "200000") || !strings.Contains(output.String(), "-parallel") {
		t.Fatalf("code=%d output=%s", code, &output)
	}
}

func TestCLIDefaultOptions(t *testing.T) {
	before := time.Now().Format("2006-01-02")
	opts, err := parseOptions(nil, &bytes.Buffer{})
	after := time.Now().Format("2006-01-02")
	if err != nil {
		t.Fatal(err)
	}
	if opts.count != 200000 || opts.parallel != 8 || opts.options.Workers != 3 || opts.options.MaxSteps != 1000 || opts.first != 0 || opts.faultSelection != "all" {
		t.Fatalf("defaults=%+v", opts)
	}
	if opts.outputPath != filepath.Join("docs", "evidence", "lab-"+before+".md") && opts.outputPath != filepath.Join("docs", "evidence", "lab-"+after+".md") {
		t.Fatalf("default report path=%q", opts.outputPath)
	}
}

func TestCLIReportIncludesWholeCampaignCommand(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a campaign's report.md")
	var output bytes.Buffer
	code := runCLI([]string{"-seeds", "2", "-parallel", "2", "-out", path}, &output, func(context.Context, int64, lab.Options) (lab.Result, error) { return lab.Result{}, nil })
	if code != 0 {
		t.Fatalf("code=%d output=%s", code, &output)
	}
	report := readReport(t, path)
	if !strings.Contains(report, "go run ./cmd/capstan-lab -seeds 2 -parallel 2 -workers 3 -max-steps 1000 -faults all -out '") || !strings.Contains(report, "a campaign'\"'\"'s report.md'") {
		t.Fatalf("missing shell-quoted campaign command:\n%s", report)
	}
}

func TestCampaignRecognizesOnlyClassifiedFailures(t *testing.T) {
	opts := campaignOptions{count: 2, parallel: 1, options: lab.Options{Workers: 3, MaxSteps: 1000}}
	failure := errors.New("recorded defect")
	summary := runCampaign(context.Background(), opts, func(_ context.Context, seed int64, _ lab.Options) (lab.Result, error) {
		return lab.Result{Seed: seed}, failure
	}, func(seed int64, err error) bool { return seed == 0 && errors.Is(err, failure) })
	if summary.known != 1 || summary.unknown != 1 || summary.passed != 0 || len(summary.failures) != 2 {
		t.Fatalf("summary=%+v", summary)
	}
	if !summary.failures[0].known || summary.failures[1].known {
		t.Fatalf("failures=%+v", summary.failures)
	}
}

func readReport(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
