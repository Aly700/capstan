package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Aly700/capstan/internal/lab"
)

type campaignOptions struct {
	first          int64
	count          int
	parallel       int
	duration       time.Duration
	outputPath     string
	faultSelection string
	options        lab.Options
}

type failureResult struct {
	seed   int64
	result lab.Result
	err    error
	known  bool
}

type campaignSummary struct {
	known, unknown, passed int
	completed              int
	steps                  int64
	rootRuns               int64
	transactionSteps       int64
	gateResponses          map[string]int
	schedulingStop         string
	faults                 map[lab.FaultKind]int
	failures               []failureResult
	started                time.Time
	elapsed                time.Duration
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "mutate" {
		os.Exit(runMutationCLI(os.Args[2:], os.Stdout))
	}
	os.Exit(runCLI(os.Args[1:], os.Stdout, lab.Run))
}

func runCLI(args []string, output io.Writer, run func(context.Context, int64, lab.Options) (lab.Result, error)) int {
	opts, err := parseOptions(args, output)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		fmt.Fprintf(output, "capstan-lab: %v\n", err)
		return 2
	}
	summary := runCampaign(context.Background(), opts, run, lab.KnownFailure)
	fmt.Fprintf(output, "Runs: %d; passed: %d; failing seeds: %d (%d known, %d unrecognized); elapsed: %s\n", summary.completed, summary.passed, len(summary.failures), summary.known, summary.unknown, summary.elapsed)
	for _, failure := range summary.failures {
		classification := "pending triage"
		if failure.known {
			classification = "recognized known failure"
		}
		fmt.Fprintf(output, "Seed %d (%s): %v\nreproduce: %s\ncampaign options: %s\n", failure.seed, classification, failure.err, testReproduction(failure.seed), campaignReproduction(opts, failure.seed))
	}
	report := renderReport(opts, summary)
	if err := os.MkdirAll(filepath.Dir(opts.outputPath), 0o755); err != nil {
		fmt.Fprintf(output, "capstan-lab: write report: %v\n", err)
		return 1
	}
	if err := os.WriteFile(opts.outputPath, []byte(report), 0o644); err != nil {
		fmt.Fprintf(output, "capstan-lab: write report: %v\n", err)
		return 1
	}
	fmt.Fprintf(output, "Report: %s\n", opts.outputPath)
	if summary.unknown > 0 || summary.completed == 0 {
		return 1
	}
	return 0
}

func parseOptions(args []string, output io.Writer) (campaignOptions, error) {
	var opts campaignOptions
	var seed int64
	flags := flag.NewFlagSet("capstan-lab", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.IntVar(&opts.count, "seeds", 200000, "number of seeds, starting at zero")
	flags.IntVar(&opts.parallel, "parallel", 8, "maximum number of concurrent seeds")
	flags.DurationVar(&opts.duration, "duration", 0, "stop scheduling new seeds after this duration; zero is unlimited")
	flags.Int64Var(&seed, "seed", 0, "run exactly this seed instead of the -seeds campaign")
	flags.IntVar(&opts.options.Workers, "workers", 3, "lab workers per seed")
	flags.IntVar(&opts.options.MaxSteps, "max-steps", 1000, "maximum scheduled steps per seed")
	flags.StringVar(&opts.options.Scenario, "scenario", "", "scenario name (default: selected by seed)")
	flags.StringVar(&opts.faultSelection, "faults", "all", "all, none, or comma-separated fault names")
	flags.StringVar(&opts.outputPath, "out", filepath.Join("docs", "evidence", "lab-"+time.Now().Format("2006-01-02")+".md"), "Markdown report path")
	if err := flags.Parse(args); err != nil {
		return opts, err
	}
	if flags.NArg() != 0 {
		return opts, fmt.Errorf("unexpected arguments: %s", strings.Join(flags.Args(), " "))
	}
	if opts.count <= 0 || opts.parallel <= 0 || opts.options.Workers <= 0 || opts.options.MaxSteps <= 0 {
		return opts, errors.New("-seeds, -parallel, -workers, and -max-steps must be positive")
	}
	if opts.duration < 0 {
		return opts, errors.New("-duration must be nonnegative")
	}
	if scenarios := lab.ScenarioNames(); opts.options.Scenario != "" && !slices.Contains(scenarios, opts.options.Scenario) {
		return opts, fmt.Errorf("unknown scenario %q; choose from %v", opts.options.Scenario, scenarios)
	}
	if strings.TrimSpace(opts.outputPath) == "" {
		return opts, errors.New("-out must name a report file")
	}
	single := false
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "seed" {
			single = true
		}
	})
	if single {
		if seed < 0 {
			return opts, errors.New("-seed must be nonnegative")
		}
		opts.first, opts.count = seed, 1
	}
	opts.faultSelection = strings.TrimSpace(opts.faultSelection)
	switch opts.faultSelection {
	case "all":
	case "none":
		opts.options.NoFaults = true
	default:
		allowed := make(map[lab.FaultKind]bool)
		for _, kind := range lab.AllFaults() {
			allowed[kind] = true
		}
		selected := make(map[lab.FaultKind]bool)
		for _, name := range strings.Split(opts.faultSelection, ",") {
			kind := lab.FaultKind(strings.TrimSpace(name))
			if !allowed[kind] {
				return opts, fmt.Errorf("unknown fault %q; use all, none, or names from %v", kind, lab.AllFaults())
			}
			if selected[kind] {
				return opts, fmt.Errorf("duplicate fault %q", kind)
			}
			selected[kind] = true
			opts.options.Faults = append(opts.options.Faults, kind)
		}
		slices.Sort(opts.options.Faults)
		names := make([]string, len(opts.options.Faults))
		for i, kind := range opts.options.Faults {
			names[i] = string(kind)
		}
		opts.faultSelection = strings.Join(names, ",")
	}
	return opts, nil
}

func runCampaign(ctx context.Context, opts campaignOptions, run func(context.Context, int64, lab.Options) (lab.Result, error), known func(int64, error) bool) campaignSummary {
	summary := campaignSummary{started: time.Now(), faults: make(map[lab.FaultKind]int), gateResponses: make(map[string]int), schedulingStop: "seed limit reached"}
	for _, kind := range lab.AllFaults() {
		summary.faults[kind] = 0
	}
	for _, response := range []string{"pending", "http-503", "timeout", "approved-after-deadline", "approved"} {
		summary.gateResponses[response] = 0
	}
	parallel := min(opts.parallel, opts.count)
	jobs := make(chan int64)
	results := make(chan failureResult, parallel)
	var workers sync.WaitGroup
	for range parallel {
		workers.Go(func() {
			for seed := range jobs {
				runOptions := opts.options
				runOptions.Faults = slices.Clone(opts.options.Faults)
				result, err := run(ctx, seed, runOptions)
				results <- failureResult{seed: seed, result: result, err: err}
			}
		})
	}
	go func() {
		// The scheduling timer does not cancel a seed's context. Every accepted
		// job finishes and is reported, leaving one contiguous completed prefix.
		func() {
			var budget <-chan time.Time
			if opts.duration > 0 {
				timer := time.NewTimer(max(0, opts.duration-time.Since(summary.started)))
				defer timer.Stop()
				budget = timer.C
			}
			for i := range opts.count {
				if ctx.Err() != nil {
					summary.schedulingStop = "context canceled"
					return
				}
				if opts.duration > 0 && time.Since(summary.started) >= opts.duration {
					summary.schedulingStop = "time budget reached"
					return
				}
				select {
				case <-ctx.Done():
					summary.schedulingStop = "context canceled"
					return
				case <-budget:
					summary.schedulingStop = "time budget reached"
					return
				case jobs <- opts.first + int64(i):
				}
			}
		}()
		close(jobs)
		workers.Wait()
		close(results)
	}()
	for outcome := range results {
		summary.completed++
		summary.steps += int64(outcome.result.Steps)
		summary.rootRuns += int64(outcome.result.RootRuns)
		summary.transactionSteps += int64(outcome.result.TransactionSteps)
		for _, response := range outcome.result.GateResponses {
			summary.gateResponses[response]++
		}
		for _, fault := range outcome.result.Faults {
			summary.faults[fault.Kind]++
		}
		if outcome.err == nil {
			summary.passed++
			continue
		}
		outcome.known = known(outcome.seed, outcome.err)
		if outcome.known {
			summary.known++
		} else {
			summary.unknown++
		}
		summary.failures = append(summary.failures, outcome)
	}
	slices.SortFunc(summary.failures, func(a, b failureResult) int {
		if a.seed < b.seed {
			return -1
		}
		if a.seed > b.seed {
			return 1
		}
		return 0
	})
	summary.elapsed = time.Since(summary.started)
	return summary
}

func renderReport(opts campaignOptions, summary campaignSummary) string {
	var report strings.Builder
	report.WriteString("# Capstan fault-lab campaign\n\n")
	if summary.completed == 0 {
		report.WriteString("Campaign incomplete: no seeds completed. No defect estimate produced.\n\n")
	} else if len(summary.failures) == 0 {
		report.WriteString("Confirmed defects found: 0\n\n")
	} else {
		report.WriteString("Defect count: pending triage. Failing seeds are not a count of unique confirmed defects. Recognized failures are classified against the exact known-failure list.\n\n")
	}
	fmt.Fprintf(&report, "- Runs: %d\n- Passed: %d\n- Failing seeds: %d\n- Recognized known failures: %d\n- Unrecognized failures: %d\n- Started: %s\n- Elapsed: %s\n- Total steps: %d\n- Root runs: %d\n- Store transaction steps: %d\n", summary.completed, summary.passed, len(summary.failures), summary.known, summary.unknown, summary.started.UTC().Format(time.RFC3339), summary.elapsed, summary.steps, summary.rootRuns, summary.transactionSteps)
	scenario := opts.options.Scenario
	if scenario == "" {
		scenario = "selected by seed"
	}
	seedRange := "none (no seeds completed)"
	if summary.completed > 0 {
		seedRange = fmt.Sprintf("%d..%d", opts.first, opts.first+int64(summary.completed-1))
	}
	budget := "unlimited"
	if opts.duration > 0 {
		budget = opts.duration.String()
	}
	fmt.Fprintf(&report, "\n## Campaign options\n\n- Seed range: %s\n- Requested seed limit: %d\n- Time budget: %s\n- Scheduling stopped: %s\n- Parallelism: %d\n- Workers per seed: %d\n- Maximum steps per seed: %d\n- Scenario: %s\n- Fault selection: %s\n", seedRange, opts.count, budget, summary.schedulingStop, opts.parallel, opts.options.Workers, opts.options.MaxSteps, scenario, opts.faultSelection)
	report.WriteString("\nRepeat the requested campaign:\n\n")
	selector := fmt.Sprintf("-seeds %d", opts.count)
	if opts.count == 1 {
		selector = fmt.Sprintf("-seed %d", opts.first)
	}
	durationArgument := ""
	if opts.duration > 0 {
		durationArgument = " -duration " + opts.duration.String()
	}
	writeIndented(&report, fmt.Sprintf("go run ./cmd/capstan-lab %s -parallel %d%s%s -out %s", selector, opts.parallel, durationArgument, runArguments(opts), shellQuote(opts.outputPath)))
	if summary.completed > 0 && summary.completed < opts.count {
		report.WriteString("\nReplay the completed seed prefix without a time limit:\n\n")
		selector = fmt.Sprintf("-seeds %d", summary.completed)
		if summary.completed == 1 {
			selector = fmt.Sprintf("-seed %d", opts.first)
		}
		writeIndented(&report, fmt.Sprintf("go run ./cmd/capstan-lab %s -parallel %d%s -out %s", selector, opts.parallel, runArguments(opts), shellQuote(opts.outputPath)))
	}
	report.WriteString("\n## Injected faults\n\n| Fault | Count |\n| --- | ---: |\n")
	kinds := make([]lab.FaultKind, 0, len(summary.faults))
	for kind := range summary.faults {
		kinds = append(kinds, kind)
	}
	slices.Sort(kinds)
	for _, kind := range kinds {
		fmt.Fprintf(&report, "| %s | %d |\n", kind, summary.faults[kind])
	}
	report.WriteString("\n## Gate responses\n\n| Response | Count |\n| --- | ---: |\n")
	responses := make([]string, 0, len(summary.gateResponses))
	for response := range summary.gateResponses {
		responses = append(responses, response)
	}
	slices.Sort(responses)
	for _, response := range responses {
		fmt.Fprintf(&report, "| %s | %d |\n", response, summary.gateResponses[response])
	}
	report.WriteString("\n## Failing seeds\n\n")
	if len(summary.failures) == 0 {
		report.WriteString("None.\n")
	}
	for _, failure := range summary.failures {
		classification := "unrecognized failure, pending triage"
		if failure.known {
			classification = "recognized known failure"
		}
		fmt.Fprintf(&report, "### Seed %d — %s\n\nScenario: %s; steps: %d.\n\nError:\n\n", failure.seed, classification, failure.result.Scenario, failure.result.Steps)
		writeIndented(&report, failure.err.Error())
		report.WriteString("\nDefault lab test reproduction (known-failure recognition disabled):\n\n")
		writeIndented(&report, testReproduction(failure.seed))
		report.WriteString("\nCampaign reproduction with the same scenario, worker, step, and fault options:\n\n")
		writeIndented(&report, campaignReproduction(opts, failure.seed))
		if len(failure.result.Faults) > 0 {
			report.WriteString("\nFaults:\n\n")
			for _, fault := range failure.result.Faults {
				writeIndented(&report, fmt.Sprintf("step %d: %s: %s", fault.Step, fault.Kind, fault.Detail))
			}
		}
		if len(failure.result.Trace) > 0 {
			report.WriteString("\nTrace:\n\n")
			for _, entry := range failure.result.Trace {
				writeIndented(&report, entry)
			}
		}
		report.WriteString("\n")
	}
	return report.String()
}

func testReproduction(seed int64) string {
	return fmt.Sprintf("go test ./internal/lab -run TestLab -seed %d -lab.known=false", seed)
}

func campaignReproduction(opts campaignOptions, seed int64) string {
	return fmt.Sprintf("go run ./cmd/capstan-lab -seed %d%s -out docs/evidence/lab-seed-%d.md", seed, runArguments(opts), seed)
}

func runArguments(opts campaignOptions) string {
	arguments := fmt.Sprintf(" -workers %d -max-steps %d", opts.options.Workers, opts.options.MaxSteps)
	if opts.options.Scenario != "" {
		arguments += " -scenario " + shellQuote(opts.options.Scenario)
	}
	return arguments + " -faults " + shellQuote(opts.faultSelection)
}

func shellQuote(value string) string {
	if value != "" && !strings.ContainsAny(value, " \t\r\n'\"`$\\;|&<>(){}[]*?!#~") {
		return value
	}
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func writeIndented(output *strings.Builder, text string) {
	for _, line := range strings.Split(text, "\n") {
		fmt.Fprintf(output, "    %s\n", line)
	}
}
