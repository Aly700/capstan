package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"
)

type mutantDefinition struct {
	ID               string `json:"id"`
	Name             string `json:"name"`
	Patch            string `json:"patch"`
	Description      string `json:"description"`
	ExpectedCheck    string `json:"expected_check"`
	EquivalentReason string `json:"equivalent_reason,omitempty"`
}
type mutationResult struct {
	Mutant    mutantDefinition `json:"mutant"`
	Status    string           `json:"status"`
	Seed      int64            `json:"seed"`
	Check     string           `json:"check"`
	PatchHash string           `json:"patch_sha256"`
	Log       string           `json:"log"`
	Elapsed   time.Duration    `json:"elapsed_ns"`
}
type mutationOptions struct {
	Catalogue string
	Seeds     int
	LogDir    string
	Timeout   time.Duration
	Keep      bool
}
type mutationReport struct {
	Revision   string           `json:"revision"`
	SourceHash string           `json:"lab_source_sha256"`
	Snapshot   string           `json:"snapshot"`
	Seeds      int              `json:"seeds"`
	Started    time.Time        `json:"started"`
	Results    []mutationResult `json:"results"`
}
type mutationCommand func(context.Context, string, string, ...string) ([]byte, error)

var announcedSeed = regexp.MustCompile(`mutation seed ([0-9]+)`)
var failedSeed = regexp.MustCompile(`seed ([0-9]+):? ((?:scenario|baseline|probe|protocol)[^\n]*)`)

func classifyMutation(output []byte, runErr error, timed bool) mutationResult {
	result := mutationResult{Seed: -1, Status: "invalid"}
	var diagnostic string
	scanner := bufio.NewScanner(bytes.NewReader(output))
	scanner.Buffer(make([]byte, 4096), 4<<20)
	passed := false
	goTimedOut := false
	for scanner.Scan() {
		var event struct{ Action, Test, Output string }
		line := scanner.Text()
		if json.Unmarshal(scanner.Bytes(), &event) == nil {
			if event.Action == "pass" && event.Test == "TestLab" {
				passed = true
			}
			line = event.Output
		}
		for _, m := range announcedSeed.FindAllStringSubmatch(line, -1) {
			fmt.Sscan(m[1], &result.Seed)
		}
		if m := failedSeed.FindStringSubmatch(line); m != nil {
			fmt.Sscan(m[1], &result.Seed)
			diagnostic = m[2]
		}
		for _, part := range strings.Split(line, "\n") {
			if strings.HasPrefix(strings.TrimSpace(part), "panic: test timed out") {
				goTimedOut = true
			}
			if strings.HasPrefix(strings.TrimSpace(part), "panic:") && diagnostic == "" {
				diagnostic = strings.TrimSpace(part)
			}
		}
	}
	result.Check = diagnostic
	switch {
	case timed || goTimedOut:
		result.Status = "timeout"
		if result.Check == "" {
			result.Check = "test command exceeded time limit"
		}
	case runErr == nil && passed:
		result.Status = "survived"
		result.Seed = -1
	case runErr != nil && result.Seed >= 0 && diagnostic != "":
		result.Status = "caught"
	default:
		if result.Check == "" {
			result.Check = "test did not produce a seeded TestLab failure or pass"
		}
	}
	return result
}

func mutationExec(ctx context.Context, dir, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOTOOLCHAIN=go1.26.4")
	// Kill the go command and its test child together on timeout or interruption.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return os.ErrProcessDone
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	cmd.WaitDelay = 2 * time.Second
	return cmd.CombinedOutput()
}
func validateMutationPaths(output []byte) error {
	count := 0
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		fields := strings.Split(line, "\t")
		if len(fields) != 3 {
			return errors.New("patch has no ordinary production file changes")
		}
		path := fields[2]
		if (!strings.HasPrefix(path, "internal/engine/") && !strings.HasPrefix(path, "internal/store/")) || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || path == "internal/engine/api.go" || path == "internal/store/store.go" || strings.Contains(path, "..") {
			return fmt.Errorf("mutant changes forbidden path %q", path)
		}
		count++
	}
	if count == 0 {
		return errors.New("empty mutant")
	}
	return nil
}

func loadMutants(root, selector string) ([]mutantDefinition, error) {
	if !filepath.IsAbs(selector) {
		selector = filepath.Join(root, selector)
	}
	var entries []mutantDefinition
	dir := filepath.Dir(selector)
	manifest := filepath.Join(dir, "catalogue.json")
	if strings.HasSuffix(selector, ".json") {
		manifest = selector
	}
	data, err := os.ReadFile(manifest)
	if err == nil {
		if err := json.Unmarshal(data, &entries); err != nil {
			return nil, fmt.Errorf("catalogue: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) || strings.HasSuffix(selector, ".json") {
		return nil, err
	}
	byPath := map[string]mutantDefinition{}
	for _, entry := range entries {
		byPath[filepath.Clean(filepath.Join(filepath.Dir(manifest), entry.Patch))] = entry
	}
	var paths []string
	if strings.HasSuffix(selector, ".json") {
		for path := range byPath {
			paths = append(paths, path)
		}
	} else {
		paths, err = filepath.Glob(selector)
		if err != nil {
			return nil, err
		}
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		return nil, fmt.Errorf("catalogue %q matches no mutants", selector)
	}
	entries = nil
	seen := map[string]bool{}
	validID := regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)
	for _, path := range paths {
		entry, ok := byPath[path]
		if !ok {
			entry = mutantDefinition{ID: strings.TrimSuffix(filepath.Base(path), filepath.Ext(path)), Name: filepath.Base(path), Patch: path}
		}
		if !validID.MatchString(entry.ID) || seen[entry.ID] {
			return nil, fmt.Errorf("invalid or duplicate mutant ID %q", entry.ID)
		}
		seen[entry.ID] = true
		entry.Patch = path
		entries = append(entries, entry)
	}
	return entries, nil
}

func copyLabSnapshot(root, scratch string) (string, error) {
	source := filepath.Join(root, "internal/lab")
	target := filepath.Join(scratch, "internal/lab")
	if err := os.RemoveAll(target); err != nil {
		return "", err
	}
	digest := sha256.New()
	err := filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		if relative == "mutants" && entry.IsDir() {
			return filepath.SkipDir
		}
		destination := filepath.Join(target, relative)
		if entry.IsDir() {
			return os.MkdirAll(destination, 0755)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("nonregular lab source %s", path)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		fmt.Fprintf(digest, "%s\x00%d\x00", filepath.ToSlash(relative), len(data))
		digest.Write(data)
		return os.WriteFile(destination, data, info.Mode().Perm())
	})
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func runMutations(ctx context.Context, root string, opts mutationOptions, command mutationCommand, progress func(mutationResult)) (report mutationReport, returnedErr error) {
	if opts.Seeds <= 0 {
		return report, errors.New("mutation seed count must be positive")
	}
	if opts.Timeout == 0 {
		opts.Timeout = 2 * time.Minute
	}
	entries, err := loadMutants(root, opts.Catalogue)
	if err != nil {
		return report, err
	}
	if opts.LogDir == "" {
		opts.LogDir = filepath.Join(root, ".lane", "mutation")
	}
	if !filepath.IsAbs(opts.LogDir) {
		opts.LogDir = filepath.Join(root, opts.LogDir)
	}
	if err := os.MkdirAll(opts.LogDir, 0755); err != nil {
		return report, err
	}
	if err := os.MkdirAll(filepath.Join(root, ".lane"), 0755); err != nil {
		return report, err
	}
	scratch, err := os.MkdirTemp(filepath.Join(root, ".lane"), "mutation-worktree-")
	if err != nil {
		return report, err
	}
	added := false
	defer func() {
		if opts.Keep {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if added {
			if b, err := command(cleanupCtx, root, "git", "worktree", "remove", "--force", scratch); err != nil {
				returnedErr = errors.Join(returnedErr, fmt.Errorf("remove scratch: %w: %s", err, b))
			}
		} else {
			returnedErr = errors.Join(returnedErr, os.RemoveAll(scratch))
		}
	}()
	git := func(dir string, args ...string) ([]byte, error) {
		b, e := command(ctx, dir, "git", args...)
		if e != nil {
			return b, fmt.Errorf("git %v: %w: %s", args, e, b)
		}
		return b, nil
	}
	revision, err := git(root, "rev-parse", "HEAD")
	if err != nil {
		return report, err
	}
	report.Revision = strings.TrimSpace(string(revision))
	report.Started = time.Now().UTC()
	report.Seeds = opts.Seeds
	if _, err := git(root, "worktree", "add", "--detach", scratch, report.Revision); err != nil {
		return report, err
	}
	added = true
	report.SourceHash, err = copyLabSnapshot(root, scratch)
	if err != nil {
		return report, err
	}
	if _, err := git(scratch, "add", "-A", "internal/lab"); err != nil {
		return report, err
	}
	if _, err := git(scratch, "-c", "commit.gpgsign=false", "commit", "--allow-empty", "-m", "test(lab): isolated mutation snapshot", "-m", "Co-Authored-By: Codex <noreply@openai.com>"); err != nil {
		return report, err
	}
	snapshot, err := git(scratch, "rev-parse", "HEAD")
	if err != nil {
		return report, err
	}
	report.Snapshot = strings.TrimSpace(string(snapshot))
	runLab := func() (b []byte, e error, timed bool) {
		testCtx, cancel := context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
		b, e = command(testCtx, scratch, "go", "test", "-json", "-count=1", "./internal/lab", "-run", "^TestLab$", "-seeds", fmt.Sprint(opts.Seeds), "-lab.known=false", "-lab.trace-seeds", "-timeout", opts.Timeout.String())
		return b, e, errors.Is(testCtx.Err(), context.DeadlineExceeded)
	}
	baseline, baseErr, baseTimeout := runLab()
	if err := os.WriteFile(filepath.Join(opts.LogDir, "baseline.jsonl"), baseline, 0644); err != nil {
		return report, err
	}
	if result := classifyMutation(baseline, baseErr, baseTimeout); result.Status != "survived" {
		return report, fmt.Errorf("unmutated baseline failed (%s): %s; see %s", result.Status, result.Check, filepath.Join(opts.LogDir, "baseline.jsonl"))
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		if _, err := git(scratch, "reset", "--hard", report.Snapshot); err != nil {
			return report, err
		}
		if _, err := git(scratch, "clean", "-fd", "--", "internal/engine", "internal/store"); err != nil {
			return report, err
		}
		result := mutationResult{Mutant: entry, Status: "invalid", Seed: -1, Log: filepath.Join(opts.LogDir, entry.ID+".jsonl")}
		patch, err := os.ReadFile(entry.Patch)
		if err != nil {
			return report, err
		}
		sum := sha256.Sum256(patch)
		result.PatchHash = hex.EncodeToString(sum[:])
		changed, patchErr := git(scratch, "apply", "--numstat", entry.Patch)
		if patchErr == nil {
			patchErr = validateMutationPaths(changed)
		}
		if patchErr == nil {
			_, patchErr = git(scratch, "apply", "--check", entry.Patch)
		}
		if patchErr == nil {
			_, patchErr = git(scratch, "apply", entry.Patch)
		}
		if patchErr != nil {
			result.Check = patchErr.Error()
			if e := os.WriteFile(result.Log, []byte(result.Check), 0644); e != nil {
				return report, e
			}
		} else {
			started := time.Now()
			output, runErr, timed := runLab()
			classification := classifyMutation(output, runErr, timed)
			result.Status, result.Seed, result.Check = classification.Status, classification.Seed, classification.Check
			result.Elapsed = time.Since(started)
			if err := os.WriteFile(result.Log, output, 0644); err != nil {
				return report, err
			}
			if result.Status == "survived" && entry.EquivalentReason != "" {
				result.Status = "equivalent"
				result.Check = entry.EquivalentReason
			}
		}
		report.Results = append(report.Results, result)
		if progress != nil {
			progress(result)
		}
		encoded, _ := json.MarshalIndent(report, "", "  ")
		if err := os.WriteFile(filepath.Join(opts.LogDir, "results.json"), encoded, 0644); err != nil {
			return report, err
		}
	}
	return report, nil
}

func mutationMarkdown(report mutationReport, opts mutationOptions) string {
	var b strings.Builder
	caught, equivalent, valid := 0, 0, 0
	for _, r := range report.Results {
		switch r.Status {
		case "caught":
			caught++
			valid++
		case "survived":
			valid++
		case "equivalent":
			equivalent++
			valid++
		}
	}
	rate := 0.0
	if valid > 0 {
		rate = 100 * float64(caught) / float64(valid)
	}
	fmt.Fprintf(&b, "# Fault-lab mutation score\n\nCaught **%d/%d valid mutants (%.2f%%)**; equivalent survivors: **%d**.\n\n", caught, valid, rate, equivalent)
	if valid > equivalent {
		fmt.Fprintf(&b, "Excluding explicitly justified equivalent survivors: **%d/%d (%.2f%%)**.\n\n", caught, valid-equivalent, 100*float64(caught)/float64(valid-equivalent))
	}
	fmt.Fprintf(&b, "Production revision: `%s`. Lab snapshot SHA-256: `%s`.\nScratch snapshot commit: `%s`. Started: %s.\n\nEvery mutant runs TestLab with %d seeds, stopping at its first failure. The unmutated\nbaseline must pass all seeds first. Builds, infrastructure failures and timeouts are\nreported separately and do not count as kills. Production sources are changed only\nin an isolated temporary worktree beneath `.lane/`, removed after the run.\n\n", report.Revision, report.SourceHash, report.Snapshot, report.Started.Format(time.RFC3339), report.Seeds)
	fmt.Fprintf(&b, "```sh\nPATH=$HOME/.local/bin:$PATH GOTOOLCHAIN=go1.26.4 go run ./cmd/capstan-lab mutate -catalogue %s -seeds %d -timeout %s -logs %s\n```\n\n", shellQuote(opts.Catalogue), opts.Seeds, opts.Timeout, shellQuote(opts.LogDir))
	b.WriteString("| ID | Mutation | Result | First seed | Property or check |\n| --- | --- | --- | ---: | --- |\n")
	clean := func(s string) string { return strings.NewReplacer("|", "\\|", "\n", " ", "\r", "").Replace(s) }
	for _, r := range report.Results {
		seed := "—"
		if r.Seed >= 0 {
			seed = fmt.Sprint(r.Seed)
		}
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s |\n", r.Mutant.ID, clean(r.Mutant.Name), r.Status, seed, clean(r.Check))
	}
	b.WriteString("\n## Survivors and invalid runs\n\n")
	any := false
	for _, r := range report.Results {
		if r.Status == "caught" {
			continue
		}
		any = true
		fmt.Fprintf(&b, "- **%s (%s):** %s\n", r.Mutant.ID, r.Status, clean(r.Check))
	}
	if !any {
		b.WriteString("None.\n")
	}
	b.WriteString("\n## Evidence\n\n")
	fmt.Fprintf(&b, "Full per-mutant output, the baseline, patch SHA-256 values and machine-readable\nresults are stored in `%s`. The patch catalogue describes the intended bug;\nthe kill table records the observed check. A survived mutant without a proved\nequivalence remains a lab coverage gap. The score concerns this catalogue and\nthese seeds; it is not a proof that production contains no defects.\n", opts.LogDir)
	return b.String()
}

func runMutationCLI(args []string, output io.Writer) int {
	var opts mutationOptions
	var out string
	flags := flag.NewFlagSet("capstan-lab mutate", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&opts.Catalogue, "catalogue", "internal/lab/mutants/*.patch", "patch glob or catalogue.json")
	flags.IntVar(&opts.Seeds, "seeds", 2000, "seed count for baseline and every mutant")
	flags.DurationVar(&opts.Timeout, "timeout", 2*time.Minute, "maximum duration per lab command")
	flags.StringVar(&opts.LogDir, "logs", ".lane/mutation", "full command output and JSON results")
	flags.StringVar(&out, "out", "docs/evidence/lab-mutation.md", "Markdown evidence path")
	flags.BoolVar(&opts.Keep, "keep", false, "retain the scratch worktree for inspection")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 || opts.Seeds <= 0 || opts.Timeout <= 0 || opts.Catalogue == "" || opts.LogDir == "" || out == "" {
		fmt.Fprintln(output, "mutation options require a catalogue, positive seeds/timeout, and output paths")
		return 2
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(output, err)
		return 1
	}
	rootBytes, err := mutationExec(ctx, cwd, "git", "rev-parse", "--show-toplevel")
	if err != nil {
		fmt.Fprintln(output, err)
		return 1
	}
	root := strings.TrimSpace(string(rootBytes))
	fmt.Fprintf(output, "Mutation baseline: %d seeds; catalogue %s\n", opts.Seeds, opts.Catalogue)
	report, err := runMutations(ctx, root, opts, mutationExec, func(result mutationResult) {
		fmt.Fprintf(output, "%s: %s; seed %d; %s\n", result.Mutant.ID, result.Status, result.Seed, result.Check)
	})
	if err != nil {
		fmt.Fprintf(output, "mutation campaign: %v\n", err)
		return 1
	}
	if !filepath.IsAbs(out) {
		out = filepath.Join(root, out)
	}
	if err := os.MkdirAll(filepath.Dir(out), 0755); err != nil {
		fmt.Fprintln(output, err)
		return 1
	}
	if err := os.WriteFile(out, []byte(mutationMarkdown(report, opts)), 0644); err != nil {
		fmt.Fprintln(output, err)
		return 1
	}
	fmt.Fprintf(output, "Report: %s\n", out)
	for _, result := range report.Results {
		if result.Status != "caught" && result.Status != "equivalent" {
			return 1
		}
	}
	return 0
}
