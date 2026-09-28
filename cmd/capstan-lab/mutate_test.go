package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestMutationClassificationRequiresASeededFailure(t *testing.T) {
	for _, tc := range []struct {
		name, log, status, check string
		failed, timed            bool
		seed                     int64
	}{
		{"caught", `{"Action":"output","Output":"    lab_test.go:40: mutation seed 7\n"}
{"Action":"output","Output":"    lab_test.go:44: seed 7 scenario pipeline: P3: timer 1 fired twice\n"}
{"Action":"fail","Test":"TestLab"}`, "caught", "P3", true, false, 7},
		{"probe", `{"Action":"output","Output":"mutation seed 8\nseed 8: protocol closed-inbox: closed run retains 1 buffered events\n"}`, "caught", "closed-inbox", true, false, 8},
		{"failure-then-timeout", `{"Action":"output","Output":"mutation seed 9\nseed 9 scenario timer: P3: lost timer\npanic: test timed out after 2m0s\n"}`, "timeout", "", true, false, 9},
		{"go-timeout", `{"Action":"output","Output":"mutation seed 9\npanic: test timed out after 2m0s\n"}`, "timeout", "timed out", true, false, 9},
		{"survived-trace", `{"Action":"output","Output":"mutation seed 1999\n"}
{"Action":"pass","Test":"TestLab"}`, "survived", "", false, false, -1},
		{"survived", `{"Action":"pass","Test":"TestLab"}`, "survived", "", false, false, -1},
		{"build", `{"Action":"output","Output":"# module [build failed]\nundefined: removedSymbol\n"}`, "invalid", "", true, false, -1},
		{"timeout", `{"Action":"output","Output":"mutation seed 8\n"}`, "timeout", "", true, true, 8},
		{"panic", `{"Action":"output","Output":"mutation seed 12\n"}
{"Action":"output","Output":"panic: timer record corrupted\n"}`, "caught", "panic", true, false, 12},
		{"infra-after-seed", `{"Action":"output","Output":"mutation seed 9\n"}`, "invalid", "", true, false, 9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			if tc.failed {
				err = errors.New("exit1")
			}
			got := classifyMutation([]byte(tc.log), err, tc.timed)
			if got.Status != tc.status || got.Seed != tc.seed || !strings.Contains(got.Check, tc.check) {
				t.Fatalf("got %+v", got)
			}
		})
	}
}

func TestMutationRunnerUsesScratchSnapshotAndRestoresProduction(t *testing.T) {
	root := t.TempDir()
	git := func(args ...string) {
		t.Helper()
		c := exec.Command("git", args...)
		c.Dir = root
		if b, e := c.CombinedOutput(); e != nil {
			t.Fatalf("git %v: %v %s", args, e, b)
		}
	}
	git("init", "-q")
	git("config", "user.name", "Lab Test")
	git("config", "user.email", "lab@example.test")
	write := func(name, data string) {
		t.Helper()
		p := filepath.Join(root, name)
		if e := os.MkdirAll(filepath.Dir(p), 0755); e != nil {
			t.Fatal(e)
		}
		if e := os.WriteFile(p, []byte(data), 0644); e != nil {
			t.Fatal(e)
		}
	}
	write("internal/engine/bug.go", "package engine\nconst value = 1\n")
	write("internal/lab/lab.go", "package lab\nconst old = true\n")
	git("add", ".")
	git("commit", "-qm", "initial")
	write("internal/lab/lab.go", "package lab\nconst fresh = true\n")
	write("internal/lab/mutants/one.patch", "diff --git a/internal/engine/bug.go b/internal/engine/bug.go\n--- a/internal/engine/bug.go\n+++ b/internal/engine/bug.go\n@@ -1,2 +1,2 @@\n package engine\n-const value = 1\n+const value = 2\n")
	calls := 0
	command := func(ctx context.Context, dir, name string, args ...string) ([]byte, error) {
		if name != "go" {
			return mutationExec(ctx, dir, name, args...)
		}
		calls++
		if dir == root || !strings.HasPrefix(dir, filepath.Join(root, ".lane")) {
			t.Fatalf("not scratch: %s", dir)
		}
		source, e := os.ReadFile(filepath.Join(dir, "internal/lab/lab.go"))
		if e != nil || !strings.Contains(string(source), "fresh") {
			t.Fatalf("missing current lab snapshot: %s %v", source, e)
		}
		source, e = os.ReadFile(filepath.Join(dir, "internal/engine/bug.go"))
		if e != nil {
			t.Fatal(e)
		}
		if strings.Contains(string(source), "value = 2") {
			return []byte(`{"Action":"output","Output":"mutation seed 3\nseed 3 scenario pipeline: P1: lost effect\n"}`), errors.New("exit1")
		}
		return []byte(`{"Action":"pass","Test":"TestLab"}`), nil
	}
	opts := mutationOptions{Catalogue: "internal/lab/mutants/*.patch", Seeds: 2000, LogDir: filepath.Join(root, ".lane/logs")}
	report, err := runMutations(context.Background(), root, opts, command, nil)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || len(report.Results) != 1 || report.Results[0].Status != "caught" || report.Results[0].Seed != 3 {
		t.Fatalf("calls%d report%+v", calls, report)
	}
	original, _ := os.ReadFile(filepath.Join(root, "internal/engine/bug.go"))
	if !strings.Contains(string(original), "value = 1") {
		t.Fatal("production was mutated")
	}
	b, err := mutationExec(context.Background(), root, "git", "worktree", "list", "--porcelain")
	if err != nil || strings.Count(string(b), "worktree ") != 1 {
		t.Fatalf("scratch worktree not cleaned: %s %v", b, err)
	}
	if report.SourceHash == "" || report.Revision == "" {
		t.Fatal("missing source provenance")
	}
}

func TestMutationPatchRejectsTestsAndContracts(t *testing.T) {
	for _, path := range []string{"internal/lab/lab.go", "internal/engine/bugs_test.go", "internal/engine/api.go", "internal/store/store.go", "../engine.go"} {
		if err := validateMutationPaths([]byte("1\t1\t" + path + "\n")); err == nil {
			t.Error("accepted", path)
		}
	}
	if err := validateMutationPaths([]byte("1\t1\tinternal/engine/timers.go\n")); err != nil {
		t.Fatal(err)
	}
}
