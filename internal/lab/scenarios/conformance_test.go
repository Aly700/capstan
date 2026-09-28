package scenarios_test

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"

	"github.com/Aly700/capstan/internal/lab/scenarios"
)

func TestScenarioRegistryCoversWorkflowExports(t *testing.T) {
	path := "../../../conformance/workflows.ts"
	if _, source, _, ok := runtime.Caller(0); ok && filepath.IsAbs(source) {
		path = filepath.Join(filepath.Dir(source), path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	exports := regexp.MustCompile(`(?m)^export\s+(?:async\s+)?function\s+([A-Za-z_$][A-Za-z0-9_$]*)\s*\(`).FindAllSubmatch(data, -1)
	if len(exports) == 0 {
		t.Fatal("no TypeScript workflow exports found")
	}
	registry := scenarios.Registry()
	for _, export := range exports {
		name := string(export[1])
		if registry[name] == nil {
			t.Errorf("missing Go twin for TypeScript workflow %q", name)
		}
	}
	t.Logf("checked %d TypeScript workflow exports", len(exports))
}
