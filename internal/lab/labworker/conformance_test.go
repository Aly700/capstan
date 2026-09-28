package labworker_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/lab/labworker"
	"github.com/Aly700/capstan/internal/lab/scenarios"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type conformanceFixture struct {
	Name        string            `json:"name"`
	Workflow    string            `json:"workflow"`
	Description string            `json:"description"`
	Only        []string          `json:"only"`
	History     []json.RawMessage `json:"history"`
	Expect      struct {
		Commands json.RawMessage `json:"commands"`
		Mismatch *struct {
			EventID int64 `json:"eventId"`
		} `json:"mismatch"`
	} `json:"expect"`
}

func TestConformance(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join(conformanceDirectory(t), "fixtures", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no conformance fixtures found")
	}
	registry := scenarios.Registry()
	shared, skipped := 0, 0
	for _, path := range paths {
		t.Run(strings.TrimSuffix(filepath.Base(path), ".json"), func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var fixture conformanceFixture
			decoder := json.NewDecoder(bytes.NewReader(data))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&fixture); err != nil {
				t.Fatal(err)
			}
			workflow, ok := registry[fixture.Workflow]
			if !ok || workflow == nil {
				t.Fatalf("no Go scenario for fixture workflow %q", fixture.Workflow)
			}
			if len(fixture.Only) > 0 && !slices.Contains(fixture.Only, "go") {
				skipped++
				t.Skipf("fixture only applies to %v (D20)", fixture.Only)
			}
			shared++
			if (len(fixture.Expect.Commands) == 0) == (fixture.Expect.Mismatch == nil) {
				t.Fatal("fixture must expect exactly one of commands or mismatch")
			}
			history := make([]*capstanv1.HistoryEvent, len(fixture.History))
			for i, raw := range fixture.History {
				history[i] = new(capstanv1.HistoryEvent)
				if err := protojson.Unmarshal(raw, history[i]); err != nil {
					t.Fatalf("history event %d: %v", i, err)
				}
			}
			commands, err := labworker.Replay("conformance", history, workflow)
			if fixture.Expect.Mismatch != nil {
				var mismatch *labworker.HistoryMismatchError
				if !errors.As(err, &mismatch) {
					t.Fatalf("expected HistoryMismatch at event %d, got %v", fixture.Expect.Mismatch.EventID, err)
				}
				if mismatch.EventID != fixture.Expect.Mismatch.EventID {
					t.Fatalf("mismatch event = %d, want %d: %v", mismatch.EventID, fixture.Expect.Mismatch.EventID, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("replay: %v", err)
			}
			var rawCommands []json.RawMessage
			if err := json.Unmarshal(fixture.Expect.Commands, &rawCommands); err != nil {
				t.Fatal(err)
			}
			if len(commands) != len(rawCommands) {
				t.Fatalf("command count = %d, want %d: %v", len(commands), len(rawCommands), commands)
			}
			for i, raw := range rawCommands {
				want := new(capstanv1.Command)
				if err := protojson.Unmarshal(raw, want); err != nil {
					t.Fatalf("expected command %d: %v", i, err)
				}
				if commands[i] == nil {
					t.Fatalf("command %d is nil", i)
				}
				got := proto.Clone(commands[i]).(*capstanv1.Command)
				for _, command := range []*capstanv1.Command{got, want} {
					if err := normalizeJSONPayloads(command.ProtoReflect()); err != nil {
						t.Fatalf("command %d: %v", i, err)
					}
				}
				if !proto.Equal(got, want) {
					t.Errorf("command %d:\n got %s\nwant %s", i, protojson.Format(got), protojson.Format(want))
				}
			}
		})
	}
	t.Logf("conformance fixtures: %d total, %d shared, %d Go-skipped", len(paths), shared, skipped)
}

func conformanceDirectory(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if ok && filepath.IsAbs(source) {
		path := filepath.Clean(filepath.Join(filepath.Dir(source), "../../../conformance"))
		if info, err := os.Stat(filepath.Join(path, "fixtures")); err == nil && info.IsDir() {
			return path
		}
	}
	// With -trimpath the caller has a module path; locate the repository from cwd.
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for dir := cwd; ; dir = filepath.Dir(dir) {
		path := filepath.Join(dir, "conformance")
		if info, err := os.Stat(filepath.Join(path, "fixtures")); err == nil && info.IsDir() {
			return path
		}
		if filepath.Dir(dir) == dir {
			break
		}
	}
	t.Fatal("cannot find conformance/fixtures relative to test source or working directory")
	return ""
}

// Keep protobuf presence, oneofs and all non-payload fields exact. Canonicalize
// only JSON payload data so object key order and whitespace are not significant.
func normalizeJSONPayloads(message protoreflect.Message) error {
	if payload, ok := message.Interface().(*capstanv1.Payload); ok {
		if payload.ContentType != "application/json" {
			return nil
		}
		var decoded any
		if err := json.Unmarshal(payload.Data, &decoded); err != nil {
			return fmt.Errorf("invalid JSON payload: %w", err)
		}
		data, err := json.Marshal(decoded)
		if err != nil {
			return err
		}
		payload.Data = data
		return nil
	}
	var visitErr error
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsMap() && field.MapValue().Message() != nil:
			value.Map().Range(func(_ protoreflect.MapKey, value protoreflect.Value) bool {
				visitErr = normalizeJSONPayloads(value.Message())
				return visitErr == nil
			})
		case field.IsList() && field.Message() != nil:
			for i := 0; i < value.List().Len(); i++ {
				if visitErr = normalizeJSONPayloads(value.List().Get(i).Message()); visitErr != nil {
					break
				}
			}
		case !field.IsMap() && !field.IsList() && field.Message() != nil:
			visitErr = normalizeJSONPayloads(value.Message())
		}
		return visitErr == nil
	})
	return visitErr
}
