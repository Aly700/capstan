package lab

import (
	"sync"
	"testing"
)

func TestEffectSinkKeepsFirstResultAndCountsAttempts(t *testing.T) {
	sink := NewEffectSink()
	for i, value := range []string{"first", "retry", "late"} {
		got, err := sink.Apply("r/1", value)
		if err != nil || got != "first" {
			t.Fatalf("attempt %d = %v, %v; want first", i+1, got, err)
		}
	}
	entries := sink.Entries()
	if len(entries) != 1 || entries[0] != (Effect{Key: "r/1", Count: 1, Attempts: 3, Value: "first"}) {
		t.Fatalf("entries = %#v", entries)
	}
}

func TestEffectSinkConcurrentAttemptsAndStableSnapshot(t *testing.T) {
	var sink EffectSink
	if _, err := sink.Apply("z/1", "first"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			value, err := sink.Apply("z/1", "retry")
			if err != nil || value != "first" {
				t.Errorf("retry = %v, %v", value, err)
			}
		})
	}
	wg.Wait()
	if _, err := sink.Apply("a/2", nil); err != nil {
		t.Fatal(err)
	}
	entries := sink.Snapshot()
	if len(entries) != 2 || entries[0].Key != "a/2" || entries[1].Key != "z/1" || entries[1].Count != 1 || entries[1].Attempts != 65 {
		t.Fatalf("snapshot = %#v", entries)
	}
	entries[1].Count = 99
	if sink.Entries()[1].Count != 1 {
		t.Fatal("mutating a snapshot changed the sink")
	}
}

func TestEffectSinkRejectsEmptyKey(t *testing.T) {
	sink := NewEffectSink()
	if _, err := sink.Apply("", "value"); err == nil {
		t.Fatal("empty idempotency key accepted")
	}
	if len(sink.Entries()) != 0 {
		t.Fatal("invalid attempt changed the sink")
	}
}
