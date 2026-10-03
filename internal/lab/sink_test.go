package lab

import "testing"

func TestEffectSinkRejectsEmptyKey(t *testing.T) {
	sink := NewEffectSink()
	if _, err := sink.Apply("", "value"); err == nil {
		t.Fatal("empty idempotency key accepted")
	}
	if len(sink.Entries()) != 0 {
		t.Fatal("invalid attempt changed the sink")
	}
}
