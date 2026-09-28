package engine

import (
	"errors"
	"testing"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
)

func TestTaskTokenRoundTrip(t *testing.T) {
	in := &capstanv1.TaskToken{Kind: capstanv1.TaskKind_TASK_KIND_ACTIVITY, RunId: "r-1", TaskId: 7, Attempt: 2, ScheduledEventId: 5, Seq: 3}
	out, err := DecodeToken(EncodeToken(in))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.GetRunId() != "r-1" || out.GetTaskId() != 7 || out.GetAttempt() != 2 || out.GetSeq() != 3 {
		t.Fatalf("round trip mismatch: %v", out)
	}
}

func TestDecodeTokenRejectsGarbage(t *testing.T) {
	for _, b := range [][]byte{nil, {}, {0xff, 0xff, 0xff}, EncodeToken(&capstanv1.TaskToken{})} {
		if _, err := DecodeToken(b); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("DecodeToken(%x) = %v, want ErrInvalidArgument", b, err)
		}
	}
}

func TestConfigDefaults(t *testing.T) {
	c := Config{}.WithDefaults()
	if c.DailyCapUSD != 2.00 || c.DefaultTaskTimeout.Seconds() != 10 || c.MaxHistoryEvents != 50_000 {
		t.Fatalf("unexpected defaults: %+v", c)
	}
	if c.CapLocation == nil || c.CapLocation.String() != "America/Toronto" {
		t.Fatalf("cap location = %v", c.CapLocation)
	}
	if IdempotencyKey("r-1", 4) != "r-1/4" {
		t.Fatal("idempotency key format")
	}
}
