package config

import (
	"testing"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/engine"
	"github.com/Aly700/capstan/internal/store"
	"github.com/Aly700/capstan/internal/store/pgstore"
	"github.com/Aly700/capstan/internal/testpg"
)

func TestModelPriceEnvironmentOverridesMatchFamilyPrefixes(t *testing.T) {
	values := env()
	values["CAPSTAN_MODEL_PRICES"] = `{"claude-opus-5":{"input":8},"custom-family":{"input":3},"custom-family-long":{"input":6}}`
	cfg, err := load(values)
	if err != nil {
		t.Fatal(err)
	}
	s, err := pgstore.Open(t.Context(), testpg.New(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	e, err := engine.New(engine.Deps{Store: s}, engine.Config{ModelPrices: cfg.ModelPrices})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	if err := s.InTx(t.Context(), func(tx store.Tx) error {
		return tx.InsertRun(&store.Run{RunID: "r", WorkflowType: "flow", TaskQueue: "q", Status: v1.RunStatus_RUN_STATUS_RUNNING, StartedAt: at, TaskTimeout: time.Second})
	}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		model string
		want  float64
	}{
		{"claude-opus-5-5", 8}, {"custom-family-v2", 3}, {"custom-family-long-v2", 6},
		{"custom-familyish", .7}, {"claude-haiku-4-5-20251001", 1},
	} {
		t.Run(tc.model, func(t *testing.T) {
			call := &store.AICall{RunID: "r", Model: tc.model, Status: store.AICallReserved, EstimateUSD: .7, At: at}
			if err := s.InTx(t.Context(), func(tx store.Tx) error { return tx.InsertAICall(call) }); err != nil {
				t.Fatal(err)
			}
			response, err := e.FinishAICall(t.Context(), &v1.FinishAICallRequest{ReservationId: call.ID, Ok: true, InputTokens: 1_000_000})
			if err != nil || response.GetCostUsd() != tc.want {
				t.Fatalf("cost = %v, err=%v; want %v", response, err, tc.want)
			}
		})
	}
}
