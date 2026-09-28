package engine

import (
	"testing"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
)

func TestFinishMatchesModelFamilyPrices(t *testing.T) {
	for _, tc := range []struct {
		name, model string
		extra       map[string]ModelPrice
		want        float64
	}{
		{"exact", "claude-opus-5", nil, 5},
		{"family", "claude-opus-5-5", nil, 5},
		{"dated_haiku", "claude-haiku-4-5-20251001", nil, 1},
		{"boundary_required", "claude-opus-50", nil, .7},
		{"unknown", "unpriced-model", nil, .7},
		{"longest", "claude-opus-5-5-20260928", map[string]ModelPrice{"claude-opus": {Input: 2}, "claude-opus-5-5": {Input: 9}}, 9},
		{"exact_over_family", "claude-opus-5-5", map[string]ModelPrice{"claude-opus-5-5": {Input: 7}}, 7},
		{"zero_price_is_present", "claude-opus-5-5", map[string]ModelPrice{"claude-opus-5-5": {}}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, _, _ := newTestEngine(t)
			for model, price := range tc.extra {
				e.cfg.ModelPrices[model] = price
			}
			r := reserveAI(t, e, ledgerActivity(t, e), tc.model, .7)
			req := &v1.FinishAICallRequest{ReservationId: r.ReservationId, Ok: true, InputTokens: 1_000_000}
			for range 2 {
				response, err := e.FinishAICall(t.Context(), req)
				if err != nil || response.GetCostUsd() != tc.want {
					t.Fatalf("cost for %s = %v, err=%v; want %v", tc.model, response, err, tc.want)
				}
			}
		})
	}
}
