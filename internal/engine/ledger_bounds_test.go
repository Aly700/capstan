package engine

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
)

func TestReserveBoundRejectsF07ZeroEstimate(t *testing.T) {
	e, _, _ := newTestEngine(t)
	e.cfg.DailyCapUSD = 1
	// F07: a $0 estimate cannot admit $2 of known-model usage under a $1 cap.
	response, err := e.ReserveAICall(t.Context(), &v1.ReserveAICallRequest{
		TaskToken: ledgerActivity(t, e), Model: "claude-sonnet-5", EstimateUsd: 0,
		InputTokensUpperBound: 500_000, MaxOutputTokens: 100_000,
	})
	if response != nil || !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("$2 bound under $1 cap: response=%v err=%v", response, err)
	}
	info, err := e.DescribeRun(t.Context(), &v1.DescribeRunRequest{RunId: "ledger-run"})
	if err != nil || info.GetRun().GetCostUsd() != 0 {
		t.Fatalf("rejected reservation changed spend: %v %v", info, err)
	}
}

func TestReserveBoundUsesLargerEstimateAndFamilyPrice(t *testing.T) {
	for _, tc := range []struct {
		name, model string
		estimate    float64
		prices      map[string]ModelPrice
		want        float64
		bounded     bool
	}{
		{"bound exceeds estimate", "claude-sonnet-5", .1, nil, 12, true},
		{"estimate exceeds bound", "claude-sonnet-5", 13, nil, 13, true},
		{"dated family", "claude-haiku-4-5-20251001", 0, nil, 6, true},
		{"longest family", "claude-opus-5-5-20260928", 0, map[string]ModelPrice{"claude-opus": {Input: 2}, "claude-opus-5-5": {Input: 9, Output: 1}}, 10, true},
		{"exact precedes family", "claude-opus-5-5", 0, map[string]ModelPrice{"claude-opus-5-5": {Input: 7, Output: 1}}, 8, true},
		{"zero price is present", "claude-opus-5-5", 0, map[string]ModelPrice{"claude-opus-5-5": {}}, 0, true},
		{"hyphen boundary required", "claude-opus-50", .7, nil, .7, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, _, s := newTestEngine(t)
			e.cfg.DailyCapUSD = 20
			for model, price := range tc.prices {
				e.cfg.ModelPrices[model] = price
			}
			response, err := e.ReserveAICall(t.Context(), &v1.ReserveAICallRequest{
				TaskToken: ledgerActivity(t, e), Model: tc.model, EstimateUsd: tc.estimate,
				InputTokensUpperBound: 1_000_000, MaxOutputTokens: 1_000_000,
			})
			if err != nil || response.GetSpentTodayUsd() != tc.want {
				t.Fatalf("reservation=%v err=%v, want $%v", response, err, tc.want)
			}
			assertReservation(t, s, response.ReservationId, tc.want, tc.bounded)
		})
	}
}

func TestReserveBoundRoundsBeforeCapComparison(t *testing.T) {
	e, _, s := newTestEngine(t)
	e.cfg.ModelPrices["half-micro"] = ModelPrice{Input: .1, Output: .4}
	e.cfg.DailyCapUSD = .000001
	token := ledgerActivity(t, e)
	req := &v1.ReserveAICallRequest{TaskToken: token, Model: "half-micro", InputTokensUpperBound: 1, MaxOutputTokens: 1}
	response, err := e.ReserveAICall(t.Context(), req)
	if err != nil || response.GetSpentTodayUsd() != .000001 {
		t.Fatalf("rounded bound at cap: %v %v", response, err)
	}
	assertReservation(t, s, response.ReservationId, .000001, true)
	if response, err := e.ReserveAICall(t.Context(), req); response != nil || !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("rounded bound beyond cap: %v %v", response, err)
	}
}

func TestConcurrentBoundedReservationsAndUsageCannotExceedCap(t *testing.T) {
	for _, actual := range []struct {
		name          string
		input, output int64
		cost          float64
	}{
		{"at bound", 25_000, 20_000, .25},
		{"below bound", 12_500, 10_000, .125},
	} {
		t.Run(actual.name, func(t *testing.T) {
			e, _, s := newTestEngine(t)
			e.cfg.DailyCapUSD = 1
			// Independent runs/tasks make the budget lock, rather than one run's
			// row lock, responsible for serializing these 16 reservations.
			tokens := make([][]byte, 16)
			for i := range tokens {
				mustStart(t, e, fmt.Sprintf("bounded-%d", i))
				w := mustPoll(t, e)
				mustComplete(t, e, w.TaskToken, activityCmd(1))
				a, found, err := e.PollActivityTask(t.Context(), &v1.PollActivityTaskRequest{TaskQueue: "q", Identity: "bounded-worker"})
				if err != nil || !found {
					t.Fatalf("activity %d: found=%v err=%v", i, found, err)
				}
				tokens[i] = a.TaskToken
			}
			var wg sync.WaitGroup
			start := make(chan struct{})
			accepted := make(chan *v1.ReserveAICallResponse, len(tokens))
			for _, token := range tokens {
				wg.Go(func() {
					<-start
					response, err := e.ReserveAICall(t.Context(), &v1.ReserveAICallRequest{
						TaskToken: token, Model: "claude-sonnet-5", EstimateUsd: 0,
						InputTokensUpperBound: 25_000, MaxOutputTokens: 20_000,
					})
					if err == nil {
						if response.SpentTodayUsd > 1 {
							t.Errorf("reservation exceeded cap: %v", response)
						}
						accepted <- response
					} else if !errors.Is(err, ErrBudgetExceeded) {
						t.Errorf("reserve: %v", err)
					}
				})
			}
			close(start)
			wg.Wait()
			close(accepted)
			if len(accepted) != 4 {
				t.Fatalf("accepted %d reservations of $.25 under $1 cap, want 4", len(accepted))
			}
			for reservation := range accepted {
				assertReservation(t, s, reservation.ReservationId, .25, true)
				wg.Go(func() {
					response, err := e.FinishAICall(t.Context(), &v1.FinishAICallRequest{
						ReservationId: reservation.ReservationId, Ok: true, InputTokens: actual.input, OutputTokens: actual.output,
					})
					if err != nil || response.GetCostUsd() != actual.cost {
						t.Errorf("finish: %v %v, want $%v", response, err, actual.cost)
					}
					if err := s.InTx(t.Context(), func(tx store.Tx) error {
						spent, err := tx.SpentSince(e.now())
						if err == nil && spent > 1 {
							t.Errorf("spend after finish exceeded cap: %v", spent)
						}
						return err
					}); err != nil {
						t.Errorf("read spend: %v", err)
					}
				})
			}
			wg.Wait()
			if err := s.InTx(t.Context(), func(tx store.Tx) error {
				spent, err := tx.SpentSince(e.now())
				if err == nil && spent != 4*actual.cost {
					t.Errorf("final spend=$%v, want $%v", spent, 4*actual.cost)
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReserveBoundLegacyStillRecordsTrueUsage(t *testing.T) {
	for _, bounds := range []struct{ input, output int64 }{{0, 0}, {1_000_000, 0}, {0, 100_000}} {
		t.Run(fmt.Sprintf("input=%d/output=%d", bounds.input, bounds.output), func(t *testing.T) {
			e, _, s := newTestEngine(t)
			e.cfg.DailyCapUSD = 1
			response, err := e.ReserveAICall(t.Context(), &v1.ReserveAICallRequest{
				TaskToken: ledgerActivity(t, e), Model: "claude-sonnet-5", EstimateUsd: 0,
				InputTokensUpperBound: bounds.input, MaxOutputTokens: bounds.output,
			})
			if err != nil || response.GetSpentTodayUsd() != 0 {
				t.Fatalf("legacy reserve: %v %v", response, err)
			}
			assertReservation(t, s, response.ReservationId, 0, false)
			finished, err := e.FinishAICall(t.Context(), &v1.FinishAICallRequest{ReservationId: response.ReservationId, Ok: true, InputTokens: 1_000_000})
			if err != nil || finished.GetCostUsd() != 2 {
				t.Fatalf("legacy Finish must record $2 even above cap: %v %v", finished, err)
			}
			assertReservation(t, s, response.ReservationId, 0, false)
		})
	}
}

func TestReserveBoundUnknownModelTrustsEstimate(t *testing.T) {
	e, _, s := newTestEngine(t)
	e.cfg.DailyCapUSD = 1
	token := ledgerActivity(t, e)
	req := &v1.ReserveAICallRequest{TaskToken: token, Model: "unpriced", EstimateUsd: .7000005, InputTokensUpperBound: math.MaxInt64, MaxOutputTokens: math.MaxInt64}
	response, err := e.ReserveAICall(t.Context(), req)
	if err != nil || response.GetSpentTodayUsd() != .700001 {
		t.Fatalf("unknown model reserve: %v %v", response, err)
	}
	assertReservation(t, s, response.ReservationId, .700001, false)
	finished, err := e.FinishAICall(t.Context(), &v1.FinishAICallRequest{ReservationId: response.ReservationId, Ok: true, InputTokens: math.MaxInt64})
	if err != nil || finished.GetCostUsd() != .700001 {
		t.Fatalf("unknown model finish: %v %v", finished, err)
	}
	if response, err := e.ReserveAICall(t.Context(), req); response != nil || !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("unknown model estimate beyond cap: %v %v", response, err)
	}
}

func TestReserveBoundFinishPreservesUsageAccounting(t *testing.T) {
	for _, tc := range []struct {
		name        string
		ok, unknown bool
		input       int64
		want        float64
	}{
		{"true usage above declared bound", true, false, 1_000_000, 2},
		{"unknown usage retains server bound", false, true, 0, .3},
		{"known failure before usage releases bound", false, false, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, _, s := newTestEngine(t)
			e.cfg.DailyCapUSD = 1
			reservation, err := e.ReserveAICall(t.Context(), &v1.ReserveAICallRequest{
				TaskToken: ledgerActivity(t, e), Model: "claude-sonnet-5", EstimateUsd: 0,
				InputTokensUpperBound: 100_000, MaxOutputTokens: 10_000,
			})
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				finished, err := e.FinishAICall(t.Context(), &v1.FinishAICallRequest{
					ReservationId: reservation.ReservationId, Ok: tc.ok, UsageUnknown: tc.unknown, InputTokens: tc.input,
				})
				if err != nil || finished.GetCostUsd() != tc.want {
					t.Fatalf("finish: %v %v, want $%v", finished, err, tc.want)
				}
			}
			assertReservation(t, s, reservation.ReservationId, .3, true)
		})
	}
}

func TestReserveBoundRejectsNegativeTokens(t *testing.T) {
	e, _, _ := newTestEngine(t)
	token := ledgerActivity(t, e)
	for _, model := range []string{"claude-sonnet-5", "unpriced"} {
		for _, bounds := range []struct{ input, output int64 }{{-1, 1}, {1, -1}, {-1, 0}, {0, -1}} {
			response, err := e.ReserveAICall(t.Context(), &v1.ReserveAICallRequest{
				TaskToken: token, Model: model, InputTokensUpperBound: bounds.input, MaxOutputTokens: bounds.output,
			})
			if response != nil || !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("model=%s bounds=%+v: response=%v err=%v", model, bounds, response, err)
			}
		}
	}
}

func TestReserveBoundLargeTokensFailClosed(t *testing.T) {
	for _, tc := range []struct {
		name          string
		input, output int64
		price         float64
	}{
		{"input exceeds int64 product", math.MaxInt64, 1, 10},
		{"output exceeds int64 product", 1, math.MaxInt64, 10},
		{"both exceed int64 product", math.MaxInt64, math.MaxInt64, 10},
		{"float product overflow", math.MaxInt64, math.MaxInt64, math.MaxFloat64},
		{"rounding overflow", 1, 1, math.MaxFloat64 / 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, _, _ := newTestEngine(t)
			e.cfg.DailyCapUSD = 1
			e.cfg.ModelPrices["large"] = ModelPrice{Input: tc.price, Output: tc.price}
			response, err := e.ReserveAICall(t.Context(), &v1.ReserveAICallRequest{
				TaskToken: ledgerActivity(t, e), Model: "large", InputTokensUpperBound: tc.input, MaxOutputTokens: tc.output,
			})
			if response != nil || !errors.Is(err, ErrBudgetExceeded) {
				t.Fatalf("large bound: response=%v err=%v", response, err)
			}
		})
	}
}

func assertReservation(t *testing.T, s store.Store, id int64, estimate float64, bounded bool) {
	t.Helper()
	if err := s.InTx(t.Context(), func(tx store.Tx) error {
		row, err := tx.GetAICall(id, false)
		if err == nil && (row.EstimateUSD != estimate || row.Bounded != bounded) {
			t.Errorf("stored reservation: %+v, want estimate=$%v bounded=%v", row, estimate, bounded)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
