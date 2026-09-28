package engine

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
)

func ledgerActivity(t *testing.T, e *Engine) []byte {
	t.Helper()
	mustStart(t, e, "ledger-run")
	w := mustPoll(t, e)
	mustComplete(t, e, w.TaskToken, activityCmd(1))
	a, found, err := e.PollActivityTask(context.Background(), &v1.PollActivityTaskRequest{TaskQueue: "q", Identity: "model-worker"})
	if err != nil || !found {
		t.Fatalf("activity: found=%v err=%v", found, err)
	}
	return a.TaskToken
}

func reserveAI(t *testing.T, e *Engine, token []byte, model string, estimate float64) *v1.ReserveAICallResponse {
	t.Helper()
	r, err := e.ReserveAICall(context.Background(), &v1.ReserveAICallRequest{TaskToken: token, Model: model, EstimateUsd: estimate})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestReserveFailsClosedAtCap(t *testing.T) {
	e, _, _ := newTestEngine(t)
	e.cfg.DailyCapUSD = 1
	token := ledgerActivity(t, e)
	r := reserveAI(t, e, token, "claude-sonnet-5", .6)
	if r.ReservationId <= 0 || r.SpentTodayUsd != .6 || r.CapUsd != 1 {
		t.Fatalf("reservation: %v", r)
	}
	for _, estimate := range []float64{.5, -.1, -.0000001, math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, err := e.ReserveAICall(context.Background(), &v1.ReserveAICallRequest{TaskToken: token, Model: "claude-sonnet-5", EstimateUsd: estimate}); !errors.Is(err, ErrBudgetExceeded) {
			t.Fatalf("estimate %v: %v", estimate, err)
		}
	}
	r = reserveAI(t, e, token, "claude-sonnet-5", .4)
	if r.SpentTodayUsd != 1 {
		t.Fatalf("exact cap: %v", r)
	}
}

func TestReservedCountsUntilFinished(t *testing.T) {
	e, _, _ := newTestEngine(t)
	e.cfg.DailyCapUSD = 1
	token := ledgerActivity(t, e)
	r := reserveAI(t, e, token, "claude-sonnet-5", .8)
	info, err := e.DescribeRun(context.Background(), &v1.DescribeRunRequest{RunId: "ledger-run"})
	if err != nil || info.GetRun().GetCostUsd() != .8 {
		t.Fatalf("reserved run cost: %v %v", info, err)
	}
	if _, err := e.ReserveAICall(context.Background(), &v1.ReserveAICallRequest{TaskToken: token, Model: "claude-sonnet-5", EstimateUsd: .3}); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("pending budget: %v", err)
	}
	if _, err := e.FinishAICall(context.Background(), &v1.FinishAICallRequest{ReservationId: r.ReservationId, Ok: true, InputTokens: 100_000}); err != nil {
		t.Fatal(err)
	}
	r = reserveAI(t, e, token, "claude-sonnet-5", .8)
	if math.Abs(r.SpentTodayUsd-1) > 1e-12 {
		t.Fatalf("finished budget: %v", r)
	}
}

func TestFinishPricesFromTable(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		input, output, read, write int64
		want                       float64
	}{
		{"input_and_output", 1_000_000, 1_000_000, 0, 0, 12},
		{"cache_classes", 0, 0, 1_000_000, 1_000_000, 2.7},
		{"all_classes", 500_000, 100_000, 500_000, 200_000, 2.6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, clock, s := newTestEngine(t)
			token := ledgerActivity(t, e)
			r := reserveAI(t, e, token, "claude-sonnet-5", 1)
			out, err := e.FinishAICall(context.Background(), &v1.FinishAICallRequest{ReservationId: r.ReservationId, Ok: true, InputTokens: tc.input, OutputTokens: tc.output, CacheReadTokens: tc.read, CacheWriteTokens: tc.write})
			if err != nil || math.Abs(out.GetCostUsd()-tc.want) > 1e-12 {
				t.Fatalf("cost: %v %v want %v", out, err, tc.want)
			}
			if err := s.InTx(context.Background(), func(tx store.Tx) error {
				row, err := tx.GetAICall(r.ReservationId, false)
				if err != nil {
					return err
				}
				if row.Status != store.AICallFinished || row.RunID != "ledger-run" || row.ActivitySeq != 1 || row.InputTokens != tc.input || row.OutputTokens != tc.output || row.CacheReadTokens != tc.read || row.CacheWriteTokens != tc.write || !row.FinishedAt.Equal(clock.Now()) {
					t.Errorf("ledger row: %+v", row)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFinishRoundsUSDToMicroDollars(t *testing.T) {
	for _, tc := range []struct {
		name   string
		model  string
		tokens int64
		want   float64
	}{
		{"one_cache_read_token", "claude-sonnet-5", 1, 0},
		{"three_cache_read_tokens", "claude-sonnet-5", 3, .000001},
		{"half_micro_dollar", "claude-haiku-4-5", 5, .000001}, // 5 × $0.10/M = $0.0000005, a tie
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, _, s := newTestEngine(t)
			r := reserveAI(t, e, ledgerActivity(t, e), tc.model, 1)
			request := &v1.FinishAICallRequest{ReservationId: r.ReservationId, Ok: true, CacheReadTokens: tc.tokens}
			first, err := e.FinishAICall(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if first.CostUsd != tc.want {
				t.Errorf("first finish cost = %.18g, want %.18g", first.CostUsd, tc.want)
			}
			if err := s.InTx(context.Background(), func(tx store.Tx) error {
				row, err := tx.GetAICall(r.ReservationId, false)
				if err == nil && row.CostUSD != tc.want {
					t.Errorf("stored cost = %.18g, want %.18g", row.CostUSD, tc.want)
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			second, err := e.FinishAICall(context.Background(), request)
			if err != nil || second.GetCostUsd() != first.CostUsd {
				t.Fatalf("repeated finish = %v, err = %v; first = %v", second, err, first)
			}
		})
	}
}

func TestReserveRoundsUSDToMicroDollars(t *testing.T) {
	for _, tc := range []struct {
		name     string
		estimate float64
		want     float64
	}{
		{"below_half", .0000004, 0},
		{"half_micro_dollar", .0000005, .000001},
		{"above_half", .1234567, .123457},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, _, s := newTestEngine(t)
			r := reserveAI(t, e, ledgerActivity(t, e), "unpriced", tc.estimate)
			if r.SpentTodayUsd != tc.want {
				t.Errorf("spent = %.18g, want %.18g", r.SpentTodayUsd, tc.want)
			}
			if err := s.InTx(context.Background(), func(tx store.Tx) error {
				row, err := tx.GetAICall(r.ReservationId, false)
				if err == nil && row.EstimateUSD != tc.want {
					t.Errorf("stored estimate = %.18g, want %.18g", row.EstimateUSD, tc.want)
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			finished, err := e.FinishAICall(context.Background(), &v1.FinishAICallRequest{ReservationId: r.ReservationId, Ok: true})
			if err != nil || finished.GetCostUsd() != tc.want {
				t.Fatalf("unpriced finish = %v, err = %v; want %.18g", finished, err, tc.want)
			}
		})
	}
}

func TestReserveComparesRoundedUSDAtCap(t *testing.T) {
	e, _, _ := newTestEngine(t)
	e.cfg.DailyCapUSD = .3
	token := ledgerActivity(t, e)
	reserveAI(t, e, token, "unpriced", .1)
	r := reserveAI(t, e, token, "unpriced", .2000004)
	if r.SpentTodayUsd != .3 {
		t.Fatalf("spent at cap = %.18g, want .3", r.SpentTodayUsd)
	}
	if _, err := e.ReserveAICall(context.Background(), &v1.ReserveAICallRequest{TaskToken: token, EstimateUsd: .0000005}); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("rounded estimate past cap: %v", err)
	}
}

func TestReserveReturnsRoundedUSDSum(t *testing.T) {
	e, _, _ := newTestEngine(t)
	token := ledgerActivity(t, e)
	reserveAI(t, e, token, "unpriced", .1)
	r := reserveAI(t, e, token, "unpriced", .2)
	if r.SpentTodayUsd != .3 {
		t.Fatalf("spent = %.18g, want .3", r.SpentTodayUsd)
	}
}

func TestFinishRejectsInvalidCostBeforeRounding(t *testing.T) {
	for _, tc := range []struct {
		name   string
		price  float64
		tokens int64
	}{
		{"negative_sub_micro_dollar", -.1, 1},
		{"nan", math.NaN(), 1},
		{"positive_infinity", math.Inf(1), 1},
		{"negative_infinity", math.Inf(-1), 1},
		{"finite_price_overflow", math.MaxFloat64, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, _, s := newTestEngine(t)
			e.cfg.ModelPrices = map[string]ModelPrice{"invalid": {Input: tc.price}}
			r := reserveAI(t, e, ledgerActivity(t, e), "invalid", .1)
			response, err := e.FinishAICall(context.Background(), &v1.FinishAICallRequest{ReservationId: r.ReservationId, Ok: true, InputTokens: tc.tokens})
			if response != nil || !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("invalid cost: response = %v, err = %v", response, err)
			}
			if err := s.InTx(context.Background(), func(tx store.Tx) error {
				row, err := tx.GetAICall(r.ReservationId, false)
				if err == nil && (row.Status != store.AICallReserved || row.CostUSD != 0 || !row.FinishedAt.IsZero()) {
					t.Errorf("invalid cost changed reservation: %+v", row)
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLedgerTimestampsUseMicroseconds(t *testing.T) {
	e, clock, s := newTestEngine(t)
	token := ledgerActivity(t, e)
	clock.Advance(1234 * time.Nanosecond)
	r := reserveAI(t, e, token, "claude-sonnet-5", .1)
	wantAt := clock.Now().Truncate(time.Microsecond)
	clock.Advance(2789 * time.Nanosecond)
	if _, err := e.FinishAICall(context.Background(), &v1.FinishAICallRequest{ReservationId: r.ReservationId, Ok: true, CacheReadTokens: 1}); err != nil {
		t.Fatal(err)
	}
	wantFinishedAt := clock.Now().Truncate(time.Microsecond)
	if err := s.InTx(context.Background(), func(tx store.Tx) error {
		row, err := tx.GetAICall(r.ReservationId, false)
		if err != nil {
			return err
		}
		if !row.At.Equal(wantAt) || !row.FinishedAt.Equal(wantFinishedAt) {
			t.Errorf("ledger times: at = %v, finished = %v; want %v and %v", row.At, row.FinishedAt, wantAt, wantFinishedAt)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestFinishUsesConfiguredPrices(t *testing.T) {
	e, _, _ := newTestEngine(t)
	e.cfg.ModelPrices = map[string]ModelPrice{"custom": {Input: 1, Output: 2, CacheRead: 3, CacheWrite: 4}}
	r := reserveAI(t, e, ledgerActivity(t, e), "custom", 1)
	out, err := e.FinishAICall(context.Background(), &v1.FinishAICallRequest{ReservationId: r.ReservationId, Ok: true, InputTokens: 1_000_000, OutputTokens: 1_000_000, CacheReadTokens: 1_000_000, CacheWriteTokens: 1_000_000})
	if err != nil || out.GetCostUsd() != 10 {
		t.Fatalf("configured cost: %v %v", out, err)
	}
}

func TestFinishUnknownModelChargesEstimate(t *testing.T) {
	e, _, _ := newTestEngine(t)
	r := reserveAI(t, e, ledgerActivity(t, e), "unpriced", .7)
	out, err := e.FinishAICall(context.Background(), &v1.FinishAICallRequest{ReservationId: r.ReservationId, Ok: true, InputTokens: 100, OutputTokens: 200})
	if err != nil || out.GetCostUsd() != .7 {
		t.Fatalf("unknown cost: %v %v", out, err)
	}
}

func TestFinishFailureChargesOnlyUsage(t *testing.T) {
	for _, model := range []string{"claude-sonnet-5", "unpriced"} {
		for _, tokens := range []int64{0, 100_000} {
			t.Run(model+"/"+time.Duration(tokens).String(), func(t *testing.T) {
				e, _, s := newTestEngine(t)
				r := reserveAI(t, e, ledgerActivity(t, e), model, .7)
				out, err := e.FinishAICall(context.Background(), &v1.FinishAICallRequest{ReservationId: r.ReservationId, Ok: false, InputTokens: tokens, ErrorCode: "RATE_LIMIT"})
				want := 0.0
				if tokens > 0 {
					want = .2
					if model == "unpriced" {
						want = .7
					}
				}
				if err != nil || out.GetCostUsd() != want {
					t.Fatalf("failed cost: %v %v want %v", out, err, want)
				}
				if err := s.InTx(context.Background(), func(tx store.Tx) error {
					row, err := tx.GetAICall(r.ReservationId, false)
					if err == nil && (row.Status != store.AICallFailed || row.ErrorCode != "RATE_LIMIT") {
						t.Errorf("failed row: %+v", row)
					}
					return err
				}); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestFinishUnknownUsageChargesAtLeastEstimate(t *testing.T) {
	for _, tc := range []struct {
		name     string
		model    string
		estimate float64
		input    int64
		ok       bool
		want     float64
	}{
		{"missing rounded usage", "claude-sonnet-5", .7000005, 0, false, .700001},
		{"reported usage below estimate", "claude-sonnet-5", .7, 100_000, false, .7},
		{"reported usage above estimate", "claude-sonnet-5", .7, 1_000_000, false, 2},
		{"unknown model", "unpriced", .7, 0, false, .7},
		{"successful call ignores flag", "claude-sonnet-5", .7, 100_000, true, .2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, _, s := newTestEngine(t)
			r := reserveAI(t, e, ledgerActivity(t, e), tc.model, tc.estimate)
			request := &v1.FinishAICallRequest{ReservationId: r.ReservationId, Ok: tc.ok, InputTokens: tc.input, UsageUnknown: true, ErrorCode: "ModelTimeoutUsageUnknown"}
			first, err := e.FinishAICall(t.Context(), request)
			if err != nil || first.GetCostUsd() != tc.want {
				t.Fatalf("unknown usage cost: %v %v want %v", first, err, tc.want)
			}
			// Even a repeat reporting different usage returns the stored rounded charge.
			second, err := e.FinishAICall(t.Context(), &v1.FinishAICallRequest{ReservationId: r.ReservationId, Ok: false})
			if err != nil || second.GetCostUsd() != tc.want {
				t.Fatalf("repeat cost: %v %v want %v", second, err, tc.want)
			}
			if err := s.InTx(t.Context(), func(tx store.Tx) error {
				row, err := tx.GetAICall(r.ReservationId, false)
				if err != nil {
					return err
				}
				status := store.AICallFailed
				if tc.ok {
					status = store.AICallFinished
				}
				if row.Status != status || row.CostUSD != tc.want || row.InputTokens != tc.input {
					t.Errorf("stored call changed after repeat: %+v", row)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFinishIsIdempotent(t *testing.T) {
	e, _, _ := newTestEngine(t)
	r := reserveAI(t, e, ledgerActivity(t, e), "claude-sonnet-5", .7)
	first, err := e.FinishAICall(context.Background(), &v1.FinishAICallRequest{ReservationId: r.ReservationId, Ok: true, InputTokens: 100_000})
	if err != nil {
		t.Fatal(err)
	}
	second, err := e.FinishAICall(context.Background(), &v1.FinishAICallRequest{ReservationId: r.ReservationId, Ok: false, InputTokens: 2_000_000})
	if err != nil || second.GetCostUsd() != first.CostUsd {
		t.Fatalf("second finish: %v %v first=%v", second, err, first)
	}
}

func TestFinishRejectsNegativeTokens(t *testing.T) {
	e, _, _ := newTestEngine(t)
	r := reserveAI(t, e, ledgerActivity(t, e), "claude-sonnet-5", .7)
	for _, req := range []*v1.FinishAICallRequest{
		{InputTokens: -1}, {OutputTokens: -1}, {CacheReadTokens: -1}, {CacheWriteTokens: -1},
	} {
		req.ReservationId = r.ReservationId
		if _, err := e.FinishAICall(context.Background(), req); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("negative tokens: %v", err)
		}
	}
	if _, err := e.FinishAICall(context.Background(), &v1.FinishAICallRequest{ReservationId: 999999}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing reservation: %v", err)
	}
}

func TestCapResetsAtTorontoMidnight(t *testing.T) {
	for _, when := range []string{"2026-09-29T03:59:59Z", "2026-12-01T04:59:59Z", "2026-03-08T04:59:59Z", "2026-11-01T03:59:59Z"} {
		t.Run(when, func(t *testing.T) {
			e, clock, _ := newTestEngine(t)
			target, err := time.Parse(time.RFC3339, when)
			if err != nil {
				t.Fatal(err)
			}
			clock.Advance(target.Sub(clock.Now()))
			e.cfg.DailyCapUSD = 1
			token := ledgerActivity(t, e)
			reserveAI(t, e, token, "claude-sonnet-5", 1)
			if _, err := e.ReserveAICall(context.Background(), &v1.ReserveAICallRequest{TaskToken: token, Model: "claude-sonnet-5", EstimateUsd: .1}); !errors.Is(err, ErrBudgetExceeded) {
				t.Fatalf("before midnight: %v", err)
			}
			clock.Advance(time.Second)
			if r := reserveAI(t, e, token, "claude-sonnet-5", 1); r.SpentTodayUsd != 1 {
				t.Fatalf("after midnight: %v", r)
			}
		})
	}
}

func TestConcurrentReservationsCannotExceedCap(t *testing.T) {
	e, _, _ := newTestEngine(t)
	e.cfg.DailyCapUSD = 1
	token := ledgerActivity(t, e)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Go(func() {
			_, err := e.ReserveAICall(context.Background(), &v1.ReserveAICallRequest{TaskToken: token, Model: "claude-sonnet-5", EstimateUsd: .6})
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	success := 0
	for err := range errs {
		if err == nil {
			success++
		} else if !errors.Is(err, ErrBudgetExceeded) {
			t.Fatal(err)
		}
	}
	if success != 1 {
		t.Fatalf("successful reservations=%d want 1", success)
	}
}

func TestReserveRequiresLiveActivityToken(t *testing.T) {
	e, _, _ := newTestEngine(t)
	token := ledgerActivity(t, e)
	if _, err := e.CompleteActivityTask(context.Background(), &v1.CompleteActivityTaskRequest{TaskToken: token}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ReserveAICall(context.Background(), &v1.ReserveAICallRequest{TaskToken: token, Model: "claude-sonnet-5", EstimateUsd: .1}); !errors.Is(err, ErrStaleTask) {
		t.Fatalf("completed activity reservation: %v", err)
	}
}

func TestReserveAICallSeeded(t *testing.T) {
	e, clock, s := newTestEngine(t)
	var task store.Task
	if err := s.InTx(context.Background(), func(tx store.Tx) error {
		if err := tx.InsertRun(&store.Run{RunID: "ledger", WorkflowType: "wf", TaskQueue: "q", Status: v1.RunStatus_RUN_STATUS_RUNNING, StartedAt: clock.Now(), TaskTimeout: time.Second}); err != nil {
			return err
		}
		task = store.Task{Kind: store.TaskActivity, RunID: "ledger", TaskQueue: "q", Attempt: 1, ScheduledEventID: 1, ScheduledAt: clock.Now(), StartedAt: clock.Now(), VisibleAt: clock.Now(), LeasedUntil: clock.Now().Add(time.Second), Activity: &v1.ActivityScheduledAttributes{Seq: 1}}
		return tx.InsertTask(&task)
	}); err != nil {
		t.Fatal(err)
	}
	token := EncodeToken(&v1.TaskToken{Kind: v1.TaskKind_TASK_KIND_ACTIVITY, RunId: "ledger", TaskId: task.ID, Attempt: 1, ScheduledEventId: 1, Seq: 1})
	r := reserveAI(t, e, token, "claude-sonnet-5", .6)
	if r.SpentTodayUsd != .6 {
		t.Fatalf("reservation: %v", r)
	}
}

func TestFinishAICallSeeded(t *testing.T) {
	e, clock, s := newTestEngine(t)
	call := &store.AICall{RunID: "ledger", ActivitySeq: 1, Model: "claude-sonnet-5", Status: store.AICallReserved, EstimateUSD: 1, At: clock.Now()}
	if err := s.InTx(context.Background(), func(tx store.Tx) error {
		if err := tx.InsertRun(&store.Run{RunID: "ledger", WorkflowType: "wf", TaskQueue: "q", Status: v1.RunStatus_RUN_STATUS_RUNNING, StartedAt: clock.Now(), TaskTimeout: time.Second}); err != nil {
			return err
		}
		return tx.InsertAICall(call)
	}); err != nil {
		t.Fatal(err)
	}
	r, err := e.FinishAICall(context.Background(), &v1.FinishAICallRequest{ReservationId: call.ID, Ok: true, InputTokens: 1_000_000, OutputTokens: 1_000_000})
	if err != nil || r.GetCostUsd() != 12 {
		t.Fatalf("finish: %v %v", r, err)
	}
}

type abortLedgerStore struct {
	store.Store
	failure      error
	transactions int
}

func (s *abortLedgerStore) InTx(ctx context.Context, fn func(store.Tx) error) error {
	s.transactions++
	return s.Store.InTx(ctx, func(tx store.Tx) error {
		if err := fn(tx); err != nil {
			return err
		}
		return s.failure
	})
}

func TestReserveAndFinishRollBackOnTransactionFailure(t *testing.T) {
	e, _, s := newTestEngine(t)
	token := ledgerActivity(t, e)
	sentinel := errors.New("injected ledger transaction failure")
	abort := &abortLedgerStore{Store: s, failure: sentinel}
	e.deps.Store = abort
	if response, err := e.ReserveAICall(context.Background(), &v1.ReserveAICallRequest{TaskToken: token, Model: "claude-sonnet-5", EstimateUsd: .6}); response != nil || !errors.Is(err, sentinel) {
		t.Fatalf("reservation rollback: %v %v", response, err)
	}
	if abort.transactions != 1 {
		t.Fatalf("reserve transactions=%d", abort.transactions)
	}
	e.deps.Store = s
	if info, err := e.DescribeRun(context.Background(), &v1.DescribeRunRequest{RunId: "ledger-run"}); err != nil || info.GetRun().GetCostUsd() != 0 {
		t.Fatalf("reserved cost survived rollback: %v %v", info, err)
	}
	r := reserveAI(t, e, token, "claude-sonnet-5", .6)
	e.deps.Store = abort
	if response, err := e.FinishAICall(context.Background(), &v1.FinishAICallRequest{ReservationId: r.ReservationId, Ok: true, InputTokens: 100_000}); response != nil || !errors.Is(err, sentinel) {
		t.Fatalf("finish rollback: %v %v", response, err)
	}
	if abort.transactions != 2 {
		t.Fatalf("finish transactions=%d", abort.transactions-1)
	}
	e.deps.Store = s
	if err := s.InTx(context.Background(), func(tx store.Tx) error {
		row, err := tx.GetAICall(r.ReservationId, false)
		if err == nil && (row.Status != store.AICallReserved || row.CostUSD != 0 || !row.FinishedAt.IsZero()) {
			t.Errorf("finish survived rollback: %+v", row)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if response, err := e.FinishAICall(context.Background(), &v1.FinishAICallRequest{ReservationId: r.ReservationId, Ok: true, InputTokens: 100_000}); err != nil || response.GetCostUsd() != .2 {
		t.Fatalf("finish retry: %v %v", response, err)
	}
}
