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
	for _, estimate := range []float64{.5, -.1, math.NaN(), math.Inf(1), math.Inf(-1)} {
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
