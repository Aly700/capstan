package engine

import (
	"context"
	"errors"
	"math"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
)

func (e *Engine) ReserveAICall(ctx context.Context, req *v1.ReserveAICallRequest) (*v1.ReserveAICallResponse, error) {
	if req == nil {
		return nil, Invalid("AI call reservation is required")
	}
	var response *v1.ReserveAICallResponse
	err := e.inTx(ctx, func(tx store.Tx) error {
		response = nil
		task, run, err := e.activityToken(tx, req.TaskToken)
		if err != nil {
			return err
		}
		if err := tx.LockBudget(); err != nil {
			return err
		}
		now := e.now()
		local := now.In(e.cfg.CapLocation)
		midnight := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, e.cfg.CapLocation)
		spent, err := tx.SpentSince(midnight)
		if err != nil {
			return err
		}
		estimate := req.EstimateUsd
		if estimate < 0 || math.IsNaN(estimate) || math.IsInf(estimate, 0) || math.IsNaN(spent) || math.IsInf(spent, 0) {
			return ErrBudgetExceeded
		}
		estimate = roundUSD(estimate)
		total := roundUSD(spent + estimate)
		if total > e.cfg.DailyCapUSD {
			return ErrBudgetExceeded
		}
		call := &store.AICall{RunID: run.RunID, ActivitySeq: task.Activity.GetSeq(), Model: req.Model, Status: store.AICallReserved, EstimateUSD: estimate, At: now}
		if err := tx.InsertAICall(call); err != nil {
			return err
		}
		response = &v1.ReserveAICallResponse{ReservationId: call.ID, SpentTodayUsd: total, CapUsd: e.cfg.DailyCapUSD}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (e *Engine) FinishAICall(ctx context.Context, req *v1.FinishAICallRequest) (*v1.FinishAICallResponse, error) {
	if req == nil {
		return nil, Invalid("AI call result is required")
	}
	var response *v1.FinishAICallResponse
	err := e.inTx(ctx, func(tx store.Tx) error {
		response = nil
		call, err := tx.GetAICall(req.ReservationId, true)
		if errors.Is(err, store.ErrNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if call.Status != store.AICallReserved {
			response = &v1.FinishAICallResponse{CostUsd: call.CostUSD}
			return nil
		}
		if req.InputTokens < 0 || req.OutputTokens < 0 || req.CacheReadTokens < 0 || req.CacheWriteTokens < 0 {
			return Invalid("token counts must not be negative")
		}
		cost := call.EstimateUSD
		if price, ok := e.cfg.ModelPrices[call.Model]; ok {
			cost = (float64(req.InputTokens)*price.Input + float64(req.OutputTokens)*price.Output + float64(req.CacheReadTokens)*price.CacheRead + float64(req.CacheWriteTokens)*price.CacheWrite) / 1_000_000
		}
		if !req.Ok && req.InputTokens == 0 && req.OutputTokens == 0 && req.CacheReadTokens == 0 && req.CacheWriteTokens == 0 {
			cost = 0
		}
		if cost < 0 || math.IsNaN(cost) || math.IsInf(cost, 0) {
			return Invalid("model price produces an invalid cost")
		}
		cost = roundUSD(cost)
		call.Status = store.AICallFinished
		if !req.Ok {
			call.Status = store.AICallFailed
		}
		call.InputTokens, call.OutputTokens = req.InputTokens, req.OutputTokens
		call.CacheReadTokens, call.CacheWriteTokens = req.CacheReadTokens, req.CacheWriteTokens
		call.ErrorCode, call.CostUSD, call.FinishedAt = req.ErrorCode, cost, e.now()
		if err := tx.UpdateAICall(call); err != nil {
			return err
		}
		response = &v1.FinishAICallResponse{CostUsd: cost}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return response, nil
}
