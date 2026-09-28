package pgstore

import (
	"time"

	"github.com/Aly700/capstan/internal/store"
)

const callFields = `run_id,activity_seq,model,status,estimate_usd,cost_usd,input_tokens,output_tokens,cache_read_tokens,cache_write_tokens,error_code,at,finished_at`
const callColumns = `id,` + callFields

var callInsert = insertSQL("ai_call", callFields) + " returning id"
var callUpdate = updateSQL("ai_call", callColumns, 1)

func callArgs(c *store.AICall) []any {
	return []any{c.ID, c.RunID, c.ActivitySeq, c.Model, c.Status, c.EstimateUSD, c.CostUSD, c.InputTokens, c.OutputTokens, c.CacheReadTokens, c.CacheWriteTokens, c.ErrorCode, nullTime(c.At), nullTime(c.FinishedAt)}
}
func scanCall(row scanner) (*store.AICall, error) {
	c := new(store.AICall)
	if err := row.Scan(&c.ID, &c.RunID, &c.ActivitySeq, &c.Model, &c.Status, &c.EstimateUSD, &c.CostUSD, &c.InputTokens, &c.OutputTokens, &c.CacheReadTokens, &c.CacheWriteTokens, &c.ErrorCode, utcTime{&c.At}, utcTime{&c.FinishedAt}); err != nil {
		return nil, dbError(err)
	}
	return c, nil
}

func (t *transaction) LockBudget() error {
	_, err := t.tx.Exec(t.ctx, `select pg_advisory_xact_lock(hashtext('capstan-budget'))`)
	return dbError(err)
}
func (t *transaction) InsertAICall(c *store.AICall) error {
	return dbError(t.tx.QueryRow(t.ctx, callInsert, callArgs(c)[1:]...).Scan(&c.ID))
}
func (t *transaction) GetAICall(id int64, forUpdate bool) (*store.AICall, error) {
	return scanCall(t.tx.QueryRow(t.ctx, "select "+callColumns+" from ai_call where id=$1"+lockClause(forUpdate), id))
}
func (t *transaction) UpdateAICall(c *store.AICall) error {
	return t.update(callUpdate, callArgs(c)...)
}

const costSum = `coalesce(sum(case when status=1 then estimate_usd else cost_usd end),0)`

func (t *transaction) SpentSince(since time.Time) (float64, error) {
	var sum float64
	err := t.tx.QueryRow(t.ctx, "select "+costSum+" from ai_call where at>=$1", since.UTC()).Scan(&sum)
	return sum, dbError(err)
}
func (t *transaction) RunCost(runID string) (float64, error) {
	var sum float64
	err := t.tx.QueryRow(t.ctx, "select "+costSum+" from ai_call where run_id=$1", runID).Scan(&sum)
	return sum, dbError(err)
}
