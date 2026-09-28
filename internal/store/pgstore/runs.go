package pgstore

import (
	"time"

	"github.com/Aly700/capstan/internal/store"
)

const runColumns = `run_id,workflow_type,task_queue,status,input,result,failure,task_timeout_ms,run_timeout_ms,run_deadline,started_at,closed_at,last_event_id,workflow_task_id,in_flight,cancel_requested,continued_from_run_id,continued_to_run_id,identity`

var runInsert = insertSQL("run", runColumns) + " on conflict (run_id) do nothing"
var runUpdate = updateSQL("run", runColumns, 1)

func runArgs(r *store.Run) ([]any, error) {
	input, err := marshal(r.Input)
	if err != nil {
		return nil, err
	}
	result, err := marshal(r.Result)
	if err != nil {
		return nil, err
	}
	failure, err := marshal(r.Failure)
	if err != nil {
		return nil, err
	}
	return []any{r.RunID, r.WorkflowType, r.TaskQueue, r.Status, input, result, failure, r.TaskTimeout.Milliseconds(), r.RunTimeout.Milliseconds(), nullTime(r.RunDeadline), nullTime(r.StartedAt), nullTime(r.ClosedAt), r.LastEventID, r.WorkflowTaskID, r.InFlight, r.CancelRequested, r.ContinuedFromRunID, r.ContinuedToRunID, r.Identity}, nil
}

func scanRun(row scanner) (*store.Run, error) {
	r := new(store.Run)
	var input, result, failure []byte
	var taskMS, runMS int64
	err := row.Scan(&r.RunID, &r.WorkflowType, &r.TaskQueue, &r.Status, &input, &result, &failure, &taskMS, &runMS, utcTime{&r.RunDeadline}, utcTime{&r.StartedAt}, utcTime{&r.ClosedAt}, &r.LastEventID, &r.WorkflowTaskID, &r.InFlight, &r.CancelRequested, &r.ContinuedFromRunID, &r.ContinuedToRunID, &r.Identity)
	if err != nil {
		return nil, dbError(err)
	}
	r.TaskTimeout = time.Duration(taskMS) * time.Millisecond
	r.RunTimeout = time.Duration(runMS) * time.Millisecond
	if err := unmarshal(input, &r.Input); err != nil {
		return nil, err
	}
	if err := unmarshal(result, &r.Result); err != nil {
		return nil, err
	}
	if err := unmarshal(failure, &r.Failure); err != nil {
		return nil, err
	}
	return r, nil
}

func (t *transaction) InsertRun(r *store.Run) error {
	args, err := runArgs(r)
	if err != nil {
		return err
	}
	return t.insertUnique(runInsert, args...)
}
func (t *transaction) GetRun(id string, forUpdate bool) (*store.Run, error) {
	clause := lockClause(forUpdate)
	if _, claimed := t.claimedRuns[id]; forUpdate && claimed {
		// Claims hold a task first. Waiting here would invert closeRun's run-then-
		// children order. A busy run aborts this transaction, undoing the claim;
		// the engine reports an empty poll after rollback.
		clause += " nowait"
	}
	return scanRun(t.tx.QueryRow(t.ctx, "select "+runColumns+" from run where run_id=$1"+clause, id))
}
func (t *transaction) UpdateRun(r *store.Run) error {
	args, err := runArgs(r)
	if err != nil {
		return err
	}
	return t.update(runUpdate, args...)
}

func (t *transaction) ListRuns(f store.RunFilter) ([]*store.Run, error) {
	rows, err := t.tx.Query(t.ctx, "select "+runColumns+` from run where ($1::smallint=0 or status=$1) and ($2::text='' or workflow_type=$2) and run_id>$3 order by run_id limit $4`, f.Status, f.WorkflowType, f.AfterRunID, readLimit(f.Limit))
	return collect(rows, err, scanRun)
}

func (t *transaction) RunsPastDeadline(now time.Time, limit int) ([]*store.Run, error) {
	rows, err := t.tx.Query(t.ctx, "select "+runColumns+` from run where status in (1,6) and run_deadline<=$1 order by run_deadline,run_id limit $2 for update skip locked`, nullTime(now), max(limit, 0))
	return collect(rows, err, scanRun)
}
