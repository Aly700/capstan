package pgstore

import (
	"errors"
	"time"

	"github.com/Aly700/capstan/internal/store"
)

const taskFields = `kind,run_id,task_queue,scheduled_event_id,attempt,visible_at,leased_until,worker_id,started_at,scheduled_at,check_at,activity,last_heartbeat_at,heartbeat_details,last_failure,cancel_requested,started_event_id`
const taskColumns = `id,` + taskFields

var taskInsert = insertSQL("task", taskFields) + " returning id"
var taskUpdate = updateSQL("task", taskColumns, 1)

func taskArgs(v *store.Task) ([]any, error) {
	activity, err := marshal(v.Activity)
	if err != nil {
		return nil, err
	}
	heartbeat, err := marshal(v.HeartbeatDetails)
	if err != nil {
		return nil, err
	}
	failure, err := marshal(v.LastFailure)
	if err != nil {
		return nil, err
	}
	// Keep this order aligned with taskColumns and scanTask.
	return []any{
		v.ID,
		v.Kind,
		v.RunID,
		v.TaskQueue,
		v.ScheduledEventID,
		v.Attempt,
		nullTime(v.VisibleAt),
		nullTime(v.LeasedUntil),
		v.WorkerID,
		nullTime(v.StartedAt),
		nullTime(v.ScheduledAt),
		nullTime(v.CheckAt),
		activity,
		nullTime(v.LastHeartbeatAt),
		heartbeat,
		failure,
		v.CancelRequested,
		v.StartedEventID,
	}, nil
}

func scanTask(row scanner) (*store.Task, error) {
	v := new(store.Task)
	var activity, heartbeat, failure []byte
	err := row.Scan(
		&v.ID,
		&v.Kind,
		&v.RunID,
		&v.TaskQueue,
		&v.ScheduledEventID,
		&v.Attempt,
		utcTime{&v.VisibleAt},
		utcTime{&v.LeasedUntil},
		&v.WorkerID,
		utcTime{&v.StartedAt},
		utcTime{&v.ScheduledAt},
		utcTime{&v.CheckAt},
		&activity,
		utcTime{&v.LastHeartbeatAt},
		&heartbeat,
		&failure,
		&v.CancelRequested,
		&v.StartedEventID,
	)
	if err != nil {
		return nil, dbError(err)
	}
	if err := unmarshal(activity, &v.Activity); err != nil {
		return nil, err
	}
	if err := unmarshal(heartbeat, &v.HeartbeatDetails); err != nil {
		return nil, err
	}
	if err := unmarshal(failure, &v.LastFailure); err != nil {
		return nil, err
	}
	return v, nil
}

func (t *transaction) InsertTask(v *store.Task) error {
	args, err := taskArgs(v)
	if err != nil {
		return err
	}
	return dbError(t.tx.QueryRow(t.ctx, taskInsert, args[1:]...).Scan(&v.ID))
}
func (t *transaction) GetTask(id int64, forUpdate bool) (*store.Task, error) {
	return scanTask(t.tx.QueryRow(t.ctx, "select "+taskColumns+" from task where id=$1"+lockClause(forUpdate), id))
}
func (t *transaction) UpdateTask(v *store.Task) error {
	args, err := taskArgs(v)
	if err != nil {
		return err
	}
	return t.update(taskUpdate, args...)
}
func (t *transaction) DeleteTask(id int64) error {
	_, err := t.tx.Exec(t.ctx, `delete from task where id=$1`, id)
	return dbError(err)
}

func (t *transaction) ClaimTask(kind store.TaskKind, queue string, now time.Time, lease time.Duration, workerID string) (*store.Task, error) {
	const claimQuery = "select " + taskColumns + `
		from task
		where kind=$1
			and task_queue=$2
			and visible_at<=$3
			and leased_until is null
			and not (run_id=any($4::text[]))
		order by visible_at,id
		limit 1
		for update skip locked`
	skipped := []string{}
	var v *store.Task
	for {
		var err error
		row := t.tx.QueryRow(t.ctx, claimQuery, kind, queue, nullTime(now), skipped)
		v, err = scanTask(row)
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		// Probe the parent without retaining its lock: store callers may claim
		// different activities of one run concurrently. A savepoint also clears
		// NOWAIT's aborted state so this poll can try the next task when busy.
		if _, err = t.tx.Exec(t.ctx, "savepoint capstan_claim_run"); err != nil {
			return nil, dbError(err)
		}
		_, probeErr := t.tx.Exec(t.ctx, "select 1 from run where run_id=$1 for update nowait", v.RunID)
		if _, err = t.tx.Exec(t.ctx, "rollback to savepoint capstan_claim_run"); err != nil {
			return nil, dbError(err)
		}
		if _, err = t.tx.Exec(t.ctx, "release savepoint capstan_claim_run"); err != nil {
			return nil, dbError(err)
		}
		if probeErr == nil {
			break
		}
		var state interface{ SQLState() string }
		if !errors.As(probeErr, &state) || state.SQLState() != "55P03" {
			return nil, dbError(probeErr)
		}
		// Every task of this busy run would hit the same lock. Skip the run
		// for this transaction so a large fan-out cannot consume the poll deadline.
		skipped = append(skipped, v.RunID)
	}
	v.LeasedUntil = now.Add(lease).UTC()
	v.WorkerID = workerID
	v.StartedAt = now.UTC()
	_, err := t.tx.Exec(t.ctx, `update task set leased_until=$2,worker_id=$3,started_at=$4 where id=$1`, v.ID, nullTime(v.LeasedUntil), v.WorkerID, nullTime(v.StartedAt))
	if err != nil {
		return nil, dbError(err)
	}
	if t.claimedRuns == nil {
		t.claimedRuns = make(map[string]struct{})
	}
	t.claimedRuns[v.RunID] = struct{}{}
	return v, nil
}

func (t *transaction) RunTasks(runID string) ([]*store.Task, error) {
	rows, err := t.tx.Query(t.ctx, "select "+taskColumns+" from task where run_id=$1 order by id", runID)
	return collect(rows, err, scanTask)
}
func (t *transaction) DueTasks(now time.Time, limit int) ([]*store.Task, error) {
	rows, err := t.tx.Query(t.ctx, "select "+qualifiedColumns("t", taskColumns)+` from task t join run r on r.run_id=t.run_id where t.check_at<=$1 order by t.check_at,t.id limit $2 for update of r,t skip locked`, nullTime(now), max(limit, 0))
	return collect(rows, err, scanTask)
}
