package pgstore

import (
	"time"

	"github.com/Aly700/capstan/internal/store"
)

const timerColumns = `run_id,seq,started_event_id,due_at`

var timerInsert = insertSQL("timer", timerColumns) + " on conflict (run_id,seq) do nothing"

func scanTimer(row scanner) (*store.Timer, error) {
	v := new(store.Timer)
	if err := row.Scan(&v.RunID, &v.Seq, &v.StartedEventID, utcTime{&v.DueAt}); err != nil {
		return nil, dbError(err)
	}
	return v, nil
}
func (t *transaction) InsertTimer(v *store.Timer) error {
	return t.insertUnique(timerInsert, v.RunID, v.Seq, v.StartedEventID, nullTime(v.DueAt))
}
func (t *transaction) DeleteTimer(runID string, seq int64) (bool, error) {
	tag, err := t.tx.Exec(t.ctx, `delete from timer where run_id=$1 and seq=$2`, runID, seq)
	return tag.RowsAffected() != 0, dbError(err)
}
func (t *transaction) DueTimers(now time.Time, limit int) ([]*store.Timer, error) {
	rows, err := t.tx.Query(t.ctx, "select "+qualifiedColumns("t", timerColumns)+` from timer t join run r on r.run_id=t.run_id where t.due_at<=$1 order by t.due_at,t.run_id,t.seq limit $2 for update of r,t skip locked`, nullTime(now), max(limit, 0))
	return collect(rows, err, scanTimer)
}
func (t *transaction) RunTimers(runID string) ([]*store.Timer, error) {
	rows, err := t.tx.Query(t.ctx, "select "+timerColumns+" from timer where run_id=$1 order by seq", runID)
	return collect(rows, err, scanTimer)
}
