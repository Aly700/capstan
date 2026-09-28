package pgstore

import (
	"context"
	"fmt"
	"math"
	"time"

	capstanv1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
)

func (t *transaction) AppendEvents(runID string, events []*capstanv1.HistoryEvent) error {
	if len(events) == 0 {
		return nil
	}
	var last int64
	if err := t.tx.QueryRow(t.ctx, `select coalesce((select max(event_id) from event where run_id=$1),0) from run where run_id=$1`, runID).Scan(&last); err != nil {
		return dbError(err)
	}
	batch := new(pgx.Batch)
	for _, e := range events {
		if e == nil || last == math.MaxInt64 || e.EventId != last+1 {
			return store.ErrConflict
		}
		last = e.EventId
		data, err := marshal(e)
		if err != nil {
			return err
		}
		batch.Queue(`insert into event(run_id,event_id,type,at,data) values ($1,$2,$3,$4,$5)`, runID, e.EventId, e.Type, e.GetTime().AsTime(), data)
	}
	// A competing writer may insert after the maximum was read. The primary key
	// rejects that overlap. A savepoint keeps a caught ErrConflict from poisoning
	// the caller's transaction, and prevents a batch from being partially appended.
	sp, err := t.tx.Begin(t.ctx)
	if err != nil {
		return dbError(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = sp.Rollback(ctx)
	}()
	if err := sp.SendBatch(t.ctx, batch).Close(); err != nil {
		return dbError(err)
	}
	return dbError(sp.Commit(t.ctx))
}

func scanEvent(row scanner) (*capstanv1.HistoryEvent, error) {
	var data []byte
	if err := row.Scan(&data); err != nil {
		return nil, dbError(err)
	}
	var e *capstanv1.HistoryEvent
	if err := unmarshal(data, &e); err != nil {
		return nil, err
	}
	return e, nil
}

func (t *transaction) ReadHistory(runID string, after int64, limit int) ([]*capstanv1.HistoryEvent, error) {
	rows, err := t.tx.Query(t.ctx, `select data from event where run_id=$1 and event_id>$2 order by event_id limit $3`, runID, after, readLimit(limit))
	return collect(rows, err, scanEvent)
}

func (t *transaction) PushInbox(runID string, e *capstanv1.HistoryEvent) error {
	if e == nil {
		return fmt.Errorf("pgstore: nil inbox event")
	}
	copy := proto.Clone(e).(*capstanv1.HistoryEvent)
	copy.EventId = 0
	data, err := marshal(copy)
	if err != nil {
		return err
	}
	_, err = t.tx.Exec(t.ctx, `insert into inbox(run_id,data,received_at) values ($1,$2,$3)`, runID, data, e.GetTime().AsTime())
	return dbError(err)
}

func (t *transaction) DrainInbox(runID string) ([]*capstanv1.HistoryEvent, error) {
	rows, err := t.tx.Query(t.ctx, `with drained as (delete from inbox where run_id=$1 returning id,data) select data from drained order by id`, runID)
	return collect(rows, err, scanEvent)
}

func (t *transaction) InboxSize(runID string) (int, error) {
	var n int
	err := t.tx.QueryRow(t.ctx, `select count(*) from inbox where run_id=$1`, runID).Scan(&n)
	return n, dbError(err)
}

func (t *transaction) RecordSignalRequest(runID, requestID string) error {
	return t.insertUnique(`insert into signal_request(run_id,request_id) values ($1,$2) on conflict (run_id,request_id) do nothing`, runID, requestID)
}
