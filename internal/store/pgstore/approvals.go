package pgstore

import (
	"time"

	"github.com/Aly700/capstan/internal/store"
)

const approvalColumns = `run_id,approval_id,seq,requested_event_id,source,gate_decision_id,status,due_at,check_at,gate_polls,requested_at,resolved_at,resolver,choice,note`

var approvalInsert = insertSQL("approval", approvalColumns) + " on conflict (run_id,approval_id) do nothing"
var approvalUpdate = updateSQL("approval", approvalColumns, 2)

func approvalArgs(a *store.Approval) []any {
	return []any{a.RunID, a.ApprovalID, a.Seq, a.RequestedEventID, a.Source, a.GateDecisionID, a.Status, nullTime(a.DueAt), nullTime(a.CheckAt), a.GatePolls, nullTime(a.RequestedAt), nullTime(a.ResolvedAt), a.Resolver, a.Choice, a.Note}
}
func scanApproval(row scanner) (*store.Approval, error) {
	a := new(store.Approval)
	if err := row.Scan(&a.RunID, &a.ApprovalID, &a.Seq, &a.RequestedEventID, &a.Source, &a.GateDecisionID, &a.Status, utcTime{&a.DueAt}, utcTime{&a.CheckAt}, &a.GatePolls, utcTime{&a.RequestedAt}, utcTime{&a.ResolvedAt}, &a.Resolver, &a.Choice, &a.Note); err != nil {
		return nil, dbError(err)
	}
	return a, nil
}

func (t *transaction) InsertApproval(a *store.Approval) error {
	return t.insertUnique(approvalInsert, approvalArgs(a)...)
}
func (t *transaction) GetApproval(runID, id string, forUpdate bool) (*store.Approval, error) {
	return scanApproval(t.tx.QueryRow(t.ctx, "select "+approvalColumns+" from approval where run_id=$1 and approval_id=$2"+lockClause(forUpdate), runID, id))
}
func (t *transaction) UpdateApproval(a *store.Approval) error {
	return t.update(approvalUpdate, approvalArgs(a)...)
}
func (t *transaction) RunApprovals(runID string) ([]*store.Approval, error) {
	rows, err := t.tx.Query(t.ctx, "select "+approvalColumns+" from approval where run_id=$1 order by approval_id", runID)
	return collect(rows, err, scanApproval)
}
func (t *transaction) DueApprovals(now time.Time, limit int) ([]*store.Approval, error) {
	rows, err := t.tx.Query(t.ctx, "select "+approvalColumns+` from approval where status=1 and check_at<=$1 order by check_at,run_id,approval_id limit $2 for update skip locked`, nullTime(now), max(limit, 0))
	return collect(rows, err, scanApproval)
}
