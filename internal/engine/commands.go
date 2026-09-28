package engine

import (
	"fmt"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"math"
	"strconv"
	"strings"
)

func allocatedSeq(ev *v1.HistoryEvent) int64 {
	switch ev.Type {
	case v1.EventType_EVENT_TYPE_ACTIVITY_SCHEDULED:
		return ev.GetActivityScheduled().GetSeq()
	case v1.EventType_EVENT_TYPE_TIMER_STARTED:
		return ev.GetTimerStarted().GetSeq()
	case v1.EventType_EVENT_TYPE_MARKER_RECORDED:
		return ev.GetMarkerRecorded().GetSeq()
	case v1.EventType_EVENT_TYPE_APPROVAL_REQUESTED:
		return ev.GetApprovalRequested().GetSeq()
	}
	return 0
}
func validRetry(p *v1.RetryPolicy) bool {
	if p == nil {
		return true
	}
	coef := p.BackoffCoefficient
	return validDuration(p.InitialInterval) && validDuration(p.MaximumInterval) && p.MaximumAttempts >= 0 && !math.IsNaN(coef) && !math.IsInf(coef, 0) && (coef == 0 || coef >= 1)
}
func (e *Engine) validateCommands(tx store.Tx, r *store.Run, cmds []*v1.Command) error {
	h, err := tx.ReadHistory(r.RunID, 0, 0)
	if err != nil {
		return err
	}
	maxSeq := int64(0)
	activities := map[int64]bool{}
	timers := map[int64]bool{}
	approvals := map[string]bool{}
	for _, ev := range h {
		maxSeq = max(maxSeq, allocatedSeq(ev))
		if a := ev.GetActivityScheduled(); a != nil {
			activities[a.Seq] = true
		}
		if a := ev.GetTimerStarted(); a != nil {
			timers[a.Seq] = true
		}
		if a := ev.GetApprovalRequested(); a != nil {
			approvals[a.ApprovalId] = true
		}
		switch ev.Type {
		case v1.EventType_EVENT_TYPE_TIMER_FIRED:
			delete(timers, ev.GetTimerFired().GetSeq())
		case v1.EventType_EVENT_TYPE_TIMER_CANCELLED:
			delete(timers, ev.GetTimerCancelled().GetSeq())
		case v1.EventType_EVENT_TYPE_ACTIVITY_COMPLETED:
			delete(activities, ev.GetActivityCompleted().GetSeq())
		case v1.EventType_EVENT_TYPE_ACTIVITY_FAILED:
			delete(activities, ev.GetActivityFailed().GetSeq())
		case v1.EventType_EVENT_TYPE_ACTIVITY_TIMED_OUT:
			delete(activities, ev.GetActivityTimedOut().GetSeq())
		case v1.EventType_EVENT_TYPE_ACTIVITY_CANCELLED:
			delete(activities, ev.GetActivityCancelled().GetSeq())
		}
	}
	for i, cmd := range cmds {
		seq := int64(0)
		alloc := false
		closing := false
		switch a := cmd.GetAttributes().(type) {
		case *v1.Command_ScheduleActivity:
			c := a.ScheduleActivity
			if c == nil || !validName(c.ActivityType) || (c.TaskQueue != "" && !validName(c.TaskQueue)) || !validDuration(c.StartToCloseTimeout) || c.StartToCloseTimeout.AsDuration() <= 0 || !validDuration(c.ScheduleToStartTimeout) || !validDuration(c.ScheduleToCloseTimeout) || !validDuration(c.HeartbeatTimeout) || !validRetry(c.RetryPolicy) {
				return Invalid("invalid activity command")
			}
			seq = c.Seq
			alloc = true
			activities[seq] = true
		case *v1.Command_StartTimer:
			c := a.StartTimer
			if c == nil || !validDuration(c.FireAfter) || c.FireAfter.AsDuration() <= 0 {
				return Invalid("invalid timer duration")
			}
			seq = c.Seq
			alloc = true
			timers[seq] = true
		case *v1.Command_RecordMarker:
			if a.RecordMarker == nil {
				return Invalid("missing marker")
			}
			seq = a.RecordMarker.Seq
			alloc = true
		case *v1.Command_RequestApproval:
			c := a.RequestApproval
			if c == nil || !validName(c.ApprovalId) || approvals[c.ApprovalId] || !validDuration(c.Timeout) || (c.Source != v1.ApprovalSource_APPROVAL_SOURCE_GATE && c.Source != v1.ApprovalSource_APPROVAL_SOURCE_HUMAN) || (c.Source == v1.ApprovalSource_APPROVAL_SOURCE_GATE && c.GateDecisionId == "") {
				return Invalid("invalid approval command")
			}
			seq = c.Seq
			alloc = true
			approvals[c.ApprovalId] = true
		case *v1.Command_CancelTimer:
			if a.CancelTimer == nil || !timers[a.CancelTimer.Seq] {
				return Invalid("unknown or settled timer cancellation target")
			}
			delete(timers, a.CancelTimer.Seq)
		case *v1.Command_RequestActivityCancel:
			if a.RequestActivityCancel == nil || !activities[a.RequestActivityCancel.Seq] {
				return Invalid("unknown or settled activity cancellation target")
			}
		case *v1.Command_CompleteRun:
			if a.CompleteRun == nil {
				return Invalid("missing completion")
			}
			closing = true
		case *v1.Command_FailRun:
			if a.FailRun == nil {
				return Invalid("missing failure")
			}
			closing = true
		case *v1.Command_CancelRun:
			if a.CancelRun == nil || !r.CancelRequested {
				return Invalid("run cancellation was not requested")
			}
			closing = true
		case *v1.Command_ContinueAsNew:
			c := a.ContinueAsNew
			if c == nil || (c.WorkflowType != "" && !validName(c.WorkflowType)) || (c.TaskQueue != "" && !validName(c.TaskQueue)) {
				return Invalid("invalid continuation")
			}
			closing = true
		default:
			return Invalid("missing or unknown command")
		}
		if alloc {
			if seq <= maxSeq {
				return Invalid("command seq must increase beyond %d", maxSeq)
			}
			maxSeq = seq
		}
		if closing && i != len(cmds)-1 {
			return Invalid("closing command must be last")
		}
	}
	return nil
}

func (e *Engine) applyCommand(tx store.Tx, r *store.Run, completed int64, cmd *v1.Command) error {
	now := e.now()
	switch a := cmd.Attributes.(type) {
	case *v1.Command_ScheduleActivity:
		c := a.ScheduleActivity
		queue := c.TaskQueue
		if queue == "" {
			queue = r.TaskQueue
		}
		spec := &v1.ActivityScheduledAttributes{Seq: c.Seq, ActivityType: c.ActivityType, TaskQueue: queue, Input: c.Input, ScheduleToCloseTimeout: c.ScheduleToCloseTimeout, ScheduleToStartTimeout: c.ScheduleToStartTimeout, StartToCloseTimeout: c.StartToCloseTimeout, HeartbeatTimeout: c.HeartbeatTimeout, RetryPolicy: c.RetryPolicy, TaskCompletedEventId: completed}
		ev := &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_ACTIVITY_SCHEDULED, Attributes: &v1.HistoryEvent_ActivityScheduled{ActivityScheduled: spec}}
		if err := e.appendEvents(tx, r, ev); err != nil {
			return err
		}
		task := &store.Task{Kind: store.TaskActivity, RunID: r.RunID, TaskQueue: queue, ScheduledEventID: ev.EventId, Attempt: 1, VisibleAt: now, ScheduledAt: now, Activity: spec}
		task.CheckAt = activityCheckAt(task, now)
		if err := tx.InsertTask(task); err != nil {
			return err
		}
		tx.Notify(store.TaskActivity, queue)
	case *v1.Command_StartTimer:
		c := a.StartTimer
		ev := &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_TIMER_STARTED, Attributes: &v1.HistoryEvent_TimerStarted{TimerStarted: &v1.TimerStartedAttributes{Seq: c.Seq, FireAfter: c.FireAfter, TaskCompletedEventId: completed}}}
		if err := e.appendEvents(tx, r, ev); err != nil {
			return err
		}
		return tx.InsertTimer(&store.Timer{RunID: r.RunID, Seq: c.Seq, StartedEventID: ev.EventId, DueAt: addDeadline(now, c.FireAfter.AsDuration())})
	case *v1.Command_CancelTimer:
		seq := a.CancelTimer.Seq
		id, err := referencedEvent(tx, r.RunID, seq, v1.EventType_EVENT_TYPE_TIMER_STARTED)
		if err != nil {
			return err
		}
		if err := e.appendEvents(tx, r, &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_TIMER_CANCELLED, Attributes: &v1.HistoryEvent_TimerCancelled{TimerCancelled: &v1.TimerCancelledAttributes{Seq: seq, StartedEventId: id, TaskCompletedEventId: completed}}}); err != nil {
			return err
		}
		_, err = tx.DeleteTimer(r.RunID, seq)
		return err
	case *v1.Command_RequestActivityCancel:
		seq := a.RequestActivityCancel.Seq
		id, err := referencedEvent(tx, r.RunID, seq, v1.EventType_EVENT_TYPE_ACTIVITY_SCHEDULED)
		if err != nil {
			return err
		}
		if err := e.appendEvents(tx, r, &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_ACTIVITY_CANCEL_REQUESTED, Attributes: &v1.HistoryEvent_ActivityCancelRequested{ActivityCancelRequested: &v1.ActivityCancelRequestedAttributes{Seq: seq, ScheduledEventId: id, TaskCompletedEventId: completed}}}); err != nil {
			return err
		}
		tasks, err := tx.RunTasks(r.RunID)
		if err != nil {
			return err
		}
		for _, task := range tasks {
			if task.Kind == store.TaskActivity && task.Activity.GetSeq() == seq {
				if task.LeasedUntil.IsZero() {
					if err := tx.DeleteTask(task.ID); err != nil {
						return err
					}
					return e.deliver(tx, r, &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_ACTIVITY_CANCELLED, Attributes: &v1.HistoryEvent_ActivityCancelled{ActivityCancelled: &v1.ActivityCancelledAttributes{Seq: seq, ScheduledEventId: id}}})
				}
				task.CancelRequested = true
				return tx.UpdateTask(task)
			}
		}
	case *v1.Command_RecordMarker:
		c := a.RecordMarker
		return e.appendEvents(tx, r, &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_MARKER_RECORDED, Attributes: &v1.HistoryEvent_MarkerRecorded{MarkerRecorded: &v1.MarkerRecordedAttributes{Seq: c.Seq, Name: c.Name, MarkerId: c.MarkerId, Details: c.Details, TaskCompletedEventId: completed}}})
	case *v1.Command_RequestApproval:
		c := a.RequestApproval
		ev := &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_APPROVAL_REQUESTED, Attributes: &v1.HistoryEvent_ApprovalRequested{ApprovalRequested: &v1.ApprovalRequestedAttributes{Seq: c.Seq, ApprovalId: c.ApprovalId, Source: c.Source, GateDecisionId: c.GateDecisionId, Tool: c.Tool, Arguments: c.Arguments, Prompt: c.Prompt, Options: c.Options, Timeout: c.Timeout, TaskCompletedEventId: completed}}}
		if err := e.appendEvents(tx, r, ev); err != nil {
			return err
		}
		row := &store.Approval{RunID: r.RunID, ApprovalID: c.ApprovalId, Seq: c.Seq, RequestedEventID: ev.EventId, Source: c.Source, GateDecisionID: c.GateDecisionId, Status: store.ApprovalPending, RequestedAt: now}
		if c.Timeout.AsDuration() > 0 {
			row.DueAt = addDeadline(now, c.Timeout.AsDuration())
			row.CheckAt = row.DueAt
		}
		if c.Source == v1.ApprovalSource_APPROVAL_SOURCE_GATE {
			row.CheckAt = earliestActivityTime(row.CheckAt, addDeadline(now, e.cfg.GatePollInitial))
		}
		return tx.InsertApproval(row)
	case *v1.Command_CompleteRun:
		r.Result = a.CompleteRun.Result
		if err := e.appendEvents(tx, r, &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_RUN_COMPLETED, Attributes: &v1.HistoryEvent_RunCompleted{RunCompleted: &v1.RunCompletedAttributes{Result: r.Result, TaskCompletedEventId: completed}}}); err != nil {
			return err
		}
		return e.closeRun(tx, r, v1.RunStatus_RUN_STATUS_COMPLETED)
	case *v1.Command_FailRun:
		r.Failure = a.FailRun.Failure
		if err := e.appendEvents(tx, r, &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_RUN_FAILED, Attributes: &v1.HistoryEvent_RunFailed{RunFailed: &v1.RunFailedAttributes{Failure: r.Failure, TaskCompletedEventId: completed}}}); err != nil {
			return err
		}
		return e.closeRun(tx, r, v1.RunStatus_RUN_STATUS_FAILED)
	case *v1.Command_CancelRun:
		if err := e.appendEvents(tx, r, &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_RUN_CANCELLED, Attributes: &v1.HistoryEvent_RunCancelled{RunCancelled: &v1.RunCancelledAttributes{Details: a.CancelRun.Details, TaskCompletedEventId: completed}}}); err != nil {
			return err
		}
		return e.closeRun(tx, r, v1.RunStatus_RUN_STATUS_CANCELLED)
	case *v1.Command_ContinueAsNew:
		c := a.ContinueAsNew
		id, err := successorID(r.RunID)
		if err != nil {
			return err
		}
		typ, queue := c.WorkflowType, c.TaskQueue
		if typ == "" {
			typ = r.WorkflowType
		}
		if queue == "" {
			queue = r.TaskQueue
		}
		if err := e.appendEvents(tx, r, &v1.HistoryEvent{Type: v1.EventType_EVENT_TYPE_RUN_CONTINUED_AS_NEW, Attributes: &v1.HistoryEvent_RunContinuedAsNew{RunContinuedAsNew: &v1.RunContinuedAsNewAttributes{NewRunId: id, WorkflowType: typ, TaskQueue: queue, Input: c.Input, TaskCompletedEventId: completed}}}); err != nil {
			return err
		}
		r.ContinuedToRunID = id
		if err := e.closeRun(tx, r, v1.RunStatus_RUN_STATUS_CONTINUED_AS_NEW); err != nil {
			return err
		}
		next := &store.Run{RunID: id, WorkflowType: typ, TaskQueue: queue, Status: v1.RunStatus_RUN_STATUS_RUNNING, Input: c.Input, TaskTimeout: r.TaskTimeout, RunTimeout: r.RunTimeout, StartedAt: now, ContinuedFromRunID: r.RunID, Identity: r.Identity}
		if next.RunTimeout > 0 {
			next.RunDeadline = addDeadline(now, next.RunTimeout)
		}
		if err := tx.InsertRun(next); err != nil {
			return err
		}
		return e.startHistory(tx, next)
	default:
		return Invalid("unknown command")
	}
	return nil
}

func referencedEvent(tx store.Tx, id string, seq int64, typ v1.EventType) (int64, error) {
	h, err := tx.ReadHistory(id, 0, 0)
	if err != nil {
		return 0, err
	}
	for _, ev := range h {
		if ev.Type == typ && allocatedSeq(ev) == seq {
			return ev.EventId, nil
		}
	}
	return 0, Invalid("missing command target %d", seq)
}
func successorID(id string) (string, error) {
	base, suffix, continued := strings.Cut(id, "~")
	n := int64(2)
	if continued {
		k, err := strconv.ParseInt(suffix, 10, 64)
		if err != nil || k < 2 || k == math.MaxInt64 {
			return "", Invalid("invalid continuation suffix")
		}
		n = k + 1
	}
	return fmt.Sprintf("%s~%d", base, n), nil
}
