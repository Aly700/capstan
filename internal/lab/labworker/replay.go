// Package labworker runs Go scenarios against Capstan's recorded history.
package labworker

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
)

// Replay starts a fresh scenario and reconstructs every successful activation.
// Coroutines exist only during this call: none survives a returned command batch,
// a failed replay, or a worker restart. The caller owns history and persists commands.
func Replay(runID string, history []*v1.HistoryEvent, workflow Workflow) ([]*v1.Command, error) {
	if len(history) == 0 || history[0].GetRunStarted() == nil {
		return nil, errors.New("history must begin with RunStarted")
	}
	if workflow == nil {
		return nil, errors.New("workflow is nil")
	}
	acts, err := activations(history)
	if err != nil {
		return nil, err
	}
	input, err := decode(history[0].GetRunStarted().GetInput())
	if err != nil {
		return nil, err
	}
	r := &runtime{runID: runID, stop: make(chan struct{}), yield: make(chan struct{}), pending: map[int64]*future{}, cancelledTimers: map[int64]bool{}, signals: map[string][]any{}, handlers: map[string]func(any){}, signalWaiters: map[string][]*future{}, patches: map[string]bool{}}
	defer r.shutdown()
	var main *future
	for _, a := range acts {
		r.activation = a
		r.commands = nil
		if a.started.Time == nil {
			return nil, fmt.Errorf("TaskStarted %d has no time", a.started.EventId)
		}
		r.now = float64(a.started.Time.Seconds)*1000 + float64(a.started.Time.Nanos)/1e6
		for _, e := range a.external {
			if err := r.deliver(e); err != nil {
				return nil, err
			}
			r.checkConditions()
			if r.fatal != nil {
				return nil, r.fatal
			}
		}
		if main == nil {
			main = r.spawn(func(wf *Context) (any, error) { return workflow(wf, input) })
		}
		r.drain()
		if r.fatal != nil {
			return nil, r.fatal
		}
		if main.settled && !r.closed {
			value, err := main.value, main.err
			if r.handlerFailure != nil {
				err = r.handlerFailure
			}
			switch {
			case err == nil:
				payload, encErr := encode(value)
				if encErr != nil {
					return nil, encErr
				}
				r.emit(&v1.Command{Attributes: &v1.Command_CompleteRun{CompleteRun: &v1.CompleteRunCommand{Result: payload}}})
			case isCancelled(err) && r.cancelled:
				r.emit(&v1.Command{Attributes: &v1.Command_CancelRun{CancelRun: &v1.CancelRunCommand{}}})
			default:
				r.emit(&v1.Command{Attributes: &v1.Command_FailRun{FailRun: &v1.FailRunCommand{Failure: failureProto(err)}}})
			}
			r.closed = true
		} else if r.handlerFailure != nil && !r.closed {
			r.emit(&v1.Command{Attributes: &v1.Command_FailRun{FailRun: &v1.FailRunCommand{Failure: failureProto(r.handlerFailure)}}})
			r.closed = true
		}
		if r.fatal != nil {
			return nil, r.fatal
		}
		if a.completed != nil {
			if err := matchCommands(r.commands, a.recorded, a.completed.EventId); err != nil {
				return nil, err
			}
		} else {
			return r.commands, nil
		}
	}
	return nil, nil
}

type runtime struct {
	runID                                   string
	activation                              activation
	now                                     float64
	seq                                     int64
	randomIndex                             int
	commands                                []*v1.Command
	pending                                 map[int64]*future
	pendingOrder                            []*future
	cancelledTimers                         map[int64]bool
	signals                                 map[string][]any
	handlers                                map[string]func(any)
	signalWaiters                           map[string][]*future
	conditions                              []*condition
	patches                                 map[string]bool
	cancelled, closed, inMarker, inCallback bool
	fatal, handlerFailure                   error
	ready                                   []*fiber
	fibers                                  []*fiber
	stop, yield                             chan struct{}
}

type stopped struct{}
type aborted struct{}
type continued struct{}

type fiber struct {
	r      *runtime
	permit chan struct{}
	stop   chan struct{}
	done   chan struct{}
}
type future struct {
	r                  *runtime
	kind, activityType string
	seq                int64
	settled            bool
	value              any
	err                error
	waiters            []*fiber
	onResolve          func(any, error)
}
type condition struct {
	predicate func() bool
	result    *future
	timer     *future
}

func (r *runtime) spawn(fn Branch) *future {
	f := &fiber{r: r, permit: make(chan struct{}), stop: make(chan struct{}), done: make(chan struct{})}
	result := &future{r: r}
	r.ready = append(r.ready, f)
	r.fibers = append(r.fibers, f)
	go func() {
		defer close(f.done)
		var value any
		var err error
		defer func() {
			if p := recover(); p != nil {
				switch p.(type) {
				case stopped:
					return
				case aborted, continued:
				default:
					err = panicError(p)
				}
			}
			select {
			case <-r.stop:
				return
			default:
			}
			result.resolve(value, err)
			select {
			case r.yield <- struct{}{}:
			case <-r.stop:
			}
		}()
		f.wait()
		value, err = fn(&Context{r: r, f: f})
	}()
	return result
}
func (f *fiber) wait() {
	select {
	case <-f.permit:
	case <-f.stop:
		panic(stopped{})
	}
}

func (r *runtime) shutdown() {
	// First forbid workflow API calls, then unwind one stack at a time. Ordinary
	// Go defers can touch shared workflow locals, so cleanup also needs ordering.
	close(r.stop)
	for _, f := range r.fibers {
		close(f.stop)
		<-f.done
	}
}
func (r *runtime) drain() {
	for {
		r.checkConditions()
		if r.fatal != nil || len(r.ready) == 0 {
			return
		}
		f := r.ready[0]
		r.ready = r.ready[1:]
		f.permit <- struct{}{}
		<-r.yield
	}
}
func (f *future) resolve(value any, err error) {
	if f.settled {
		return
	}
	f.settled = true
	f.value = value
	f.err = err
	f.r.ready = append(f.r.ready, f.waiters...)
	f.waiters = nil
	if f.onResolve != nil {
		f.onResolve(value, err)
	}
}
func (wf *Context) await(f *future) (any, error) {
	wf.checkActive()
	if wf.r.inMarker || wf.r.inCallback {
		wf.abort(errors.New("blocking workflow call inside a synchronous callback"))
	}
	if !f.settled {
		f.waiters = append(f.waiters, wf.f)
		wf.r.yield <- struct{}{}
		wf.f.wait()
	}
	return f.value, f.err
}
func (r *runtime) newPending(kind, name string, seq int64) *future {
	p := &future{r: r, kind: kind, activityType: name, seq: seq}
	r.pending[seq] = p
	r.pendingOrder = append(r.pendingOrder, p)
	return p
}
func (r *runtime) emit(command *v1.Command) {
	if r.fatal != nil {
		return
	}
	if r.closed {
		r.fatal = errors.New("workflow emitted a command after closing")
		return
	}
	if r.inMarker {
		r.fatal = errors.New("sideEffect callbacks cannot emit workflow commands")
		return
	}
	position := len(r.commands)
	r.commands = append(r.commands, command)
	if r.activation.completed != nil {
		var expected []*v1.HistoryEvent
		if position < len(r.activation.recorded) {
			expected = r.activation.recorded[position : position+1]
		}
		r.fatal = matchCommands([]*v1.Command{command}, expected, r.activation.completed.EventId)
	}
}
func (wf *Context) emit(command *v1.Command) {
	wf.r.emit(command)
	if wf.r.fatal != nil {
		panic(aborted{})
	}
}
func (wf *Context) abort(err error) {
	if wf.r.fatal == nil {
		wf.r.fatal = err
	}
	panic(aborted{})
}
func (r *runtime) callHandler(handler func(any), value any) {
	defer func() {
		if p := recover(); p != nil {
			r.handlerFailure = panicError(p)
		}
	}()
	previous := r.inCallback
	r.inCallback = true
	defer func() { r.inCallback = previous }()
	handler(value)
}
func (r *runtime) checkConditions() {
	// Conditions are ordered by registration; maps are only used for lookup.
	for _, c := range r.conditions {
		if c.result.settled {
			continue
		}
		ready, err := predicate(c.predicate)
		if err != nil {
			c.result.resolve(nil, err)
			continue
		}
		if !ready {
			continue
		}
		if c.timer != nil && !c.timer.settled {
			delete(r.pending, c.timer.seq)
			r.cancelledTimers[c.timer.seq] = true
			c.timer.onResolve = nil
			c.timer.resolve(nil, nil)
			r.emit(&v1.Command{Attributes: &v1.Command_CancelTimer{CancelTimer: &v1.CancelTimerCommand{Seq: c.timer.seq}}})
		}
		c.result.resolve(true, nil)
	}
}
func predicate(fn func() bool) (value bool, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = panicError(p)
		}
	}()
	return fn(), nil
}
func (r *runtime) deliver(e *v1.HistoryEvent) error {
	if s := e.GetSignalReceived(); s != nil {
		value, err := decode(s.Input)
		if err != nil {
			return err
		}
		if handler := r.handlers[s.Name]; handler != nil {
			r.callHandler(handler, value)
		} else if waiters := r.signalWaiters[s.Name]; len(waiters) > 0 {
			r.signalWaiters[s.Name] = waiters[1:]
			waiters[0].resolve(value, nil)
		} else {
			r.signals[s.Name] = append(r.signals[s.Name], value)
		}
		return nil
	}
	if cancel := e.GetRunCancelRequested(); cancel != nil {
		r.cancelled = true
		reason := cancel.Reason
		if reason == "" {
			reason = "run cancellation requested"
		}
		for _, p := range r.pendingOrder {
			if !p.settled && p.kind != "approval" {
				delete(r.pending, p.seq)
				p.resolve(nil, &Failure{Type: "CancelledFailure", Message: reason})
			}
		}
		return nil
	}
	var seq int64
	kind := "activity"
	switch a := e.Attributes.(type) {
	case *v1.HistoryEvent_ActivityCompleted:
		seq = a.ActivityCompleted.Seq
	case *v1.HistoryEvent_ActivityFailed:
		seq = a.ActivityFailed.Seq
	case *v1.HistoryEvent_ActivityTimedOut:
		seq = a.ActivityTimedOut.Seq
	case *v1.HistoryEvent_ActivityCancelled:
		seq = a.ActivityCancelled.Seq
	case *v1.HistoryEvent_TimerFired:
		seq = a.TimerFired.Seq
		kind = "timer"
		if r.cancelledTimers[seq] {
			return nil
		}
	case *v1.HistoryEvent_ApprovalResolved:
		seq = a.ApprovalResolved.Seq
		kind = "approval"
	default:
		return fmt.Errorf("unexpected external event %d", e.EventId)
	}
	p := r.pending[seq]
	if p == nil || p.settled || p.kind != kind {
		return fmt.Errorf("event %d refers to unknown or already-settled %s seq %d", e.EventId, kind, seq)
	}
	delete(r.pending, seq)
	var value any
	var err error
	switch a := e.Attributes.(type) {
	case *v1.HistoryEvent_ActivityCompleted:
		value, err = decode(a.ActivityCompleted.Result)
		if err != nil {
			return err
		}
	case *v1.HistoryEvent_ActivityFailed:
		err = &Failure{Type: "ActivityFailure", Message: fmt.Sprintf("activity %s failed", p.activityType), ActivityType: p.activityType, Seq: seq, Cause: a.ActivityFailed.Failure}
	case *v1.HistoryEvent_ActivityTimedOut:
		err = &Failure{Type: "TimeoutFailure", Message: fmt.Sprintf("activity %s timed out", p.activityType), TimeoutType: strings.TrimPrefix(a.ActivityTimedOut.TimeoutType.String(), "TIMEOUT_TYPE_"), Cause: a.ActivityTimedOut.LastFailure}
	case *v1.HistoryEvent_ActivityCancelled:
		err = &Failure{Type: "CancelledFailure", Message: fmt.Sprintf("activity %s cancelled", p.activityType), Details: a.ActivityCancelled.Details}
	case *v1.HistoryEvent_ApprovalResolved:
		resolution := a.ApprovalResolved
		outcome := "expired"
		if resolution.Outcome == v1.ApprovalOutcome_APPROVAL_OUTCOME_APPROVED {
			outcome = "approved"
		} else if resolution.Outcome == v1.ApprovalOutcome_APPROVAL_OUTCOME_DENIED {
			outcome = "denied"
		}
		value = map[string]any{"outcome": outcome, "choice": resolution.Choice, "resolver": resolution.Resolver, "note": resolution.Note}
	}
	p.resolve(value, err)
	return nil
}
func encode(value any) (payload *v1.Payload, err error) {
	defer func() {
		if p := recover(); p != nil {
			payload, err = nil, fmt.Errorf("encode payload: %w", panicError(p))
		}
	}()
	b, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return &v1.Payload{ContentType: "application/json", Data: b}, nil
}
func decode(p *v1.Payload) (any, error) {
	if p == nil {
		return nil, nil
	}
	if p.ContentType != "application/json" {
		return nil, fmt.Errorf("unsupported payload content type %q", p.ContentType)
	}
	var value any
	err := json.Unmarshal(p.Data, &value)
	return value, err
}
func panicError(p any) error {
	if err, ok := p.(error); ok {
		return err
	}
	return fmt.Errorf("%v", p)
}
func isCancelled(err error) bool {
	var f *Failure
	return errors.As(err, &f) && f.Type == "CancelledFailure"
}
func failureProto(err error) *v1.Failure {
	result := &v1.Failure{Type: "Error", Message: err.Error()}
	var f *Failure
	if errors.As(err, &f) {
		result.Type = f.Type
		result.Details = f.Details
		result.Cause = f.Cause
		if f.Type == "ActivityFailure" || f.Type == "TimeoutFailure" {
			metadata := map[string]any{"kind": f.Type}
			if f.Type == "ActivityFailure" {
				metadata["activityType"] = f.ActivityType
				metadata["seq"] = f.Seq
			} else {
				metadata["timeoutType"] = f.TimeoutType
			}
			envelope := map[string]any{"$capstan": metadata}
			if f.Details != nil {
				if details, err := decode(f.Details); err == nil {
					envelope["details"] = details
				}
			}
			result.Details, _ = encode(envelope)
		}
	}
	return result
}
