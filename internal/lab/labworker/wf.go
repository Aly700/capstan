package labworker

import (
	"crypto/rand"
	"errors"
	"fmt"
	"google.golang.org/protobuf/types/known/durationpb"
	"time"
	"unicode/utf16"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
)

// Workflow is re-entered from its first line for every polled task. All work that
// can block must use Context; scenario code must not create goroutines or do I/O.
type Workflow func(wf *Context, input any) (any, error)

// Branch is one ordered child of All or Race.
type Branch func(wf *Context) (any, error)

type ActivityOptions struct {
	TaskQueue                                                                             string
	StartToCloseTimeout, ScheduleToCloseTimeout, ScheduleToStartTimeout, HeartbeatTimeout time.Duration
	Retry                                                                                 *v1.RetryPolicy
}

type ApprovalRequest struct {
	ApprovalID, GateDecisionID, Tool, Prompt string
	Source                                   v1.ApprovalSource
	Arguments                                any
	Options                                  []string
	Timeout                                  time.Duration
}

// Failure exposes recorded activity failures to scenario code.
type Failure struct {
	Type, Message, TimeoutType, ActivityType string
	Seq                                      int64
	Details                                  *v1.Payload
	Cause                                    *v1.Failure
}

func (f *Failure) Error() string { return f.Message }

// Context is a replay-local coroutine context. It is valid only inside its
// workflow or branch. Handlers are synchronous; use All/Race for waiting branches.
type Context struct {
	r *runtime
	f *fiber
}

// Activity schedules one durable activity and waits for its recorded result.
// Omitting options uses the corpus's ten-second start-to-close timeout.
func (wf *Context) Activity(name string, input any, options ...ActivityOptions) (any, error) {
	wf.checkActive()

	if wf.r.cancelled {
		return nil, cancelledFailure()
	}
	opts := ActivityOptions{StartToCloseTimeout: 10 * time.Second}
	if len(options) > 0 {
		opts = options[0]
	}
	if name == "" || opts.StartToCloseTimeout <= 0 {
		return nil, errors.New("activity requires a name and positive start-to-close timeout")
	}
	payload, err := encode(input)
	if err != nil {
		return nil, err
	}
	wf.r.seq++
	a := &v1.ScheduleActivityCommand{Seq: wf.r.seq, ActivityType: name, TaskQueue: opts.TaskQueue, Input: payload, StartToCloseTimeout: durationpb.New(opts.StartToCloseTimeout), RetryPolicy: opts.Retry}
	if opts.ScheduleToCloseTimeout != 0 {
		a.ScheduleToCloseTimeout = durationpb.New(opts.ScheduleToCloseTimeout)
	}
	if opts.ScheduleToStartTimeout != 0 {
		a.ScheduleToStartTimeout = durationpb.New(opts.ScheduleToStartTimeout)
	}
	if opts.HeartbeatTimeout != 0 {
		a.HeartbeatTimeout = durationpb.New(opts.HeartbeatTimeout)
	}
	wf.emit(&v1.Command{Attributes: &v1.Command_ScheduleActivity{ScheduleActivity: a}})
	return wf.await(wf.r.newPending("activity", name, a.Seq))
}
func (wf *Context) Sleep(duration time.Duration) error {
	wf.checkActive()

	if wf.r.cancelled {
		return cancelledFailure()
	}
	if duration <= 0 {
		return errors.New("sleep duration must be positive")
	}
	timer := wf.startTimer(duration)
	_, err := wf.await(timer)
	return err
}
func (wf *Context) startTimer(duration time.Duration) *future {
	wf.r.seq++
	wf.emit(&v1.Command{Attributes: &v1.Command_StartTimer{StartTimer: &v1.StartTimerCommand{Seq: wf.r.seq, FireAfter: durationpb.New(duration)}}})
	return wf.r.newPending("timer", "", wf.r.seq)
}
func (wf *Context) Signal(name string) (any, error) {
	wf.checkActive()

	if values := wf.r.signals[name]; len(values) > 0 {
		wf.r.signals[name] = values[1:]
		return values[0], nil
	}
	result := &future{r: wf.r}
	wf.r.signalWaiters[name] = append(wf.r.signalWaiters[name], result)
	return wf.await(result)
}
func (wf *Context) Approval(request ApprovalRequest) (any, error) {
	wf.checkActive()

	wf.r.seq++
	source := request.Source
	if source == v1.ApprovalSource_APPROVAL_SOURCE_UNSPECIFIED {
		source = v1.ApprovalSource_APPROVAL_SOURCE_HUMAN
	}
	a := &v1.RequestApprovalCommand{Seq: wf.r.seq, ApprovalId: request.ApprovalID, Source: source, GateDecisionId: request.GateDecisionID, Tool: request.Tool, Prompt: request.Prompt, Options: request.Options}
	if request.Arguments != nil {
		payload, err := encode(request.Arguments)
		if err != nil {
			return nil, err
		}
		a.Arguments = payload
	}
	if request.Timeout != 0 {
		a.Timeout = durationpb.New(request.Timeout)
	}
	wf.emit(&v1.Command{Attributes: &v1.Command_RequestApproval{RequestApproval: a}})
	return wf.await(wf.r.newPending("approval", "", a.Seq))
}

// All starts branches in argument order. Results retain argument order even when
// history resolves them in another order. Only this API and Race spawn branches.
func (wf *Context) All(branches ...Branch) ([]any, error) {
	wf.checkActive()

	result := &future{r: wf.r}
	values := make([]any, len(branches))
	remaining := len(branches)
	if remaining == 0 {
		return values, nil
	}
	for i, branch := range branches {
		child := wf.r.spawn(branch)
		child.onResolve = func(value any, err error) {
			if err != nil {
				result.resolve(nil, err)
				return
			}
			values[i] = value
			remaining--
			if remaining == 0 {
				result.resolve(values, nil)
			}
		}
	}
	value, err := wf.await(result)
	if err != nil {
		return nil, err
	}
	return value.([]any), nil
}

// Race uses the first branch completed by the ordered replay scheduler. It does
// not model V8 continuation depth; fixtures restricted to TS are excluded by D20.
func (wf *Context) Race(branches ...Branch) (any, error) {
	wf.checkActive()

	result := &future{r: wf.r}
	for _, branch := range branches {
		child := wf.r.spawn(branch)
		child.onResolve = func(value any, err error) { result.resolve(value, err) }
	}
	return wf.await(result)
}
func (wf *Context) Now() float64 {
	wf.checkActive()
	return wf.r.now
}
func (wf *Context) Random() float64 {
	wf.checkActive()

	seed := fmt.Sprintf("%s/%d/%d", wf.r.runID, wf.r.seq, wf.r.randomIndex)
	wf.r.randomIndex++
	hash := uint32(2166136261)
	// JavaScript hashes UTF-16 code units, including surrogate pairs.
	for _, c := range utf16.Encode([]rune(seed)) {
		hash = (hash ^ uint32(c)) * 16777619
	}
	return float64(hash) / 4294967296
}
func (wf *Context) SideEffect(fn func() any) any {
	wf.checkActive()
	return wf.marker("side_effect", "", fn)
}
func (wf *Context) UUID() string {
	wf.checkActive()

	return wf.marker("uuid", "", func() any {
		var b [16]byte
		if _, err := rand.Read(b[:]); err != nil {
			panic(err)
		}
		b[6] = (b[6] & 15) | 64
		b[8] = (b[8] & 63) | 128
		return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
	}).(string)
}
func (wf *Context) marker(name, id string, fn func() any) any {
	if wf.r.fatal != nil {
		panic(aborted{})
	}
	if wf.r.inMarker {
		wf.abort(errors.New("nested workflow calls inside sideEffect"))
	}
	wf.r.seq++
	m := &v1.RecordMarkerCommand{Seq: wf.r.seq, Name: name, MarkerId: id}
	command := &v1.Command{Attributes: &v1.Command_RecordMarker{RecordMarker: m}}
	var value any
	if a := wf.r.activation; a.completed != nil {
		var recorded []*v1.HistoryEvent
		if len(wf.r.commands) < len(a.recorded) {
			recorded = a.recorded[len(wf.r.commands) : len(wf.r.commands)+1]
		}
		if err := matchCommands([]*v1.Command{command}, recorded, a.completed.EventId); err != nil {
			wf.abort(err)
		}
		var err error
		value, err = decode(recorded[0].GetMarkerRecorded().GetDetails())
		if err != nil {
			wf.abort(err)
		}
	} else {
		func() {
			wf.r.inMarker = true
			defer func() {
				wf.r.inMarker = false
				if p := recover(); p != nil {
					if wf.r.fatal == nil {
						wf.r.fatal = panicError(p)
					}
				}
			}()
			value = fn()
		}()
		if wf.r.fatal != nil {
			panic(aborted{})
		}
	}
	payload, err := encode(value)
	if err != nil {
		wf.abort(err)
	}
	m.Details = payload
	wf.emit(command)
	// Return a fresh decoded value: the marker's value cannot retain aliases to
	// caller-owned data that changes after this command has been emitted.
	value, err = decode(payload)
	if err != nil {
		wf.abort(err)
	}
	return value
}
func (wf *Context) Patched(id string) bool {
	wf.checkActive()
	return wf.patch(id, false)
}
func (wf *Context) DeprecatePatch(id string) {
	wf.checkActive()
	wf.patch(id, true)
}
func (wf *Context) patch(id string, deprecated bool) bool {
	if value, ok := wf.r.patches[id]; ok {
		return value
	}
	name := "patch"
	if deprecated {
		name = "deprecated_patch"
	}
	if a := wf.r.activation; a.completed != nil {
		var marker *v1.MarkerRecordedAttributes
		if len(wf.r.commands) < len(a.recorded) {
			marker = a.recorded[len(wf.r.commands)].GetMarkerRecorded()
		}
		if marker == nil || (marker.Name != "patch" && marker.Name != "deprecated_patch") || marker.MarkerId != id {
			wf.r.patches[id] = false
			return false
		}
		name = marker.Name
	}
	wf.marker(name, id, func() any { return true })
	wf.r.patches[id] = true
	return true
}

// SetHandler consumes buffered and future signals in history order. The handler
// must return without waiting; blocking workflow calls fail the task.
func (wf *Context) SetHandler(name string, handler func(any)) {
	wf.checkActive()

	wf.r.handlers[name] = handler
	values := wf.r.signals[name]
	delete(wf.r.signals, name)
	for _, value := range values {
		wf.r.callHandler(handler, value)
	}
}
func (wf *Context) Condition(fn func() bool, timeout ...time.Duration) (bool, error) {
	wf.checkActive()

	if fn() {
		return true, nil
	}
	if len(timeout) > 0 && timeout[0] == 0 {
		return false, nil
	}
	result := &future{r: wf.r}
	c := &condition{predicate: fn, result: result}
	if len(timeout) > 0 {
		if timeout[0] < 0 {
			return false, errors.New("condition timeout must not be negative")
		}
		if wf.r.cancelled {
			return false, cancelledFailure()
		}
		c.timer = wf.startTimer(timeout[0])
		c.timer.onResolve = func(_ any, err error) { result.resolve(false, err) }
	}
	wf.r.conditions = append(wf.r.conditions, c)
	value, err := wf.await(result)
	if err != nil {
		return false, err
	}
	return value.(bool), nil
}
func (wf *Context) IsCancellationRequested() bool {
	wf.checkActive()
	return wf.r.cancelled
}
func (wf *Context) ContinueAsNew(input any) {
	wf.checkActive()

	payload, err := encode(input)
	if err != nil {
		wf.abort(err)
	}
	wf.emit(&v1.Command{Attributes: &v1.Command_ContinueAsNew{ContinueAsNew: &v1.ContinueAsNewCommand{Input: payload}}})
	wf.r.closed = true
	panic(continued{})
}
func cancelledFailure() *Failure {
	return &Failure{Type: "CancelledFailure", Message: "run cancellation requested"}
}

// CancelActivity requests cancellation without settling the activity or allocating
// another sequence. A result that wins the cancellation race is still delivered.
func (wf *Context) CancelActivity(seq int64) error {
	wf.checkActive()

	pending := wf.r.pending[seq]
	if pending == nil || pending.kind != "activity" || pending.settled {
		return fmt.Errorf("cannot cancel unknown or settled activity seq %d", seq)
	}
	wf.emit(&v1.Command{Attributes: &v1.Command_RequestActivityCancel{RequestActivityCancel: &v1.RequestActivityCancelCommand{Seq: seq}}})
	return nil
}

// Disposing a blocked stack runs Go defers. A defer may use the workflow API,
// but it must not emit commands or execute side effects after Replay has returned
// its batch. Stop before touching replay state, including during branch cleanup.
func (wf *Context) checkActive() {
	select {
	case <-wf.r.stop:
		panic(stopped{})
	default:
	}
}
