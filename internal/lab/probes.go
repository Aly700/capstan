package lab

import (
	"context"
	"errors"
	"fmt"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/engine"
	"github.com/Aly700/capstan/internal/store"
	"github.com/Aly700/capstan/internal/store/memstore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Protocol probes cover API obligations that a successful workflow result alone
// cannot establish. Each seed selects one bounded sequence on a fresh real engine.
// Logical time keeps these sequences identical in CLI and synctest campaigns.
var protocolProbeNames = []string{"heartbeat-renewal", "retry-deadline", "cancel-before-start", "failed-workflow-release", "approval-once", "signal-order", "retry-attempt", "workflow-token-fields", "closed-inbox", "exclusive-claim"}

func runProtocolProbes(ctx context.Context, seed int64) error {
	checks := []func(*protocolProbe, *v1.PollWorkflowTaskResponse) error{
		probeHeartbeat, probeRetryDeadline, probeCancelBeforeStart, probeFailedWorkflow,
		probeApprovalOnce, probeSignalOrder, probeRetryAttempt, probeTokenFields,
		probeClosedInbox, probeExclusiveClaim,
	}
	index := int(seed % int64(len(checks)))
	if index < 0 {
		return errors.New("protocol probe seed must be nonnegative")
	}
	p := &protocolProbe{ctx: ctx, store: memstore.New(), clock: &logicalClock{at: time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)}}
	defer p.store.Close()
	var err error
	p.engine, err = engine.New(engine.Deps{Store: p.store, Clock: p.clock}, engine.Config{
		DefaultTaskTimeout: 20 * time.Millisecond, TaskRetryInitial: time.Millisecond, TaskRetryMax: 4 * time.Millisecond,
		DefaultRetry: &v1.RetryPolicy{InitialInterval: durationpb.New(time.Millisecond), MaximumInterval: durationpb.New(4 * time.Millisecond), MaximumAttempts: 3},
	})
	if err == nil {
		_, err = p.engine.StartRun(ctx, "probe", &v1.StartRunRequest{RunId: "probe", WorkflowType: "probe", TaskQueue: "probe", TaskTimeout: durationpb.New(20 * time.Millisecond)})
	}
	var task *v1.PollWorkflowTaskResponse
	if err == nil {
		task, err = p.pollWorkflow()
	}
	if err == nil {
		err = checks[index](p, task)
	}
	if err != nil {
		return fmt.Errorf("protocol %s: %w", protocolProbeNames[index], err)
	}
	return nil
}

type protocolProbe struct {
	ctx    context.Context
	engine *engine.Engine
	store  store.Store
	clock  *logicalClock
}

func (p *protocolProbe) pollWorkflow() (*v1.PollWorkflowTaskResponse, error) {
	task, found, err := p.engine.PollWorkflowTask(p.ctx, &v1.PollWorkflowTaskRequest{TaskQueue: "probe", Identity: "probe-worker"})
	if err == nil && !found {
		err = errors.New("expected workflow task was not delivered")
	}
	return task, err
}
func (p *protocolProbe) pollActivity() (*v1.PollActivityTaskResponse, error) {
	task, found, err := p.engine.PollActivityTask(p.ctx, &v1.PollActivityTaskRequest{TaskQueue: "probe", Identity: "probe-worker"})
	if err == nil && !found {
		err = errors.New("expected activity task was not delivered")
	}
	return task, err
}
func (p *protocolProbe) completeWorkflow(task *v1.PollWorkflowTaskResponse, commands ...*v1.Command) error {
	_, err := p.engine.CompleteWorkflowTask(p.ctx, &v1.CompleteWorkflowTaskRequest{TaskToken: task.TaskToken, Commands: commands, Identity: "probe-worker"})
	return err
}
func (p *protocolProbe) signal(name string) error {
	_, err := p.engine.SignalRun(p.ctx, "probe-client", &v1.SignalRunRequest{RunId: "probe", Name: name, RequestId: name})
	return err
}
func (p *protocolProbe) history() ([]*v1.HistoryEvent, error) {
	response, err := p.engine.GetHistory(p.ctx, &v1.GetHistoryRequest{RunId: "probe"})
	if err != nil {
		return nil, err
	}
	return response.Events, nil
}
func probeActivity(edit func(*v1.ScheduleActivityCommand)) *v1.Command {
	c := &v1.ScheduleActivityCommand{Seq: 1, ActivityType: "probe", StartToCloseTimeout: durationpb.New(20 * time.Millisecond)}
	if edit != nil {
		edit(c)
	}
	return &v1.Command{Attributes: &v1.Command_ScheduleActivity{ScheduleActivity: c}}
}
func probeComplete() *v1.Command {
	return &v1.Command{Attributes: &v1.Command_CompleteRun{CompleteRun: &v1.CompleteRunCommand{}}}
}

func probeHeartbeat(p *protocolProbe, w *v1.PollWorkflowTaskResponse) error {
	if err := p.completeWorkflow(w, probeActivity(func(c *v1.ScheduleActivityCommand) { c.HeartbeatTimeout = durationpb.New(5 * time.Millisecond) })); err != nil {
		return err
	}
	a, err := p.pollActivity()
	if err != nil {
		return err
	}
	p.clock.advance(4 * time.Millisecond)
	if _, err = p.engine.HeartbeatActivityTask(p.ctx, &v1.HeartbeatActivityTaskRequest{TaskToken: a.TaskToken, Details: &v1.Payload{Data: []byte(`"progress"`)}}); err != nil {
		return err
	}
	p.clock.advance(4 * time.Millisecond)
	if n, err := p.engine.ProcessDueTasks(p.ctx, 10); err != nil {
		return err
	} else if n != 0 {
		return fmt.Errorf("renewed heartbeat task expired early: processed %d", n)
	}
	if _, err = p.engine.CompleteActivityTask(p.ctx, &v1.CompleteActivityTaskRequest{TaskToken: a.TaskToken}); err != nil {
		return fmt.Errorf("completion before renewed heartbeat deadline: %w", err)
	}
	return nil
}
func probeRetryDeadline(p *protocolProbe, w *v1.PollWorkflowTaskResponse) error {
	c := probeActivity(func(c *v1.ScheduleActivityCommand) {
		c.ScheduleToCloseTimeout = durationpb.New(5 * time.Millisecond)
		c.RetryPolicy = &v1.RetryPolicy{InitialInterval: durationpb.New(10 * time.Millisecond), MaximumInterval: durationpb.New(10 * time.Millisecond), MaximumAttempts: 3}
	})
	if err := p.completeWorkflow(w, c); err != nil {
		return err
	}
	a, err := p.pollActivity()
	if err != nil {
		return err
	}
	if _, err = p.engine.FailActivityTask(p.ctx, &v1.FailActivityTaskRequest{TaskToken: a.TaskToken, Failure: &v1.Failure{Type: "Transient"}}); err != nil {
		return err
	}
	if err = p.store.InTx(p.ctx, func(tx store.Tx) error {
		tasks, err := tx.RunTasks("probe")
		if err != nil {
			return err
		}
		for _, task := range tasks {
			if task.Kind == store.TaskActivity {
				return fmt.Errorf("activity retry remains queued beyond schedule-to-close: attempt %d", task.Attempt)
			}
		}
		return nil
	}); err != nil {
		return err
	}
	history, err := p.history()
	if err != nil {
		return err
	}
	count := 0
	for _, event := range history {
		if event.GetActivityFailed() != nil {
			count++
		}
	}
	if count != 1 {
		return fmt.Errorf("unretryable deadline must record one terminal failure, got %d", count)
	}
	return nil
}
func probeCancelBeforeStart(p *protocolProbe, w *v1.PollWorkflowTaskResponse) error {
	cancel := &v1.Command{Attributes: &v1.Command_RequestActivityCancel{RequestActivityCancel: &v1.RequestActivityCancelCommand{Seq: 1}}}
	if err := p.completeWorkflow(w, probeActivity(nil), cancel); err != nil {
		return err
	}
	_, found, err := p.engine.PollActivityTask(p.ctx, &v1.PollActivityTaskRequest{TaskQueue: "probe", Identity: "probe-worker"})
	if err != nil {
		return err
	}
	if found {
		return errors.New("cancelled unstarted activity was delivered")
	}
	history, err := p.history()
	if err != nil {
		return err
	}
	count := 0
	for _, event := range history {
		if event.GetActivityCancelled() != nil {
			count++
		}
	}
	if count != 1 {
		return fmt.Errorf("cancel-before-start emitted %d cancellation events", count)
	}
	return nil
}
func probeFailedWorkflow(p *protocolProbe, w *v1.PollWorkflowTaskResponse) error {
	if err := p.signal("buffered"); err != nil {
		return err
	}
	_, err := p.engine.FailWorkflowTask(p.ctx, &v1.FailWorkflowTaskRequest{TaskToken: w.TaskToken, Cause: v1.TaskFailedCause_TASK_FAILED_CAUSE_SDK_ERROR, Failure: &v1.Failure{Type: "SDKFailure"}})
	if err != nil {
		return err
	}
	if err = p.store.InTx(p.ctx, func(tx store.Tx) error {
		run, err := tx.GetRun("probe", false)
		if err != nil {
			return err
		}
		if run.InFlight {
			return errors.New("failed workflow task still owns InFlight")
		}
		n, err := tx.InboxSize("probe")
		if err != nil {
			return err
		}
		if n != 0 {
			return errors.New("failed workflow task did not flush inbox")
		}
		return nil
	}); err != nil {
		return err
	}
	history, err := p.history()
	if err != nil {
		return err
	}
	count := 0
	for _, event := range history {
		if event.GetSignalReceived().GetName() == "buffered" {
			count++
		}
	}
	if count != 1 {
		return fmt.Errorf("failure flush retained %d copies of accepted signal", count)
	}
	p.clock.advance(time.Millisecond)
	next, err := p.pollWorkflow()
	if err != nil {
		return err
	}
	if next.Attempt != 2 {
		return fmt.Errorf("workflow retry attempt = %d, want 2", next.Attempt)
	}
	return nil
}
func probeApprovalOnce(p *protocolProbe, w *v1.PollWorkflowTaskResponse) error {
	request := &v1.Command{Attributes: &v1.Command_RequestApproval{RequestApproval: &v1.RequestApprovalCommand{Seq: 1, ApprovalId: "approval", Source: v1.ApprovalSource_APPROVAL_SOURCE_HUMAN}}}
	if err := p.completeWorkflow(w, request); err != nil {
		return err
	}
	req := &v1.ResolveApprovalRequest{RunId: "probe", ApprovalId: "approval", Outcome: v1.ApprovalOutcome_APPROVAL_OUTCOME_APPROVED}
	if _, err := p.engine.ResolveApproval(p.ctx, "probe-human", req); err != nil {
		return err
	}
	if _, err := p.engine.ResolveApproval(p.ctx, "probe-human", req); !errors.Is(err, engine.ErrFailedPrecondition) {
		return fmt.Errorf("duplicate approval resolution must be rejected: %v", err)
	}
	history, err := p.history()
	if err != nil {
		return err
	}
	count := 0
	for _, event := range history {
		if event.GetApprovalResolved() != nil {
			count++
		}
	}
	if count != 1 {
		return fmt.Errorf("approval emitted %d terminal decisions", count)
	}
	return nil
}
func probeSignalOrder(p *protocolProbe, w *v1.PollWorkflowTaskResponse) error {
	names := []string{"first", "second", "third"}
	for _, name := range names {
		if err := p.signal(name); err != nil {
			return err
		}
	}
	if err := p.signal("second"); err != nil {
		return err
	}
	if err := p.completeWorkflow(w); err != nil {
		return err
	}
	history, err := p.history()
	if err != nil {
		return err
	}
	next := 0
	for _, event := range history {
		if signal := event.GetSignalReceived(); signal != nil {
			if next >= len(names) || signal.Name != names[next] {
				return fmt.Errorf("accepted signal order[%d] = %q", next, signal.Name)
			}
			next++
		}
	}
	if next != len(names) {
		return fmt.Errorf("accepted signal count = %d, want %d", next, len(names))
	}
	return nil
}
func probeRetryAttempt(p *protocolProbe, w *v1.PollWorkflowTaskResponse) error {
	if err := p.completeWorkflow(w, probeActivity(nil)); err != nil {
		return err
	}
	first, err := p.pollActivity()
	if err != nil {
		return err
	}
	if _, err = p.engine.FailActivityTask(p.ctx, &v1.FailActivityTaskRequest{TaskToken: first.TaskToken, Failure: &v1.Failure{Type: "Transient"}}); err != nil {
		return err
	}
	p.clock.advance(time.Millisecond)
	second, err := p.pollActivity()
	if err != nil {
		return err
	}
	if second.Attempt != first.Attempt+1 {
		return fmt.Errorf("activity retry attempt = %d after %d", second.Attempt, first.Attempt)
	}
	if second.IdempotencyKey != first.IdempotencyKey {
		return errors.New("activity retry changed idempotency key")
	}
	if _, err = p.engine.CompleteActivityTask(p.ctx, &v1.CompleteActivityTaskRequest{TaskToken: first.TaskToken}); !errors.Is(err, engine.ErrStaleTask) {
		return fmt.Errorf("previous activity attempt completion accepted: %v", err)
	}
	if _, err = p.engine.CompleteActivityTask(p.ctx, &v1.CompleteActivityTaskRequest{TaskToken: second.TaskToken}); err != nil {
		return err
	}
	return nil
}
func probeTokenFields(p *protocolProbe, w *v1.PollWorkflowTaskResponse) error {
	edits := []struct {
		name string
		edit func(*v1.TaskToken)
	}{
		{"kind", func(t *v1.TaskToken) { t.Kind = v1.TaskKind_TASK_KIND_ACTIVITY }},
		{"run", func(t *v1.TaskToken) { t.RunId = "other" }},
		{"task", func(t *v1.TaskToken) { t.TaskId++ }},
		{"attempt", func(t *v1.TaskToken) { t.Attempt++ }},
		{"scheduled", func(t *v1.TaskToken) { t.ScheduledEventId++ }},
		{"started", func(t *v1.TaskToken) { t.StartedEventId++ }},
		{"seq", func(t *v1.TaskToken) { t.Seq++ }},
	}
	before, err := p.history()
	if err != nil {
		return err
	}
	original, err := engine.DecodeToken(w.TaskToken)
	if err != nil {
		return err
	}
	for _, edit := range edits {
		token := proto.Clone(original).(*v1.TaskToken)
		edit.edit(token)
		_, err = p.engine.CompleteWorkflowTask(p.ctx, &v1.CompleteWorkflowTaskRequest{TaskToken: engine.EncodeToken(token), Commands: []*v1.Command{probeComplete()}})
		if !errors.Is(err, engine.ErrStaleTask) {
			return fmt.Errorf("workflow token field %s accepted: %v", edit.name, err)
		}
	}
	after, err := p.history()
	if err != nil {
		return err
	}
	if len(before) != len(after) {
		return errors.New("stale workflow tokens changed history length")
	}
	for i := range before {
		if !proto.Equal(before[i], after[i]) {
			return errors.New("stale workflow tokens changed history")
		}
	}
	return p.completeWorkflow(w, probeComplete())
}
func probeClosedInbox(p *protocolProbe, w *v1.PollWorkflowTaskResponse) error {
	if err := p.signal("closing"); err != nil {
		return err
	}
	if err := p.completeWorkflow(w, probeComplete()); err != nil {
		return err
	}
	return p.store.InTx(p.ctx, func(tx store.Tx) error {
		run, err := tx.GetRun("probe", false)
		if err != nil {
			return err
		}
		if run.Status != v1.RunStatus_RUN_STATUS_COMPLETED || run.InFlight || run.WorkflowTaskID != 0 {
			return errors.New("closed run retains active workflow projection")
		}
		n, err := tx.InboxSize("probe")
		if err != nil {
			return err
		}
		if n != 0 {
			return fmt.Errorf("closed run retains %d buffered events", n)
		}
		tasks, err := tx.RunTasks("probe")
		if err != nil {
			return err
		}
		if len(tasks) != 0 {
			return errors.New("closed run retains tasks")
		}
		return nil
	})
}
func probeExclusiveClaim(p *protocolProbe, w *v1.PollWorkflowTaskResponse) error {
	_, found, err := p.engine.PollWorkflowTask(p.ctx, &v1.PollWorkflowTaskRequest{TaskQueue: "probe", Identity: "another-worker"})
	if err != nil {
		return err
	}
	if found {
		return errors.New("active workflow lease was claimed twice")
	}
	if err = p.completeWorkflow(w, probeActivity(nil)); err != nil {
		return err
	}
	if _, err = p.pollActivity(); err != nil {
		return err
	}
	_, found, err = p.engine.PollActivityTask(p.ctx, &v1.PollActivityTaskRequest{TaskQueue: "probe", Identity: "another-worker"})
	if err != nil {
		return err
	}
	if found {
		return errors.New("active activity lease was claimed twice")
	}
	return nil
}
