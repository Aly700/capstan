package lab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/engine"
	"github.com/Aly700/capstan/internal/lab/labworker"
	"github.com/Aly700/capstan/internal/store"
	"github.com/Aly700/capstan/internal/store/memstore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"time"
)

type FaultKind string

const (
	KillWorkflow    FaultKind = "kill-workflow"
	KillActivity    FaultKind = "kill-activity"
	CrashServer     FaultKind = "crash-server"
	DuplicateTask   FaultKind = "duplicate-task"
	LateAck         FaultKind = "late-ack"
	DatabaseFailure FaultKind = "database-failure"
	EarlyTimer      FaultKind = "early-timer"
	DuplicateTimer  FaultKind = "duplicate-timer"
	SignalInFlight  FaultKind = "signal-in-flight"
)

func AllFaults() []FaultKind {
	return []FaultKind{KillWorkflow, KillActivity, CrashServer, DuplicateTask, LateAck, DatabaseFailure, EarlyTimer, DuplicateTimer, SignalInFlight}
}

type Options struct {
	Workers, MaxSteps int
	Faults            []FaultKind
	Weights           map[FaultKind]int
	NoFaults          bool
	Clock             engine.Clock
	Advance           func(time.Duration)
	Scenario          string
}
type FaultRecord struct {
	Step   int
	Kind   FaultKind
	Detail string
}
type Result struct {
	Seed             int64
	Scenario         string
	Steps            int
	Faults           []FaultRecord
	Trace            []string
	RootRuns         int
	TransactionSteps int
	GateResponses    []string
}

// Run checks a fault-free oracle, then runs the same scenario with seeded faults.
// Tests supply the synctest clock; campaigns use the same scheduler with a logical
// clock. Neither mode sleeps in real time to simulate durable waits.
func Run(ctx context.Context, seed int64, opts Options) (Result, error) {
	if seed < 0 {
		return Result{}, errors.New("seed must be nonnegative")
	}
	if opts.Workers == 0 {
		opts.Workers = 3
	}
	if opts.MaxSteps == 0 {
		opts.MaxSteps = 1000
	}
	if opts.Workers < 1 || opts.MaxSteps < 1 {
		return Result{}, errors.New("workers and max steps must be positive")
	}
	if (opts.Clock == nil) != (opts.Advance == nil) {
		return Result{}, errors.New("clock and advance must be supplied together")
	}
	for _, kind := range opts.Faults {
		if !validFault(kind) {
			return Result{}, fmt.Errorf("unknown fault %q", kind)
		}
	}
	for kind, weight := range opts.Weights {
		if !validFault(kind) || weight < 0 {
			return Result{}, fmt.Errorf("invalid fault weight %q=%d", kind, weight)
		}
	}
	catalog := scenarioCatalog()
	chosen := catalog[int(seed%int64(len(catalog)))]
	if opts.Scenario != "" {
		found := false
		for _, s := range catalog {
			if s.name == opts.Scenario {
				chosen = s
				found = true
				break
			}
		}
		if !found {
			return Result{}, fmt.Errorf("unknown scenario %q", opts.Scenario)
		}
	}
	baselineOpts := opts
	baselineOpts.NoFaults = true
	baseline, baseSnapshot, err := runScenario(ctx, seed, chosen, baselineOpts)
	if err != nil {
		return baseline, fmt.Errorf("seed %d baseline %s: %w", seed, chosen.name, err)
	}
	if opts.NoFaults {
		if err := runProtocolProbes(ctx, seed); err != nil {
			return baseline, fmt.Errorf("seed %d: %w", seed, err)
		}
		return baseline, nil
	}
	result, snapshot, err := runScenario(ctx, seed, chosen, opts)
	if err == nil {
		err = CompareOutcome(baseSnapshot, snapshot)
	}
	if err != nil {
		return result, fmt.Errorf("seed %d scenario %s: %w", seed, chosen.name, err)
	}
	if err := runProtocolProbes(ctx, seed); err != nil {
		return result, fmt.Errorf("seed %d: %w", seed, err)
	}
	return result, nil
}

// The known-defect list is intentionally exact. Add only independently reproduced
// engine/store defects with an entry in docs/evidence/defects.md.
func KnownFailure(seed int64, err error) bool { return false }
func validFault(kind FaultKind) bool {
	for _, value := range AllFaults() {
		if kind == value {
			return true
		}
	}
	return false
}

type logicalClock struct {
	at time.Time
	mu sync.Mutex
}

func (c *logicalClock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.at }
func (c *logicalClock) advance(d time.Duration) { c.mu.Lock(); defer c.mu.Unlock(); c.at = c.at.Add(d) }

type actor struct {
	work   chan func() error
	result chan error
	done   chan struct{}
}

func newActor() *actor {
	a := &actor{work: make(chan func() error), result: make(chan error), done: make(chan struct{})}
	go func() {
		defer close(a.done)
		for fn := range a.work {
			a.result <- fn()
		}
	}()
	return a
}
func (a *actor) step(fn func() error) error { a.work <- fn; return <-a.result }
func (a *actor) close()                     { close(a.work); <-a.done }

type workerState struct {
	identity string
	workflow *v1.PollWorkflowTaskResponse
	commands *labworker.Result
	activity *v1.PollActivityTaskResponse
	effect   *v1.Payload
}

func (w *workerState) kill() { w.workflow = nil; w.commands = nil; w.activity = nil; w.effect = nil }

type harness struct {
	ctx                   context.Context
	opts                  Options
	scenario              scenario
	roots                 []scenarioRoot
	scheduler             *scheduledStore
	gate                  *scriptedGate
	rng                   *rand.Rand
	store                 store.Store
	faults                *FaultStore
	engine                *engine.Engine
	sink                  *EffectSink
	clock                 engine.Clock
	advance               func(time.Duration)
	workers               []*workerState
	actors                []*actor
	snapshot              Snapshot
	result                Result
	selected              FaultKind
	budget                int
	seenFaults            map[FaultKind]bool
	sentSignal, cancelled map[string]bool
	generation            int
	expectedSignals       []signalExpectation
}

func runScenario(ctx context.Context, seed int64, s scenario, opts Options) (result Result, snapshot Snapshot, err error) {
	h := &harness{ctx: ctx, opts: opts, scenario: s, rng: rand.New(rand.NewSource(seed)), store: memstore.New(), sink: NewEffectSink(), result: Result{Seed: seed, Scenario: s.name}, budget: 6}
	defer h.store.Close()
	h.faults = NewFaultStore(h.store)
	h.scheduler = &scheduledStore{Store: h.faults}
	h.sentSignal = make(map[string]bool)
	h.cancelled = make(map[string]bool)
	peer := peerScenario()
	h.roots = []scenarioRoot{{id: "lab", scenario: s}, {id: "peer", scenario: peer}}
	h.result.RootRuns = len(h.roots)
	h.clock, h.advance = opts.Clock, opts.Advance
	if h.clock == nil {
		clock := &logicalClock{at: time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)}
		h.clock, h.advance = clock, clock.advance
	}
	h.gate = &scriptedGate{advance: h.advance}
	if err := h.restartEngine(); err != nil {
		return h.result, Snapshot{}, err
	}
	for i := 0; i < opts.Workers; i++ {
		h.workers = append(h.workers, &workerState{identity: fmt.Sprintf("lab-%d", i)})
	}
	// Each worker and each of the four background loops is a real goroutine. The
	// seeded scheduler grants one step at a time, including the external client.
	for i := 0; i < opts.Workers+5; i++ {
		h.actors = append(h.actors, newActor())
	}
	defer func() {
		for _, a := range h.actors {
			a.close()
		}
	}()
	for _, root := range h.roots {
		input, inputErr := payload(root.scenario.input)
		if inputErr != nil {
			return h.result, Snapshot{}, inputErr
		}
		request := &v1.StartRunRequest{RunId: root.id, WorkflowType: root.scenario.name, TaskQueue: "lab", Input: input, TaskTimeout: durationpb.New(25 * time.Millisecond)}
		if root.scenario.timeout > 0 {
			request.RunTimeout = durationpb.New(root.scenario.timeout)
		}
		if _, startErr := h.engine.StartRun(ctx, "lab", request); startErr != nil {
			return h.result, Snapshot{}, startErr
		}
	}
	h.snapshot, err = Capture(ctx, h.store)
	if err != nil {
		return h.result, h.snapshot, err
	}
	for step := 0; step < opts.MaxSteps; step++ {
		if err := ctx.Err(); err != nil {
			return h.result, h.snapshot, err
		}
		transactionsBefore := h.scheduler.transactions.Load()
		h.result.Steps = step + 1
		h.selected = h.chooseFault()
		index := h.rng.Intn(len(h.actors) + 2)
		var actionErr error
		if index == len(h.actors)+1 {
			actionErr = h.interleavedStep()
		} else if index == len(h.actors) {
			duration := h.clockStep()
			h.result.Trace = append(h.result.Trace, "clock +"+duration.String())
			h.advance(duration)
		} else {
			h.result.Trace = append(h.result.Trace, fmt.Sprintf("actor %d fault %s", index, h.selected))
			actionErr = h.actors[index].step(func() error {
				switch {
				case index < len(h.workers):
					return h.workerStep(h.workers[index])
				case index == len(h.workers):
					return h.timerStep()
				case index == len(h.workers)+1:
					return h.call(func() error { _, err := h.engine.ProcessDueTasks(ctx, 100); return err })
				case index == len(h.workers)+2:
					return h.call(func() error { _, err := h.engine.ProcessDueApprovals(ctx, 100); return err })
				case index == len(h.workers)+3:
					return h.call(func() error { _, err := h.engine.TimeoutRuns(ctx, 100); return err })
				default:
					return h.clientStep()
				}
			})
		}
		if actionErr != nil && !errors.Is(actionErr, ErrInjected) && !errors.Is(actionErr, ErrServerCrash) {
			return h.result, h.snapshot, actionErr
		}
		if h.scheduler.transactions.Load() == transactionsBefore {
			continue
		}
		current := h.snapshot
		if index != len(h.actors)+1 {
			var captureErr error
			current, captureErr = Capture(ctx, h.store)
			if captureErr != nil {
				return h.result, h.snapshot, captureErr
			}
		}
		if err := CheckHistory(h.snapshot, current); err != nil {
			return h.result, current, err
		}
		if err := CheckTimers(current); err != nil {
			return h.result, current, err
		}
		h.snapshot = current
		if finished(current) {
			if err := checkConcurrentScenarios(current, h.roots); err != nil {
				return h.result, current, err
			}
			if err := checkSignals(current, h.expectedSignals); err != nil {
				return h.result, current, err
			}
			if err := CheckEffects(current, h.sink); err != nil {
				return h.result, current, err
			}
			h.result.GateResponses = h.gate.observed()
			if err := checkGateResponses(s.name, h.result.GateResponses); err != nil {
				return h.result, current, err
			}
			return h.result, current, nil
		}
	}
	return h.result, h.snapshot, fmt.Errorf("did not finish in %d steps", opts.MaxSteps)
}
func finished(s Snapshot) bool {
	if len(s.Runs) == 0 {
		return false
	}
	for _, run := range s.Runs {
		if run.Open() {
			return false
		}
	}
	return true
}
func (h *harness) restartEngine() error {
	e, err := engine.New(engine.Deps{Store: h.scheduler, Clock: h.clock, Gate: h.gate}, engine.Config{TaskRetryInitial: time.Millisecond, TaskRetryMax: 4 * time.Millisecond, DefaultRetry: &v1.RetryPolicy{InitialInterval: durationpb.New(time.Millisecond), MaximumInterval: durationpb.New(4 * time.Millisecond), BackoffCoefficient: 2}, GatePollInitial: 2 * time.Millisecond, GatePollMax: 8 * time.Millisecond})
	if err == nil {
		h.engine = e
		h.generation++
	}
	return err
}
func (h *harness) adapter(w *workerState) *labworker.Worker {
	return labworker.NewWorker(h.engine, "lab", w.identity, "lab-v1", h.registry())
}
func (h *harness) chooseFault() FaultKind {
	if h.opts.NoFaults || h.budget == 0 || h.rng.Intn(3) != 0 {
		return ""
	}
	kinds := h.opts.Faults
	if len(kinds) == 0 {
		kinds = AllFaults()
	}
	total := 0
	for _, kind := range kinds {
		if h.seenFaults[kind] {
			continue
		}
		weight := 1
		if value, ok := h.opts.Weights[kind]; ok {
			weight = value
		}
		total += weight
	}
	if total == 0 {
		return ""
	}
	selection := h.rng.Intn(total)
	for _, kind := range kinds {
		if h.seenFaults[kind] {
			continue
		}
		weight := 1
		if value, ok := h.opts.Weights[kind]; ok {
			weight = value
		}
		selection -= weight
		if selection < 0 {
			return kind
		}
	}
	return ""
}
func (h *harness) record(kind FaultKind, detail string) {
	if h.seenFaults == nil {
		h.seenFaults = make(map[FaultKind]bool)
	}
	h.seenFaults[kind] = true
	h.result.Faults = append(h.result.Faults, FaultRecord{Step: h.result.Steps, Kind: kind, Detail: detail})
	h.budget--
	h.selected = ""
}
func (h *harness) call(fn func() error) (err error) {
	fault := h.selected
	injected := fault == CrashServer || fault == DatabaseFailure
	before := h.faults.Hits()
	if injected {
		h.faults.ArmTransaction(1+h.rng.Intn(2), 1+h.rng.Intn(12), fault == CrashServer)
	}
	defer func() {
		if p := recover(); p != nil {
			if e, ok := p.(error); ok && (errors.Is(e, ErrInjected) || errors.Is(e, ErrServerCrash)) {
				err = e
			} else {
				panic(p)
			}
		}
		h.faults.Disarm()
		if injected && h.faults.Hits() > before {
			h.record(fault, fmt.Sprintf("transaction interrupted; engine generation %d", h.generation))
			if fault == CrashServer {
				if restartErr := h.restartEngine(); restartErr != nil {
					err = restartErr
				}
			}
		}
	}()
	return fn()
}

var errWorkerKilled = errors.New("lab worker killed")

func (h *harness) workerStep(w *workerState) error {
	// Randomly interleave the worker's independent workflow and activity pollers.
	if h.rng.Intn(2) == 0 {
		return h.workflowStep(w)
	}
	return h.activityStep(w)
}
func (h *harness) workflowStep(w *workerState) error {
	if w.workflow == nil {
		return h.call(func() error {
			task, found, err := h.adapter(w).Poll(h.ctx)
			if err == nil && found {
				w.workflow = task
			}
			return err
		})
	}
	if w.commands == nil {
		if h.selected == KillWorkflow {
			boundary := h.rng.Intn(3)
			commands, err := labworker.ReplayWithOptions(w.workflow.RunId, w.workflow.History, h.registry()[w.workflow.WorkflowType], labworker.ReplayOptions{BeforeCommand: func(index int, _ *v1.Command) error {
				if index == boundary {
					return errWorkerKilled
				}
				return nil
			}})
			if errors.Is(err, errWorkerKilled) {
				h.record(KillWorkflow, fmt.Sprintf("before command %d on %s", boundary, w.identity))
				w.kill()
				h.advance(30 * time.Millisecond)
				return nil
			}
			if err != nil {
				return fmt.Errorf("replay: %w", err)
			}
			w.commands = &labworker.Result{Commands: commands}
			return nil
		}
		result := h.adapter(w).Execute(w.workflow)
		if result.Failure != nil {
			return fmt.Errorf("worker replay failed: %s", result.Failure.Message)
		}
		w.commands = &result
		return nil
	}
	if h.selected == LateAck {
		h.record(LateAck, "workflow completion after lease deadline")
		h.advance(30 * time.Millisecond)
		err := h.adapter(w).Respond(h.ctx, w.workflow, *w.commands)
		w.workflow = nil
		w.commands = nil
		if !errors.Is(err, engine.ErrStaleTask) {
			return fmt.Errorf("late workflow completion accepted: %v", err)
		}
		return nil
	}
	duplicate := h.selected == DuplicateTask
	task, result := w.workflow, *w.commands
	err := h.call(func() error { return h.adapter(w).Respond(h.ctx, task, result) })
	if err == nil || errors.Is(err, engine.ErrStaleTask) {
		w.workflow = nil
		w.commands = nil
	}
	if duplicate && err == nil {
		replayed := h.adapter(w).Execute(task)
		if !commandsEqual(result.Commands, replayed.Commands) || replayed.Failure != nil {
			return errors.New("duplicate workflow delivery changed commands")
		}
		repeatErr := h.adapter(w).Respond(h.ctx, task, replayed)
		if !errors.Is(repeatErr, engine.ErrStaleTask) {
			return fmt.Errorf("duplicate workflow completion accepted: %v", repeatErr)
		}
		h.record(DuplicateTask, "same workflow task replayed and acknowledged twice")
	}
	if errors.Is(err, engine.ErrStaleTask) {
		return nil
	}
	return err
}
func (h *harness) activityStep(w *workerState) error {
	if w.activity == nil {
		return h.call(func() error {
			task, found, err := h.engine.PollActivityTask(h.ctx, &v1.PollActivityTaskRequest{TaskQueue: "lab", Identity: w.identity})
			if err == nil && found {
				w.activity = task
			}
			return err
		})
	}
	if w.effect == nil {
		if w.activity.WorkflowType == "retry" && w.activity.Attempt == 1 {
			err := h.call(func() error {
				_, err := h.engine.FailActivityTask(h.ctx, &v1.FailActivityTaskRequest{TaskToken: w.activity.TaskToken, Identity: w.identity, Failure: &v1.Failure{Type: "Transient", Message: "scripted first attempt"}})
				return err
			})
			if err == nil || errors.Is(err, engine.ErrStaleTask) {
				w.activity = nil
			}
			if errors.Is(err, engine.ErrStaleTask) {
				return nil
			}
			return err
		}
		var value any
		if err := json.Unmarshal(w.activity.Input.Data, &value); err != nil {
			return err
		}
		value, err := h.sink.Apply(w.activity.IdempotencyKey, value)
		if err != nil {
			return err
		}
		w.effect, err = payload(value)
		if err != nil {
			return err
		}
		if h.selected == KillActivity {
			h.record(KillActivity, "effect committed before activity acknowledgement")
			w.kill()
			h.advance(30 * time.Millisecond)
		}
		return nil
	}
	if h.selected == LateAck {
		h.record(LateAck, "activity completion after lease deadline")
		h.advance(30 * time.Millisecond)
		_, err := h.engine.CompleteActivityTask(h.ctx, &v1.CompleteActivityTaskRequest{TaskToken: w.activity.TaskToken, Result: w.effect, Identity: w.identity})
		w.activity = nil
		w.effect = nil
		if !errors.Is(err, engine.ErrStaleTask) {
			return fmt.Errorf("late activity completion accepted: %v", err)
		}
		return nil
	}
	duplicate := h.selected == DuplicateTask
	request := &v1.CompleteActivityTaskRequest{TaskToken: w.activity.TaskToken, Result: w.effect, Identity: w.identity}
	key := w.activity.IdempotencyKey
	err := h.call(func() error { _, err := h.engine.CompleteActivityTask(h.ctx, request); return err })
	if err == nil || errors.Is(err, engine.ErrStaleTask) {
		w.activity = nil
		w.effect = nil
	}
	if duplicate && err == nil {
		var value any
		if decodeErr := json.Unmarshal(request.Result.Data, &value); decodeErr != nil {
			return decodeErr
		}
		if _, applyErr := h.sink.Apply(key, value); applyErr != nil {
			return applyErr
		}
		_, repeatErr := h.engine.CompleteActivityTask(h.ctx, request)
		if !errors.Is(repeatErr, engine.ErrStaleTask) {
			return fmt.Errorf("duplicate activity completion accepted: %v", repeatErr)
		}
		h.record(DuplicateTask, "same activity effect and acknowledgement delivered twice")
	}
	if errors.Is(err, engine.ErrStaleTask) {
		return nil
	}
	return err
}
func (h *harness) timerStep() error {
	early := h.selected == EarlyTimer
	hasTimer := false
	for _, timers := range h.snapshot.Timers {
		for _, timer := range timers {
			hasTimer = true
			if !timer.DueAt.After(h.clock.Now()) {
				early = false
			}
		}
	}
	early = early && hasTimer
	duplicate := h.selected == DuplicateTimer
	count := 0
	err := h.call(func() error { var err error; count, err = h.engine.FireDueTimers(h.ctx, 100); return err })
	if err != nil {
		return err
	}
	if early {
		if count != 0 {
			return errors.New("early timer sweep fired a future timer")
		}
		h.record(EarlyTimer, "sweeper invoked before every pending deadline")
	}
	if duplicate && count > 0 {
		again, err := h.engine.FireDueTimers(h.ctx, 100)
		if err != nil {
			return err
		}
		if again != 0 {
			return errors.New("duplicate timer sweep fired again")
		}
		h.record(DuplicateTimer, "timer sweeper repeated at the same clock reading")
	}
	return nil
}
func (h *harness) clientStep() error {
	if h.selected == SignalInFlight {
		for _, run := range h.snapshot.Runs {
			// The first successful activation of every catalogue workflow remains
			// open. Later closing activations may legally discard the inbox.
			if run.InFlight && firstActivation(h.snapshot.Histories[run.RunID]) {
				name := "noise"
				if run.WorkflowType == "signal" && !h.sentSignal[run.RunID] {
					name = "go"
				}
				requestID := fmt.Sprintf("inflight-%d", h.result.Steps)
				err := h.call(func() error {
					_, err := h.engine.SignalRun(h.ctx, "lab", &v1.SignalRunRequest{RunId: run.RunID, Name: name, Input: &v1.Payload{ContentType: "application/json", Data: []byte("7")}, RequestId: requestID})
					return err
				})
				if err == nil {
					h.expectedSignals = append(h.expectedSignals, signalExpectation{run.RunID, requestID, name})
					if name == "go" {
						h.sentSignal[run.RunID] = true
					}
					h.record(SignalInFlight, "signal buffered while a workflow task is leased; delivery checked")
				}
				return err
			}
		}
	}
	for _, run := range h.snapshot.Runs {
		if !run.Open() {
			continue
		}
		if run.WorkflowType == "signal" && !h.sentSignal[run.RunID] {
			err := h.call(func() error {
				_, err := h.engine.SignalRun(h.ctx, "lab", &v1.SignalRunRequest{RunId: run.RunID, Name: "go", Input: &v1.Payload{ContentType: "application/json", Data: []byte("7")}, RequestId: "logical-input"})
				return err
			})
			if err == nil {
				h.sentSignal[run.RunID] = true
				h.expectedSignals = append(h.expectedSignals, signalExpectation{run.RunID, "logical-input", "go"})
			}
			return err
		}
		if run.WorkflowType == "cancel" && !h.cancelled[run.RunID] && len(h.snapshot.Timers[run.RunID]) > 0 {
			err := h.call(func() error {
				_, err := h.engine.CancelRun(h.ctx, "lab", &v1.CancelRunRequest{RunId: run.RunID, Reason: "scenario"})
				return err
			})
			if err == nil {
				h.cancelled[run.RunID] = true
			}
			return err
		}
		if run.WorkflowType == "human" {
			for _, approval := range h.snapshot.Approvals[run.RunID] {
				if approval.Status == store.ApprovalPending {
					return h.call(func() error {
						_, err := h.engine.ResolveApproval(h.ctx, "lab", &v1.ResolveApprovalRequest{RunId: run.RunID, ApprovalId: approval.ApprovalID, Outcome: v1.ApprovalOutcome_APPROVAL_OUTCOME_APPROVED, Resolver: "lab-human"})
						return err
					})
				}
			}
		}
	}
	return nil
}
func commandsEqual(a, b []*v1.Command) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !proto.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}
func payload(value any) (*v1.Payload, error) {
	b, err := json.Marshal(value)
	return &v1.Payload{ContentType: "application/json", Data: b}, err
}

// A Gate poll crash may leave a persisted minute-long lease. Once no task can
// act, jump to the next poll instead of spending the step budget on empty ticks.
func (h *harness) clockStep() time.Duration {
	if !strings.HasPrefix(h.scenario.name, "gate") {
		return time.Millisecond
	}
	for _, tasks := range h.snapshot.Tasks {
		if len(tasks) > 0 {
			return time.Millisecond
		}
	}
	var next time.Time
	now := h.clock.Now()
	for _, approvals := range h.snapshot.Approvals {
		for _, a := range approvals {
			if a.Status == store.ApprovalPending && a.CheckAt.After(now) && (next.IsZero() || a.CheckAt.Before(next)) {
				next = a.CheckAt
			}
		}
	}
	if !next.IsZero() {
		return next.Sub(now)
	}
	return time.Millisecond
}
