package lab

import (
	"context"
	"fmt"
	"github.com/Aly700/capstan/internal/lab/labworker"
	"math/rand"
	"strings"
	"sync/atomic"

	"github.com/Aly700/capstan/internal/store"
)

// scheduledStore pauses overlapping API calls at InTx boundaries. Memstore holds
// one lock across the callback, so transactions are never split internally.
// Each actor must reach its next boundary before the scheduler draws again;
// operating-system goroutine scheduling cannot change the seeded order.
type scheduledStore struct {
	store.Store
	transactions atomic.Uint64
}
type transactionActorKey struct{}
type transactionRequest struct {
	ctx   context.Context
	fn    func(store.Tx) error
	reply chan error
	done  bool
	err   error
}
type transactionActor struct{ events chan transactionRequest }

func (s *scheduledStore) InTx(ctx context.Context, fn func(store.Tx) error) error {
	actor, ok := ctx.Value(transactionActorKey{}).(*transactionActor)
	if !ok {
		defer s.transactions.Add(1)
		return s.Store.InTx(ctx, fn)
	}
	request := transactionRequest{ctx: ctx, fn: fn, reply: make(chan error)}
	actor.events <- request
	return <-request.reply
}
func (s *scheduledStore) interleave(ctx context.Context, rng *rand.Rand, actions []func(context.Context) error, after func(int) error) error {
	actors := make([]*transactionActor, len(actions))
	pending := make([]transactionRequest, len(actions))
	for i, action := range actions {
		a := &transactionActor{events: make(chan transactionRequest)}
		actors[i] = a
		go func() {
			err := action(context.WithValue(ctx, transactionActorKey{}, a))
			a.events <- transactionRequest{done: true, err: err}
		}()
	}
	for i, a := range actors {
		pending[i] = <-a.events
	}
	var firstErr error
	for {
		ready := make([]int, 0, len(actors))
		for i, p := range pending {
			if !p.done {
				ready = append(ready, i)
			} else if firstErr == nil && p.err != nil {
				firstErr = p.err
			}
		}
		if len(ready) == 0 {
			return firstErr
		}
		i := ready[rng.Intn(len(ready))]
		p := pending[i]
		err := s.Store.InTx(p.ctx, p.fn)
		s.transactions.Add(1)
		if after != nil && firstErr == nil {
			firstErr = after(i)
		}
		p.reply <- err
		pending[i] = <-actors[i].events
	}
}

type scenarioRoot struct {
	id       string
	scenario scenario
}

func (h *harness) registry() map[string]labworker.Workflow {
	result := make(map[string]labworker.Workflow, len(h.roots))
	for _, root := range h.roots {
		result[root.scenario.name] = root.scenario.workflow
	}
	return result
}
func checkConcurrentScenarios(snapshot Snapshot, roots []scenarioRoot) error {
	visited := 0
	for _, root := range roots {
		sub := Snapshot{Tasks: make(map[string][]*store.Task), Timers: make(map[string][]*store.Timer), Approvals: make(map[string][]*store.Approval), InboxSizes: make(map[string]int)}
		for _, run := range snapshot.Runs {
			if run.RunID != root.id && !strings.HasPrefix(run.RunID, root.id+"~") {
				continue
			}
			visited++
			id := "lab" + strings.TrimPrefix(run.RunID, root.id)
			copy := *run
			copy.RunID = id
			if copy.ContinuedToRunID != "" {
				copy.ContinuedToRunID = "lab" + strings.TrimPrefix(copy.ContinuedToRunID, root.id)
			}
			if copy.ContinuedFromRunID != "" {
				copy.ContinuedFromRunID = "lab" + strings.TrimPrefix(copy.ContinuedFromRunID, root.id)
			}
			sub.Runs = append(sub.Runs, &copy)
			sub.Tasks[id] = snapshot.Tasks[run.RunID]
			sub.Timers[id] = snapshot.Timers[run.RunID]
			sub.Approvals[id] = snapshot.Approvals[run.RunID]
			sub.InboxSizes[id] = snapshot.InboxSizes[run.RunID]
		}
		if err := checkScenario(sub, root.scenario.name); err != nil {
			return fmt.Errorf("root %s: %w", root.id, err)
		}
	}
	if visited != len(snapshot.Runs) {
		return fmt.Errorf("unrecognized run among %d concurrent roots", len(roots))
	}
	return nil
}

func (h *harness) interleavedStep() error {
	// The two sweepers overlap as API calls, including each approval call's
	// lease transaction, external response and resolution transaction. The timer
	// and timeout sweepers also perform one transaction per item.
	operations := []func(context.Context) error{
		func(ctx context.Context) error { _, err := h.engine.FireDueTimers(ctx, 2); return err },
		func(ctx context.Context) error { _, err := h.engine.ProcessDueTasks(ctx, 2); return err },
		func(ctx context.Context) error { _, err := h.engine.ProcessDueApprovals(ctx, 2); return err },
		func(ctx context.Context) error { _, err := h.engine.TimeoutRuns(ctx, 2); return err },
	}
	first, second := h.rng.Intn(len(operations)), h.rng.Intn(len(operations))
	h.result.Trace = append(h.result.Trace, fmt.Sprintf("overlap loops %d/%d", first, second))
	return h.scheduler.interleave(h.ctx, h.rng, []func(context.Context) error{operations[first], operations[second]}, func(actor int) error {
		h.result.TransactionSteps++
		h.result.Trace = append(h.result.Trace, fmt.Sprintf("transaction actor %d", actor))
		current, err := Capture(h.ctx, h.store)
		if err != nil {
			return err
		}
		if err = CheckHistory(h.snapshot, current); err != nil {
			return err
		}
		if err = CheckTimers(current); err != nil {
			return err
		}
		h.snapshot = current
		return nil
	})
}
