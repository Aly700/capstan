package lab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"time"

	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/engine"
	"github.com/Aly700/capstan/internal/lab/labworker"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/types/known/durationpb"
)

type postgresResult struct {
	steps, duplicateAcks, databaseErrors, serverCrashes int
}

func runPostgresSeed(ctx context.Context, seed int64, baseline, faulted store.Store) (postgresResult, error) {
	before, baseResult, err := runPostgresVariant(ctx, seed, baseline, false)
	if err != nil {
		return baseResult, fmt.Errorf("baseline: %w", err)
	}
	after, result, err := runPostgresVariant(ctx, seed, faulted, true)
	result.steps += baseResult.steps
	if err != nil {
		return result, fmt.Errorf("faulted: %w", err)
	}
	return result, CompareOutcome(before, after)
}

// postgresScope limits property snapshots to the current seed. Earlier seeds keep
// their append-only histories in the same temporary database until test cleanup.
// Both databases use identical run IDs, allowing the ordinary P2 oracle to apply.
type postgresScope struct {
	store.Store
	ids []string
}

type postgresScopeTx struct {
	store.Tx
	ids []string
}

func (s postgresScope) InTx(ctx context.Context, fn func(store.Tx) error) error {
	return s.Store.InTx(ctx, func(tx store.Tx) error { return fn(postgresScopeTx{Tx: tx, ids: s.ids}) })
}

func (tx postgresScopeTx) ListRuns(store.RunFilter) ([]*store.Run, error) {
	runs := make([]*store.Run, 0, len(tx.ids))
	for _, id := range tx.ids {
		run, err := tx.GetRun(id, false)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, nil
}

// The PostgreSQL driver deliberately has a narrower fault catalogue than Run.
// Real network latency advances SystemClock, so each claimed task is replayed
// and acknowledged in the same actor step, with a 500 ms lease. Transaction
// rollback/crash, retries of acknowledgements and duplicate deliveries remain
// observable without pretending that PostgreSQL runs in a synctest bubble.
func runPostgresVariant(ctx context.Context, seed int64, base store.Store, withFaults bool) (Snapshot, postgresResult, error) {
	ids := []string{fmt.Sprintf("pg-%06d-r0", seed), fmt.Sprintf("pg-%06d-r1", seed)}
	queue := fmt.Sprintf("pg-%06d", seed)
	scoped := postgresScope{Store: base, ids: ids}
	faults := NewFaultStore(scoped)
	sink := NewEffectSink()
	timers := newPostgresTimerChecker()
	rng := rand.New(rand.NewSource(seed))
	result := postgresResult{}
	config := engine.Config{TaskRetryInitial: 2 * time.Millisecond, TaskRetryMax: 8 * time.Millisecond}
	newEngine := func() (*engine.Engine, error) {
		return engine.New(engine.Deps{Store: faults, Clock: engine.SystemClock{}}, config)
	}
	eng, err := newEngine()
	if err != nil {
		return Snapshot{}, result, err
	}
	workflow := func(wf *labworker.Context, input any) (any, error) {
		n := input.(float64)
		effect := func(child *labworker.Context, value float64) (any, error) {
			return child.Activity("effect", value, labworker.ActivityOptions{StartToCloseTimeout: 500 * time.Millisecond})
		}
		first, err := effect(wf, n)
		if err != nil {
			return nil, err
		}
		if err := wf.Sleep(time.Duration(1+seed%3) * time.Millisecond); err != nil {
			return nil, err
		}
		rest, err := wf.All(
			func(child *labworker.Context) (any, error) { return effect(child, n+1) },
			func(child *labworker.Context) (any, error) { return effect(child, n+2) },
		)
		if err != nil {
			return nil, err
		}
		return []any{first, rest[0], rest[1]}, nil
	}
	for i, id := range ids {
		input, err := payload(float64(seed*10 + int64(i)*100000))
		if err != nil {
			return Snapshot{}, result, err
		}
		_, err = eng.StartRun(ctx, "pg-lab", &v1.StartRunRequest{RunId: id, WorkflowType: "pg-pipeline", TaskQueue: queue, Input: input, TaskTimeout: durationpb.New(500 * time.Millisecond)})
		if err != nil {
			return Snapshot{}, result, err
		}
	}
	snapshot, err := Capture(ctx, scoped)
	if err != nil {
		return snapshot, result, err
	}
	injected := false
	for step := 0; step < 1500; step++ {
		if err := ctx.Err(); err != nil {
			return snapshot, result, err
		}
		result.steps++
		identity := fmt.Sprintf("pg-worker-%d", rng.Intn(3))
		switch rng.Intn(6) {
		case 0, 5:
			worker := labworker.NewWorker(eng, queue, identity, "pg-lab-v1", map[string]labworker.Workflow{"pg-pipeline": workflow})
			task, found, pollErr := worker.Poll(ctx)
			if pollErr != nil {
				return snapshot, result, pollErr
			}
			if found {
				replayed := worker.Execute(task)
				if replayed.Failure != nil {
					return snapshot, result, fmt.Errorf("replay: %s", replayed.Failure.Message)
				}
				callStarted := time.Now().UTC()
				if err := worker.Respond(ctx, task, replayed); err != nil {
					return snapshot, result, err
				}
				callFinished := time.Now().UTC()
				for _, command := range replayed.Commands {
					if timer := command.GetStartTimer(); timer != nil {
						timers.command(task.RunId, timer.Seq, timer.FireAfter.AsDuration(), callStarted, callFinished)
					}
				}
				if withFaults {
					if err := worker.Respond(ctx, task, replayed); !errors.Is(err, engine.ErrStaleTask) {
						return snapshot, result, fmt.Errorf("duplicate workflow acknowledgement accepted: %v", err)
					}
					result.duplicateAcks++
				}
			}
		case 1:
			task, found, pollErr := eng.PollActivityTask(ctx, &v1.PollActivityTaskRequest{TaskQueue: queue, Identity: identity})
			if pollErr != nil {
				return snapshot, result, pollErr
			}
			if found {
				var value any
				if err := json.Unmarshal(task.Input.Data, &value); err != nil {
					return snapshot, result, err
				}
				value, err = sink.Apply(task.IdempotencyKey, value)
				if err != nil {
					return snapshot, result, err
				}
				body, err := payload(value)
				if err != nil {
					return snapshot, result, err
				}
				request := &v1.CompleteActivityTaskRequest{TaskToken: task.TaskToken, Identity: identity, Result: body}
				complete := func() error { _, err := eng.CompleteActivityTask(ctx, request); return err }
				if withFaults && !injected {
					before, err := Capture(ctx, scoped)
					if err != nil {
						return snapshot, result, err
					}
					crash := seed%2 == 1
					faults.Arm(1+rng.Intn(12), crash)
					err = postgresInvoke(complete)
					faults.Disarm()
					if !errors.Is(err, ErrInjected) && !errors.Is(err, ErrServerCrash) {
						return snapshot, result, fmt.Errorf("acknowledgement fault did not fire: %v", err)
					}
					after, err := Capture(ctx, scoped)
					if err != nil {
						return snapshot, result, err
					}
					if !reflect.DeepEqual(before, after) {
						return after, result, errors.New("transaction rollback changed persisted state")
					}
					if crash {
						eng, err = newEngine()
						if err != nil {
							return snapshot, result, err
						}
						result.serverCrashes++
					} else {
						result.databaseErrors++
					}
					injected = true
					if _, err := sink.Apply(task.IdempotencyKey, value); err != nil {
						return snapshot, result, err
					}
				}
				if err := complete(); err != nil {
					return snapshot, result, err
				}
				if withFaults {
					if err := complete(); !errors.Is(err, engine.ErrStaleTask) {
						return snapshot, result, fmt.Errorf("duplicate activity acknowledgement accepted: %v", err)
					}
					result.duplicateAcks++
				}
			}
		case 2:
			if _, err := eng.FireDueTimers(ctx, 100); err != nil {
				return snapshot, result, err
			}
		case 3:
			if _, err := eng.ProcessDueTasks(ctx, 100); err != nil {
				return snapshot, result, err
			}
		case 4:
			time.Sleep(time.Millisecond)
		}
		current, err := Capture(ctx, scoped)
		if err != nil {
			return snapshot, result, err
		}
		if err := CheckHistory(snapshot, current); err != nil {
			return current, result, err
		}
		if err := timers.check(current); err != nil {
			return current, result, err
		}
		snapshot = current
		if finished(current) {
			if err := CheckEffects(current, sink); err != nil {
				return current, result, err
			}
			if err := checkPostgresOutcome(current, seed); err != nil {
				return current, result, err
			}
			if withFaults && !injected {
				return current, result, errors.New("scenario finished without its selected fault")
			}
			return current, result, nil
		}
	}
	return snapshot, result, errors.New("PostgreSQL scenario did not finish in 1500 steps")
}

func postgresInvoke(fn func() error) (err error) {
	defer func() {
		if p := recover(); p != nil {
			if e, ok := p.(error); ok && (errors.Is(e, ErrInjected) || errors.Is(e, ErrServerCrash)) {
				err = e
				return
			}
			panic(p)
		}
	}()
	return fn()
}
