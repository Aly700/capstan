package server

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Aly700/capstan/internal/engine"
)

type loopAPI struct {
	engine.API
	work [4]func(context.Context, int) (int, error)
	wake func(context.Context) (time.Time, bool, error)
}

func (f *loopAPI) FireDueTimers(ctx context.Context, limit int) (int, error) {
	return f.work[0](ctx, limit)
}
func (f *loopAPI) ProcessDueTasks(ctx context.Context, limit int) (int, error) {
	return f.work[1](ctx, limit)
}
func (f *loopAPI) ProcessDueApprovals(ctx context.Context, limit int) (int, error) {
	return f.work[2](ctx, limit)
}
func (f *loopAPI) TimeoutRuns(ctx context.Context, limit int) (int, error) {
	return f.work[3](ctx, limit)
}
func (f *loopAPI) NextWakeup(ctx context.Context) (time.Time, bool, error) { return f.wake(ctx) }

func TestLoopsDrainBatchesThenSleepAndCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls [4]atomic.Int32
		f := &loopAPI{wake: func(context.Context) (time.Time, bool, error) { return time.Time{}, false, nil }}
		for i := range 4 {
			f.work[i] = func(ctx context.Context, limit int) (int, error) {
				if limit != 100 {
					t.Errorf("limit=%d", limit)
				}
				if ctx.Err() != nil {
					t.Error("work called after cancellation")
				}
				if calls[i].Add(1) <= 2 {
					return 100, nil
				}
				return 3, nil
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		s := New(testConfig(), f, nil, Options{})
		go func() { defer close(done); s.RunLoops(ctx) }()
		synctest.Wait()
		for i := range 4 {
			if calls[i].Load() != 3 {
				t.Fatalf("loop %d calls=%d; did not drain", i, calls[i].Load())
			}
		}
		time.Sleep(999 * time.Millisecond)
		synctest.Wait()
		for i := range 4 {
			if calls[i].Load() != 3 {
				t.Fatalf("loop %d spun while idle: calls=%d", i, calls[i].Load())
			}
		}
		time.Sleep(time.Millisecond)
		synctest.Wait()
		for i := range 4 {
			if calls[i].Load() != 4 {
				t.Errorf("loop %d did not recheck in 1s", i)
			}
		}
		cancel()
		<-done
		time.Sleep(time.Second)
		for i := range 4 {
			if calls[i].Load() != 4 {
				t.Errorf("loop %d called after cancel", i)
			}
		}
	})
}

func TestLoopsHonorNearWakeupAndPaceOverdueWork(t *testing.T) {
	for _, overdue := range []bool{false, true} {
		t.Run(map[bool]string{false: "near", true: "overdue"}[overdue], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var calls atomic.Int32
				f := &loopAPI{wake: func(context.Context) (time.Time, bool, error) {
					d := 100 * time.Millisecond
					if overdue {
						d = -time.Second
					}
					return time.Now().Add(d), true, nil
				}}
				s := New(testConfig(), f, nil, Options{})
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan struct{})
				go func() {
					defer close(done)
					s.runLoop(ctx, "timers", func(context.Context, int) (int, error) { calls.Add(1); return 0, nil })
				}()
				synctest.Wait()
				time.Sleep(time.Second)
				synctest.Wait()
				cancel()
				<-done
				n := calls.Load()
				if !overdue && n != 11 {
					t.Fatalf("near wakeup calls=%d", n)
				}
				if overdue && (n < 2 || n > 6) {
					t.Fatalf("overdue wakeup spun or stalled: calls=%d", n)
				}
			})
		})
	}
}

func TestLoopsBackOffWorkAndWakeupErrors(t *testing.T) {
	for _, wakeError := range []bool{false, true} {
		t.Run(map[bool]string{false: "work", true: "wakeup"}[wakeError], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var calls atomic.Int32
				var logs bytes.Buffer
				f := &loopAPI{wake: func(context.Context) (time.Time, bool, error) {
					return time.Time{}, false, errors.New("next wakeup failed")
				}}
				s := New(testConfig(), f, nil, Options{Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan struct{})
				go func() {
					defer close(done)
					s.runLoop(ctx, "timers", func(context.Context, int) (int, error) {
						calls.Add(1)
						if wakeError {
							return 0, nil
						}
						return 0, errors.New("store failed")
					})
				}()
				synctest.Wait()
				time.Sleep(10 * time.Second)
				synctest.Wait()
				cancel()
				<-done
				if n := calls.Load(); n < 3 || n > 8 {
					t.Fatalf("not backed off: calls=%d", n)
				}
				if !strings.Contains(logs.String(), `"loop":"timers"`) || (!wakeError && !strings.Contains(logs.String(), "store failed")) || (wakeError && !strings.Contains(logs.String(), "next wakeup failed")) {
					t.Fatalf("missing error log: %s", logs.String())
				}
			})
		})
	}
}

func TestLoopCancellationInterruptsBatchDrainAndEngineCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, inCall := range []bool{false, true} {
			f := &loopAPI{wake: func(context.Context) (time.Time, bool, error) {
				t.Error("unexpected wakeup after cancel")
				return time.Time{}, false, nil
			}}
			s := New(testConfig(), f, nil, Options{})
			ctx, cancel := context.WithCancel(context.Background())
			calls := 0
			done := make(chan struct{})
			go func() {
				defer close(done)
				s.runLoop(ctx, "tasks", func(ctx context.Context, _ int) (int, error) {
					calls++
					if inCall {
						<-ctx.Done()
						return 0, ctx.Err()
					}
					cancel()
					return 100, nil
				})
			}()
			synctest.Wait()
			cancel()
			<-done
			if calls != 1 {
				t.Errorf("calls=%d", calls)
			}
		}
	})
}
