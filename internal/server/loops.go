package server

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"
)

// RunLoops runs each maintenance job independently and waits until they all stop.
// Cancel ctx before closing the store. Engine calls receive that same context.
func (s *Server) RunLoops(ctx context.Context) {
	var wg sync.WaitGroup
	for _, job := range []struct {
		name string
		work func(context.Context, int) (int, error)
	}{
		{"timers", s.api.FireDueTimers}, {"tasks", s.api.ProcessDueTasks},
		{"approvals", s.api.ProcessDueApprovals}, {"run_timeouts", s.api.TimeoutRuns},
	} {
		wg.Go(func() { s.runLoop(ctx, job.name, job.work) })
	}
	wg.Wait()
}

func (s *Server) runLoop(ctx context.Context, name string, work func(context.Context, int) (int, error)) {
	backoff := 250 * time.Millisecond
	for ctx.Err() == nil {
		count, err := work(ctx, 100)
		s.metrics.loopItems(name, count)
		if ctx.Err() != nil {
			return
		}
		if err == nil && count >= 100 {
			backoff = 250 * time.Millisecond
			continue
		}
		delay := time.Second
		if err == nil {
			var at time.Time
			var ok bool
			at, ok, err = s.api.NextWakeup(ctx)
			if ok && err == nil {
				until := time.Until(at)
				if until <= 0 {
					// NextWakeup is global: another loop or a locked row may remain overdue.
					// Back off locally when this job made no progress instead of spinning.
					delay = jitter(250 * time.Millisecond)
				} else {
					delay = min(delay, until)
				}
			}
		}
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			s.logger.ErrorContext(ctx, "background work failed", "loop", name, "error", err)
			delay = jitter(backoff)
			backoff = min(backoff*2, 5*time.Second)
		} else {
			backoff = 250 * time.Millisecond
		}
		if !waitFor(ctx, delay) {
			return
		}
	}
}

func jitter(d time.Duration) time.Duration {
	return d - d/10 + time.Duration(rand.Int64N(int64(d/5)+1))
}
func waitFor(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}
