package lab

import (
	"fmt"
	"time"
)

type postgresTimerWindow struct{ first, last time.Time }

type postgresTimerChecker struct {
	windows   map[string]map[int64]postgresTimerWindow
	deadlines map[string]map[int64]time.Time
}

func newPostgresTimerChecker() *postgresTimerChecker {
	return &postgresTimerChecker{windows: make(map[string]map[int64]postgresTimerWindow), deadlines: make(map[string]map[int64]time.Time)}
}

func (c *postgresTimerChecker) command(run string, seq int64, duration time.Duration, before, after time.Time) {
	if c.windows[run] == nil {
		c.windows[run] = make(map[int64]postgresTimerWindow)
		c.deadlines[run] = make(map[int64]time.Time)
	}
	c.windows[run][seq] = postgresTimerWindow{first: before.Add(duration).Truncate(time.Microsecond), last: after.Add(duration).Truncate(time.Microsecond)}
}

func (c *postgresTimerChecker) check(snapshot Snapshot) error {
	for run, timers := range snapshot.Timers {
		for _, timer := range timers {
			if timer == nil {
				return fmt.Errorf("P3: nil timer row in %s", run)
			}
			window, ok := c.windows[run][timer.Seq]
			if !ok || timer.DueAt.Before(window.first) || timer.DueAt.After(window.last) {
				return fmt.Errorf("P3: timer %s/%d deadline is outside its command clock window", run, timer.Seq)
			}
			if previous, ok := c.deadlines[run][timer.Seq]; ok && !previous.Equal(timer.DueAt) {
				return fmt.Errorf("P3: timer %s/%d persisted deadline changed", run, timer.Seq)
			}
			c.deadlines[run][timer.Seq] = timer.DueAt
		}
	}
	return CheckTimersWithDeadlines(snapshot, c.deadlines)
}
