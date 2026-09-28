package engine

import "time"

// now is the sole clock read; persistent time has PostgreSQL microsecond precision.
func (e *Engine) now() time.Time {
	return e.deps.Clock.Now().Truncate(time.Microsecond)
}

// addDeadline also truncates the result: activity durations keep their protobuf precision.
func addDeadline(at time.Time, after time.Duration) time.Time {
	return at.Add(after).Truncate(time.Microsecond)
}
