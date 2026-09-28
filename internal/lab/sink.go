package lab

import (
	"errors"
	"slices"
	"strings"
	"sync"
)

// Effect records the externally applied operation separately from delivery attempts.
type Effect struct {
	Key      string
	Count    int
	Attempts int
	Value    any
}

// EffectSink is an in-memory destination that honours activity idempotency keys.
// Values are opaque and must be treated as immutable by callers. Its zero value is usable.
type EffectSink struct {
	mu      sync.Mutex
	effects map[string]Effect
}

func NewEffectSink() *EffectSink { return &EffectSink{} }

// Apply records an effect once and returns the first result on every subsequent attempt.
func (s *EffectSink) Apply(key string, value any) (any, error) {
	if key == "" {
		return nil, errors.New("effect sink: empty idempotency key")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.effects == nil {
		s.effects = make(map[string]Effect)
	}
	effect, exists := s.effects[key]
	if !exists {
		effect = Effect{Key: key, Count: 1, Value: value}
	}
	effect.Attempts++
	s.effects[key] = effect
	return effect.Value, nil
}

// Entries returns copied records ordered by key, so reports are stable across schedules.
func (s *EffectSink) Entries() []Effect {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := make([]Effect, 0, len(s.effects))
	for _, effect := range s.effects {
		entries = append(entries, effect)
	}
	slices.SortFunc(entries, func(a, b Effect) int { return strings.Compare(a.Key, b.Key) })
	return entries
}

// Snapshot returns the same stable records as Entries.
func (s *EffectSink) Snapshot() []Effect { return s.Entries() }
