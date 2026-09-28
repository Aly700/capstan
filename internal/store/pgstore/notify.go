package pgstore

import (
	"context"
	"time"

	"github.com/Aly700/capstan/internal/store"
	"github.com/jackc/pgx/v5"
)

func topic(kind store.TaskKind, queue string) string { return kind.String() + ":" + queue }

func (t *transaction) Notify(kind store.TaskKind, queue string) {
	t.notify(topic(kind, queue))
}

func (t *transaction) NotifyRunClosed(runID string) {
	// Run topics share the existing LISTEN connection and fanout path with task topics.
	t.notify("run:" + runID)
}

func (t *transaction) notify(key string) {
	if t.notifyErr != nil {
		return
	}
	_, err := t.tx.Exec(t.ctx, `select pg_notify('capstan_tasks',$1)`, key)
	t.notifyErr = dbError(err)
}

func (s *pgStore) Subscribe(kind store.TaskKind, queue string) (<-chan struct{}, func()) {
	return s.subscribe(topic(kind, queue))
}

func (s *pgStore) SubscribeRun(runID string) (<-chan struct{}, func()) {
	return s.subscribe("run:" + runID)
}

func (s *pgStore) subscribe(key string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		close(ch)
		return ch, func() {}
	}
	if s.subs[key] == nil {
		s.subs[key] = make(map[chan struct{}]struct{})
	}
	s.subs[key][ch] = struct{}{}
	s.mu.Unlock()
	return ch, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, ok := s.subs[key][ch]; ok {
			delete(s.subs[key], ch)
			if len(s.subs[key]) == 0 {
				delete(s.subs, key)
			}
			closeSubscription(ch)
		}
	}
}

func closeSubscription(ch chan struct{}) {
	select {
	case <-ch:
	default:
	}
	close(ch)
}

func (s *pgStore) wake(key string, all bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, subscribers := range s.subs {
		if !all && k != key {
			continue
		}
		for ch := range subscribers {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}
}

func (s *pgStore) connectListener(ctx context.Context) (*pgx.Conn, error) {
	conn, err := pgx.ConnectConfig(ctx, s.listenerConfig)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Exec(ctx, "listen capstan_tasks"); err != nil {
		closeListener(conn)
		return nil, err
	}
	return conn, nil
}

func closeListener(conn *pgx.Conn) {
	if conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = conn.Close(ctx)
}

func (s *pgStore) listen(conn *pgx.Conn) {
	defer close(s.done)
	defer func() { closeListener(conn) }()
	for s.listenCtx.Err() == nil {
		n, err := conn.WaitForNotification(s.listenCtx)
		if err == nil {
			s.wake(n.Payload, false)
			continue
		}
		closeListener(conn)
		conn = nil
		backoff := 100 * time.Millisecond
		for s.listenCtx.Err() == nil {
			timer := time.NewTimer(backoff)
			select {
			case <-s.listenCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			conn, err = s.connectListener(s.listenCtx)
			if err == nil {
				// Notifications during the outage may be lost; make every poller query
				// durable state once after the replacement LISTEN is established.
				s.wake("", true)
				break
			}
			backoff = min(2*backoff, 5*time.Second)
		}
	}
}
