package pgstore

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/Aly700/capstan/internal/store"
	"github.com/Aly700/capstan/internal/store/subscriptions"
	"github.com/jackc/pgx/v5"
)

func topic(kind store.TaskKind, queue string) string { return kind.String() + ":" + queue }

func (t *transaction) Notify(kind store.TaskKind, queue string) {
	// PostgreSQL folds identical payloads within a transaction. A sequence keeps
	// one wake-up per new task when one activation schedules several activities.
	t.notifySeq++
	t.notify("task:" + strconv.FormatUint(t.notifySeq, 10) + ":" + topic(kind, queue))
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
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		ch := make(chan struct{})
		close(ch)
		return ch, func() {}
	}
	if s.subs[key] == nil {
		s.subs[key] = new(subscriptions.Group)
	}
	ch := s.subs[key].Add()
	s.mu.Unlock()
	return ch, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if group := s.subs[key]; group != nil {
			group.Remove(ch)
			if group.Len() == 0 {
				delete(s.subs, key)
			}
		}
	}
}

func (s *pgStore) wake(key string, all bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if all {
		for _, group := range s.subs {
			group.WakeAll()
		}
		return
	}
	if group := s.subs[key]; group != nil {
		if strings.HasPrefix(key, "run:") {
			group.WakeAll()
		} else {
			group.WakeOne()
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
			key := n.Payload
			if payload, ok := strings.CutPrefix(key, "task:"); ok {
				_, key, _ = strings.Cut(payload, ":")
			}
			s.wake(key, false)
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
