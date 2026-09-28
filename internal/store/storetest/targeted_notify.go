package storetest

import (
	"testing"

	"github.com/Aly700/capstan/internal/store"
)

// A later run notification fences asynchronous listener delivery: by the time it
// arrives, the listener has finished delivering every earlier task notification.
func commitTaskNotifications(t *testing.T, s store.Store, count int) {
	t.Helper()
	fence, stop := s.SubscribeRun("notification-test-fence")
	defer stop()
	mustTx(t, s, func(tx store.Tx) error {
		for range count {
			tx.Notify(store.TaskActivity, "q:quotes'雪")
		}
		tx.NotifyRunClosed("notification-test-fence")
		return nil
	})
	awaitWake(t, fence)
}

func taskSubscribers(t *testing.T, s store.Store, count int) []<-chan struct{} {
	t.Helper()
	channels := make([]<-chan struct{}, count)
	for i := range channels {
		ch, cancel := s.Subscribe(store.TaskActivity, "q:quotes'雪")
		t.Cleanup(cancel)
		channels[i] = ch
	}
	return channels
}

func assertTaskHint(t *testing.T, ch <-chan struct{}, want bool) {
	t.Helper()
	select {
	case _, open := <-ch:
		if !open {
			t.Fatal("live task subscription closed")
		}
		if !want {
			t.Fatal("one task woke an extra poller")
		}
	default:
		if want {
			t.Fatal("task did not wake the next waiting poller")
		}
	}
}

func notifyOnePerTask(t *testing.T, s store.Store) {
	channels := taskSubscribers(t, s, 40)
	commitTaskNotifications(t, s, 1)
	for i, ch := range channels {
		assertTaskHint(t, ch, i == 0)
	}
}

func notifyMultipleTasks(t *testing.T, s store.Store) {
	channels := taskSubscribers(t, s, 8)
	commitTaskNotifications(t, s, 5)
	for i, ch := range channels {
		assertTaskHint(t, ch, i < 5)
	}
}

func notifyRoundRobin(t *testing.T, s store.Store) {
	channels := taskSubscribers(t, s, 4)
	for i := range 12 {
		commitTaskNotifications(t, s, 1)
		for j, ch := range channels {
			assertTaskHint(t, ch, j == i%len(channels))
		}
	}
}

func notifySkipsPending(t *testing.T, s store.Store) {
	channels := taskSubscribers(t, s, 2)
	commitTaskNotifications(t, s, 2)
	assertTaskHint(t, channels[1], true)
	for range 4 {
		commitTaskNotifications(t, s, 1)
		assertTaskHint(t, channels[1], true)
	}
	assertTaskHint(t, channels[0], true)
	assertTaskHint(t, channels[0], false)
}

func notifyCancelRotation(t *testing.T, s store.Store) {
	first, stopFirst := s.Subscribe(store.TaskActivity, "q:quotes'雪")
	defer stopFirst()
	second, stopSecond := s.Subscribe(store.TaskActivity, "q:quotes'雪")
	defer stopSecond()
	third, stopThird := s.Subscribe(store.TaskActivity, "q:quotes'雪")
	defer stopThird()
	stopFirst()
	stopFirst()
	commitTaskNotifications(t, s, 1)
	assertTaskHint(t, second, true)
	assertTaskHint(t, third, false)
	stopThird()
	stopThird()
	commitTaskNotifications(t, s, 1)
	assertTaskHint(t, second, true)
	for _, ch := range []<-chan struct{}{first, third} {
		select {
		case _, open := <-ch:
			if open {
				t.Fatal("cancelled subscriber received a hint")
			}
		default:
			t.Fatal("cancelled subscriber remains open")
		}
	}
}
