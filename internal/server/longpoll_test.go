package server

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/engine"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/proto"
)

type testNotifier struct {
	ch         chan struct{}
	subscribed atomic.Int32
	cancelled  atomic.Int32
	kind       store.TaskKind
	queue      string
}

func (n *testNotifier) Subscribe(kind store.TaskKind, queue string) (<-chan struct{}, func()) {
	n.kind, n.queue = kind, queue
	n.subscribed.Add(1)
	return n.ch, func() { n.cancelled.Add(1) }
}
func poll(s *Server, ctx context.Context, kind store.TaskKind) ([]byte, error) {
	if kind == store.TaskWorkflow {
		r, err := s.PollWorkflowTask(ctx, connect.NewRequest(&v1.PollWorkflowTaskRequest{TaskQueue: "q"}))
		if err != nil {
			return nil, err
		}
		return r.Msg.TaskToken, nil
	}
	r, err := s.PollActivityTask(ctx, connect.NewRequest(&v1.PollActivityTaskRequest{TaskQueue: "q"}))
	if err != nil {
		return nil, err
	}
	return r.Msg.TaskToken, nil
}
func taskResponse(kind store.TaskKind) proto.Message {
	if kind == store.TaskWorkflow {
		return &v1.PollWorkflowTaskResponse{TaskToken: []byte("claimed")}
	}
	return &v1.PollActivityTaskResponse{TaskToken: []byte("claimed")}
}

func TestLongPollNotificationAndClaimWaitRace(t *testing.T) {
	for _, kind := range []store.TaskKind{store.TaskWorkflow, store.TaskActivity} {
		for _, between := range []bool{false, true} {
			t.Run(kind.String()+map[bool]string{true: "/between-claim-and-wait", false: "/mid-wait"}[between], func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					n := &testNotifier{ch: make(chan struct{}, 1)}
					var available bool
					calls := 0
					f := &fakeAPI{call: func(context.Context, string, string, proto.Message) (proto.Message, bool, error) {
						calls++
						if n.subscribed.Load() != 1 {
							t.Error("claim ran before Subscribe")
						}
						if available {
							return taskResponse(kind), true, nil
						}
						if between {
							available = true
							n.ch <- struct{}{}
						}
						return nil, false, nil
					}}
					s := New(testConfig(), f, n, Options{})
					done := make(chan struct{})
					go func() {
						defer close(done)
						token, err := poll(s, context.Background(), kind)
						if err != nil || string(token) != "claimed" {
							t.Errorf("token=%q err=%v", token, err)
						}
					}()
					synctest.Wait()
					start := time.Now()
					if !between {
						available = true
						n.ch <- struct{}{}
					}
					<-done
					if time.Since(start) >= 100*time.Millisecond {
						t.Error("notification took >=100ms")
					}
					if calls != 2 || n.cancelled.Load() != 1 || n.kind != kind || n.queue != "q" {
						t.Fatalf("calls=%d cancelled=%d kind=%v queue=%s", calls, n.cancelled.Load(), n.kind, n.queue)
					}
				})
			})
		}
	}
}

func TestLongPollFallbackAndTimeout(t *testing.T) {
	for _, kind := range []store.TaskKind{store.TaskWorkflow, store.TaskActivity} {
		for _, closed := range []bool{false, true} {
			t.Run(kind.String()+map[bool]string{true: "/closed-subscription", false: "/no-notifier"}[closed], func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					calls := 0
					f := &fakeAPI{call: func(context.Context, string, string, proto.Message) (proto.Message, bool, error) {
						calls++
						if calls == 2 {
							return taskResponse(kind), true, nil
						}
						return nil, false, nil
					}}
					var notifier Notifier
					if closed {
						ch := make(chan struct{})
						close(ch)
						notifier = &testNotifier{ch: ch}
					}
					s := New(testConfig(), f, notifier, Options{})
					start := time.Now()
					token, err := poll(s, context.Background(), kind)
					if err != nil || string(token) != "claimed" || time.Since(start) != time.Second {
						t.Fatalf("token=%q err=%v wait=%v", token, err, time.Since(start))
					}
					calls = 0
					f.call = func(context.Context, string, string, proto.Message) (proto.Message, bool, error) {
						calls++
						return nil, false, nil
					}
					cfg := testConfig()
					cfg.PollTimeout = 2500 * time.Millisecond
					s = New(cfg, f, notifier, Options{})
					start = time.Now()
					token, err = poll(s, context.Background(), kind)
					if err != nil || len(token) != 0 || time.Since(start) != cfg.PollTimeout || calls != 3 {
						t.Fatalf("timeout: token=%q err=%v wait=%v calls=%d", token, err, time.Since(start), calls)
					}
				})
			})
		}
	}
}

func TestLongPollCancellationStopsEngineCalls(t *testing.T) {
	for _, kind := range []store.TaskKind{store.TaskWorkflow, store.TaskActivity} {
		t.Run(kind.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				n := &testNotifier{ch: make(chan struct{}, 1)}
				f := &fakeAPI{call: func(context.Context, string, string, proto.Message) (proto.Message, bool, error) {
					calls++
					return nil, false, nil
				}}
				s := New(testConfig(), f, n, Options{})
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan error, 1)
				go func() { _, err := poll(s, ctx, kind); done <- err }()
				synctest.Wait()
				cancel()
				n.ch <- struct{}{}
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Fatalf("err=%v", err)
				}
				time.Sleep(2 * time.Second)
				if calls != 1 || n.cancelled.Load() != 1 {
					t.Fatalf("calls=%d cancel=%d", calls, n.cancelled.Load())
				}
				_, err := poll(s, ctx, kind)
				if !errors.Is(err, context.Canceled) || calls != 1 {
					t.Fatalf("pre-cancelled request called engine: calls=%d err=%v", calls, err)
				}
			})
		})
	}
}

func TestLongPollBudgetsEngineCallAndPreservesClaimedTask(t *testing.T) {
	for _, kind := range []store.TaskKind{store.TaskWorkflow, store.TaskActivity} {
		for _, found := range []bool{false, true} {
			t.Run(kind.String()+map[bool]string{true: "/claim-wins", false: "/timeout"}[found], func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					cfg := testConfig()
					cfg.PollTimeout = 50 * time.Millisecond
					f := &fakeAPI{call: func(ctx context.Context, _ string, _ string, _ proto.Message) (proto.Message, bool, error) {
						<-ctx.Done()
						if found {
							return taskResponse(kind), true, nil
						}
						return nil, false, ctx.Err()
					}}
					s := New(cfg, f, nil, Options{})
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					start := time.Now()
					token, err := poll(s, ctx, kind)
					if err != nil || (len(token) > 0) != found || time.Since(start) != cfg.PollTimeout {
						t.Fatalf("token=%q err=%v wait=%v", token, err, time.Since(start))
					}
				})
			})
		}
	}
}

func TestLongPollEngineError(t *testing.T) {
	n := &testNotifier{ch: make(chan struct{}, 1)}
	f := &fakeAPI{call: func(context.Context, string, string, proto.Message) (proto.Message, bool, error) {
		return nil, false, engine.ErrInvalidArgument
	}}
	_, err := poll(New(testConfig(), f, n, Options{}), context.Background(), store.TaskWorkflow)
	if !errors.Is(err, engine.ErrInvalidArgument) || n.cancelled.Load() != 1 {
		t.Fatalf("err=%v cancel=%d", err, n.cancelled.Load())
	}
}

func TestAwaitRunWaitsForClosureAndTimeoutKeepsLastRun(t *testing.T) {
	for _, status := range []v1.RunStatus{v1.RunStatus_RUN_STATUS_COMPLETED, v1.RunStatus_RUN_STATUS_FAILED, v1.RunStatus_RUN_STATUS_CANCELLED, v1.RunStatus_RUN_STATUS_TIMED_OUT, v1.RunStatus_RUN_STATUS_CONTINUED_AS_NEW, v1.RunStatus_RUN_STATUS_BLOCKED} {
		t.Run(status.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				cfg := testConfig()
				cfg.PollTimeout = 600 * time.Millisecond
				f := &fakeAPI{call: func(_ context.Context, name, _ string, req proto.Message) (proto.Message, bool, error) {
					calls++
					if name != "DescribeRun" || req.(*v1.DescribeRunRequest).RunId != "r" {
						t.Error("bad describe request")
					}
					s := v1.RunStatus_RUN_STATUS_RUNNING
					if calls > 1 {
						s = status
					}
					return &v1.DescribeRunResponse{Run: &v1.RunInfo{RunId: "r", Status: s}}, true, nil
				}}
				start := time.Now()
				resp, err := New(cfg, f, nil, Options{}).AwaitRun(context.Background(), connect.NewRequest(&v1.AwaitRunRequest{RunId: "r"}))
				if err != nil {
					t.Fatal(err)
				}
				blocked := status == v1.RunStatus_RUN_STATUS_BLOCKED
				wantWait := 250 * time.Millisecond
				wantCalls := 2
				if blocked {
					wantWait = cfg.PollTimeout
					wantCalls = 3
				}
				if resp.Msg.Run.Status != status || resp.Msg.Closed == blocked || calls != wantCalls || time.Since(start) != wantWait {
					t.Fatalf("resp=%v calls=%d wait=%v", resp.Msg, calls, time.Since(start))
				}
			})
		})
	}
}

func TestAwaitRunCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		f := &fakeAPI{call: func(context.Context, string, string, proto.Message) (proto.Message, bool, error) {
			calls++
			return &v1.DescribeRunResponse{Run: &v1.RunInfo{Status: v1.RunStatus_RUN_STATUS_RUNNING}}, true, nil
		}}
		s := New(testConfig(), f, nil, Options{})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { _, err := s.AwaitRun(ctx, connect.NewRequest(&v1.AwaitRunRequest{RunId: "r"})); done <- err }()
		synctest.Wait()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("err=%v", err)
		}
		if calls != 1 {
			t.Fatalf("calls=%d", calls)
		}
	})
}
