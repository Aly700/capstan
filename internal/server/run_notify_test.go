package server

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/engine"
	"github.com/Aly700/capstan/internal/store/memstore"
	"google.golang.org/protobuf/proto"
)

func (n *testNotifier) SubscribeRun(runID string) (<-chan struct{}, func()) {
	n.queue = runID
	n.subscribed.Add(1)
	return n.ch, func() { n.cancelled.Add(1) }
}

func TestAwaitRunCloseNotification(t *testing.T) {
	for _, between := range []bool{false, true} {
		t.Run(map[bool]string{false: "while-waiting", true: "read-wait-gap"}[between], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				n := &testNotifier{ch: make(chan struct{}, 1)}
				closed, calls := false, 0
				f := &fakeAPI{call: func(context.Context, string, string, proto.Message) (proto.Message, bool, error) {
					calls++
					if n.subscribed.Load() != 1 {
						t.Error("DescribeRun ran before SubscribeRun")
					}
					status := v1.RunStatus_RUN_STATUS_RUNNING
					if closed {
						status = v1.RunStatus_RUN_STATUS_COMPLETED
					}
					if between && calls == 1 {
						closed = true
						n.ch <- struct{}{}
					}
					return &v1.DescribeRunResponse{Run: &v1.RunInfo{RunId: "r", Status: status}}, true, nil
				}}
				done := make(chan struct{})
				go func() {
					defer close(done)
					resp, err := New(testConfig(), f, n, Options{}).AwaitRun(context.Background(), connect.NewRequest(&v1.AwaitRunRequest{RunId: "r"}))
					if err != nil || !resp.Msg.Closed {
						t.Errorf("response=%v error=%v", resp, err)
					}
				}()
				synctest.Wait()
				start := time.Now()
				if !between {
					closed = true
					n.ch <- struct{}{}
				}
				<-done
				if time.Since(start) >= 10*time.Millisecond {
					t.Errorf("close wake took %v", time.Since(start))
				}
				if calls != 2 || n.cancelled.Load() != 1 || n.queue != "r" {
					t.Errorf("calls=%d cancel=%d run=%s", calls, n.cancelled.Load(), n.queue)
				}
			})
		})
	}
}

func TestAwaitRunEngineClosure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db := memstore.New()
		defer db.Close()
		e, err := engine.New(engine.Deps{Store: db}, engine.Config{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = e.StartRun(t.Context(), "test", &v1.StartRunRequest{RunId: "r", WorkflowType: "f", TaskQueue: "q"}); err != nil {
			t.Fatal(err)
		}
		s := New(testConfig(), e, db, Options{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			resp, err := s.AwaitRun(t.Context(), connect.NewRequest(&v1.AwaitRunRequest{RunId: "r"}))
			if err != nil || !resp.Msg.Closed || resp.Msg.Run.Status != v1.RunStatus_RUN_STATUS_FAILED {
				t.Errorf("response=%v error=%v", resp, err)
			}
		}()
		synctest.Wait()
		start := time.Now()
		if _, err = e.TerminateRun(t.Context(), "test", &v1.TerminateRunRequest{RunId: "r"}); err != nil {
			t.Fatal(err)
		}
		<-done
		if time.Since(start) >= 10*time.Millisecond {
			t.Fatalf("committed engine close took %v to wake AwaitRun", time.Since(start))
		}
	})
}
