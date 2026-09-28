package server

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
	"google.golang.org/protobuf/proto"
)

func TestShutdownReturnsEmptyPolls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, kind := range []store.TaskKind{store.TaskWorkflow, store.TaskActivity} {
			calls := 0
			f := &fakeAPI{call: func(context.Context, string, string, proto.Message) (proto.Message, bool, error) {
				calls++
				return nil, false, nil
			}}
			s := New(testConfig(), f, nil, Options{})
			done := make(chan struct{})
			go func() {
				defer close(done)
				token, err := poll(s, context.Background(), kind)
				if err != nil || len(token) != 0 {
					t.Errorf("token=%q err=%v", token, err)
				}
			}()
			synctest.Wait()
			s.cancelPolls()
			<-done
			if calls != 1 {
				t.Fatalf("calls=%d", calls)
			}
			token, err := poll(s, context.Background(), kind)
			if err != nil || len(token) != 0 || calls != 1 {
				t.Fatalf("after shutdown token=%q err=%v calls=%d", token, err, calls)
			}
		}
		f := &fakeAPI{call: func(context.Context, string, string, proto.Message) (proto.Message, bool, error) {
			return &v1.DescribeRunResponse{Run: &v1.RunInfo{RunId: "r", Status: v1.RunStatus_RUN_STATUS_RUNNING}}, true, nil
		}}
		s := New(testConfig(), f, nil, Options{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			r, err := s.AwaitRun(context.Background(), connect.NewRequest(&v1.AwaitRunRequest{RunId: "r"}))
			if err != nil || r.Msg.Closed {
				t.Errorf("response=%v err=%v", r, err)
			}
		}()
		synctest.Wait()
		s.cancelPolls()
		<-done
	})
}

// delayedCloseListener keeps Accept registered briefly after Close, exposing
// ownership races between a caller's Close and net/http's Shutdown.
type delayedCloseListener struct {
	accepting chan struct{}
	closed    chan struct{}
	closes    atomic.Int32
}

func (l *delayedCloseListener) Accept() (net.Conn, error) {
	close(l.accepting)
	<-l.closed
	time.Sleep(time.Second)
	return nil, net.ErrClosed
}
func (l *delayedCloseListener) Close() error {
	if l.closes.Add(1) != 1 {
		return net.ErrClosed
	}
	close(l.closed)
	return nil
}
func (l *delayedCloseListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 7233}
}

func TestShutdownOwnsListenerClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := &loopAPI{wake: func(context.Context) (time.Time, bool, error) { return time.Time{}, false, nil }}
		for i := range 4 {
			f.work[i] = func(context.Context, int) (int, error) { return 0, nil }
		}
		s := New(testConfig(), f, nil, Options{Logger: slog.New(slog.NewJSONHandler(io.Discard, nil))})
		l := &delayedCloseListener{accepting: make(chan struct{}), closed: make(chan struct{})}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- s.Serve(ctx, l) }()
		<-l.accepting
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("normal shutdown failed: %v", err)
		}
		if l.closes.Load() != 1 {
			t.Fatalf("listener closed %d times", l.closes.Load())
		}
	})
}
