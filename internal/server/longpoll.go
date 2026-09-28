package server

import (
	"context"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
)

func pollTask[T any](ctx context.Context, s *Server, kind store.TaskKind, queue string, claim func(context.Context) (*T, bool, error)) (*T, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pollCtx, cancel := context.WithTimeout(ctx, s.cfg.PollTimeout)
	defer cancel()
	var notifications <-chan struct{}
	if s.notifier != nil {
		var unsubscribe func()
		// Subscribe before claiming so a commit in the claim/wait gap is observable.
		notifications, unsubscribe = s.notifier.Subscribe(kind, queue)
		defer unsubscribe()
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if pollCtx.Err() != nil {
			return new(T), nil
		}
		resp, found, err := claim(pollCtx)
		// A successful claim wins a simultaneous timeout: never discard a leased task.
		if err == nil && found {
			return resp, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if pollCtx.Err() != nil {
			return new(T), nil
		}
		if err != nil {
			return nil, err
		}
		waiting := true
		for waiting {
			select {
			case <-pollCtx.Done():
				waiting = false
			case _, open := <-notifications:
				if !open {
					notifications = nil
				} else {
					waiting = false
				}
			case <-ticker.C:
				waiting = false
			}
		}
	}
}

func (s *Server) AwaitRun(ctx context.Context, req *connect.Request[v1.AwaitRunRequest]) (*connect.Response[v1.AwaitRunResponse], error) {
	pollCtx, cancel := context.WithTimeout(ctx, s.cfg.PollTimeout)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	result := &v1.AwaitRunResponse{}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if pollCtx.Err() != nil {
			return connect.NewResponse(result), nil
		}
		resp, err := s.api.DescribeRun(pollCtx, &v1.DescribeRunRequest{RunId: req.Msg.RunId})
		if err == nil {
			result.Run = resp.GetRun()
			result.Closed = runClosed(result.Run.GetStatus())
			if result.Closed {
				return connect.NewResponse(result), nil
			}
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if pollCtx.Err() != nil {
			return connect.NewResponse(result), nil
		}
		if err != nil {
			return nil, err
		}
		select {
		case <-pollCtx.Done():
		case <-ticker.C:
		}
	}
}
