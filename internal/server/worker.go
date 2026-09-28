package server

import (
	"connectrpc.com/connect"
	"context"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/internal/store"
)

func (s *Server) PollWorkflowTask(ctx context.Context, req *connect.Request[v1.PollWorkflowTaskRequest]) (*connect.Response[v1.PollWorkflowTaskResponse], error) {
	resp, err := pollTask(ctx, s, store.TaskWorkflow, req.Msg.TaskQueue, func(ctx context.Context) (*v1.PollWorkflowTaskResponse, bool, error) {
		return s.api.PollWorkflowTask(ctx, req.Msg)
	})
	if err != nil {
		return nil, err
	}
	if resp == nil {
		resp = &v1.PollWorkflowTaskResponse{}
	}
	return connect.NewResponse(resp), nil
}
func (s *Server) CompleteWorkflowTask(ctx context.Context, req *connect.Request[v1.CompleteWorkflowTaskRequest]) (*connect.Response[v1.CompleteWorkflowTaskResponse], error) {
	resp, err := s.api.CompleteWorkflowTask(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		resp = &v1.CompleteWorkflowTaskResponse{}
	}
	return connect.NewResponse(resp), nil
}
func (s *Server) FailWorkflowTask(ctx context.Context, req *connect.Request[v1.FailWorkflowTaskRequest]) (*connect.Response[v1.FailWorkflowTaskResponse], error) {
	resp, err := s.api.FailWorkflowTask(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		resp = &v1.FailWorkflowTaskResponse{}
	}
	return connect.NewResponse(resp), nil
}
func (s *Server) PollActivityTask(ctx context.Context, req *connect.Request[v1.PollActivityTaskRequest]) (*connect.Response[v1.PollActivityTaskResponse], error) {
	resp, err := pollTask(ctx, s, store.TaskActivity, req.Msg.TaskQueue, func(ctx context.Context) (*v1.PollActivityTaskResponse, bool, error) {
		return s.api.PollActivityTask(ctx, req.Msg)
	})
	if err != nil {
		return nil, err
	}
	if resp == nil {
		resp = &v1.PollActivityTaskResponse{}
	}
	return connect.NewResponse(resp), nil
}
func (s *Server) CompleteActivityTask(ctx context.Context, req *connect.Request[v1.CompleteActivityTaskRequest]) (*connect.Response[v1.CompleteActivityTaskResponse], error) {
	resp, err := s.api.CompleteActivityTask(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		resp = &v1.CompleteActivityTaskResponse{}
	}
	return connect.NewResponse(resp), nil
}
func (s *Server) FailActivityTask(ctx context.Context, req *connect.Request[v1.FailActivityTaskRequest]) (*connect.Response[v1.FailActivityTaskResponse], error) {
	resp, err := s.api.FailActivityTask(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		resp = &v1.FailActivityTaskResponse{}
	}
	return connect.NewResponse(resp), nil
}
func (s *Server) HeartbeatActivityTask(ctx context.Context, req *connect.Request[v1.HeartbeatActivityTaskRequest]) (*connect.Response[v1.HeartbeatActivityTaskResponse], error) {
	resp, err := s.api.HeartbeatActivityTask(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		resp = &v1.HeartbeatActivityTaskResponse{}
	}
	return connect.NewResponse(resp), nil
}
func (s *Server) ReserveAICall(ctx context.Context, req *connect.Request[v1.ReserveAICallRequest]) (*connect.Response[v1.ReserveAICallResponse], error) {
	resp, err := s.api.ReserveAICall(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		resp = &v1.ReserveAICallResponse{}
	}
	return connect.NewResponse(resp), nil
}
func (s *Server) FinishAICall(ctx context.Context, req *connect.Request[v1.FinishAICallRequest]) (*connect.Response[v1.FinishAICallResponse], error) {
	resp, err := s.api.FinishAICall(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		resp = &v1.FinishAICallResponse{}
	}
	return connect.NewResponse(resp), nil
}
