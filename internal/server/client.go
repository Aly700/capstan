package server

import (
	"connectrpc.com/connect"
	"context"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
)

func (s *Server) StartRun(ctx context.Context, req *connect.Request[v1.StartRunRequest]) (*connect.Response[v1.StartRunResponse], error) {
	resp, err := s.api.StartRun(ctx, Identity(ctx), req.Msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		resp = &v1.StartRunResponse{}
	}
	return connect.NewResponse(resp), nil
}
func (s *Server) SignalRun(ctx context.Context, req *connect.Request[v1.SignalRunRequest]) (*connect.Response[v1.SignalRunResponse], error) {
	resp, err := s.api.SignalRun(ctx, Identity(ctx), req.Msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		resp = &v1.SignalRunResponse{}
	}
	return connect.NewResponse(resp), nil
}
func (s *Server) CancelRun(ctx context.Context, req *connect.Request[v1.CancelRunRequest]) (*connect.Response[v1.CancelRunResponse], error) {
	resp, err := s.api.CancelRun(ctx, Identity(ctx), req.Msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		resp = &v1.CancelRunResponse{}
	}
	return connect.NewResponse(resp), nil
}
func (s *Server) TerminateRun(ctx context.Context, req *connect.Request[v1.TerminateRunRequest]) (*connect.Response[v1.TerminateRunResponse], error) {
	resp, err := s.api.TerminateRun(ctx, Identity(ctx), req.Msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		resp = &v1.TerminateRunResponse{}
	}
	return connect.NewResponse(resp), nil
}
func (s *Server) ResumeRun(ctx context.Context, req *connect.Request[v1.ResumeRunRequest]) (*connect.Response[v1.ResumeRunResponse], error) {
	resp, err := s.api.ResumeRun(ctx, Identity(ctx), req.Msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		resp = &v1.ResumeRunResponse{}
	}
	return connect.NewResponse(resp), nil
}
func (s *Server) DescribeRun(ctx context.Context, req *connect.Request[v1.DescribeRunRequest]) (*connect.Response[v1.DescribeRunResponse], error) {
	resp, err := s.api.DescribeRun(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		resp = &v1.DescribeRunResponse{}
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) ListRuns(ctx context.Context, req *connect.Request[v1.ListRunsRequest]) (*connect.Response[v1.ListRunsResponse], error) {
	resp, err := s.api.ListRuns(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		resp = &v1.ListRunsResponse{}
	}
	return connect.NewResponse(resp), nil
}
func (s *Server) GetHistory(ctx context.Context, req *connect.Request[v1.GetHistoryRequest]) (*connect.Response[v1.GetHistoryResponse], error) {
	resp, err := s.api.GetHistory(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		resp = &v1.GetHistoryResponse{}
	}
	return connect.NewResponse(resp), nil
}
func (s *Server) ResolveApproval(ctx context.Context, req *connect.Request[v1.ResolveApprovalRequest]) (*connect.Response[v1.ResolveApprovalResponse], error) {
	resp, err := s.api.ResolveApproval(ctx, Identity(ctx), req.Msg)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		resp = &v1.ResolveApprovalResponse{}
	}
	return connect.NewResponse(resp), nil
}

func runClosed(status v1.RunStatus) bool {
	switch status {
	case v1.RunStatus_RUN_STATUS_COMPLETED, v1.RunStatus_RUN_STATUS_FAILED, v1.RunStatus_RUN_STATUS_CANCELLED, v1.RunStatus_RUN_STATUS_TIMED_OUT, v1.RunStatus_RUN_STATUS_CONTINUED_AS_NEW:
		return true
	}
	return false
}
