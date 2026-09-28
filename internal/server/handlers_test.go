package server

import (
	"bytes"
	"connectrpc.com/connect"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	rpc "github.com/Aly700/capstan/gen/capstan/v1/capstanv1connect"
	"github.com/Aly700/capstan/internal/config"
	"github.com/Aly700/capstan/internal/engine"
	"google.golang.org/protobuf/proto"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type fakeAPI struct {
	engine.API
	call func(context.Context, string, string, proto.Message) (proto.Message, bool, error)
}

func (f *fakeAPI) PollWorkflowTask(ctx context.Context, req *v1.PollWorkflowTaskRequest) (*v1.PollWorkflowTaskResponse, bool, error) {
	msg, found, err := f.call(ctx, "PollWorkflowTask", "", req)
	_ = found
	if msg == nil {
		return nil, found, err
	}
	return msg.(*v1.PollWorkflowTaskResponse), found, err
}
func (f *fakeAPI) CompleteWorkflowTask(ctx context.Context, req *v1.CompleteWorkflowTaskRequest) (*v1.CompleteWorkflowTaskResponse, error) {
	msg, found, err := f.call(ctx, "CompleteWorkflowTask", "", req)
	_ = found
	if msg == nil {
		return nil, err
	}
	return msg.(*v1.CompleteWorkflowTaskResponse), err
}
func (f *fakeAPI) FailWorkflowTask(ctx context.Context, req *v1.FailWorkflowTaskRequest) (*v1.FailWorkflowTaskResponse, error) {
	msg, found, err := f.call(ctx, "FailWorkflowTask", "", req)
	_ = found
	if msg == nil {
		return nil, err
	}
	return msg.(*v1.FailWorkflowTaskResponse), err
}
func (f *fakeAPI) PollActivityTask(ctx context.Context, req *v1.PollActivityTaskRequest) (*v1.PollActivityTaskResponse, bool, error) {
	msg, found, err := f.call(ctx, "PollActivityTask", "", req)
	_ = found
	if msg == nil {
		return nil, found, err
	}
	return msg.(*v1.PollActivityTaskResponse), found, err
}
func (f *fakeAPI) CompleteActivityTask(ctx context.Context, req *v1.CompleteActivityTaskRequest) (*v1.CompleteActivityTaskResponse, error) {
	msg, found, err := f.call(ctx, "CompleteActivityTask", "", req)
	_ = found
	if msg == nil {
		return nil, err
	}
	return msg.(*v1.CompleteActivityTaskResponse), err
}
func (f *fakeAPI) FailActivityTask(ctx context.Context, req *v1.FailActivityTaskRequest) (*v1.FailActivityTaskResponse, error) {
	msg, found, err := f.call(ctx, "FailActivityTask", "", req)
	_ = found
	if msg == nil {
		return nil, err
	}
	return msg.(*v1.FailActivityTaskResponse), err
}
func (f *fakeAPI) HeartbeatActivityTask(ctx context.Context, req *v1.HeartbeatActivityTaskRequest) (*v1.HeartbeatActivityTaskResponse, error) {
	msg, found, err := f.call(ctx, "HeartbeatActivityTask", "", req)
	_ = found
	if msg == nil {
		return nil, err
	}
	return msg.(*v1.HeartbeatActivityTaskResponse), err
}
func (f *fakeAPI) ReserveAICall(ctx context.Context, req *v1.ReserveAICallRequest) (*v1.ReserveAICallResponse, error) {
	msg, found, err := f.call(ctx, "ReserveAICall", "", req)
	_ = found
	if msg == nil {
		return nil, err
	}
	return msg.(*v1.ReserveAICallResponse), err
}
func (f *fakeAPI) FinishAICall(ctx context.Context, req *v1.FinishAICallRequest) (*v1.FinishAICallResponse, error) {
	msg, found, err := f.call(ctx, "FinishAICall", "", req)
	_ = found
	if msg == nil {
		return nil, err
	}
	return msg.(*v1.FinishAICallResponse), err
}
func (f *fakeAPI) StartRun(ctx context.Context, identity string, req *v1.StartRunRequest) (*v1.StartRunResponse, error) {
	msg, found, err := f.call(ctx, "StartRun", identity, req)
	_ = found
	if msg == nil {
		return nil, err
	}
	return msg.(*v1.StartRunResponse), err
}
func (f *fakeAPI) SignalRun(ctx context.Context, identity string, req *v1.SignalRunRequest) (*v1.SignalRunResponse, error) {
	msg, found, err := f.call(ctx, "SignalRun", identity, req)
	_ = found
	if msg == nil {
		return nil, err
	}
	return msg.(*v1.SignalRunResponse), err
}
func (f *fakeAPI) CancelRun(ctx context.Context, identity string, req *v1.CancelRunRequest) (*v1.CancelRunResponse, error) {
	msg, found, err := f.call(ctx, "CancelRun", identity, req)
	_ = found
	if msg == nil {
		return nil, err
	}
	return msg.(*v1.CancelRunResponse), err
}
func (f *fakeAPI) TerminateRun(ctx context.Context, identity string, req *v1.TerminateRunRequest) (*v1.TerminateRunResponse, error) {
	msg, found, err := f.call(ctx, "TerminateRun", identity, req)
	_ = found
	if msg == nil {
		return nil, err
	}
	return msg.(*v1.TerminateRunResponse), err
}
func (f *fakeAPI) ResumeRun(ctx context.Context, identity string, req *v1.ResumeRunRequest) (*v1.ResumeRunResponse, error) {
	msg, found, err := f.call(ctx, "ResumeRun", identity, req)
	_ = found
	if msg == nil {
		return nil, err
	}
	return msg.(*v1.ResumeRunResponse), err
}
func (f *fakeAPI) DescribeRun(ctx context.Context, req *v1.DescribeRunRequest) (*v1.DescribeRunResponse, error) {
	msg, found, err := f.call(ctx, "DescribeRun", "", req)
	_ = found
	if msg == nil {
		return nil, err
	}
	return msg.(*v1.DescribeRunResponse), err
}
func (f *fakeAPI) ListRuns(ctx context.Context, req *v1.ListRunsRequest) (*v1.ListRunsResponse, error) {
	msg, found, err := f.call(ctx, "ListRuns", "", req)
	_ = found
	if msg == nil {
		return nil, err
	}
	return msg.(*v1.ListRunsResponse), err
}
func (f *fakeAPI) GetHistory(ctx context.Context, req *v1.GetHistoryRequest) (*v1.GetHistoryResponse, error) {
	msg, found, err := f.call(ctx, "GetHistory", "", req)
	_ = found
	if msg == nil {
		return nil, err
	}
	return msg.(*v1.GetHistoryResponse), err
}
func (f *fakeAPI) ResolveApproval(ctx context.Context, identity string, req *v1.ResolveApprovalRequest) (*v1.ResolveApprovalResponse, error) {
	msg, found, err := f.call(ctx, "ResolveApproval", identity, req)
	_ = found
	if msg == nil {
		return nil, err
	}
	return msg.(*v1.ResolveApprovalResponse), err
}

func testConfig() config.Config {
	return config.Config{APIKeyHashes: map[string][32]byte{"owner": sha256.Sum256([]byte("secret"))}, PollTimeout: 20 * time.Second, MaxMessageBytes: 4 << 20}
}
func closedRun() *v1.RunInfo {
	return &v1.RunInfo{RunId: "r", Status: v1.RunStatus_RUN_STATUS_COMPLETED, Result: opaque()}
}
func opaque() *v1.Payload {
	return &v1.Payload{ContentType: "application/octet-stream", Data: []byte{0xff, 0, 0x81}}
}
func rpcHTTP(t *testing.T, handler http.Handler) (*http.Client, string) {
	t.Helper()
	ts := httptest.NewUnstartedServer(handler)
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	ts.Config.Protocols = protocols
	ts.Start()
	t.Cleanup(ts.Close)
	cp := new(http.Protocols)
	cp.SetUnencryptedHTTP2(true)
	tr := &http.Transport{Protocols: cp}
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr, Timeout: 5 * time.Second}, ts.URL
}

type rpcCase struct {
	name              string
	request, response proto.Message
	invoke            func(context.Context, http.Header) (proto.Message, error)
}

func rpcTestCase[Req, Res any](name string, req *Req, res *Res, call func(context.Context, *connect.Request[Req]) (*connect.Response[Res], error)) rpcCase {
	return rpcCase{name: name, request: any(req).(proto.Message), response: any(res).(proto.Message), invoke: func(ctx context.Context, headers http.Header) (proto.Message, error) {
		r := connect.NewRequest(req)
		for k, vs := range headers {
			r.Header()[k] = vs
		}
		resp, err := call(ctx, r)
		if err != nil {
			return nil, err
		}
		return any(resp.Msg).(proto.Message), nil
	}}
}
func allRPCs(hc *http.Client, url string) []rpcCase {
	w, c := rpc.NewWorkerServiceClient(hc, url), rpc.NewClientServiceClient(hc, url)
	return []rpcCase{
		rpcTestCase("PollWorkflowTask", &v1.PollWorkflowTaskRequest{TaskQueue: "q", Identity: "worker:123", BuildId: "build"}, &v1.PollWorkflowTaskResponse{TaskToken: []byte("claimed"), RunId: "r", Attempt: 2}, w.PollWorkflowTask),
		rpcTestCase("CompleteWorkflowTask", &v1.CompleteWorkflowTaskRequest{TaskToken: []byte("token"), Identity: "worker:123", BuildId: "build", Commands: []*v1.Command{{Attributes: &v1.Command_CompleteRun{CompleteRun: &v1.CompleteRunCommand{Result: opaque()}}}}}, &v1.CompleteWorkflowTaskResponse{}, w.CompleteWorkflowTask),
		rpcTestCase("FailWorkflowTask", &v1.FailWorkflowTaskRequest{TaskToken: []byte("token"), Identity: "worker:123", Cause: v1.TaskFailedCause_TASK_FAILED_CAUSE_SDK_ERROR, Failure: &v1.Failure{Message: "failed"}}, &v1.FailWorkflowTaskResponse{}, w.FailWorkflowTask),
		rpcTestCase("PollActivityTask", &v1.PollActivityTaskRequest{TaskQueue: "q", Identity: "worker:123"}, &v1.PollActivityTaskResponse{TaskToken: []byte("claimed"), RunId: "r", Seq: 10, Input: opaque(), IdempotencyKey: "r/10"}, w.PollActivityTask),
		rpcTestCase("CompleteActivityTask", &v1.CompleteActivityTaskRequest{TaskToken: []byte("token"), Identity: "worker:123", Result: opaque()}, &v1.CompleteActivityTaskResponse{}, w.CompleteActivityTask),
		rpcTestCase("FailActivityTask", &v1.FailActivityTaskRequest{TaskToken: []byte("token"), Identity: "worker:123", Failure: &v1.Failure{Type: "retry"}}, &v1.FailActivityTaskResponse{}, w.FailActivityTask),
		rpcTestCase("HeartbeatActivityTask", &v1.HeartbeatActivityTaskRequest{TaskToken: []byte("token"), Details: opaque()}, &v1.HeartbeatActivityTaskResponse{CancelRequested: true}, w.HeartbeatActivityTask),
		rpcTestCase("ReserveAICall", &v1.ReserveAICallRequest{TaskToken: []byte("token"), Model: "model", EstimateUsd: 1.25}, &v1.ReserveAICallResponse{ReservationId: 33, SpentTodayUsd: 1.25, CapUsd: 2}, w.ReserveAICall),
		rpcTestCase("FinishAICall", &v1.FinishAICallRequest{ReservationId: 99, Ok: true, InputTokens: 11, OutputTokens: 22}, &v1.FinishAICallResponse{CostUsd: 0.2}, w.FinishAICall),
		rpcTestCase("StartRun", &v1.StartRunRequest{RunId: "r", WorkflowType: "wf", TaskQueue: "q", Input: opaque()}, &v1.StartRunResponse{Run: closedRun(), Started: true}, c.StartRun),
		rpcTestCase("SignalRun", &v1.SignalRunRequest{RunId: "r", Name: "signal", RequestId: "dedupe", Input: opaque()}, &v1.SignalRunResponse{}, c.SignalRun),
		rpcTestCase("CancelRun", &v1.CancelRunRequest{RunId: "r", Reason: "cancel"}, &v1.CancelRunResponse{}, c.CancelRun),
		rpcTestCase("TerminateRun", &v1.TerminateRunRequest{RunId: "r", Reason: "operator stop"}, &v1.TerminateRunResponse{}, c.TerminateRun),
		rpcTestCase("ResumeRun", &v1.ResumeRunRequest{RunId: "r", Reason: "fixed"}, &v1.ResumeRunResponse{}, c.ResumeRun),
		rpcTestCase("DescribeRun", &v1.DescribeRunRequest{RunId: "r"}, &v1.DescribeRunResponse{Run: closedRun()}, c.DescribeRun),
		rpcTestCase("AwaitRun", &v1.AwaitRunRequest{RunId: "r"}, &v1.AwaitRunResponse{Run: closedRun(), Closed: true}, c.AwaitRun),
		rpcTestCase("ListRuns", &v1.ListRunsRequest{PageSize: 123, PageToken: "page", WorkflowType: "wf"}, &v1.ListRunsResponse{Runs: []*v1.RunInfo{closedRun()}, NextPageToken: "next"}, c.ListRuns),
		rpcTestCase("GetHistory", &v1.GetHistoryRequest{RunId: "r", AfterEventId: 10, PageSize: 12}, &v1.GetHistoryResponse{Events: []*v1.HistoryEvent{{EventId: 1}}, More: true}, c.GetHistory),
		rpcTestCase("ResolveApproval", &v1.ResolveApprovalRequest{RunId: "r", ApprovalId: "a", Outcome: v1.ApprovalOutcome_APPROVAL_OUTCOME_APPROVED, Resolver: "human", Choice: "yes", Note: "checked"}, &v1.ResolveApprovalResponse{}, c.ResolveApproval),
	}
}
func TestEveryRPCAuthenticatesAndForwards(t *testing.T) {
	f := &fakeAPI{}
	s := New(testConfig(), f, nil, Options{})
	hc, url := rpcHTTP(t, s)
	for _, tc := range allRPCs(hc, url) {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			f.call = func(ctx context.Context, name, identity string, req proto.Message) (proto.Message, bool, error) {
				calls.Add(1)
				if Identity(ctx) != "owner" {
					t.Errorf("context identity=%q", Identity(ctx))
				}
				wantName, wantReq, wantResp := tc.name, tc.request, tc.response
				if tc.name == "AwaitRun" {
					wantName = "DescribeRun"
					wantReq = &v1.DescribeRunRequest{RunId: "r"}
					wantResp = &v1.DescribeRunResponse{Run: closedRun()}
				}
				if name != wantName || !proto.Equal(req, wantReq) {
					t.Errorf("forwarded %s %v; want %s %v", name, req, wantName, wantReq)
				}
				switch name {
				case "StartRun", "SignalRun", "CancelRun", "TerminateRun", "ResumeRun", "ResolveApproval":
					if identity != "owner" {
						t.Errorf("mutation identity=%q", identity)
					}
				}
				return proto.Clone(wantResp), true, nil
			}
			for _, headers := range []http.Header{{}, {"Authorization": {"Bearer wrong"}}, {"Authorization": {"Bearer secret", "Bearer secret"}}} {
				_, err := tc.invoke(context.Background(), headers)
				if connect.CodeOf(err) != connect.CodeUnauthenticated {
					t.Errorf("unauthenticated code=%v", err)
				}
			}
			if calls.Load() != 0 {
				t.Fatalf("unauthenticated engine calls=%d", calls.Load())
			}
			got, err := tc.invoke(context.Background(), http.Header{"Authorization": {"Bearer secret"}})
			if err != nil || !proto.Equal(got, tc.response) {
				t.Fatalf("response=%v err=%v want=%v", got, err, tc.response)
			}
			if calls.Load() != 1 {
				t.Fatalf("engine calls=%d", calls.Load())
			}
		})
	}
}
func TestTerminateRunErrorMapping(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		code connect.Code
	}{
		{"closed", engine.ErrRunClosed, connect.CodeFailedPrecondition},
		{"missing", engine.ErrNotFound, connect.CodeNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeAPI{call: func(context.Context, string, string, proto.Message) (proto.Message, bool, error) {
				return nil, false, fmt.Errorf("terminate: %w", tc.err)
			}}
			hc, url := rpcHTTP(t, New(testConfig(), f, nil, Options{}))
			req := connect.NewRequest(&v1.TerminateRunRequest{RunId: "r", Reason: "operator stop"})
			req.Header().Set("Authorization", "Bearer secret")
			_, err := rpc.NewClientServiceClient(hc, url).TerminateRun(t.Context(), req)
			if connect.CodeOf(err) != tc.code {
				t.Fatalf("err=%v; want %v", err, tc.code)
			}
		})
	}
}
func TestErrorMappingAndInternalPrivacy(t *testing.T) {
	tests := []struct {
		err  error
		code connect.Code
	}{
		{engine.ErrNotFound, connect.CodeNotFound}, {engine.ErrAlreadyExists, connect.CodeAlreadyExists},
		{engine.ErrInvalidArgument, connect.CodeInvalidArgument}, {engine.ErrStaleTask, connect.CodeFailedPrecondition},
		{engine.ErrRunClosed, connect.CodeFailedPrecondition}, {engine.ErrFailedPrecondition, connect.CodeFailedPrecondition},
		{engine.ErrBudgetExceeded, connect.CodeResourceExhausted}, {context.Canceled, connect.CodeCanceled}, {context.DeadlineExceeded, connect.CodeDeadlineExceeded},
		{errors.New("database-password-secret"), connect.CodeInternal}, {connect.NewError(connect.CodeInternal, errors.New("database-password-secret")), connect.CodeInternal},
	}
	for _, tc := range tests {
		t.Run(tc.code.String()+tc.err.Error(), func(t *testing.T) {
			var logs bytes.Buffer
			f := &fakeAPI{call: func(context.Context, string, string, proto.Message) (proto.Message, bool, error) {
				return nil, false, fmt.Errorf("wrapped: %w", tc.err)
			}}
			hc, url := rpcHTTP(t, New(testConfig(), f, nil, Options{Logger: slog.New(slog.NewJSONHandler(&logs, nil))}))
			req := connect.NewRequest(&v1.DescribeRunRequest{RunId: "r"})
			req.Header().Set("Authorization", "Bearer secret")
			_, err := rpc.NewClientServiceClient(hc, url).DescribeRun(context.Background(), req)
			if connect.CodeOf(err) != tc.code {
				t.Fatalf("err=%v; want %v", err, tc.code)
			}
			if tc.code == connect.CodeInternal {
				var ce *connect.Error
				if !errors.As(err, &ce) || ce.Message() != "internal error" {
					t.Fatalf("leaked internal error: %v", err)
				}
				if !strings.Contains(logs.String(), "database-password-secret") || !strings.Contains(logs.String(), rpc.ClientServiceDescribeRunProcedure) {
					t.Fatalf("missing original error/procedure: %s", logs.String())
				}
			}
		})
	}
}
func TestOversizedRequestNeverReachesEngine(t *testing.T) {
	var calls atomic.Int32
	f := &fakeAPI{call: func(context.Context, string, string, proto.Message) (proto.Message, bool, error) {
		calls.Add(1)
		return &v1.StartRunResponse{}, true, nil
	}}
	cfg := testConfig()
	cfg.MaxMessageBytes = 128
	hc, url := rpcHTTP(t, New(cfg, f, nil, Options{}))
	req := connect.NewRequest(&v1.StartRunRequest{Input: &v1.Payload{Data: bytes.Repeat([]byte("x"), 1024)}})
	req.Header().Set("Authorization", "Bearer secret")
	_, err := rpc.NewClientServiceClient(hc, url).StartRun(context.Background(), req)
	if connect.CodeOf(err) != connect.CodeResourceExhausted || calls.Load() != 0 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
}
