package server

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	"github.com/Aly700/capstan/internal/engine"
)

func (s *Server) rpcError(ctx context.Context, procedure string, err error, key string) error {
	if err == nil {
		return nil
	}
	var code connect.Code
	switch {
	case errors.Is(err, context.Canceled):
		code = connect.CodeCanceled
	case errors.Is(err, context.DeadlineExceeded):
		code = connect.CodeDeadlineExceeded
	case errors.Is(err, engine.ErrNotFound):
		code = connect.CodeNotFound
	case errors.Is(err, engine.ErrAlreadyExists):
		code = connect.CodeAlreadyExists
	case errors.Is(err, engine.ErrInvalidArgument):
		code = connect.CodeInvalidArgument
	case errors.Is(err, engine.ErrStaleTask), errors.Is(err, engine.ErrRunClosed), errors.Is(err, engine.ErrFailedPrecondition):
		code = connect.CodeFailedPrecondition
	case errors.Is(err, engine.ErrBudgetExceeded):
		code = connect.CodeResourceExhausted
	default:
		s.logger.ErrorContext(ctx, "RPC failed", "procedure", procedure, "error", s.cfg.Redact(err.Error(), key))
		return connect.NewError(connect.CodeInternal, errors.New("internal error"))
	}
	return connect.NewError(code, errors.New(s.cfg.Redact(err.Error(), key)))
}
