package server

import (
	"context"
	"errors"
	"time"

	"connectrpc.com/connect"
	"github.com/Aly700/capstan/internal/auth"
)

type identityKey struct{}

// Identity is the authenticated API-key name, independent of a worker's own
// request identity (for example hostname:pid).
func Identity(ctx context.Context) string {
	name, _ := ctx.Value(identityKey{}).(string)
	return name
}

func (s *Server) authenticate() connect.Interceptor {
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			headers := req.Header().Values("Authorization")
			if len(headers) != 1 {
				return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid API key"))
			}
			name, ok := auth.Identify(headers[0], s.cfg.APIKeyHashes)
			if !ok {
				return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid API key"))
			}
			return next(context.WithValue(ctx, identityKey{}, name), req)
		}
	})
}

func (s *Server) errors() connect.Interceptor {
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			resp, err := next(ctx, req)
			return resp, s.rpcError(ctx, req.Spec().Procedure, err)
		}
	})
}

func (s *Server) measure() connect.Interceptor {
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			start := time.Now()
			resp, err := next(ctx, req)
			s.metrics.recordRPC(req.Spec().Procedure, err, time.Since(start))
			return resp, err
		}
	})
}
