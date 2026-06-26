package grpcHandlers

import (
	"context"
	"log/slog"
	"strings"

	"google.golang.org/grpc"
)

type loggerCtxKey struct{}

// loggerFrom returns the request-scoped logger attached by LoggingInterceptor,
// falling back to def when none is present (e.g. the interceptor isn't wired).
func loggerFrom(ctx context.Context, def *slog.Logger) *slog.Logger {
	if l, ok := ctx.Value(loggerCtxKey{}).(*slog.Logger); ok {
		return l
	}
	return def
}

// LoggingInterceptor attaches a logger tagged with the RPC name to the context
// of every unary request, so individual handlers don't have to repeat it. The
// interceptor only enriches the logger; handlers remain the single place that
// decides what to log.
func LoggingInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		// info.FullMethod is "/engine.v1.EngineService/PollWorkflowTask"; keep
		// only the trailing method name.
		rpc := info.FullMethod[strings.LastIndex(info.FullMethod, "/")+1:]
		ctx = context.WithValue(ctx, loggerCtxKey{}, logger.With("rpc", rpc))
		return handler(ctx, req)
	}
}
