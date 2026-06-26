package grpcHandlers

import (
	"errors"

	"github.com/qppffod/myTemp/internal/history"
	"github.com/qppffod/myTemp/internal/persistence"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// toStatus maps a domain error to a gRPC status. Known sentinels become precise
// client-facing codes; anything else is codes.Internal with a generic message so
// internal details aren't leaked to callers (the full error is logged at the
// call site).
func toStatus(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, history.ErrWorkflowAlreadyRunning):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, history.ErrWorkflowNotRunning):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, persistence.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	default:
		return status.Error(codes.Internal, "internal error")
	}
}
