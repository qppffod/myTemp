package grpcHandlers

import (
	"context"

	"github.com/qppffod/myTemp/internal/history"
	"github.com/qppffod/myTemp/internal/persistence"
	enginev1 "github.com/qppffod/myTemp/proto/engine/v1"
)

type Handler struct {
	enginev1.UnimplementedEngineServiceServer
	persistence *persistence.Persistence
	history     *history.History
}

func New(p *persistence.Persistence, h *history.History) *Handler {
	return &Handler{
		persistence: p,
		history:     h,
	}
}

func (s *Handler) StartWorkflow(ctx context.Context, req *enginev1.StartWorkflowRequest) (*enginev1.StartWorkflowResponse, error) {
	runID, err := s.history.StartWorkflow(ctx, req.WorkflowId, req.WorkflowType, req.TaskQueue, req.Input)
	if err != nil {
		return nil, err
	}
	return &enginev1.StartWorkflowResponse{RunId: runID}, nil
}
