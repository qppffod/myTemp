package grpcHandlers

import (
	"context"

	"github.com/qppffod/myTemp/internal/history"
	"github.com/qppffod/myTemp/internal/persistence"
	enginev1 "github.com/qppffod/myTemp/proto/engine/v1"
)

type Server struct {
	enginev1.UnimplementedEngineServiceServer
	persistence *persistence.Persistence
	history     *history.History
}

func New(p *persistence.Persistence, h *history.History) *Server {
	return &Server{
		persistence: p,
		history:     h,
	}
}

func (s *Server) StartWorkflow(ctx context.Context, req *enginev1.StartWorkflowRequest) (*enginev1.StartWorkflowResponse, error) {
	return &enginev1.StartWorkflowResponse{}, nil
}
