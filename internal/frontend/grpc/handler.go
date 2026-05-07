package grpcHandlers

import (
	"context"

	enginev1 "github.com/qppffod/myTemp/proto/engine/v1"
)

type Server struct {
	enginev1.UnimplementedEngineServiceServer
}

func New() *Server {
	return &Server{}
}

func (s *Server) StartWorkflow(ctx context.Context, req *enginev1.StartWorkflowRequest) (*enginev1.StartWorkflowResponse, error) {
	return &enginev1.StartWorkflowResponse{}, nil
}
