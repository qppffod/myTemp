package grpcHandlers

import (
	"context"
	"log/slog"

	"github.com/qppffod/myTemp/internal/history"
	"github.com/qppffod/myTemp/internal/persistence"
	pb "github.com/qppffod/myTemp/proto/engine/v1"
)

type Handler struct {
	pb.UnimplementedEngineServiceServer
	persistence *persistence.Persistence
	history     *history.History
	logger      *slog.Logger
}

func New(p *persistence.Persistence, h *history.History, logger *slog.Logger) *Handler {
	return &Handler{
		persistence: p,
		history:     h,
		logger:      logger,
	}
}

func (s *Handler) StartWorkflow(ctx context.Context, req *pb.StartWorkflowRequest) (*pb.StartWorkflowResponse, error) {
	runID, err := s.history.StartWorkflow(ctx, req.WorkflowId, req.WorkflowType, req.TaskQueue, req.Input)
	if err != nil {
		return nil, err
	}
	return &pb.StartWorkflowResponse{RunId: runID}, nil
}

func (h *Handler) PollWorkflowTask(ctx context.Context, req *pb.PollWorkflowTaskRequest) (*pb.PollWorkflowTaskResponse, error) {
	task, err := h.persistence.PollTask(ctx, req.TaskQueue, "workflow", req.LeaseOwner)
	if err != nil || task == nil {
		return &pb.PollWorkflowTaskResponse{}, err
	}

	events, err := h.persistence.GetEvents(ctx, task.WorkflowID, task.RunID)
	if err != nil {
		return nil, err
	}

	return &pb.PollWorkflowTaskResponse{
		TaskId:       task.ID,
		WorkflowId:   task.WorkflowID,
		RunId:        task.RunID,
		WorkflowType: task.WorkflowType,
		History:      marshalEvents(events),
	}, nil
}

func (h *Handler) RespondWorkflowTaskCompleted(ctx context.Context, req *pb.RespondWorkflowTaskCompletedRequest) (*pb.RespondWorkflowTaskCompletedResponse, error) {
	err := h.history.CompleteWorkflowTask(ctx, req.TaskId, req.WorkflowId, req.RunId, unmarshalCommands(req.Commands))
	return &pb.RespondWorkflowTaskCompletedResponse{}, err
}

func (h *Handler) PollActivityTask(ctx context.Context, req *pb.PollActivityTaskRequest) (*pb.PollActivityTaskResponse, error) {
	task, err := h.persistence.PollTask(ctx, req.TaskQueue, "activity", req.LeaseOwner)
	if err != nil || task == nil {
		return &pb.PollActivityTaskResponse{}, err
	}

	return &pb.PollActivityTaskResponse{
		TaskId:       task.ID,
		WorkflowId:   task.WorkflowID,
		RunId:        task.RunID,
		ActivityName: task.ActivityName,
		Input:        task.Input,
	}, nil
}

func (h *Handler) RespondActivityTaskCompleted(ctx context.Context, req *pb.RespondActivityTaskCompletedRequest) (*pb.RespondActivityTaskCompletedResponse, error) {
	err := h.history.CompleteActivityTask(ctx, req.TaskId, req.WorkflowId, req.RunId, req.Result)
	return &pb.RespondActivityTaskCompletedResponse{}, err
}

func (h *Handler) RespondActivityTaskFailed(ctx context.Context, req *pb.RespondActivityTaskFailedRequest) (*pb.RespondActivityTaskFailedResponse, error) {
	err := h.history.FailActivityTask(ctx, req.TaskId, req.WorkflowId, req.RunId, req.Error)
	return &pb.RespondActivityTaskFailedResponse{}, err
}

func (h *Handler) SignalWorkflow(ctx context.Context, req *pb.SignalWorkflowRequest) (*pb.SignalWorkflowResponse, error) {
	err := h.history.SignalWorkflow(ctx, req.WorkflowId, req.RunId, req.SignalName, req.Input)
	return &pb.SignalWorkflowResponse{}, err
}

func unmarshalCommands(commands []*pb.Command) []history.Command {
	result := make([]history.Command, len(commands))
	for i, c := range commands {
		result[i] = history.Command{
			Type:          c.Type,
			ActivityName:  c.ActivityName,
			ActivityIndex: c.ActivityIndex,
			TaskQueue:     c.TaskQueue,
			Input:         c.Input,
			TimerIndex:    c.TimerIndex,
			DurationMs:    c.DurationMs,
		}
	}
	return result
}

func marshalEvents(events []persistence.Event) []*pb.HistoryEvent {
	result := make([]*pb.HistoryEvent, len(events))
	for i, e := range events {
		result[i] = &pb.HistoryEvent{
			EventId:       e.EventID,
			EventType:     e.EventType,
			ActivityName:  e.ActivityName,
			ActivityIndex: e.ActivityIndex,
			TimerIndex:    e.TimerIndex,
			Data:          e.Data,
			SignalName:    e.SignalName,
		}
	}
	return result
}
