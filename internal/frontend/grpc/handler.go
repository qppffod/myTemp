package grpcHandlers

import (
	"context"
	"log/slog"

	"github.com/qppffod/myTemp/internal/history"
	"github.com/qppffod/myTemp/internal/metrics"
	"github.com/qppffod/myTemp/internal/persistence"
	pb "github.com/qppffod/myTemp/proto/engine/v1"
)

type Handler struct {
	pb.UnimplementedEngineServiceServer
	persistence *persistence.Persistence
	history     *history.History
	logger      *slog.Logger
	metrics     *metrics.Metrics
}

func New(p *persistence.Persistence, h *history.History, logger *slog.Logger, m *metrics.Metrics) *Handler {
	return &Handler{
		persistence: p,
		history:     h,
		logger:      logger,
		metrics:     m,
	}
}

func (h *Handler) StartWorkflow(ctx context.Context, req *pb.StartWorkflowRequest) (*pb.StartWorkflowResponse, error) {
	runID, err := h.history.StartWorkflow(ctx, req.WorkflowId, req.WorkflowType, req.TaskQueue, req.Input)
	if err != nil {
		loggerFrom(ctx, h.logger).Error("rpc failed",
			"workflow_id", req.WorkflowId,
			"error", err,
		)
		return nil, toStatus(err)
	}
	return &pb.StartWorkflowResponse{RunId: runID}, nil
}

func (h *Handler) PollWorkflowTask(ctx context.Context, req *pb.PollWorkflowTaskRequest) (*pb.PollWorkflowTaskResponse, error) {
	logger := loggerFrom(ctx, h.logger)

	task, err := h.persistence.PollTask(ctx, req.TaskQueue, "workflow", req.LeaseOwner)
	if err != nil {
		logger.Error("rpc failed", "task_queue", req.TaskQueue, "error", err)
		return &pb.PollWorkflowTaskResponse{}, toStatus(err)
	}
	if task == nil {
		h.metrics.TaskPolls.WithLabelValues("workflow", "miss").Inc()
		return &pb.PollWorkflowTaskResponse{}, nil
	}
	// hit counted at lease time, before GetEvents
	h.metrics.TaskPolls.WithLabelValues("workflow", "hit").Inc()

	events, err := h.persistence.GetEvents(ctx, task.WorkflowID, task.RunID)
	if err != nil {
		logger.Error("rpc failed",
			"workflow_id", task.WorkflowID,
			"run_id", task.RunID,
			"task_id", task.ID,
			"error", err,
		)
		return nil, toStatus(err)
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
	if err := h.history.CompleteWorkflowTask(ctx, req.TaskId, req.WorkflowId, req.RunId, unmarshalCommands(req.Commands)); err != nil {
		loggerFrom(ctx, h.logger).Error("rpc failed",
			"workflow_id", req.WorkflowId,
			"run_id", req.RunId,
			"task_id", req.TaskId,
			"error", err,
		)
		return &pb.RespondWorkflowTaskCompletedResponse{}, toStatus(err)
	}
	return &pb.RespondWorkflowTaskCompletedResponse{}, nil
}

func (h *Handler) PollActivityTask(ctx context.Context, req *pb.PollActivityTaskRequest) (*pb.PollActivityTaskResponse, error) {
	task, err := h.persistence.PollTask(ctx, req.TaskQueue, "activity", req.LeaseOwner)
	if err != nil {
		loggerFrom(ctx, h.logger).Error("rpc failed", "task_queue", req.TaskQueue, "error", err)
		return &pb.PollActivityTaskResponse{}, toStatus(err)
	}
	if task == nil {
		h.metrics.TaskPolls.WithLabelValues("activity", "miss").Inc()
		return &pb.PollActivityTaskResponse{}, nil
	}
	h.metrics.TaskPolls.WithLabelValues("activity", "hit").Inc()

	return &pb.PollActivityTaskResponse{
		TaskId:       task.ID,
		WorkflowId:   task.WorkflowID,
		RunId:        task.RunID,
		ActivityName: task.ActivityName,
		Input:        task.Input,
	}, nil
}

func (h *Handler) RespondActivityTaskCompleted(ctx context.Context, req *pb.RespondActivityTaskCompletedRequest) (*pb.RespondActivityTaskCompletedResponse, error) {
	if err := h.history.CompleteActivityTask(ctx, req.TaskId, req.WorkflowId, req.RunId, req.Result); err != nil {
		loggerFrom(ctx, h.logger).Error("rpc failed",
			"workflow_id", req.WorkflowId,
			"run_id", req.RunId,
			"task_id", req.TaskId,
			"error", err,
		)
		return &pb.RespondActivityTaskCompletedResponse{}, toStatus(err)
	}
	return &pb.RespondActivityTaskCompletedResponse{}, nil
}

func (h *Handler) RespondActivityTaskFailed(ctx context.Context, req *pb.RespondActivityTaskFailedRequest) (*pb.RespondActivityTaskFailedResponse, error) {
	if err := h.history.FailActivityTask(ctx, req.TaskId, req.WorkflowId, req.RunId, req.Error); err != nil {
		loggerFrom(ctx, h.logger).Error("rpc failed",
			"workflow_id", req.WorkflowId,
			"run_id", req.RunId,
			"task_id", req.TaskId,
			"error", err,
		)
		return &pb.RespondActivityTaskFailedResponse{}, toStatus(err)
	}
	return &pb.RespondActivityTaskFailedResponse{}, nil
}

func (h *Handler) SignalWorkflow(ctx context.Context, req *pb.SignalWorkflowRequest) (*pb.SignalWorkflowResponse, error) {
	if err := h.history.SignalWorkflow(ctx, req.WorkflowId, req.RunId, req.SignalName, req.Input); err != nil {
		loggerFrom(ctx, h.logger).Error("rpc failed",
			"workflow_id", req.WorkflowId,
			"run_id", req.RunId,
			"signal_name", req.SignalName,
			"error", err,
		)
		return &pb.SignalWorkflowResponse{}, toStatus(err)
	}
	return &pb.SignalWorkflowResponse{}, nil
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
