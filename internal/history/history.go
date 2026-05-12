package history

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/qppffod/myTemp/internal/persistence"
)

type History struct {
	p *persistence.Persistence
}

func New(p *persistence.Persistence) *History {
	return &History{
		p: p,
	}
}

func (h *History) StartWorkflow(ctx context.Context, workflowID, workflowType, taskQueue string, input []byte) (string, error) {
	tx, err := h.p.BeginTx(ctx)
	if err != nil {
		return "", fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	runID := uuid.New().String()

	if err := h.p.InsertWorkflowExecution(ctx, tx, persistence.WorkflowExecution{
		WorkflowID:   workflowID,
		RunID:        runID,
		WorkflowType: workflowType,
		TaskQueue:    taskQueue,
		Status:       "Running",
	}); err != nil {
		return "", fmt.Errorf("insert workflow execution: %w", err)
	}

	if err := h.p.InsertEvent(ctx, tx, persistence.Event{
		WorkflowID: workflowID,
		RunID:      runID,
		EventID:    1,
		EventType:  "WorkflowStarted",
		Data:       input,
	}); err != nil {
		return "", fmt.Errorf("insert workflow started event: %w", err)
	}

	if err := h.p.InsertTask(ctx, tx, persistence.Task{
		TaskQueue:        taskQueue,
		TaskType:         "workflow",
		WorkflowID:       workflowID,
		RunID:            runID,
		ScheduledEventID: 1,
		Input:            input,
	}); err != nil {
		return "", fmt.Errorf("insert workflow task: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit transaction: %w", err)
	}
	return runID, nil
}
