package history

import (
	"context"

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
	tx, _ := h.p.BeginTx(ctx)
	defer tx.Rollback(ctx)

	runID := uuid.New().String()

	h.p.InsertWorkflowExecution(ctx, tx, persistence.WorkflowExecution{
		WorkflowID:   workflowID,
		RunID:        runID,
		WorkflowType: workflowType,
		TaskQueue:    taskQueue,
		Status:       "Running",
	})

	h.p.InsertEvent(ctx, tx, persistence.Event{
		WorkflowID: workflowID,
		RunID:      runID,
		EventID:    1,
		EventType:  "WorkflowStarted",
		Data:       input,
	})

	h.p.InsertTask(ctx, tx, persistence.Task{
		TaskQueue:        taskQueue,
		TaskType:         "workflow",
		WorkflowID:       workflowID,
		RunID:            runID,
		ScheduledEventID: 1,
		Input:            input,
	})

	tx.Commit(ctx)
	return runID, nil
}
