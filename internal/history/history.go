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
		WorkflowID:   workflowID,
		RunID:        runID,
		EventID:      1,
		EventType:    "WorkflowStarted",
		ActivityName: "",
		Data:         input,
	}); err != nil {
		return "", fmt.Errorf("insert workflow started event: %w", err)
	}

	if err := h.p.InsertTask(ctx, tx, persistence.Task{
		TaskQueue:        taskQueue,
		TaskType:         "workflow",
		WorkflowType:     workflowType,
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

func (h *History) CompleteWorkflowTask(ctx context.Context, taskID int64, workflowID, runID string, commands []Command) error {
	tx, err := h.p.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	exec, err := h.p.GetWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		return fmt.Errorf("get workflow execution: %w", err)
	}

	if exec.Status != "Running" {
		if err := h.p.CompleteTask(ctx, tx, taskID); err != nil {
			return fmt.Errorf("complete workflow task: %w", err)
		}
		return tx.Commit(ctx)
	}

	events, err := h.p.GetEvents(ctx, workflowID, runID)
	if err != nil {
		return fmt.Errorf("get events: %w", err)
	}
	nextEventID := int64(len(events)) + 1

	for _, cmd := range commands {
		switch cmd.Type {
		case "ScheduleActivity":

			if err := h.p.InsertEvent(ctx, tx, persistence.Event{
				WorkflowID:    workflowID,
				RunID:         runID,
				EventID:       nextEventID,
				EventType:     "ActivityScheduled",
				ActivityName:  cmd.ActivityName,
				ActivityIndex: cmd.ActivityIndex,
				Data:          cmd.Input,
			}); err != nil {
				return err
			}

			if err := h.p.InsertTask(ctx, tx, persistence.Task{
				TaskQueue:        cmd.TaskQueue,
				TaskType:         "activity",
				WorkflowType:     exec.WorkflowType,
				WorkflowID:       workflowID,
				RunID:            runID,
				ScheduledEventID: nextEventID,
				ActivityName:     cmd.ActivityName,
				ActivityIndex:    cmd.ActivityIndex,
				Input:            cmd.Input,
			}); err != nil {
				return err
			}
			nextEventID++

		case "CompleteWorkflow":
			if err := h.p.InsertEvent(ctx, tx, persistence.Event{
				WorkflowID: workflowID,
				RunID:      runID,
				EventID:    nextEventID,
				EventType:  "WorkflowCompleted",
				Data:       cmd.Input,
			}); err != nil {
				return fmt.Errorf("insert workflow completed event: %w", err)
			}

			err := h.p.UpdateWorkflowStatus(ctx, tx, workflowID, runID, "Completed")
			if err != nil {
				return err
			}

		case "FailWorkflow":
			if err := h.p.InsertEvent(ctx, tx, persistence.Event{
				WorkflowID:   workflowID,
				RunID:        runID,
				EventID:      nextEventID,
				EventType:    "WorkflowFailed",
				ActivityName: cmd.ActivityName,
				Data:         cmd.Input,
			}); err != nil {
				return fmt.Errorf("insert workflow failed event: %w", err)
			}

			err := h.p.UpdateWorkflowStatus(ctx, tx, workflowID, runID, "Failed")
			if err != nil {
				return err
			}
		}
	}

	if err := h.p.CompleteTask(ctx, tx, taskID); err != nil {
		return fmt.Errorf("complete workflow task: %w", err)
	}

	return tx.Commit(ctx)
}

func (h *History) CompleteActivityTask(ctx context.Context, taskID int64, workflowID, runID string, result []byte) error {
	tx, err := h.p.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	task, err := h.p.GetTask(ctx, taskID)
	if err != nil {
		return fmt.Errorf("GetTask: %w", err)
	}

	events, err := h.p.GetEvents(ctx, workflowID, runID)
	if err != nil {
		return fmt.Errorf("get events: %w", err)
	}
	nextEventID := int64(len(events)) + 1

	if err := h.p.InsertEvent(ctx, tx, persistence.Event{
		WorkflowID:    workflowID,
		RunID:         runID,
		EventID:       nextEventID,
		EventType:     "ActivityCompleted",
		ActivityName:  task.ActivityName,
		ActivityIndex: task.ActivityIndex,
		Data:          result,
	}); err != nil {
		return fmt.Errorf("insert complete activity event: %w", err)
	}

	if err := h.p.CompleteTask(ctx, tx, taskID); err != nil {
		return fmt.Errorf("complete activity task: %w", err)
	}

	exec, err := h.p.GetWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		return fmt.Errorf("GetWorkflowExecution: %w", err)
	}
	if err := h.p.InsertTask(ctx, tx, persistence.Task{
		TaskQueue:        exec.TaskQueue,
		TaskType:         "workflow",
		WorkflowType:     exec.WorkflowType,
		WorkflowID:       exec.WorkflowID,
		RunID:            exec.RunID,
		ScheduledEventID: nextEventID,
	}); err != nil {
		return fmt.Errorf("InsertTask: %w", err)
	}

	return tx.Commit(ctx)
}

func (h *History) FailActivityTask(ctx context.Context, taskID int64, workflowID, runID, errMsg string) error {
	tx, err := h.p.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	task, err := h.p.GetTask(ctx, taskID)
	if err != nil {
		return fmt.Errorf("get task: %w", err)
	}

	events, err := h.p.GetEvents(ctx, workflowID, runID)
	if err != nil {
		return fmt.Errorf("get events: %w", err)
	}
	nextEventID := int64(len(events)) + 1

	if err := h.p.InsertEvent(ctx, tx, persistence.Event{
		WorkflowID:    workflowID,
		RunID:         runID,
		EventID:       nextEventID,
		EventType:     "ActivityFailed",
		ActivityName:  task.ActivityName,
		ActivityIndex: task.ActivityIndex,
		Data:          []byte(errMsg),
	}); err != nil {
		return fmt.Errorf("insert activity failed event: %w", err)
	}

	if err := h.p.CompleteTask(ctx, tx, taskID); err != nil {
		return fmt.Errorf("complete activity task: %w", err)
	}

	exec, err := h.p.GetWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		return fmt.Errorf("get workflow execution: %w", err)
	}

	if err := h.p.InsertTask(ctx, tx, persistence.Task{
		TaskQueue:        exec.TaskQueue,
		TaskType:         "workflow",
		WorkflowType:     exec.WorkflowType,
		WorkflowID:       workflowID,
		RunID:            runID,
		ScheduledEventID: nextEventID,
	}); err != nil {
		return fmt.Errorf("insert workflow task: %w", err)
	}

	return tx.Commit(ctx)
}
