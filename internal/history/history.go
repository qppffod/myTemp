package history

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/qppffod/myTemp/internal/metrics"
	"github.com/qppffod/myTemp/internal/persistence"
)

// ErrWorkflowAlreadyRunning is returned by StartWorkflow when an execution for
// the same workflow ID is already in the Running state. It is a client/
// precondition error, so the transport layer can map it to a distinct status
// (e.g. gRPC codes.AlreadyExists) via errors.Is.
var ErrWorkflowAlreadyRunning = errors.New("workflow already running")

// ErrWorkflowNotRunning is returned when an operation requires a Running
// execution but the workflow is in a terminal or absent state (e.g. signaling a
// completed workflow). Maps to gRPC codes.FailedPrecondition.
var ErrWorkflowNotRunning = errors.New("workflow not running")

type History struct {
	p       *persistence.Persistence
	logger  *slog.Logger
	metrics *metrics.Metrics
}

func New(p *persistence.Persistence, logger *slog.Logger, m *metrics.Metrics) *History {
	return &History{p: p, logger: logger, metrics: m}
}

func (h *History) StartWorkflow(ctx context.Context, workflowID, workflowType, taskQueue string, input []byte) (string, error) {
	tx, err := h.p.BeginTx(ctx)
	if err != nil {
		return "", fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	existing, err := h.p.GetLatestExecution(ctx, workflowID)
	if err != nil && !errors.Is(err, persistence.ErrNotFound) {
		return "", fmt.Errorf("get latest execution: %w", err)
	}
	if err == nil && existing.Status == "Running" {
		return "", fmt.Errorf("%s: %w", workflowID, ErrWorkflowAlreadyRunning)
	}

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
	h.metrics.WorkflowsStarted.Inc()

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

	// Tally the durable outcomes applied in this transaction; the counters are
	// incremented only after a successful commit (a task can carry several
	// commands, e.g. multiple scheduled activities).
	var scheduled, timersStarted, completed, failed int

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
				return fmt.Errorf("insert activity scheduled event: %w", err)
			}
			scheduled++

			if err := h.p.InsertTask(ctx, tx, persistence.Task{
				TaskQueue:        cmd.TaskQueue,
				TaskType:         "activity",
				WorkflowType:     exec.WorkflowType,
				WorkflowID:       workflowID,
				RunID:            runID,
				ScheduledEventID: nextEventID,
				ActivityName:     cmd.ActivityName,
				ActivityIndex:    cmd.ActivityIndex,
				Attempt:          1,
				MaxAttempts:      DefaultRetryPolicy.MaxAttempts,
				Input:            cmd.Input,
			}); err != nil {
				return fmt.Errorf("insert activity task: %w", err)
			}
			nextEventID++

		case "StartTimer":

			if err := h.p.InsertEvent(ctx, tx, persistence.Event{
				WorkflowID: workflowID,
				RunID:      runID,
				EventID:    nextEventID,
				EventType:  "TimerStarted",
				TimerIndex: cmd.TimerIndex,
			}); err != nil {
				return fmt.Errorf("insert timer started event: %w", err)
			}
			fireAt := time.Now().Add(time.Duration(cmd.DurationMs) * time.Millisecond)
			if err := h.p.InsertTimer(ctx, tx, persistence.Timer{
				WorkflowID: workflowID,
				RunID:      runID,
				TimerIndex: cmd.TimerIndex,
				FireAt:     fireAt,
			}); err != nil {
				return fmt.Errorf("insert timer: %w", err)
			}
			timersStarted++

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

			if err := h.p.UpdateWorkflowStatus(ctx, tx, workflowID, runID, "Completed"); err != nil {
				return fmt.Errorf("update workflow status: %w", err)
			}
			completed++

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

			if err := h.p.UpdateWorkflowStatus(ctx, tx, workflowID, runID, "Failed"); err != nil {
				return fmt.Errorf("update workflow status: %w", err)
			}
			failed++
		}
	}

	if err := h.p.CompleteTask(ctx, tx, taskID); err != nil {
		return fmt.Errorf("complete workflow task: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	h.metrics.ActivitiesScheduled.Add(float64(scheduled))
	h.metrics.TimersStarted.Add(float64(timersStarted))
	h.metrics.WorkflowsTerminated.WithLabelValues("completed").Add(float64(completed))
	h.metrics.WorkflowsTerminated.WithLabelValues("failed").Add(float64(failed))

	return nil
}

func (h *History) CompleteActivityTask(ctx context.Context, taskID int64, workflowID, runID string, result []byte) error {
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
		EventType:     "ActivityCompleted",
		ActivityName:  task.ActivityName,
		ActivityIndex: task.ActivityIndex,
		Data:          result,
	}); err != nil {
		return fmt.Errorf("insert activity completed event: %w", err)
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
		WorkflowID:       exec.WorkflowID,
		RunID:            exec.RunID,
		ScheduledEventID: nextEventID,
	}); err != nil {
		return fmt.Errorf("insert workflow task: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	h.metrics.ActivitiesFinished.WithLabelValues("completed").Inc()

	return nil
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

	if task.Attempt < task.MaxAttempts {

		delay := netxtRetryDelay(DefaultRetryPolicy, task.Attempt)

		if err := h.p.CompleteTask(ctx, tx, taskID); err != nil {
			return fmt.Errorf("complete failed activity task: %w", err)
		}

		if err := h.p.InsertTask(ctx, tx, persistence.Task{
			TaskQueue:        task.TaskQueue,
			TaskType:         "activity",
			WorkflowType:     task.WorkflowType,
			WorkflowID:       task.WorkflowID,
			RunID:            task.RunID,
			ScheduledEventID: task.ScheduledEventID, // same scheduling event
			ActivityName:     task.ActivityName,
			ActivityIndex:    task.ActivityIndex,
			Input:            task.Input,
			Attempt:          task.Attempt + 1,
			MaxAttempts:      task.MaxAttempts,
			VisibilityTime:   time.Now().Add(delay), // ← the backoff
		}); err != nil {
			return fmt.Errorf("reschedule activity: %w", err)
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit transaction: %w", err)
		}
		h.metrics.ActivityRetries.Inc()

		return nil
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
		return fmt.Errorf("complete failed activity task: %w", err)
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

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	h.metrics.ActivitiesFinished.WithLabelValues("failed").Inc()

	return nil
}

func (h *History) ScanTimers(ctx context.Context) error {
	tx, err := h.p.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	dueTimers, err := h.p.GetDueTimers(ctx, tx)
	if err != nil {
		return fmt.Errorf("get due timers: %w", err)
	}

	for _, timer := range dueTimers {
		events, err := h.p.GetEvents(ctx, timer.WorkflowID, timer.RunID)
		if err != nil {
			return fmt.Errorf("get events: %w", err)
		}
		nextEventID := int64(len(events)) + 1

		if err := h.p.InsertEvent(ctx, tx, persistence.Event{
			WorkflowID: timer.WorkflowID,
			RunID:      timer.RunID,
			EventID:    nextEventID,
			EventType:  "TimerFired",
			TimerIndex: timer.TimerIndex,
		}); err != nil {
			return fmt.Errorf("insert timer fired event: %w", err)
		}

		if err := h.p.MarkTimerFired(ctx, tx, timer.ID); err != nil {
			return fmt.Errorf("mark timer fired: %w", err)
		}

		exec, err := h.p.GetWorkflowExecution(ctx, timer.WorkflowID, timer.RunID)
		if err != nil {
			return fmt.Errorf("get workflow execution: %w", err)
		}
		if err := h.p.InsertTask(ctx, tx, persistence.Task{
			TaskQueue:        exec.TaskQueue,
			TaskType:         "workflow",
			WorkflowType:     exec.WorkflowType,
			WorkflowID:       timer.WorkflowID,
			RunID:            timer.RunID,
			ScheduledEventID: nextEventID,
		}); err != nil {
			return fmt.Errorf("insert workflow task: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	h.metrics.TimersFired.Add(float64(len(dueTimers)))

	return nil
}

func (h *History) SignalWorkflow(ctx context.Context, workflowID, runID, signalName string, input []byte) error {
	if runID == "" {
		exec, err := h.p.GetLatestExecution(ctx, workflowID)
		if err != nil {
			return fmt.Errorf("get latest execution: %w", err)
		}
		runID = exec.RunID
	}

	tx, err := h.p.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Verify the workflow is running.
	exec, err := h.p.GetWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		return fmt.Errorf("get workflow execution: %w", err)
	}
	if exec.Status != "Running" {
		return fmt.Errorf("status %s: %w", exec.Status, ErrWorkflowNotRunning)
	}

	events, err := h.p.GetEvents(ctx, workflowID, runID)
	if err != nil {
		return fmt.Errorf("get events: %w", err)
	}
	nextEventID := int64(len(events)) + 1

	if err := h.p.InsertEvent(ctx, tx, persistence.Event{
		WorkflowID: workflowID,
		RunID:      runID,
		EventID:    nextEventID,
		EventType:  "SignalReceived",
		SignalName: signalName,
		Data:       input,
	}); err != nil {
		return fmt.Errorf("insert signal received event: %w", err)
	}

	if err := h.p.InsertWorkflowTaskIfNotExists(ctx, tx, persistence.Task{
		TaskQueue:        exec.TaskQueue,
		TaskType:         "workflow",
		WorkflowType:     exec.WorkflowType,
		WorkflowID:       workflowID,
		RunID:            runID,
		ScheduledEventID: nextEventID,
	}); err != nil {
		return fmt.Errorf("insert workflow task: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	h.metrics.SignalsDelivered.Inc()

	return nil
}
