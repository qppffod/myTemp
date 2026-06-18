package persistence

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Persistence struct {
	db *pgxpool.Pool
}

func New(db *pgxpool.Pool) *Persistence {
	return &Persistence{
		db: db,
	}
}

func (p *Persistence) InsertWorkflowExecution(ctx context.Context, tx pgx.Tx, w WorkflowExecution) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO workflow_executions (workflow_id, run_id, workflow_type, task_queue, status)
		VALUES ($1, $2, $3, $4, $5)`,
		w.WorkflowID, w.RunID, w.WorkflowType, w.TaskQueue, w.Status,
	)
	return err
}

func (p *Persistence) GetWorkflowExecution(ctx context.Context, workflowID, runID string) (*WorkflowExecution, error) {
	row := p.db.QueryRow(ctx,
		`SELECT workflow_id, run_id, workflow_type, task_queue, status, started_at, closed_at
	     FROM workflow_executions
		 WHERE workflow_id = $1 AND run_id = $2`,
		workflowID, runID,
	)

	var w WorkflowExecution
	err := row.Scan(
		&w.WorkflowID,
		&w.RunID,
		&w.WorkflowType,
		&w.TaskQueue,
		&w.Status,
		&w.StartedAt,
		&w.ClosedAt,
	)
	if err != nil {
		return nil, err
	}

	return &w, nil
}

func (p *Persistence) UpdateWorkflowStatus(ctx context.Context, tx pgx.Tx, workflowID, runID, status string) error {
	_, err := tx.Exec(ctx,
		`UPDATE workflow_executions
		 SET status = $1,
		 	closed_at = CASE WHEN $1 IN ('Completed', 'Failed', 'Canceled') THEN now() ELSE closed_at END
		 WHERE workflow_id = $2 AND run_id = $3`,
		status, workflowID, runID,
	)
	return err
}

func (p *Persistence) InsertEvent(ctx context.Context, tx pgx.Tx, e Event) error {
	if e.Data == nil {
		e.Data = []byte{}
	}
	_, err := tx.Exec(ctx,
		`INSERT INTO events (workflow_id, run_id, event_id, event_type, activity_name,
							 activity_index, signal_name, timer_index, data)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		e.WorkflowID, e.RunID, e.EventID, e.EventType, e.ActivityName, e.ActivityIndex, e.SignalName, e.TimerIndex, e.Data,
	)
	return err
}

func (p *Persistence) GetEvents(ctx context.Context, workflowID, runID string) ([]Event, error) {
	rows, err := p.db.Query(ctx,
		`SELECT id, workflow_id, run_id, event_id, event_type, activity_name, activity_index, signal_name, timer_index, data, created_at
		 FROM events
		 WHERE workflow_id = $1 AND run_id = $2
		 ORDER BY event_id`,
		workflowID, runID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []Event

	for rows.Next() {
		var e Event

		if err := rows.Scan(
			&e.ID,
			&e.WorkflowID,
			&e.RunID,
			&e.EventID,
			&e.EventType,
			&e.ActivityName,
			&e.ActivityIndex,
			&e.SignalName,
			&e.TimerIndex,
			&e.Data,
			&e.CreatedAt,
		); err != nil {
			return nil, err
		}

		events = append(events, e)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return events, nil
}

func (p *Persistence) InsertTask(ctx context.Context, tx pgx.Tx, t Task) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO tasks (task_queue, task_type, workflow_type, workflow_id, run_id,
							scheduled_event_id, input, activity_name, activity_index,
							attempt, max_attempts, visibility_time, lease_owner, lease_expires_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
		t.TaskQueue, t.TaskType, t.WorkflowType, t.WorkflowID, t.RunID, t.ScheduledEventID,
		t.Input, t.ActivityName, t.ActivityIndex, t.Attempt, t.MaxAttempts, t.VisibilityTime,
		t.LeaseOwner, t.LeaseExpiresAt,
	)
	return err
}

func (p *Persistence) PollTask(ctx context.Context, queue, taskType, leaseOwner string) (*Task, error) {
	tx, err := p.BeginTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx,
		`SELECT id, task_queue, task_type, workflow_type, workflow_id, run_id,
				scheduled_event_id, input, activity_name, activity_index,
				attempt, max_attempts, visibility_time, lease_owner, lease_expires_at
		 FROM tasks
		 WHERE task_queue = $1
		   AND task_type = $2
		   AND lease_owner IS NULL
		   AND visibility_time <= now()
		 ORDER BY id
		 LIMIT 1
		 FOR UPDATE SKIP LOCKED`,
		queue, taskType,
	)

	var t Task
	err = row.Scan(
		&t.ID,
		&t.TaskQueue,
		&t.TaskType,
		&t.WorkflowType,
		&t.WorkflowID,
		&t.RunID,
		&t.ScheduledEventID,
		&t.Input,
		&t.ActivityName,
		&t.ActivityIndex,
		&t.Attempt,
		&t.MaxAttempts,
		&t.VisibilityTime,
		&t.LeaseOwner,
		&t.LeaseExpiresAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil // no task availabe, not an error
	}
	if err != nil {
		return nil, err
	}

	if _, err := tx.Exec(ctx,
		`UPDATE tasks
		 SET lease_owner = $1,
		 	lease_expires_at = now() + interval '30 seconds'
		 WHERE id = $2`,
		leaseOwner, t.ID,
	); err != nil {
		return nil, fmt.Errorf("Set lease_owner: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit transaction: %w", err)
	}

	return &t, nil
}

func (p *Persistence) GetTask(ctx context.Context, taskID int64) (*Task, error) {
	row := p.db.QueryRow(ctx,
		`SELECT id, task_queue, task_type, workflow_type, workflow_id, run_id,
				scheduled_event_id, input, activity_name, activity_index,
				attempt, max_attempts, visibility_time, lease_owner, lease_expires_at
		 FROM tasks
		 WHERE id = $1`,
		taskID,
	)

	var t Task
	err := row.Scan(
		&t.ID,
		&t.TaskQueue,
		&t.TaskType,
		&t.WorkflowType,
		&t.WorkflowID,
		&t.RunID,
		&t.ScheduledEventID,
		&t.Input,
		&t.ActivityName,
		&t.ActivityIndex,
		&t.Attempt,
		&t.MaxAttempts,
		&t.VisibilityTime,
		&t.LeaseOwner,
		&t.LeaseExpiresAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return &t, nil
}

func (p *Persistence) ReclaimExpiredLeases(ctx context.Context) error {
	_, err := p.db.Exec(ctx,
		`UPDATE tasks
		 SET lease_owner = NULL,
		 	 lease_expires_at = NULL
		 WHERE lease_owner IS NOT NULL AND lease_expires_at < now()`)
	return err
}

func (p *Persistence) CompleteTask(ctx context.Context, tx pgx.Tx, taskID int64) error {
	_, err := tx.Exec(ctx,
		`DELETE FROM tasks
		 WHERE id = $1`,
		taskID,
	)
	return err
}

func (p *Persistence) InsertTimer(ctx context.Context, tx pgx.Tx, t Timer) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO timers (workflow_id, run_id, timer_index, fire_at)
		 VALUES ($1, $2, $3, $4)`,
		t.WorkflowID, t.RunID, t.TimerIndex, t.FireAt,
	)
	return err
}

func (p *Persistence) GetDueTimers(ctx context.Context, tx pgx.Tx) ([]Timer, error) {
	rows, err := tx.Query(ctx,
		`SELECT id, workflow_id, run_id, timer_index, fire_at, fired
		 FROM timers
		 WHERE fire_at <= now()
		 	AND fired = false
		 FOR UPDATE SKIP LOCKED
		 LIMIT 100`,
	)
	if err != nil {
		return nil, fmt.Errorf("GetDueTimers: %w", err)
	}
	defer rows.Close()

	var timers []Timer

	for rows.Next() {
		var t Timer

		if err := rows.Scan(
			&t.ID,
			&t.WorkflowID,
			&t.RunID,
			&t.TimerIndex,
			&t.FireAt,
			&t.Fired,
		); err != nil {
			return nil, fmt.Errorf("scan timer: %w", err)
		}

		timers = append(timers, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return timers, nil
}

func (p *Persistence) MarkTimerFired(ctx context.Context, tx pgx.Tx, timerID int64) error {
	_, err := tx.Exec(ctx,
		`UPDATE timers
		 SET fired = true
		 WHERE id = $1
		 	AND fired = false`,
		timerID,
	)
	return err
}

func (p *Persistence) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return p.db.Begin(ctx)
}

func (p *Persistence) InsertWorkflowTaskIfNotExists(ctx context.Context, tx pgx.Tx, t Task) error {
	_, err := tx.Exec(ctx, `
        INSERT INTO tasks (task_queue, task_type, workflow_type,
                           workflow_id, run_id, scheduled_event_id)
        SELECT $1, $2, $3, $4, $5, $6
        WHERE NOT EXISTS (
            SELECT 1 FROM tasks
            WHERE workflow_id = $4 AND run_id = $5 AND task_type = 'workflow'
        )`,
		t.TaskQueue, t.TaskType, t.WorkflowType, t.WorkflowID, t.RunID, t.ScheduledEventID,
	)
	return err
}
