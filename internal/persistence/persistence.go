package persistence

import (
	"context"

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
		 	closed_at = CASE WHEN $1 IN ('Completed', 'Failed', "Canceled') THEN now() ELSE closed_at END
		 WHERE workflow_id = $2 AND run_id = $3`,
		status, workflowID, runID,
	)
	return err
}

func (p *Persistence) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return p.db.Begin(ctx)
}
