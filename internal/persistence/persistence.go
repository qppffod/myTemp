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

func (p *Persistence) InsertWorkflowExecution(ctx, tx pgx.Tx, w WorkflowExecution) error
func (p *Persistence) GetWorkflowExecution(ctx, workflowID, runID string) (*WorkflowExecution, error)
func (p *Persistence) UpdateWorkflowStatus(ctx, tx pgx.Tx, workflowID, runID, status string) error

func (p *Persistence) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return p.db.Begin(ctx)
}
