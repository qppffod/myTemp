package testutil

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/qppffod/myTemp/internal/history"
	"github.com/qppffod/myTemp/internal/persistence"
	"github.com/qppffod/myTemp/migrations"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func SetupEngine(t *testing.T) (*history.History, *persistence.Persistence, *pgxpool.Pool) {
	t.Helper()
	pool, _ := StartTestPostgres(t)
	p := persistence.New(pool)
	h := history.New(p)
	return h, p, pool
}

func StartTestPostgres(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	ctx := context.Background()

	container, err := postgres.Run(ctx, "postgres:16",
		postgres.WithDatabase("test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2)),
	)
	require.NoError(t, err)

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	require.NoError(t, migrations.RunMigrations(connStr))

	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)

	cleanup := func() {
		pool.Close()
		container.Terminate(ctx)
	}

	return pool, cleanup
}

func GetEvents(t *testing.T, p *persistence.Persistence, workflowID, runID string) []persistence.Event {
	t.Helper()
	events, err := p.GetEvents(t.Context(), workflowID, runID)
	require.NoError(t, err)
	return events
}
