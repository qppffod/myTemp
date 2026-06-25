package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/qppffod/myTemp/internal/frontend"
	grpcHandlers "github.com/qppffod/myTemp/internal/frontend/grpc"
	"github.com/qppffod/myTemp/internal/history"
	"github.com/qppffod/myTemp/internal/persistence"
	"github.com/qppffod/myTemp/migrations"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	logLevel := slog.LevelInfo
	if os.Getenv("DEBUG") != "" {
		logLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = "postgres://postgres:password@localhost:5432/myengine?sslmode=disable"
	}

	if err := migrations.RunMigrations(connStr); err != nil {
		logger.Error("migrations failed", "error", err)
		os.Exit(1)
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		logger.Error("create pool failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		logger.Error("ping database failed", "error", err)
		os.Exit(1)
	}

	logger.Info("engine starting", "db", "connected")

	p := persistence.New(pool)
	h := history.New(p, logger)
	handler := grpcHandlers.New(p, h, logger)

	go reclaimLoop(ctx, p, logger)
	go scanTimers(ctx, h, logger)

	if err := frontend.Start(ctx, handler, logger); err != nil {
		logger.Error("server failed", "error", err)
		os.Exit(1)
	}

	logger.Info("engine stopped")
}

func reclaimLoop(ctx context.Context, p *persistence.Persistence, logger *slog.Logger) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logger.Info("reclaim loop stopping")
			return
		case <-ticker.C:
			if err := p.ReclaimExpiredLeases(ctx); err != nil {
				logger.Error("reclaim leases failed", "error", err)
				continue
			}
		}
	}
}

func scanTimers(ctx context.Context, h *history.History, logger *slog.Logger) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logger.Info("timer scanner stopping")
			return
		case <-ticker.C:
			if err := h.ScanTimers(ctx); err != nil {
				logger.Error("scan timers failed", "error", err)
			}
		}
	}
}
