package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/qppffod/myTemp/internal/frontend"
	grpcHandlers "github.com/qppffod/myTemp/internal/frontend/grpc"
	"github.com/qppffod/myTemp/internal/history"
	"github.com/qppffod/myTemp/internal/metrics"
	"github.com/qppffod/myTemp/internal/persistence"
	"github.com/qppffod/myTemp/migrations"
)

const metricsAddr = ":2112"

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

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	m := metrics.New(reg)

	p := persistence.New(pool)
	h := history.New(p, logger, m)
	handler := grpcHandlers.New(p, h, logger, m)

	go reclaimLoop(ctx, p, m, logger)
	go scanTimers(ctx, h, logger)
	go serveMetrics(ctx, reg, logger)

	if err := frontend.Start(ctx, handler, logger, reg); err != nil {
		logger.Error("server failed", "error", err)
		os.Exit(1)
	}

	logger.Info("engine stopped")
}

func serveMetrics(ctx context.Context, reg *prometheus.Registry, logger *slog.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	srv := &http.Server{Addr: metricsAddr, Handler: mux}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("metrics server shutdown failed", "error", err)
		}
	}()

	logger.Info("metrics listening", "addr", metricsAddr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("metrics server failed", "error", err)
	}
}

func reclaimLoop(ctx context.Context, p *persistence.Persistence, m *metrics.Metrics, logger *slog.Logger) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logger.Info("reclaim loop stopping")
			return
		case <-ticker.C:
			n, err := p.ReclaimExpiredLeases(ctx)
			if err != nil {
				logger.Error("reclaim leases failed", "error", err)
				continue
			}
			m.LeasesReclaimed.Add(float64(n))
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
