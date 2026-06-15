package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/qppffod/myTemp/internal/frontend"
	grpcHandlers "github.com/qppffod/myTemp/internal/frontend/grpc"
	"github.com/qppffod/myTemp/internal/history"
	"github.com/qppffod/myTemp/internal/persistence"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = "postgres://postgres:password@localhost:5432/myengine?sslmode=disable"
	}

	if err := persistence.RunMigrations(connStr); err != nil {
		log.Fatalf("migrations: %v", err)
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		log.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("ping: %v", err)
	}

	p := persistence.New(pool)
	h := history.New(p)

	handler := grpcHandlers.New(p, h)

	go reclaimLoop(ctx, p)

	if err := frontend.Start(ctx, handler); err != nil {
		log.Fatalf("server: %v", err)
	}
}

func reclaimLoop(ctx context.Context, p *persistence.Persistence) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.ReclaimExpiredLeases(ctx); err != nil {
				log.Printf("reclaim leases: %v", err)
			}
		}
	}
}
