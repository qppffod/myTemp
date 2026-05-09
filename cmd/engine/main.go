package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/qppffod/myTemp/internal/frontend"
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

	frontend.Start(ctx)
}
