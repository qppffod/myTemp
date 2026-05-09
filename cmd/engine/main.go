package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/qppffod/myTemp/internal/frontend"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	frontend.Start(ctx)
}
