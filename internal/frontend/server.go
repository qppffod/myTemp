package frontend

import (
	"context"
	"log/slog"
	"net"
	"os"

	grpcHandlers "github.com/qppffod/myTemp/internal/frontend/grpc"
	enginev1 "github.com/qppffod/myTemp/proto/engine/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const grpcAddr = ":7233"

func Start(ctx context.Context, handler *grpcHandlers.Handler) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("listen failed", "addr", grpcAddr, "err", err)
		os.Exit(1)
	}

	srv := grpc.NewServer()
	enginev1.RegisterEngineServiceServer(srv, handler)
	reflection.Register(srv)

	go func() {
		<-ctx.Done()
		logger.Info("shutting down")
		srv.GracefulStop()
	}()

	logger.Info("engine listening", "addr", grpcAddr)
	if err := srv.Serve(lis); err != nil {
		logger.Error("serve failed", "err", err)
		os.Exit(1)
	}
}
