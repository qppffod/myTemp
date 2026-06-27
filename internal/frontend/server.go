package frontend

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	grpcHandlers "github.com/qppffod/myTemp/internal/frontend/grpc"
	enginev1 "github.com/qppffod/myTemp/proto/engine/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const grpcAddr = ":7233"

func Start(ctx context.Context, handler *grpcHandlers.Handler, logger *slog.Logger, reg prometheus.Registerer) error {
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("listen failed", "addr", grpcAddr, "err", err)
		return fmt.Errorf("listen on %s: %w", grpcAddr, err)
	}

	srvMetrics := grpcprom.NewServerMetrics(
		grpcprom.WithServerHandlingTimeHistogram(),
	)
	reg.MustRegister(srvMetrics)

	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			srvMetrics.UnaryServerInterceptor(),
			grpcHandlers.LoggingInterceptor(logger),
		),
	)
	enginev1.RegisterEngineServiceServer(srv, handler)
	reflection.Register(srv)

	// Pre-create the series for every method at zero so they appear before the
	// first request rather than popping into existence mid-scrape.
	srvMetrics.InitializeMetrics(srv)

	go func() {
		<-ctx.Done()
		logger.Info("shutting down")
		srv.GracefulStop()
	}()

	logger.Info("engine listening", "addr", grpcAddr)
	if err := srv.Serve(lis); err != nil {
		logger.Error("serve failed", "err", err)
		return fmt.Errorf("serve: %w", err)
	}

	return nil
}
