package sdk

import (
	"context"
	"fmt"

	pb "github.com/qppffod/myTemp/proto/engine/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn   *grpc.ClientConn
	engine pb.EngineServiceClient
}

func NewClient(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to grpc: [%v]", err)
	}

	client := pb.NewEngineServiceClient(conn)

	return &Client{
		conn:   conn,
		engine: client,
	}, nil
}

func (c *Client) Close() {
	c.conn.Close()
}

func (c *Client) StartNewWorkflow(ctx context.Context, workflowType, taskQueue string, data any) {
	c.engine.StartWorkflow(ctx, &pb.StartWorkflowRequest{})
}
