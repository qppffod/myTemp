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

func (c *Client) StartNewWorkflow(ctx context.Context, workflowID, workflowType, taskQueue string, input []byte) (string, error) {
	resp, err := c.engine.StartWorkflow(ctx, &pb.StartWorkflowRequest{
		WorkflowId:   workflowID,
		WorkflowType: workflowType,
		TaskQueue:    taskQueue,
		Input:        input,
	})
	if err != nil {
		return "", err
	}

	return resp.RunId, nil
}

func (c *Client) PollWorkflowTask(ctx context.Context, queue string) (*pb.PollWorkflowTaskResponse, error) {
	return c.engine.PollWorkflowTask(ctx, &pb.PollWorkflowTaskRequest{TaskQueue: queue})
}

func (c *Client) RespondWorkflowTaskCompleted(ctx context.Context, taskID int64, workflowID, runID string, commands []*pb.Command) error {
	_, err := c.engine.RespondWorkflowTaskCompleted(ctx, &pb.RespondWorkflowTaskCompletedRequest{
		TaskId:     taskID,
		WorkflowId: workflowID,
		RunId:      runID,
		Commands:   commands,
	})
	return err
}

func (c *Client) PollActivityTask(ctx context.Context, queue string) (*pb.PollActivityTaskResponse, error) {
	return c.engine.PollActivityTask(ctx, &pb.PollActivityTaskRequest{TaskQueue: queue})
}

func (c *Client) RespondActivityTaskCompleted(ctx context.Context, taskID int64, workflowID, runID string, result []byte) error {
	_, err := c.engine.RespondActivityTaskCompleted(ctx, &pb.RespondActivityTaskCompletedRequest{
		TaskId:     taskID,
		WorkflowId: workflowID,
		RunId:      runID,
		Result:     result,
	})
	return err
}
