package workflow

import (
	"context"
	"encoding/json"

	pb "github.com/qppffod/myTemp/proto/engine/v1"
)

type Context struct {
	ctx      context.Context
	history  []*pb.HistoryEvent
	eventIdx int
	commands []*pb.Command
}

func New(ctx context.Context, history []*pb.HistoryEvent) *Context {
	return &Context{
		ctx:     ctx,
		history: history,
	}
}

func (c *Context) Commands() []*pb.Command {
	return c.commands
}

func CompleteWorkflow(ctx *Context, result []byte) {
	ctx.commands = append(ctx.commands, &pb.Command{
		Type:  "CompleteWorkflow",
		Input: result,
	})
}

func ExecuteActivity(ctx *Context, activityName string, input any) []byte {
	for _, event := range ctx.history {
		if event.EventType == "ActivityCompleted" && event.ActivityName == activityName {
			return event.Data
		}
	}

	for _, event := range ctx.history {
		if event.EventType == "ActivityScheduled" && event.ActivityName == activityName {
			return event.Data
		}
	}

	inputBytes, _ := json.Marshal(input)
	ctx.commands = append(ctx.commands, &pb.Command{
		Type:         "ScheduleActivity",
		ActivityName: activityName,
		Input:        inputBytes,
	})

	return nil
}
