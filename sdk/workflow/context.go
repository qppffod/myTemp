package workflow

import (
	"context"
	"encoding/json"

	pb "github.com/qppffod/myTemp/proto/engine/v1"
)

type Context struct {
	ctx      context.Context
	history  []*pb.HistoryEvent
	queue    string
	eventIdx int
	commands []*pb.Command
}

func New(ctx context.Context, history []*pb.HistoryEvent, queue string) *Context {
	return &Context{
		ctx:     ctx,
		history: history,
		queue:   queue,
	}
}

func (c *Context) Commands() []*pb.Command {
	return c.commands
}

func (c *Context) HasPendingActivities() bool {
	scheduled := make(map[string]bool)
	completed := make(map[string]bool)

	for _, event := range c.history {
		if event.EventType == "ActivityScheduled" {
			scheduled[event.ActivityName] = true
		}
		if event.EventType == "ActivityCompleted" {
			completed[event.ActivityName] = true
		}
	}

	for name := range scheduled {
		if !completed[name] {
			return true
		}
	}
	return false
}

type ActivityFuture struct {
	ctx          *Context
	activityName string
	callIndex    int64
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
		TaskQueue:    ctx.queue,
		ActivityName: activityName,
		Input:        inputBytes,
	})

	return nil
}
