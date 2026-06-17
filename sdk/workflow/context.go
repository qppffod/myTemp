package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	pb "github.com/qppffod/myTemp/proto/engine/v1"
)

type Context struct {
	ctx               context.Context
	history           []*pb.HistoryEvent
	commands          []*pb.Command
	queue             string
	activityCallCount int
	timerCallCount    int
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

var ErrPendingActivity = errors.New("activity pending")

type ActivityFuture struct {
	ctx          *Context
	activityName string
	callIndex    int
}

func (f *ActivityFuture) Get(out any) error {
	for _, event := range f.ctx.history {
		if event.ActivityName != f.activityName {
			continue
		}
		if event.ActivityIndex != int32(f.callIndex) {
			continue
		}
		if event.EventType == "ActivityCompleted" {
			if out != nil && len(event.Data) > 0 {
				if err := json.Unmarshal(event.Data, out); err != nil {
					return err
				}
			}
			return nil
		}
		if event.EventType == "ActivityFailed" {
			return errors.New(string(event.Data))
		}
	}

	// Not in history yet — workflow can't proceed past this Get.
	// Panic out of the workflow function entirely.
	panic(ErrPendingActivity)
}

func ExecuteActivity(ctx *Context, activityName string, input any) *ActivityFuture {
	callIndex := ctx.activityCallCount
	ctx.activityCallCount++

	alreadyScheduled := false
	for _, event := range ctx.history {
		if event.EventType == "ActivityScheduled" &&
			event.ActivityName == activityName &&
			event.ActivityIndex == int32(callIndex) {
			alreadyScheduled = true
			break
		}
	}

	if !alreadyScheduled {
		inputBytes, err := json.Marshal(input)
		if err != nil {
			log.Println("ExecuteActivity: marshal input bytes - ", err)
		}
		ctx.commands = append(ctx.commands, &pb.Command{
			Type:          "ScheduleActivity",
			TaskQueue:     ctx.queue,
			ActivityName:  activityName,
			ActivityIndex: int32(callIndex),
			Input:         inputBytes,
		})
	}

	return &ActivityFuture{
		ctx:          ctx,
		activityName: activityName,
		callIndex:    callIndex,
	}
}

func Sleep(ctx *Context, duration time.Duration) {
	timerIndex := ctx.timerCallCount
	ctx.timerCallCount++

	for _, event := range ctx.history {
		if event.EventType == "TimerFired" && event.TimerIndex == int32(timerIndex) {
			return
		}
	}

	for _, event := range ctx.history {
		if event.EventType == "TimerStarted" && event.TimerIndex == int32(timerIndex) {
			panic(ErrPendingActivity)
		}
	}

	log.Printf("Sleep: setting DurationMs=%d", duration.Milliseconds())
	ctx.commands = append(ctx.commands, &pb.Command{
		Type:       "StartTimer",
		TimerIndex: int32(timerIndex),
		DurationMs: duration.Milliseconds(),
	})
	panic(ErrPendingActivity)
}
