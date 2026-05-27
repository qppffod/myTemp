package sdk

import (
	"context"
	"log"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	pb "github.com/qppffod/myTemp/proto/engine/v1"
	"github.com/qppffod/myTemp/sdk/workflow"
)

type Worker struct {
	client *Client
	queue  string

	workflowFunctions map[string]reflect.Value
	activityFunctions map[string]reflect.Value
}

func NewWorker(c *Client, queue string) *Worker {
	return &Worker{
		client:            c,
		queue:             queue,
		workflowFunctions: make(map[string]reflect.Value),
		activityFunctions: make(map[string]reflect.Value),
	}
}

func (w *Worker) RegisterWorkflow(fn interface{}) {
	fnType := reflect.TypeOf(fn)

	if fnType == nil || fnType.Kind() != reflect.Func {
		panic("RegisterWorkflow: fn missing type, must be a function")
	}

	fnValue := reflect.ValueOf(fn)
	fullName := runtime.FuncForPC(fnValue.Pointer()).Name()

	words := strings.Split(fullName, ".")
	name := words[len(words)-1]

	w.workflowFunctions[name] = fnValue
}

func (w *Worker) RegisterActivity(fn interface{}) {
	fnType := reflect.TypeOf(fn)

	if fnType == nil || fnType.Kind() != reflect.Func {
		panic("RegisterActivity: fn missing type, must be a function")
	}

	fnValue := reflect.ValueOf(fn)
	fullName := runtime.FuncForPC(fnValue.Pointer()).Name()

	words := strings.Split(fullName, ".")
	name := words[len(words)-1]

	w.activityFunctions[name] = fnValue
}

func (w *Worker) Run(ctx context.Context) {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		w.pollWorkflowTasks(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		w.pollActivityTasks(ctx)
	}()

	wg.Wait()
}

func (w *Worker) pollWorkflowTasks(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			resp, err := w.client.engine.PollWorkflowTask(ctx, &pb.PollWorkflowTaskRequest{
				TaskQueue: w.queue,
			})
			if err != nil || resp.TaskId == 0 {
				time.Sleep(time.Second)
				continue
			}
			w.executeWorkflowTask(ctx, resp)
		}
	}
}

func (w *Worker) executeWorkflowTask(ctx context.Context, task *pb.PollWorkflowTaskResponse) {
	fn, ok := w.workflowFunctions[task.WorkflowType]
	if !ok {
		return
	}

	wfCtx := workflow.New(ctx, task.History)

	fn.Call([]reflect.Value{reflect.ValueOf(wfCtx)})

	commands := wfCtx.Commands()

	if len(commands) == 0 && !wfCtx.HasPendingActivities() {
		commands = append(commands, &pb.Command{Type: "CompleteWorkflow"})
	}

	if _, err := w.client.engine.RespondWorkflowTaskCompleted(ctx, &pb.RespondWorkflowTaskCompletedRequest{
		TaskId:     task.TaskId,
		WorkflowId: task.WorkflowId,
		RunId:      task.RunId,
		Commands:   commands,
	}); err != nil {
		log.Printf("RespondWorkflowTaskCompleted (task=%d): %v", task.TaskId, err)
	}
}

func (w *Worker) pollActivityTasks(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			resp, err := w.client.engine.PollActivityTask(ctx, &pb.PollActivityTaskRequest{
				TaskQueue: w.queue,
			})
			if err != nil || resp.TaskId == 0 {
				time.Sleep(time.Second)
				continue
			}
			w.executeActivityTask(ctx, resp)
		}
	}
}

func (w *Worker) executeActivityTask(ctx context.Context, task *pb.PollActivityTaskResponse) {
	fn, ok := w.activityFunctions[task.ActivityName]
	if !ok {
		return
	}

	result := fn.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(task.Input)})

	w.client.engine.RespondActivityTaskCompleted(ctx, &pb.RespondActivityTaskCompletedRequest{
		TaskId:     task.TaskId,
		WorkflowId: task.WorkflowId,
		RunId:      task.RunId,
		Result:     result[0].Bytes(),
	})
}
