package sdk

import (
	"reflect"
	"runtime"
	"strings"
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
