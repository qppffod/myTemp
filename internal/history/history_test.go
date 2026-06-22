package history_test

import (
	"testing"

	"github.com/qppffod/myTemp/internal/history"
	"github.com/qppffod/myTemp/internal/persistence"
	"github.com/qppffod/myTemp/internal/testutil"
	"github.com/stretchr/testify/require"
)

func eventTypes(events []persistence.Event) []string {
	types := make([]string, len(events))
	for i, e := range events {
		types[i] = e.EventType
	}
	return types
}

// ---------------------------------------------------------------------------
//
// 							  Basic lifecycle
//
// ---------------------------------------------------------------------------

func TestStartWorkflow_WritesStartedEventAndTask(t *testing.T) {
	h, p, _ := testutil.SetupEngine(t)

	runID, err := h.StartWorkflow(t.Context(), "order-1", "TestWorkflow", "default", []byte(`{"OrderID":1}"`))
	require.NoError(t, err)
	require.NotEmpty(t, runID)

	events := testutil.GetEvents(t, p, "order-1", runID)
	require.Len(t, events, 1)
	require.Equal(t, "WorkflowStarted", events[0].EventType)

	task, err := p.PollTask(t.Context(), "default", "workflow", "test-worker")
	require.NoError(t, err)
	require.NotNil(t, task)
	require.Equal(t, "order-1", task.WorkflowID)
}

func TestSingleActivityWorkflow_CompletesCleanly(t *testing.T) {
	h, p, _ := testutil.SetupEngine(t)

	runID, err := h.StartWorkflow(t.Context(), "order-2", "TestWorkflow", "default", []byte(`{}`))
	require.NoError(t, err)

	wfTsk, err := p.PollTask(t.Context(), "default", "workflow", "w1")
	require.NoError(t, err)

	err = h.CompleteWorkflowTask(t.Context(), wfTsk.ID, "order-2", runID, []history.Command{
		{Type: "ScheduleActivity", ActivityName: "DoNothing", ActivityIndex: 0, TaskQueue: "default"},
	})
	require.NoError(t, err)

	actTask, err := p.PollTask(t.Context(), "default", "activity", "w1")
	require.NoError(t, err)
	require.Equal(t, "DoNothing", actTask.ActivityName)

	err = h.CompleteActivityTask(t.Context(), actTask.ID, "order-2", runID, []byte(`"done"`))
	require.NoError(t, err)

	wfTsk2, err := p.PollTask(t.Context(), "default", "workflow", "w1")
	require.NoError(t, err)

	err = h.CompleteWorkflowTask(t.Context(), wfTsk2.ID, "order-2", runID, []history.Command{
		{Type: "CompleteWorkflow"},
	})
	require.NoError(t, err)

	events := testutil.GetEvents(t, p, "order-2", runID)
	require.Len(t, events, 4)
	require.Equal(t, []string{
		"WorkflowStarted",
		"ActivityScheduled",
		"ActivityCompleted",
		"WorkflowCompleted",
	}, eventTypes(events))

	exec, err := p.GetWorkflowExecution(t.Context(), "order-2", runID)
	require.NoError(t, err)
	require.Equal(t, "Completed", exec.Status)
}
