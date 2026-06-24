package history_test

import (
	"testing"
	"time"

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

// ---------------------------------------------------------------------------
//
// 							  Failure handling
//
// ---------------------------------------------------------------------------

func TestActivityFailure_ExhaustsAndFailsWorkflow(t *testing.T) {
	h, p, _ := testutil.SetupEngine(t)

	runID, err := h.StartWorkflow(t.Context(), "order-3", "TestWorkflow", "default", []byte(`{}`))
	require.NoError(t, err)

	wfTask, _ := p.PollTask(t.Context(), "default", "workflow", "w1")
	err = h.CompleteWorkflowTask(t.Context(), wfTask.ID, "order-3", runID, []history.Command{
		{Type: "ScheduleActivity", ActivityName: "Flaky", ActivityIndex: 0, TaskQueue: "default"},
	})
	require.NoError(t, err)

	// Fail the activity on every attempt. A non-terminal failure deletes the task
	// and re-schedules a fresh one (new ID, future visibility_time from the
	// backoff), so each attempt must poll for the newly scheduled retry task
	// before failing it again. DefaultRetryPolicy allows 3 attempts.
	const maxAttempts = 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		var actTask *persistence.Task
		require.Eventually(t, func() bool {
			tsk, e := p.PollTask(t.Context(), "default", "activity", "w1")
			if e != nil || tsk == nil {
				return false
			}
			actTask = tsk
			return true
		}, 5*time.Second, 50*time.Millisecond, "retry task for attempt #%d should become pollable", attempt)

		require.Equal(t, "Flaky", actTask.ActivityName)
		require.EqualValues(t, attempt, actTask.Attempt)

		err = h.FailActivityTask(t.Context(), actTask.ID, "order-3", runID, "connection refused")
		require.NoError(t, err)
	}

	// Attempts exhausted: ActivityFailed is written and a workflow task enqueued.
	// The workflow replays and propagates the error.
	wfTask2, err := p.PollTask(t.Context(), "default", "workflow", "w1")
	require.NoError(t, err)
	require.NotNil(t, wfTask2)
	err = h.CompleteWorkflowTask(t.Context(), wfTask2.ID, "order-3", runID, []history.Command{
		{Type: "FailWorkflow", Input: []byte("connection refused")},
	})
	require.NoError(t, err)

	events := testutil.GetEvents(t, p, "order-3", runID)
	types := eventTypes(events)
	require.Contains(t, types, "ActivityFailed")
	require.Equal(t, "WorkflowFailed", types[len(types)-1])

	exec, _ := p.GetWorkflowExecution(t.Context(), "order-3", runID)
	require.Equal(t, "Failed", exec.Status)
}

// ---------------------------------------------------------------------------
//
// 									Retries
//
// ---------------------------------------------------------------------------

func TestActivityRetyr_ReschedulesWithoutRetry(t *testing.T) {
	h, p, _ := testutil.SetupEngine(t)

	runID, err := h.StartWorkflow(t.Context(), "order-4", "TestWorkflow", "default", []byte(`{}`))
	require.NoError(t, err)

	wfTsk, err := p.PollTask(t.Context(), "default", "workflow", "w1")
	require.NoError(t, err)

	h.CompleteWorkflowTask(t.Context(), wfTsk.ID, "order-4", runID, []history.Command{
		{Type: "ScheduleActivity", ActivityName: "Flaky", ActivityIndex: 0, TaskQueue: "default"},
	})

	actTask, err := p.PollTask(t.Context(), "default", "activity", "w1")
	require.NoError(t, err)
	require.Equal(t, int32(1), actTask.Attempt)

	// Should NOT write an ActivityFailed event
	err = h.FailActivityTask(t.Context(), actTask.ID, "order-4", runID, "transient")
	require.NoError(t, err)

	events := testutil.GetEvents(t, p, "order-4", runID)
	require.NotContains(t, eventTypes(events), "ActivityFailed")

	delayedTsk, err := p.PollTask(t.Context(), "default", "activity", "w1")
	require.NoError(t, err)
	require.Nil(t, delayedTsk, "retried task should be hidden until its backoff delay passes")
}

// ---------------------------------------------------------------------------
//
// 								Signals
//
// ---------------------------------------------------------------------------

func TestSignalWorkflow_AppendsEventAndCreateTask(t *testing.T) {
	h, p, _ := testutil.SetupEngine(t)

	runID, err := h.StartWorkflow(t.Context(), "order-5", "TestWorkflow", "default", []byte(`{}`))
	require.NoError(t, err)

	wfTsk, err := p.PollTask(t.Context(), "default", "workflow", "w1")
	require.NoError(t, err)

	err = h.CompleteWorkflowTask(t.Context(), wfTsk.ID, "order-5", runID, nil)
	require.NoError(t, err)

	err = h.SignalWorkflow(t.Context(), "order-5", "", "approval", []byte(`{"Approved": true}`))
	require.NoError(t, err)

	events := testutil.GetEvents(t, p, "order-5", runID)
	require.Contains(t, eventTypes(events), "SignalReceived")

	var found bool
	for _, e := range events {
		if e.EventType == "SignalReceived" {
			require.Equal(t, "approval", e.SignalName)
			require.JSONEq(t, `{"Approved": true}`, string(e.Data))
			found = true
		}
	}
	require.True(t, found)

	task, err := p.PollTask(t.Context(), "default", "workflow", "w1")
	require.NoError(t, err)
	require.NotNil(t, task)
}

func TestSignalWofklow_NotRunning_Errors(t *testing.T) {
	h, _, _ := testutil.SetupEngine(t)

	err := h.SignalWorkflow(t.Context(), "does-not-exist", "", "approval", []byte(`{}`))
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
//
// 						Duplicate completion protection
//
// ---------------------------------------------------------------------------

func TestCompleteWorkflowTask_AlreadyCompleted_Discarded(t *testing.T) {
	h, p, _ := testutil.SetupEngine(t)

	runID, err := h.StartWorkflow(t.Context(), "order-6", "TestWorkflow", "default", []byte(`{}`))
	require.NoError(t, err)

	wfTsk, err := p.PollTask(t.Context(), "default", "workflow", "w1")
	require.NoError(t, err)

	err = h.CompleteWorkflowTask(t.Context(), wfTsk.ID, "order-6", runID, []history.Command{
		{Type: "CompleteWorkflow"},
	})

	exec, err := p.GetWorkflowExecution(t.Context(), "order-6", runID)
	require.NoError(t, err)
	require.Equal(t, "Completed", exec.Status)

	events := testutil.GetEvents(t, p, "order-6", runID)
	count := 0
	for _, e := range events {
		if e.EventType == "WorkflowCompleted" {
			count++
		}
	}
	require.Equal(t, 1, count)
}

// ---------------------------------------------------------------------------
//
//                          Timer scanner
//
// ---------------------------------------------------------------------------

func TestTimerScanner_FiresDueTimer(t *testing.T) {
	h, p, _ := testutil.SetupEngine(t)

	runID, err := h.StartWorkflow(t.Context(), "order-timer", "TestWorkflow", "default", []byte(`{}`))
	require.NoError(t, err)

	wfTsk, err := p.PollTask(t.Context(), "default", "workflow", "w1")
	require.NoError(t, err)

	err = h.CompleteWorkflowTask(t.Context(), wfTsk.ID, "order-timer", runID, []history.Command{
		{Type: "StartTimer", TimerIndex: 0, DurationMs: 100},
	})
	require.NoError(t, err)

	events := testutil.GetEvents(t, p, "order-timer", runID)
	require.Contains(t, eventTypes(events), "TimerStarted")
	require.NotContains(t, eventTypes(events), "TimerFired")

	time.Sleep(200 * time.Millisecond)
	require.NoError(t, h.ScanTimers(t.Context()))

	events = testutil.GetEvents(t, p, "order-timer", runID)
	require.Contains(t, eventTypes(events), "TimerFired")

	task, err := p.PollTask(t.Context(), "default", "workflow", "w1")
	require.NoError(t, err)
	require.NotNil(t, task, "scanner should create a workflow task so the workflow resumes")
}

func TestTimerScanner_DoesNotFireBeforeDue(t *testing.T) {
	h, p, _ := testutil.SetupEngine(t)

	runID, err := h.StartWorkflow(t.Context(), "order-timer2", "TestWorkflow", "default", []byte(`{}`))
	require.NoError(t, err)

	wfTsk, err := p.PollTask(t.Context(), "default", "workflow", "w1")
	require.NoError(t, err)

	err = h.CompleteWorkflowTask(t.Context(), wfTsk.ID, "order-timer2", runID, []history.Command{
		{Type: "StartTimer", TimerIndex: 0, DurationMs: 60000},
	})
	require.NoError(t, err)

	require.NoError(t, h.ScanTimers(t.Context()))

	events := testutil.GetEvents(t, p, "order-timer2", runID)
	require.NotContains(t, eventTypes(events), "TimerFired",
		"a timer that is not yet due must not fire")
}
