package workflow

import (
	"context"
	"testing"
	"time"

	pb "github.com/qppffod/myTemp/proto/engine/v1"
	"github.com/stretchr/testify/require"
)

func newTestContext(history []*pb.HistoryEvent) *Context {
	return New(context.Background(), history, "test-queue")
}

// ---------------------------------------------------------------------------
//
// 					ExecuteActivity + ActivityFuture.Get
//
// ---------------------------------------------------------------------------

func TestExecuteActivity_NotInHistory_RecordsCommand(t *testing.T) {
	ctx := newTestContext([]*pb.HistoryEvent{
		{EventType: "WorkflowStarted"},
	})

	ExecuteActivity(ctx, "DoNothing", nil)

	cmds := ctx.Commands()
	require.Len(t, cmds, 1)
	require.Equal(t, "ScheduleActivity", cmds[0].Type)
	require.Equal(t, "DoNothing", cmds[0].ActivityName)
	require.Equal(t, int32(0), cmds[0].ActivityIndex)
}

func TestExecuteActivity_AlreadyScheduled_DoesNotRecordAgain(t *testing.T) {
	ctx := newTestContext([]*pb.HistoryEvent{
		{EventType: "WorkflowStarted"},
		{EventType: "ActivityScheduled", ActivityName: "DoNothing", ActivityIndex: 0},
	})

	ExecuteActivity(ctx, "DoNothing", nil)

	require.Empty(t, ctx.Commands(), "should not re-schedule an already-scheduled activity")
}

func TestGet_ActivityNotInHistory_Panics(t *testing.T) {
	ctx := newTestContext([]*pb.HistoryEvent{
		{EventType: "WorkflowStarted"},
	})

	require.PanicsWithValue(t, ErrPendingActivity, func() {
		var out []byte
		_ = ExecuteActivity(ctx, "DoNothing", nil).Get(&out)
	})
}

func TestGet_ActivityCompleted_ReturnsTypedData(t *testing.T) {
	ctx := newTestContext([]*pb.HistoryEvent{
		{EventType: "WorkflowStarted"},
		{EventType: "ActivityScheduled", ActivityName: "DoThing", ActivityIndex: 0},
		{EventType: "ActivityCompleted", ActivityName: "DoThing", ActivityIndex: 0, Data: []byte(`"hello"`)},
	})

	var out string
	err := ExecuteActivity(ctx, "DoThing", nil).Get(&out)

	require.NoError(t, err)
	require.Equal(t, "hello", out)
	require.Empty(t, ctx.Commands(), "completed activity should not produce a command")
}

func TestGet_ActivityCompleted_NilOut_NoError(t *testing.T) {
	ctx := newTestContext([]*pb.HistoryEvent{
		{EventType: "ActivityScheduled", ActivityName: "DoNothing", ActivityIndex: 0},
		{EventType: "ActivityCompleted", ActivityName: "DoNothing", ActivityIndex: 0, Data: []byte(`"hello"`)},
	})

	err := ExecuteActivity(ctx, "DoNothing", nil).Get(nil)
	require.NoError(t, err)
}

func TestGet_ActivityFailed_ReturnsErro(t *testing.T) {
	ctx := newTestContext([]*pb.HistoryEvent{
		{EventType: "ActivityScheduled", ActivityName: "DoThing", ActivityIndex: 0},
		{EventType: "ActivityFailed", ActivityName: "DoThing", ActivityIndex: 0, Data: []byte("boom")},
	})

	var out []byte
	err := ExecuteActivity(ctx, "DoThing", nil).Get(&out)

	require.Error(t, err)
	require.Equal(t, "boom", err.Error())
}

func TestGet_MultipleActivities_MatchByIndex(t *testing.T) {
	ctx := newTestContext([]*pb.HistoryEvent{
		{EventType: "ActivityScheduled", ActivityName: "A", ActivityIndex: 0},
		{EventType: "ActivityCompleted", ActivityName: "A", ActivityIndex: 0, Data: []byte(`"a-result"`)},
		{EventType: "ActivityScheduled", ActivityName: "B", ActivityIndex: 1},
		{EventType: "ActivityCompleted", ActivityName: "B", ActivityIndex: 1, Data: []byte(`"b-result"`)},
	})

	var a, b string
	require.NoError(t, ExecuteActivity(ctx, "A", nil).Get(&a))
	require.NoError(t, ExecuteActivity(ctx, "B", nil).Get(&b))

	require.Equal(t, "a-result", a)
	require.Equal(t, "b-result", b)
}

func TestGet_SameActivityNameTwice_DistinguishedByIndex(t *testing.T) {
	ctx := newTestContext([]*pb.HistoryEvent{
		{EventType: "ActivityScheduled", ActivityName: "Charge", ActivityIndex: 0},
		{EventType: "ActivityCompleted", ActivityName: "Charge", ActivityIndex: 0, Data: []byte(`"first"`)},
		{EventType: "ActivityScheduled", ActivityName: "Charge", ActivityIndex: 1},
		{EventType: "ActivityCompleted", ActivityName: "Charge", ActivityIndex: 1, Data: []byte(`"second"`)},
	})

	var first, second string
	require.NoError(t, ExecuteActivity(ctx, "Charge", nil).Get(&first))
	require.NoError(t, ExecuteActivity(ctx, "Charge", nil).Get(&second))

	require.Equal(t, "first", first)
	require.Equal(t, "second", second)
}

func TestGet_SuspendActivityPending_Panics(t *testing.T) {
	ctx := newTestContext([]*pb.HistoryEvent{
		{EventType: "ActivityScheduled", ActivityName: "A", ActivityIndex: 0},
		{EventType: "ActivityCompleted", ActivityName: "A", ActivityIndex: 0, Data: []byte(`"a"`)},
	})

	var a string
	require.NoError(t, ExecuteActivity(ctx, "A", nil).Get(&a))

	require.PanicsWithValue(t, ErrPendingActivity, func() {
		var b string
		_ = ExecuteActivity(ctx, "B", nil).Get(&b)
	})
}

// ---------------------------------------------------------------------------
//
// 									Sleep
//
// ---------------------------------------------------------------------------

func TestSleep_NoTimerInHistory_RecordsStartTimerAndPanics(t *testing.T) {
	ctx := newTestContext([]*pb.HistoryEvent{
		{EventType: "WorkflowStarted"},
	})

	require.PanicsWithValue(t, ErrPendingActivity, func() {
		Sleep(ctx, 5*time.Second)
	})

	cmds := ctx.Commands()

	require.Len(t, cmds, 1)
	require.Equal(t, "StartTimer", cmds[0].Type)
	require.Equal(t, int32(0), cmds[0].TimerIndex)
	require.Equal(t, int64(5000), cmds[0].DurationMs)
}

func TestSleep_TimerStartedNotFired_Panics(t *testing.T) {
	ctx := newTestContext([]*pb.HistoryEvent{
		{EventType: "TimerStarted", TimerIndex: 0},
	})

	require.PanicsWithValue(t, ErrPendingActivity, func() {
		Sleep(ctx, 5*time.Second)
	})

	require.Empty(t, ctx.Commands())
}

func TestSleep_TimerFired_ReturnsAndContinues(t *testing.T) {
	ctx := newTestContext([]*pb.HistoryEvent{
		{EventType: "TimerStarted", TimerIndex: 0},
		{EventType: "TimerFired", TimerIndex: 0},
	})

	require.NotPanics(t, func() {
		Sleep(ctx, 5*time.Second)
	})

	require.Empty(t, ctx.Commands())
}

// ---------------------------------------------------------------------------
//
// 								ReceiveSignal
//
// ---------------------------------------------------------------------------

func TestReceiveSignal_NoSignal_Panics(t *testing.T) {
	ctx := newTestContext([]*pb.HistoryEvent{
		{EventType: "WorkflowStarted"},
	})

	require.PanicsWithValue(t, ErrPendingActivity, func() {
		var out struct{ Approved bool }
		ReceiveSignal(ctx, "approval", &out)
	})
}

func TestReceiveSignal_SignalPresent_ReturnsPayload(t *testing.T) {
	ctx := newTestContext([]*pb.HistoryEvent{
		{EventType: "SignalReceived", SignalName: "approval", Data: []byte(`{"Approved": true}`)},
	})

	var out struct{ Approved bool }
	require.NotPanics(t, func() {
		ReceiveSignal(ctx, "approval", &out)
	})
	require.True(t, out.Approved)
}

func TestReceiveSignal_DifferentName_Panics(t *testing.T) {
	ctx := newTestContext([]*pb.HistoryEvent{
		{EventType: "SignalReceived", SignalName: "cancel", Data: []byte(`{}`)},
	})

	require.PanicsWithValue(t, ErrPendingActivity, func() {
		var out struct{ Approved bool }
		ReceiveSignal(ctx, "approval", &out)
	})
}

func TestReceiveSignal_MultipleSameName_ConsumedItOrder(t *testing.T) {
	ctx := newTestContext([]*pb.HistoryEvent{
		{EventType: "SignalReceived", SignalName: "update", Data: []byte(`"first"`)},
		{EventType: "SignalReceived", SignalName: "update", Data: []byte(`"second"`)},
	})

	var first, second string
	ReceiveSignal(ctx, "update", &first)
	ReceiveSignal(ctx, "update", &second)

	require.Equal(t, "first", first)
	require.Equal(t, "second", second)
}

func TestReceiveSignal_ConsumedThenWaitsForNext(t *testing.T) {
	ctx := newTestContext([]*pb.HistoryEvent{
		{EventType: "SignalReceived", SignalName: "update", Data: []byte(`"only"`)},
	})

	var first string
	ReceiveSignal(ctx, "update", &first)
	require.Equal(t, "only", first)

	require.PanicsWithValue(t, ErrPendingActivity, func() {
		var second string
		ReceiveSignal(ctx, "update", &second)
	})
}

// ---------------------------------------------------------------------------
//
// 							HasPendingActivities
//
// ---------------------------------------------------------------------------

func TestHasPendingActivities_AllCompleted_False(t *testing.T) {
	ctx := newTestContext([]*pb.HistoryEvent{
		{EventType: "ActivityScheduled", ActivityName: "A"},
		{EventType: "ActivityCompleted", ActivityName: "A"},
	})
	require.False(t, ctx.HasPendingActivities())
}

func TestHasPendingActivities_OneIncomplete_True(t *testing.T) {
	ctx := newTestContext([]*pb.HistoryEvent{
		{EventType: "ActivityScheduled", ActivityName: "A"},
		{EventType: "ActivityCompleted", ActivityName: "A"},
		{EventType: "ActivityScheduled", ActivityName: "B"},
	})
	require.True(t, ctx.HasPendingActivities())
}
