package persistence

import "time"

type WorkflowExecution struct {
	WorkflowID   string
	RunID        string
	WorkflowType string
	TaskQueue    string
	Status       string
	StartedAt    time.Time
	ClosedAt     *time.Time
}

type Event struct {
	ID            int64
	WorkflowID    string
	RunID         string
	EventID       int64
	EventType     string
	SignalName    string
	ActivityName  string
	ActivityIndex int32
	TimerIndex    int32
	Data          []byte
	CreatedAt     time.Time
}

type Task struct {
	ID               int64
	TaskQueue        string
	TaskType         string
	WorkflowType     string
	WorkflowID       string
	RunID            string
	ScheduledEventID int64
	Input            []byte
	ActivityName     string
	ActivityIndex    int32
	Attempt          int32
	MaxAttempts      int32
	VisibilityTime   time.Time
	LeaseOwner       *string
	LeaseExpiresAt   *time.Time
}

type Timer struct {
	ID         int64
	WorkflowID string
	RunID      string
	TimerIndex int32
	FireAt     time.Time
	Fired      bool
}
