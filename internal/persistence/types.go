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
	ID         int64
	WorkflowID string
	RunID      string
	EventID    int64
	EventType  string
	Data       []byte
	CreatedAt  time.Time
}

type Task struct {
	ID               int64
	TaskQueue        string
	TaskType         string
	WorkflowID       string
	RunID            string
	ScheduledEventID int64
	Input            []byte
	ActivityName     string
	VisibilityTime   time.Time
	LeaseOwner       *string
	LeaseExpiresAt   *time.Time
}
