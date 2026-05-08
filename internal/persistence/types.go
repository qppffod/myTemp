package persistence

type WorkflowExecution struct {
	WorkflowID   string
	RunID        string
	WorkflowType string
	TaskQueue    string
	Status       string
}

type Event struct {
	WorkflowID string
	RunID      string
	EventID    int64
	EventType  string
	Data       []byte
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
}
