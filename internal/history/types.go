package history

type Command struct {
	Type          string
	ActivityName  string
	TaskQueue     string
	Input         []byte
	ActivityIndex int32
	TimerIndex    int32
	DurationMs    int64
}
