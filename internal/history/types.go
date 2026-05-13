package history

type Command struct {
	Type         string
	ActivityName string
	TaskQueue    string
	Input        []byte
}
