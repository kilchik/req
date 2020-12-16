package req

type TaskState int

const (
	StateDelayed TaskState = iota
	StateReady   TaskState = iota
	StateTaken   TaskState = iota
	StateBuried  TaskState = iota
)
