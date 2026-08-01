package mr

type GetTaskArgs struct {
}

type TaskKind uint

const (
	TaskKindMap TaskKind = iota
	TaskKindReduce
	TaskKindDone
)

func TaskKindAsString(taskKind TaskKind) string {
	if taskKind == TaskKindMap {
		return "Map"
	}
	if taskKind == TaskKindReduce {
		return "Reduce"
	}
	return "Unknown"
}

type TaskInput struct {
	Kind       TaskKind
	TaskId     uint
	Attempt    uint
	InputFiles []string
	NReduce    uint
}

type TaskOutput struct {
	Kind        TaskKind
	TaskId      uint
	Attempt     uint
	OutputFiles []string
	BucketIds   []uint
}

type SendTaskOutputReply struct {
}
