package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskType uint32

const (
	MAP      TaskType = iota
	REDUCE   TaskType = iota
	TRYAGAIN TaskType = iota // there are no task now, but there may be more after a while, e.g. a task assigned to a crushed worker has been added to "TODO" queue again
	NOMORE   TaskType = iota
)

type Task struct {
	// a global Id used to identify a task uniquely
	GId uint32

	TType TaskType
	// Id of mapper/reducer
	Id        uint32
	StartTime time.Time

	InputFilenames []string

	workerId uint32

	M uint32
	R uint32
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
