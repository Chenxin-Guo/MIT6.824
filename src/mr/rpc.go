package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
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

// RequestTaskArgs is the Request Message for RequestTask
type RequestTaskArgs struct{}

// RequestTaskReply is the Reply Message for RequestTask
type RequestTaskReply struct {
	// True if there is a task assigned.
	HasTask bool
	// True if the task is a map task, otherwise it is a reduce task.
	IsMapTask bool
	// The index of the task.
	Index int
	// Number of mapper of this MapReduce Job.
	NMapper int
	// Number of reducer of this MapReduce Job.
	NReducer int
	// The paht of the file to be mapped. Only vaid if IsMapTask is True.
	MapFile string
}

// SubmitTaskArgs is the Request Message for SubmitTask
type SubmitTaskArgs struct {
	// True if the task to be submitted is a map task, otherwise is a reduce task.
	IsMapTask bool
	// The index of the task.
	Index int
}

// SubmitTaskReply is the Reply Message for SubmitTask
type SubmitTaskReply struct{}

// DoneArgs is the Request Message for QueryDone
type DoneArgs struct{}

// DoneReply is the Reply Message for QueryDone
type DoneReply struct {
	// True if the map reduce job is done. Woker should exit upon seeing true.
	IsDone bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
