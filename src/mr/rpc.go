package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

// Task 任务传输的结构定义，用于Coordinator和Map以及Reduce任务的通信交互
type PollTaskArgs struct {
}

type Task struct {
	Id       int
	TaskType TaskType
	Filename string
	HashMod  int
}

type TaskType int

const (
	Unknow = iota
	MapTask
	ReduceTask
	WatiTask
	ExitTask
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
