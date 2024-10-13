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

type SendArgs struct {
}

type SendReply struct {
	Filename    string
	Finish_mapf bool
	Id          int
	NReduce     int
}

type FinishedMapArgs struct {
	TempFileNames []string
	Id            int
}

type FinishedMapReply struct {
}

type SendReduceArgs struct {
}

type SendReduceReply struct {
	TaskId   int
	FileLen  int
	Finished bool
}

type FinishedReduceArgs struct {
	Id       int
	FileName string
}

type FinishedReduceReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
