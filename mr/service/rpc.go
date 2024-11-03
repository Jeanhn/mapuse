package service

import (
	"os"
	"strconv"

	"mapuce/mr/coordinate"
)

type AcquireArgs struct {
	WorkerId string
}

type AcquireReply struct {
	WorkerId string
	Task     *coordinate.Task
}

type FinishArgs struct {
	WorkerId string
	Task     coordinate.Task
}

type FinishReply struct {
}

type IsDoneArgs struct {
	WorkerId string
	TaskId   string
}

type IsDoneReply struct {
	IsDone bool
}

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
