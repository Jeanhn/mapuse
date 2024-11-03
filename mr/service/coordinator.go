package service

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"

	"mapuce/mr/coordinate"
	"mapuce/mr/util"
)

type Coordinator struct {
	// Your definitions here.
	tm      *coordinate.TaskManager
	se      *coordinate.SplitExecutor
	nReduce int
	taskId  string
}

func (c *Coordinator) Acquire(args *AcquireArgs, reply *AcquireReply) error {
	task, err := c.tm.Acquire()
	if err != nil {
		return err
	}
	reply.Task = task
	reply.WorkerId = args.WorkerId

	log.Default().Printf("Coordinator.Acquire, args:%v, reply:%v\n", args, reply)

	return nil
}

func (c *Coordinator) Finish(args *FinishArgs, reply *FinishReply) error {
	err := c.tm.Finish(args.Task.Id)
	if err != nil {
		return err
	}
	log.Default().Printf("Coordinator.Finish, args:%v, reply:%v\n", args, reply)

	return nil
}

func (c *Coordinator) IsDone(args *IsDoneArgs, reply *IsDoneReply) error {
	done := c.tm.Done()

	reply.IsDone = done

	log.Default().Printf("Coordinator.IsDone, args:%v, reply:%v\n", args, reply)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	ret := c.tm.Done()

	return ret
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {

	// Your code here.

	// split the input first
	defer util.FlushLogs()
	defer util.RemoveTempFiles()

	log.Default().Printf("MakeCoordinator start, files:%v, nReduce:%v\n", files, nReduce)

	taskId := util.RandomTaskId()
	se, err := coordinate.NewSplitExecutor(files, coordinate.SPLIT_SIZE, taskId)
	if err != nil {
		log.Default().Printf("MakeCoordinator error:%v\n", err)
		panic(err)
	}

	ok, err := se.Iterate()
	for ok && err == nil {
		ok, err = se.Iterate()
	}
	if err != nil {
		panic(err)
	}

	splitFiles := se.GetSplitFiles()

	tm, err := coordinate.NewTaskManager(splitFiles, taskId, 16, nReduce)
	if err != nil {
		panic(err)
	}

	c := Coordinator{
		taskId: taskId,
		se:     se,
		tm:     tm,
	}

	c.server()

	for !c.Done() {
		time.Sleep(time.Second)
	}
	return &c
}
