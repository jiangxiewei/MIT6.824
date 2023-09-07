package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

/**
1. 初始化待处理文件列表，每个文件初始化成一个map任务。
2. Worker轮询来获取任务，map阶段获取map任务。
3. Worker处理完任务后发送完成功信号，Coordinator将任务置为完成，或者任务超时Coordinator重新生成Task
4. 待所有map任务完成后，进入reduce阶段，Worker轮询获取reduce任务
5. 同3
6. 待所有reduce任务完成后，Coordinator进入完成阶段，Worker来获取任务得到结束标记，Worker结束进程。
*/
type Coordinator struct {
	// Your definitions here.
	Filenames []string

	ReduceNum int
}

// GetTask
// 用于Worker来获取操蛋的任务
// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *PollTaskArgs, reply *Task) error {

	reply = &Task{
		HashMod: c.ReduceNum,
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Filenames: files,
		ReduceNum: nReduce,
	}

	// Your code here.

	c.server()
	return &c
}
