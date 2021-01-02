package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

//
type Master struct {
	// Your definitions here.
	manager   *TaskManager
	isMapDone bool
	NReduce   int
	NMapper   int
}

//
func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	task := m.manager.GetAssignment()
	fmt.Printf("Get %v\n", task)
	if task == nil {
		reply.HasTask = false
	} else {
		reply.HasTask = true
		reply.IsMapTask = task.IsMapTask
		reply.MapFile = task.FilePath
		reply.Index = task.Index
		reply.NMapper = m.NMapper
		reply.NReducer = m.NReduce
	}

	fmt.Printf(" the index is %v ", reply.Index)
	return nil
}

//
func (m *Master) SubmitTask(args *SubmitTaskArgs, reply *SubmitTaskReply) error {
	fmt.Printf("Submit task %v\n", args.Index)
	if !m.isMapDone {
		if args.IsMapTask {
			m.manager.RemoveTask(args.Index)
		} else {
			return nil
		}
		fmt.Printf("is map done %v/n", m.manager.Done())
		if m.manager.Done() {
			m.isMapDone = true
			for i := 0; i < m.NReduce; i++ {
				m.manager.AddReduceTask(i)
			}
		}
	} else {
		if args.IsMapTask {
			return nil
		} else {
			m.manager.RemoveTask(args.Index)
		}
	}
	return nil
}

//
func (m *Master) DoneQuery(args *DoneArgs, reply *DoneReply) error {
	reply.IsDone = m.Done()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.manager.Done()
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		manager:   NewTaskManager(),
		isMapDone: false,
		NReduce:   nReduce,
		NMapper:   len(files),
	}
	for i := 0; i < len(files); i++ {
		m.manager.AddMapTask(i, files[i])
	}
	// Your code here.

	m.server()
	return &m
}
