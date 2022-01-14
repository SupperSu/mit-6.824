package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStatus int

const (
	Working TaskStatus = iota
	Unsigned
	Success
	Failed
)

type Coordinator struct {
	// Your definitions here.
	succeededTask int
	mapTaskId     int
	reduceTaskId  int
	nReduce       int
	tasksMap      TasksMap
	tasksToDo     chan Task
	mu            sync.Mutex
	files         []string
	taskPhase     TaskType
}

type TasksMap struct {
	rwmu     sync.RWMutex
	tasksMap map[int]*Task
}

type Task struct {
	taskId      int
	taskPhase   TaskType
	monitorChan chan bool
	taskStatus  TaskStatus
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// go routine to monitor task table

func (c *Coordinator) MonitorTask(pTask *Task) {
	select {
	case success := <-pTask.monitorChan:
		if success && pTask.taskPhase == MapPhase {
			goto mapTaskSuccess
		} else if success && pTask.taskPhase == ReducePhase {
			goto reduceTaskSuccess
		} else {
			goto taskFailure
		}
	case <-time.After(10 * time.Second):
		goto taskFailure
	}

mapTaskSuccess:
	c.mu.Lock()
	pTask.taskStatus = Success
	c.succeededTask++
	fmt.Printf("monitor task in map succees %d \n", pTask.taskId)
	fmt.Printf("files number are %d \n", len(c.files))

	if c.succeededTask == len(c.files) {
		// switch to reduce phase
		c.taskPhase = ReducePhase
		c.succeededTask = 0
		c.tasksMap.tasksMap = make(map[int]*Task)
	}
	c.mu.Unlock()
	return

reduceTaskSuccess:
	c.mu.Lock()
	pTask.taskStatus = Success
	c.succeededTask++
	fmt.Printf("monitor task in reduce succees %d \n", pTask.taskId)

	if c.succeededTask == c.nReduce {
		// switch to reduce phase
		c.taskPhase = Done
	}
	c.mu.Unlock()
	return

taskFailure:
	c.mu.Lock()
	pTask.taskStatus = Unsigned
	fmt.Printf("monitor task failed %d \n", pTask.taskId)
	c.mu.Unlock()
	return
}

func (c *Coordinator) retrieveUnsignedTask() *Task {
	select {
	case task, ok := <-c.tasksToDo:
		if ok {

		}
	}
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	switch c.taskPhase {
	case MapPhase:
		pTask := c.retrieveUnsignedTask()
		if pTask == nil {
			reply.MapTaskId = c.mapTaskId
			pTask = &Task{taskId: c.mapTaskId, taskPhase: MapPhase, monitorChan: make(chan bool), taskStatus: Working}
			c.tasksMap.tasksMap[pTask.taskId] = pTask
			c.mu.Lock()
			c.mapTaskId++
			c.mu.Unlock()
		}
		reply.FileName = c.files[pTask.taskId]
		reply.CntReduceTask = c.nReduce
		reply.TaskType = pTask.taskPhase
		go c.MonitorTask(pTask)
		fmt.Printf("monitor task in map phase %d \n", pTask.taskId)

		break
	case ReducePhase:
		pTask := c.retrieveUnsignedTask()
		if pTask == nil {
			reply.ReduceTaskId = c.reduceTaskId
			pTask = &Task{taskId: c.reduceTaskId, taskPhase: ReducePhase, monitorChan: make(chan bool), taskStatus: Working}
			c.tasksMap.tasksMap[pTask.taskId] = pTask
			c.mu.Lock()
			c.reduceTaskId++
			c.mu.Unlock()
		}
		reply.ReduceTaskId = pTask.taskId
		reply.TaskType = ReducePhase
		go c.MonitorTask(pTask)
		fmt.Printf("monitor task in reduce phase %d", pTask.taskId)
		break
	case StopPhase:
		reply.TaskType = StopPhase
		break
	default:
		// stop phase don't need to assign any additional variables

		break
	}
	return nil
}

func (c *Coordinator) MarkMapTaskFinish(args *FinishMapTaskArgs, reply *TaskFinishedReply) error {
	c.tasksMap.tasksMap[args.TaskId].monitorChan <- true
	return nil
}

func (c *Coordinator) MarkReduceTaskFinish(args *FinishReduceTaskArgs, reply *TaskFinishedReply) error {
	c.tasksMap.tasksMap[args.TaskId].monitorChan <- true
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
	c.mu.Lock()
	if c.taskPhase == Done {
		c.mu.Unlock()
		return true
	}
	c.mu.Unlock()
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{mapTaskId: 0, reduceTaskId: 0, taskPhase: MapPhase, succeededTask: 0}

	// Your code here.
	c.nReduce = nReduce
	c.files = files
	c.tasksMap.tasksMap = make(map[int]*Task)
	c.server()
	// initial phase is mapping phase
	return &c
}
