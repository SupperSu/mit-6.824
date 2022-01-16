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

type Coordinator struct {
	// Your definitions here.
	succeededTask int
	mapTaskId     int
	reduceTaskId  int
	nReduce       int
	tasksMap      TasksMap
	taskToDo      chan *Task
	taskFailed    chan *Task
	taskSucceeded chan *Task
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
	fileName    string // for mapping task
	taskPhase   TaskType
	monitorChan chan bool
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
	fmt.Printf("Monitor task %d \n", pTask.taskId)
	select {
	case success := <-pTask.monitorChan:
		if success {
			goto taskSuccess
		} else {
			goto taskFailure
		}
	case <-time.After(10 * time.Second):
		goto taskFailure
	}

taskSuccess:
	c.taskSucceeded <- pTask
	fmt.Printf("monitor task success %d \n", pTask.taskId)
	return

taskFailure:
	c.taskFailed <- pTask
	fmt.Printf("monitor task failed %d \n", pTask.taskId)
	return
}

func (c *Coordinator) taskManager() {
	for {
		if c.taskPhase == Done {
			break
		}
		fmt.Printf("Task Manager running \n")
		select {
		case t1 := <-c.taskFailed:
			fmt.Printf("Pushed Failed task into todo queue \n")
			c.taskToDo <- t1
			break
		case <-c.taskSucceeded:
			c.succeededTask++
			c.mu.Lock()
			fmt.Printf("Success task %d pushed \n", c.succeededTask)
			if c.succeededTask == len(c.files) && c.taskPhase == MapPhase {
				c.taskPhase = ReducePhase
				c.succeededTask = 0
				c.tasksMap.tasksMap = make(map[int]*Task)

				for i := 0; i < c.nReduce; i++ {
					task := &Task{taskId: i, taskPhase: ReducePhase, monitorChan: make(chan bool, 1)}
					c.taskToDo <- task
					fmt.Printf("reduce task %d pushed \n", i)
				}
			} else if c.succeededTask == c.nReduce && c.taskPhase == ReducePhase {
				c.taskPhase = Done
			}
			c.mu.Unlock()
			break
		}
	}
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	select {
	case task := <-c.taskToDo:
		c.tasksMap.rwmu.Lock()
		c.tasksMap.tasksMap[task.taskId] = task
		c.tasksMap.rwmu.Unlock()
		reply.TaskType = task.taskPhase
		reply.TaskId = task.taskId
		reply.CntReduceTask = c.nReduce
		reply.FileName = task.fileName
		fmt.Printf("Sent out task with id %d, phase %s \n", task.taskId, task.taskPhase)
		go c.MonitorTask(task)
	case <-time.After(12 * time.Second):
		fmt.Printf("Timeout for pulling out a new task")
		reply.TaskType = Relax
	}
	return nil
}

func (c *Coordinator) MarkTaskFinish(args *FinishTaskArgs, reply *TaskFinishedReply) error {
	c.tasksMap.rwmu.RLock()
	c.tasksMap.tasksMap[args.TaskId].monitorChan <- true
	c.tasksMap.rwmu.RUnlock()
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
	c.taskToDo = make(chan *Task, len(files)+nReduce)
	c.taskSucceeded = make(chan *Task, len(files)+nReduce)
	c.taskFailed = make(chan *Task, len(files)+nReduce)
	// initialize mapping tasks and push to toTo channel
	go c.taskManager()
	go func() {
		for id, fil := range files {
			task := &Task{taskId: id, fileName: fil, taskPhase: MapPhase, monitorChan: make(chan bool, 1)}
			c.taskToDo <- task
		}
	}()

	c.server()
	return &c
}
