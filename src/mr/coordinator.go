package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

const MAX_WAIT_TIME_FOR_TASK time.Duration = time.Second * 10 // Wait for max 10 seconds for the workers to respond
const DEFAULT_N_REDUCE int = 10                               // Default nReduce

var coordinatorId string

type task struct {
	isDone         bool
	inputFiles     []string
	currentAttempt uint
	outputFiles    []string
}

type Coordinator struct {
	sockName string
	lock     sync.RWMutex
	// Map Tasks Status
	mapTasksStatus     []task
	mapTasksQue        chan uint
	nRemainingMapTasks uint
	// Reduce Tasks Status
	reduceTasksStatus     []task
	reduceTasksQue        chan uint
	nRemainingReduceTasks uint
	nReduce               uint
}

func handleGetTask(lock *sync.RWMutex, nReduce uint, tasksQue chan uint, tasksStatus []task, taskKind TaskKind, args *GetTaskArgs, reply *TaskInput) {
	_ = args
	log.Printf("Coordinator %s: Run handleGetTask for taskKind: %s\n", coordinatorId, TaskKindAsString(taskKind))
	taskId := <-tasksQue
	reply.Kind = taskKind
	reply.TaskId = taskId
	reply.Attempt = tasksStatus[taskId].currentAttempt
	reply.InputFiles = tasksStatus[taskId].inputFiles
	reply.NReduce = nReduce
	go func() {
		time.Sleep(MAX_WAIT_TIME_FOR_TASK)
		lock.Lock()
		reschedule := false
		defer func() {
			lock.Unlock()
			if reschedule {
				tasksQue <- taskId
			}
		}()
		if tasksStatus[taskId].isDone {
			reschedule = false
		} else {
			// Task not done within the wait time, i.e.; no response from the worker in max wait time
			// Reschedule the task id
			tasksStatus[taskId].currentAttempt += 1 // Increment attempt
			log.Printf("Coordinator %s: TaskKind: %s, TaskId: %d not done within %v duration, Rescheduling\n", coordinatorId, TaskKindAsString(taskKind), taskId, MAX_WAIT_TIME_FOR_TASK)
			reschedule = true
		}
	}()
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *TaskInput) error {
	c.lock.RLock()
	isDone := false
	tasksStatus := c.mapTasksStatus
	tasksQue := c.mapTasksQue
	taskKind := TaskKindMap
	defer func() {
		c.lock.RUnlock()
		if isDone {
			reply.Kind = TaskKindDone
		} else {
			handleGetTask(&c.lock, c.nReduce, tasksQue, tasksStatus, taskKind, args, reply)
		}
	}()
	if c.nRemainingMapTasks == 0 {
		if c.nRemainingReduceTasks == 0 {
			isDone = true
			return nil
		}
		tasksStatus = c.reduceTasksStatus
		tasksQue = c.reduceTasksQue
		taskKind = TaskKindReduce
	}
	return nil
}

func (c *Coordinator) SendTaskOutput(args *TaskOutput, reply *SendTaskOutputReply) error {
	_ = reply
	log.Printf("Coordinator %s: TaskOutput: %+v\n", coordinatorId, args)
	c.lock.Lock()
	defer c.lock.Unlock()
	switch args.Kind {
	case TaskKindMap:
		if c.nRemainingMapTasks > 0 && c.mapTasksStatus[args.TaskId].currentAttempt == args.Attempt {
			// Mark as done and save outputFiles
			c.mapTasksStatus[args.TaskId].outputFiles = args.OutputFiles
			c.mapTasksStatus[args.TaskId].isDone = true
			c.nRemainingMapTasks -= 1 // Decrementing number of remaining map tasks
			// Also, updating reduce tasks
			for idx, bucketId := range args.BucketIds {
				if len(c.reduceTasksStatus[bucketId].inputFiles) == 0 {
					// If some input for the bucketId'th reduce task, then push to reduceTasksQue and update nRemainingReduceTasks
					c.reduceTasksQue <- bucketId
					c.nRemainingReduceTasks += 1
				}
				c.reduceTasksStatus[bucketId].inputFiles = append(c.reduceTasksStatus[bucketId].inputFiles, args.OutputFiles[idx])
			}
			if c.nRemainingMapTasks == 0 {
				// Close map que once no remaining map tasks
				close(c.mapTasksQue)
			}
		}
	case TaskKindReduce:
		if c.nRemainingReduceTasks > 0 && c.reduceTasksStatus[args.TaskId].currentAttempt == args.Attempt {
			// Mark as done and save outputFiles
			c.reduceTasksStatus[args.TaskId].outputFiles = args.OutputFiles
			c.reduceTasksStatus[args.TaskId].isDone = true
			c.nRemainingReduceTasks -= 1 // Decrementing number of remaining reduce tasks
			if c.nRemainingReduceTasks == 0 {
				// Close reduce que once no remaining reduce tasks
				close(c.reduceTasksQue)
			}
		}
	default:
		log.Printf("Coordinator %s: TaskKind %s not supported yet!\n", coordinatorId, TaskKindAsString(args.Kind))
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockName string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockName)
	var e error
	var l net.Listener
	sockName, isTCP := strings.CutPrefix(sockName, "tcp://")
	if isTCP {
		log.Printf("Coordinator %s: Listening on tcp://%s\n", coordinatorId, sockName)
		l, e = net.Listen("tcp", sockName)
	} else {
		log.Printf("Coordinator %s: Listening on unix socker: %s\n", coordinatorId, sockName)
		l, e = net.Listen("unix", sockName)
	}
	if e != nil {
		log.Fatalf("Coordinator %s: listen error %s: %v", coordinatorId, sockName, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.lock.RLock()
	defer c.lock.RUnlock()
	log.Printf("Coordinator %s: nRemainingMapTasks: %d, nRemainingReduceTasks: %d\n", coordinatorId, c.nRemainingMapTasks, c.nRemainingReduceTasks)
	return c.nRemainingMapTasks == 0 && c.nRemainingReduceTasks == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockName string, files []string, nReduce int) *Coordinator {
	coordinatorHostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("failed to get worker hostname: %v", err)
	}
	coordinatorId = fmt.Sprintf("%s-%d", coordinatorHostname, os.Getpid())
	if nReduce <= 0 {
		nReduce = DEFAULT_N_REDUCE
	}
	// Map Tasks Status
	mapTasksStatus := make([]task, len(files))
	mapTasksQue := make(chan uint, len(files))
	for taskId, file := range files {
		mapTasksStatus[taskId].isDone = false     // Not done
		mapTasksStatus[taskId].currentAttempt = 1 // By default, first attempt
		mapTasksStatus[taskId].inputFiles = []string{file}
		mapTasksQue <- uint(taskId)
	}
	// Reduce Tasks Status
	reduceTasksStatus := make([]task, nReduce)
	reduceTasksQue := make(chan uint, nReduce)
	for taskId := range nReduce {
		reduceTasksStatus[taskId].isDone = false     // Not done
		reduceTasksStatus[taskId].currentAttempt = 1 // By default, first attempt
	}
	c := Coordinator{
		sockName: sockName,
		lock:     sync.RWMutex{},
		// Map Tasks Status
		mapTasksStatus:     mapTasksStatus,
		mapTasksQue:        mapTasksQue,
		nRemainingMapTasks: uint(len(files)),
		// Reduce Tasks Status
		reduceTasksStatus:     reduceTasksStatus,
		reduceTasksQue:        reduceTasksQue,
		nRemainingReduceTasks: 0,
		nReduce:               uint(nReduce),
	}
	c.server(sockName)
	return &c
}
