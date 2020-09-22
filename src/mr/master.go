package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "strconv"
import "sync"
import "time"


type Master struct {
	mapTasks []Task
	reduceTasks []Task
	done	bool
	mu      sync.Mutex
}

type Task struct {
	TaskType string // "map" or "reduce" or "exit" or "wait"
	TaskState string // "idle" or "in-progress" or "completed"
	TaskNumber int // the index in Master.mapTasks or Master.reduceTasks
	Nreduce int
	Pid int // who finished this task
	StartTime int64
	InputFilenames []string
	OutputFilenames []string
}

func (m *Master) AskForTask(args *EmptyArg, reply *Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.checkAllMapTasksFinished() {
		*reply = m.getMapTask()
	} else {
		*reply = m.getReduceTask()
	}
	if reply.TaskType == "exit" {
		m.done = true
	}
	return nil
}

func (m *Master) FinishTask(args *Task, reply *EmptyArg) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if args.TaskType == "map" {
		m.mapTasks[args.TaskNumber] = *args
	} else {
		m.reduceTasks[args.TaskNumber] = *args
	}
	return nil
}

func (m *Master) checkAllMapTasksFinished() bool {
	for _, mt := range m.mapTasks {
		if mt.TaskState != "completed" {
			return false
		}
	}
	return true
}

func (m *Master) getMapTask() Task {
	for i := range m.mapTasks {
		if (m.mapTasks[i].TaskState == "idle") || (m.mapTasks[i].TaskState == "in-progress" && (time.Now().Unix() - m.mapTasks[i].StartTime) > 10) {
			m.mapTasks[i].TaskState = "in-progress"
			m.mapTasks[i].StartTime = time.Now().Unix()
			return m.mapTasks[i]
		}
	}
	return Task{TaskType:"wait"}
}

func (m *Master) getReduceTask() Task {
	for i := range m.reduceTasks {
		if m.reduceTasks[i].TaskState == "idle" || (m.reduceTasks[i].TaskState == "in-progress" && (time.Now().Unix() - m.reduceTasks[i].StartTime) > 10) {
			m.reduceTasks[i].TaskState = "in-progress"
			m.reduceTasks[i].StartTime = time.Now().Unix()
			return m.reduceTasks[i]
		}
	}
	for i := range m.reduceTasks {
		if m.reduceTasks[i].TaskState == "in-progress" {
			return Task{TaskType:"wait"}
		}
	}
	return Task{TaskType:"exit"}
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
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{done:false}
	for x, originalFilename := range files {
		mt := Task{Nreduce:nReduce}
		mt.TaskType = "map"
		mt.TaskState = "idle"
		mt.TaskNumber = x
		mt.InputFilenames = append(mt.InputFilenames, originalFilename)
		for y := 0; y < nReduce; y++ {
			mt.OutputFilenames = append(mt.OutputFilenames, "mr"+"-"+strconv.Itoa(x)+"-"+strconv.Itoa(y))
		}
		m.mapTasks = append(m.mapTasks, mt)
	}
	for y := 0; y < nReduce; y++ {
		rt := Task{Nreduce:nReduce}
		rt.TaskType = "reduce"
		rt.TaskState = "idle"
		rt.TaskNumber = y
		for x := range files {
			rt.InputFilenames = append(rt.InputFilenames, "mr"+"-"+strconv.Itoa(x)+"-"+strconv.Itoa(y))
		}
		rt.OutputFilenames = append(rt.OutputFilenames, "mr-out-"+strconv.Itoa(y))
		m.reduceTasks = append(m.reduceTasks, rt)
	}
	m.server()
	return &m
}
