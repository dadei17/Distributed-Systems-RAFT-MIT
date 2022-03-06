package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Master struct {
	servers      int
	mapHelper    helper
	reduceHelper helper

	lock sync.Mutex
}

type helper struct {
	inProcess map[string]bool
	done      map[string]bool
	counter   map[string]string
	timer     map[string]time.Time
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

func (m *Master) CreateWorker(args *TaskArgs, reply *TaskArgs) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	reply.ServerNum = m.servers
	return nil
}

func (h *helper) isAllDone() bool {
	ret := true
	for task, done := range h.done {
		if !done && time.Since(h.timer[task]) >= 10*time.Second && h.inProcess[task] {
			h.inProcess[task] = false
		}
		ret = done && ret
	}
	return ret
}

func (m *Master) GetTask(args *TaskArgs, reply *TaskReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.mapHelper.makeTask(reply, MAP) {
		return nil
	}
	if m.reduceHelper.makeTask(reply, REDUCE) {
		return nil
	}
	reply.Status = DONE
	return nil
}

func (h *helper) makeTask(reply *TaskReply, status TaskStatus) bool {
	if h.isAllDone() {
		return false
	}
	for task, inProcess := range h.inProcess {
		if inProcess || h.done[task] {
			continue
		}
		reply.Filename = task
		reply.Status = status
		reply.TaskNum = h.counter[task]
		h.timer[task] = time.Now()
		h.inProcess[task] = true
		return true
	}
	reply.Status = WAIT
	return true
}

func (m *Master) ReduceDone(args *TaskReply, reply *TaskReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.reduceHelper.done[args.TaskNum] = true
	return nil
}

func (m *Master) MapDone(args *TaskReply, reply *TaskReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.mapHelper.done[args.Filename] = true
	return nil
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.mapHelper.isAllDone() && m.reduceHelper.isAllDone()
}

func (m *Master) generateFiles() {
	for i := 0; i < m.servers; i++ {
		for j := 0; j < m.servers; j++ {
			os.Create("mr-" + strconv.Itoa(j) + "." + strconv.Itoa(i) + ".json")
		}
		os.Create("mr-out-" + strconv.Itoa(i) + ".txt")
	}
}

func (h *helper) initHelper() {
	h.inProcess = make(map[string]bool)
	h.timer = make(map[string]time.Time)
	h.done = make(map[string]bool)
	h.counter = make(map[string]string)
}

func (m *Master) initMasterReduce(files []string) {
	m.mapHelper.initHelper()
	for i, file := range files {
		m.mapHelper.inProcess[file] = false
		m.mapHelper.done[file] = false
		m.mapHelper.counter[file] = strconv.Itoa(i)
	}
}

func (m *Master) initMasterMap(servercounter int) {
	m.reduceHelper.initHelper()
	for i := 0; i < servercounter; i++ {
		srv := strconv.Itoa(i)
		m.reduceHelper.inProcess[srv] = false
		m.reduceHelper.done[srv] = false
		m.reduceHelper.counter[srv] = srv
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.servers = nReduce
	m.initMasterReduce(files)
	m.initMasterMap(nReduce)
	m.generateFiles()
	m.server()
	return &m
}
