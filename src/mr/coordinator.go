package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Phase int

const (
	MapPhase = iota
	ReducePhase
	AllDone
)

type JobStatus int

const (
	Processing = iota
	Idle
	Done
)

type WorkerInfo struct {
	workerId string
	//workerIp   string
	//workerPort string
	lastConnTime time.Time
}

type JobInfo struct {
	job        *Job
	workerInfo *WorkerInfo
	status     JobStatus
	startTime  time.Time
}

type Coordinator struct {
	// Your definitions here.
	nReduce      int
	nMap         int
	reduceUndone int
	mapUndone    int
	files        []string
	jobChannel   chan *Job
	phase        Phase
	globalJobId  int
	workerInfos  map[string]*WorkerInfo // 使用sync.Map也行
	jobInfos     map[int]*JobInfo
}

var mutex sync.Mutex
var phaseMutex sync.Mutex

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) genJobId() int {
	ret := c.globalJobId
	c.globalJobId++
	return ret
}

func (c *Coordinator) JobHandler(req *WorkerRequest, reply *Job) error {
	// jobid != -1表示，提交完成的任务
	if req.JobId != -1 {
		c.jobDone(req.WorkerId, req.JobId)
	}

	c.jobDispatch(req.WorkerId, reply)
	return nil
}

func (c *Coordinator) Ping(req *WorkerRequest, reply *EmptyReply) error {
	mutex.Lock()

	defer mutex.Unlock()
	if workerInfo, ok := c.workerInfos[req.WorkerId]; ok {
		workerInfo.lastConnTime = time.Now()
	}

	return nil
}

func (c *Coordinator) jobDispatch(workerId string, reply *Job) {

	fmt.Println("[Coordinator JobDispatch]: dispatching for worker ", workerId)
	phaseMutex.Lock()
	mutex.Lock()
	if c.phase == AllDone {
		reply.JobType = Kill
	} else if len(c.jobChannel) > 0 {
		// job可能被完成了。某个连不上的worker又连上了，然后还把work提交了
		for *reply = *<-c.jobChannel; len(c.jobChannel) > 0 && c.jobInfos[reply.JobId].status == Done; *reply = *<-c.jobChannel {
		}

		if c.jobInfos[reply.JobId].status == Done {
			reply.JobType = Waitting
		}

	} else {
		reply.JobType = Waitting
	}
	phaseMutex.Unlock()

	defer mutex.Unlock()
	if reply.JobType != Map && reply.JobType != Reduce {
		fmt.Println("[Coordinator JobDispatch]: dispatching a empty job for worker ", workerId, " done!")
		return
	}

	// 更新时间
	workerInfo, ok := c.workerInfos[workerId]
	if !ok {
		workerInfo = &WorkerInfo{
			workerId:     workerId,
			lastConnTime: time.Now(),
		}
		c.workerInfos[workerId] = workerInfo
	} else {
		workerInfo.lastConnTime = time.Now()
	}

	jobInfo, ok := c.jobInfos[reply.JobId]
	jobInfo.status = Processing
	jobInfo.workerInfo = workerInfo
	jobInfo.startTime = workerInfo.lastConnTime

	fmt.Println("[Coordinator JobDispatch]: dispatching job for worker ", workerId, " done!")
}

func (c *Coordinator) genReduceJob() {
	dir, _ := os.Getwd()
	for i := 0; i < c.nReduce; i++ {
		jobId := c.genJobId()
		job := Job{
			JobType:    Reduce,
			InputFiles: getTempFileForReducer(i, dir),
			JobId:      jobId,
		}
		jobInfo := JobInfo{
			job:        &job,
			status:     Idle,
			workerInfo: nil,
		}

		c.jobInfos[jobId] = &jobInfo
		c.jobChannel <- &job
	}
}

func (c *Coordinator) jobDone(workerId string, jobId int) {
	mutex.Lock()
	// 这个锁要放在这里，不然放在里面会造成饥饿
	phaseMutex.Lock()
	defer phaseMutex.Unlock()
	defer mutex.Unlock()
	jobInfo, ok := c.jobInfos[jobId]
	if ok && jobInfo.status != Done {
		jobInfo.status = Done
		delete(c.jobInfos, jobId)
		if jobInfo.job.JobType == Map {
			c.mapUndone--
			if c.mapUndone == 0 {
				c.genReduceJob()
				c.phase = ReducePhase
			}
		} else {
			c.reduceUndone--
			if c.reduceUndone == 0 {
				c.phase = AllDone
			}
		}
	}
}

// 重置那些长时间没有响应的worker所分配的job
func (c *Coordinator) crashHandler() {
	for {
		time.Sleep(time.Second * 2)
		mutex.Lock()
		phaseMutex.Lock()
		if c.phase == AllDone {
			phaseMutex.Unlock()
			mutex.Unlock()
			return
		}
		phaseMutex.Unlock()
		for _, jobInfo := range c.jobInfos {
			if jobInfo.status != Processing {
				continue
			}
			workerInfo := jobInfo.workerInfo
			now := time.Now()
			if now.Sub(workerInfo.lastConnTime) > 10*time.Second || now.Sub(jobInfo.startTime) > 20*time.Second {

				fmt.Println("[Coordinator crashHandler] detect a crash on job ", jobInfo.job.JobId)
				jobInfo.status = Idle
				c.jobChannel <- jobInfo.job
				fmt.Println("[Coordinator crashHandler] put into channel again ", jobInfo.job.JobId)
			}
		}

		mutex.Unlock()
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	socket, err := net.Listen("unix", sockname)
	if err != nil {
		log.Fatal("listen error:", err)
	}
	go http.Serve(socket, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
// 读也要mutex，不然冲突会被检测出来
func (c *Coordinator) Done() bool {
	// Your code here.
	phaseMutex.Lock()
	defer phaseMutex.Unlock()
	return c.phase == AllDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
// 创建Coordinator，会被mrCoordinator调用
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	nMap := len(files)
	// Your code here.
	fmt.Println("[Coordenator]: making coordinator!")

	chanSz := nMap
	if nReduce > nMap {
		chanSz = nReduce
	}

	c := &Coordinator{
		jobChannel:   make(chan *Job, chanSz),
		phase:        MapPhase,
		nReduce:      nReduce,
		reduceUndone: nReduce,
		mapUndone:    nMap,
		nMap:         nMap,
		globalJobId:  0,
		workerInfos:  make(map[string]*WorkerInfo),
		jobInfos:     make(map[int]*JobInfo),
	}
	fmt.Println("[Coordenator]: generating map job!")
	for _, file := range files {
		jobId := c.genJobId()
		job := Job{
			JobType:    Map,
			InputFiles: []string{file},
			JobId:      jobId,
			NReduce:    nReduce,
		}

		jobInfo := JobInfo{
			job:        &job,
			status:     Idle,
			workerInfo: nil,
		}

		c.jobInfos[jobId] = &jobInfo
		c.jobChannel <- &job
	}
	fmt.Println("[Coordenator]: coordinator made!")
	c.server()
	go c.crashHandler()
	return c
}

func getTempFileForReducer(reduceId int, dir string) []string {
	var files []string
	rd, _ := ioutil.ReadDir(dir)
	for _, file := range rd {
		parts := strings.Split(file.Name(), "-")
		_, err := strconv.Atoi(parts[2])

		if len(parts) == 5 && err == nil &&
			strings.HasPrefix(file.Name(), "mr-tmp-") &&
			strings.HasSuffix(file.Name(), "-"+strconv.Itoa(reduceId)) {
			files = append(files, file.Name())
		}
	}

	fmt.Println("[Coordinator getTempFileForReducer]: temp file for reducer", files)

	return files
}
