package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type WorkerStatus struct {
	workerId string
	alive    bool
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var statusMutex sync.Mutex

//
// main/mrworker.go calls this function.
// 这个会被mrworker调用
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
	workerId string) {

	// Your worker implementation here.
	time.Sleep(time.Second)
	status := WorkerStatus{
		workerId: workerId,
		alive:    true,
	}
	go doPing(&status)

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	attempt := 0
	successCnt := 0
	preDone := false
	var job *Job = nil
	var err error = nil
	statusMutex.Lock()
	for status.alive {
		statusMutex.Unlock()
		attempt++
		fmt.Println("[Worker]: ", workerId, " attempting ", attempt, " times! ")

		// 通知Coordinator完成的时候，顺便申请一个新任务
		var newJob *Job = nil
		if preDone && job != nil {
			newJob, err = requestNewTask(workerId, job.JobId)
		} else {
			newJob, err = requestNewTask(workerId, -1)
		}

		if err != nil {
			fmt.Printf("[Worker]: worker %s attempted failed! \n", workerId)

			if attempt > 10 {
				fmt.Printf("worker exit after several tries !")
				statusMutex.Lock()
				status.alive = false
				statusMutex.Unlock()
			} else {
				fmt.Printf("[Worker]: %d times attempting after 2 seconds, error %s \n", attempt, err)
				time.Sleep(time.Second * 2)
			}

			continue
		}

		job = newJob
		preDone = false
		successCnt++
		attempt = 0
		fmt.Println("[Worker]: worker ", workerId, " get job ", job.JobId, " type = ", job.JobType)

		switch job.JobType {
		case Map:
			fmt.Println("[Worker]: doing map")
			if doMap(mapf, job, workerId) {
				preDone = true
				fmt.Println("[Worker Status]: map job ", job.JobId, " done by ", workerId)
			} else {
				fmt.Println(workerId, "[Worker Status]:  worker ", workerId, " failed in map job ", job.JobId)
			}
		case Reduce:
			fmt.Println("[Worker]: doing reduce")
			if doReduce(reducef, job, workerId) {
				preDone = true
				fmt.Println("[Worker Status]: reduce job ", job.JobId, " done by ", workerId)
			} else {
				fmt.Println(workerId, "[Worker Status]:  worker ", workerId, " failed reduce job ", job.JobId)
			}
		case Waitting:
			fmt.Printf("[Worker Status]: worker %s is waiting for a job!\n", workerId)
			time.Sleep(time.Second)
		case Kill:
			fmt.Printf("[Worker Status]: worker %s is terminated!\n", workerId)
			statusMutex.Lock()
			status.alive = false
			statusMutex.Unlock()
		}
		time.Sleep(time.Second)
		statusMutex.Lock()

	}
}

func doMap(mapf func(string, string) []KeyValue, job *Job, workerId string) bool {
	// map任务只有一个输入文件
	filename := job.InputFiles[0]

	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("[Worker doMap]: cannot open input file %v\n", filename)
		return false
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("[Worker doMap]: cannot read %v", filename)
		return false
	}
	intermediate := mapf(filename, string(content))

	nReduce := job.NReduce
	HashedKV := make([][]KeyValue, nReduce)

	for _, kv := range intermediate {
		HashedKV[ihash(kv.Key)%nReduce] = append(HashedKV[ihash(kv.Key)%nReduce], kv)
	}

	dir, _ := os.Getwd()

	tempFileNames := make([]string, nReduce)
	for i := 0; i < nReduce; i++ {
		tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
		tempFileNames[i] = tempFile.Name()
		if err != nil {
			tempFile.Close()
			return false
		}
		enc := json.NewEncoder(tempFile)
		for _, kv := range HashedKV[i] {
			err := enc.Encode(kv)
			if err != nil {
				return false
			}
		}
		err = tempFile.Close()
		if err != nil {
			return false
		}
	}

	for i := 0; i < nReduce; i++ {
		tmpfileName := tempFileNames[i]
		outputfileName := "mr-tmp-" + workerId + "-" + strconv.Itoa(job.JobId) + "-" + strconv.Itoa(i)
		err := os.Rename(tmpfileName, outputfileName)
		if err != nil {
			return false
		}
	}

	return true
}

func doReduce(reducef func(string, []string) string, job *Job, workerId string) bool {

	intermediate := readFromLocalFile(job.InputFiles)
	sort.Sort(ByKey(intermediate))

	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		tempFile.Close()
		log.Fatal("[Worker doReduce]: Failed to create temp file ", err)
		return false
	}
	fmt.Println("[Worker Reduce]: intermediate key cnt ", len(intermediate))
	cnt := 0
	for i, j := 0, 0; i < len(intermediate); i = j {

		// 找到一个key相等的区间[i, j)
		for j = i + 1; j < len(intermediate) && intermediate[j].Key == intermediate[i].Key; j++ {
		}
		cnt++
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
	}
	tempFile.Close()
	fmt.Println("[Worker Reduce]: different intermediate key ", cnt)
	fmt.Println("[Worker Reduce]: rename file to mr-out-", job.JobId)
	outfilename := fmt.Sprintf("mr-out-%d", job.JobId)
	os.Rename(tempFile.Name(), outfilename)

	return true
}

/*
 * 读取某个reduce任务的所有输入
 */
func readFromLocalFile(files []string) []KeyValue {
	var kvs []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			// 直接忽略decode失败的
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		file.Close()
	}
	return kvs
}

//func jobDone(workerId string, job *Job) {
//	args := job
//	reply := EmptyReply{}
//	err := rpcCall("Coordinator.jobDone", &args, &reply)
//	if err != nil {
//		log.Fatal("[Worker jobDone]: worker", workerId, " rpc failed!")
//	}
//	// 失敗則嘗試10次
//	for attemp := 0; err != nil && attemp < 10; attemp++ {
//		time.Sleep(time.Second)
//		log.Println("[Worker jobDone]: ", workerId, " attemp ", attemp, " times")
//		err = rpcCall("Coordinator.obDone", &args, &reply)
//	}
//	if err != nil {
//		log.Fatal("[Worker jobDone]: worker", workerId, " rpc failed after attempt !")
//	}
//}

func requestNewTask(workerId string, jobId int) (*Job, error) {
	request := WorkerRequest{workerId, jobId}

	reply := Job{}

	fmt.Println("[Worker requestNewTask]: request new task!!!")
	err := rpcCall("Coordinator.JobHandler", &request, &reply)

	if err != nil {
		fmt.Printf("[Worker]: request task rpc error %s! \n", err)
	} else {
		fmt.Println("[Worker]: get job dispatch jobid = ", reply.JobId, " jobType = ", reply.JobType)
	}

	return &reply, err
}

// 有冲突，但是没问题
func doPing(status *WorkerStatus) {
	req := WorkerRequest{WorkerId: status.workerId}

	statusMutex.Lock()
	//	每两秒ping一次
	for status.alive {
		statusMutex.Unlock()
		rpcCall("Coordinator.Ping", &req, nil)
		time.Sleep(time.Second * 2)
		statusMutex.Lock()
	}

	statusMutex.Unlock()
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func rpcCall(rpcname string, args interface{}, reply interface{}) error {
	// 使用Unix socket，因为脚本里面，需要检测这个socket file。如果你要改成tcp，那要改脚本
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	//c, err := rpc.DialHTTP("tcp", ":1234")
	if err != nil {
		log.Fatal("[Worker rpcCall] Dialing error: ", err)
	}
	defer func(c *rpc.Client) {
		err := c.Close()
		if err != nil {
			log.Fatal("[Worker rpcCall] rpc close error!")
		}
	}(c)

	return c.Call(rpcname, args, reply)
}
