package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type WorkerStatus uint32

const (
	IDLE       WorkerStatus = iota
	RUNNING    WorkerStatus = iota
	EXITED     WorkerStatus = iota
	MAYBECRASH WorkerStatus = iota
)

type CoordinatorStatus uint32

const (
	INIT        CoordinatorStatus = iota
	MAPPING     CoordinatorStatus = iota
	REDUCING    CoordinatorStatus = iota
	ALLTASKDONE CoordinatorStatus = iota
	CANEXIT     CoordinatorStatus = iota
)

type Coordinator struct {
	// Your definitions here.
	M uint32
	R uint32

	timeThreshold int64

	inputFilenames []string

	mtx sync.Mutex

	status CoordinatorStatus

	nextWorkerId uint32
	workerStatus map[uint32]WorkerStatus

	unstartedTasks map[uint32]*Task
	runningTasks   map[uint32]*Task
	doneTasks      map[uint32]*Task
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RegisterWorker(args *uint32, workerId *uint32) error {
	// *workerId = c.nextWorkerId
	// c.nextWorkerId++

	// id of "EXITED" worker can be reassigned
	// if there is no "EXITED" worker, then assign a new id
	var workerId_ uint32 = 0
	for {
		v, ok := c.workerStatus[workerId_]
		if !ok || v == EXITED {
			break
		}
		workerId_++
	}
	*workerId = workerId_

	return nil
}

func (c *Coordinator) GetTask(workerId *uint32, task *Task) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if c.status != ALLTASKDONE {
		if len(c.unstartedTasks) == 0 {
			task.TType = TRYAGAIN
		} else {
			var taskId uint32
			var pTask *Task
			for k, v := range c.unstartedTasks {
				taskId = k
				pTask = v
				*task = *v
				break
			}
			fmt.Println("GetTask", task.GId)
			delete(c.unstartedTasks, taskId)
			pTask.workerId = *workerId
			c.runningTasks[taskId] = pTask
			c.runningTasks[taskId].StartTime = time.Now()
		}

		c.workerStatus[*workerId] = RUNNING
		// c.workerStatus[*workerId] = RUNNING*1000 + WorkerStatus(task.Id)
	} else {
		task.TType = NOMORE

		c.workerStatus[*workerId] = EXITED
	}

	return nil
}

func (c *Coordinator) NotifyDone(taskId *uint32, reply *uint32) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	fmt.Println("NotifyDone", *taskId)

	task := c.runningTasks[*taskId]
	delete(c.runningTasks, *taskId)
	c.doneTasks[*taskId] = task

	c.workerStatus[task.workerId] = IDLE

	return nil
}

func (c *Coordinator) checkExpiredTasks() error {
	for k, v := range c.runningTasks {
		if time.Since(v.StartTime) > time.Duration(c.timeThreshold)*time.Second {
			c.unstartedTasks[k] = v
			delete(c.runningTasks, k)
			// delete(c.workerStatus, v.worker)
			c.workerStatus[v.workerId] = MAYBECRASH
		}
	}

	return nil
}

func (c *Coordinator) coordinate() error {
	for {
		c.mtx.Lock()

		fmt.Println("Coordinator::coordinate: c.status", c.status)

		switch c.status {
		case INIT:
			var curTaskId uint32 = 0
			var curMapId uint32 = 0

			for curMapId = 0; curMapId < c.M; curMapId++ {
				c.unstartedTasks[curTaskId] = &Task{
					GId: curTaskId,

					TType: MAP,
					Id:    uint32(curMapId),
					// StartTime: time.Now(),

					M: c.M,
					R: c.R,
				}
				curTaskId++
			}

			curMapId = 0
			for _, filename := range c.inputFilenames {
				c.unstartedTasks[uint32(curMapId)%c.M].InputFilenames = append(c.unstartedTasks[uint32(curMapId)%c.M].InputFilenames, filename)
				curMapId++
			}

			c.status = MAPPING

		case MAPPING:
			c.checkExpiredTasks()
			if len(c.doneTasks) == int(c.M) {
				if len(c.unstartedTasks) != 0 {
					fmt.Printf("Coordinator::CheckTasks: MAPPING, len(c.unstartedTasks) = %d\n", len(c.unstartedTasks))
				}
				if len(c.runningTasks) != 0 {
					fmt.Printf("Coordinator::CheckTasks: MAPPING, len(c.runningTasks) = %d\n", len(c.runningTasks))
				}
				for k := range c.doneTasks {
					delete(c.doneTasks, k)
				}

				var curTaskId uint32 = 100000
				for curReduceId := 0; curReduceId < int(c.R); curReduceId++ {
					c.unstartedTasks[curTaskId] = &Task{
						GId: curTaskId,

						TType:     REDUCE,
						Id:        uint32(curReduceId),
						StartTime: time.Now(),

						M: c.M,
						R: c.R,
					}
					curTaskId++
				}

				c.status = REDUCING
			}

		case REDUCING:
			c.checkExpiredTasks()
			if len(c.doneTasks) == int(c.R) {
				if len(c.unstartedTasks) != 0 {
					fmt.Printf("Coordinator::CheckTasks: REDUCING, len(c.unstartedTasks) != 0")
				}
				if len(c.runningTasks) != 0 {
					fmt.Printf("Coordinator::CheckTasks: REDUCING, len(c.runningTasks) != 0")
				}
				for k := range c.doneTasks {
					delete(c.doneTasks, k)
				}

				c.status = ALLTASKDONE
			}

		case ALLTASKDONE:
			canExit := true
			fmt.Println(c.workerStatus)
			for _, v := range c.workerStatus {
				// if v != EXITED || v != MAYBECRASH {
				if v == RUNNING {
					canExit = false
					break
				}
			}
			if canExit {
				c.status = CANEXIT
				c.mtx.Unlock()
				goto end
			}

		}
		c.mtx.Unlock()

		time.Sleep(1 * time.Second)
		// time.Sleep(100 * time.Millisecond)
	}

end:
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
	// Your code here.
	c.mtx.Lock()
	defer c.mtx.Unlock()

	return c.status == CANEXIT
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// inputFilenames, _ := filepath.Glob(files[0])

	// fmt.Println(files)

	c := Coordinator{
		M:              8,
		R:              uint32(nReduce),
		inputFilenames: files,
		// inputFilenames: inputFilenames,
		timeThreshold: 10,

		mtx: sync.Mutex{},

		status: INIT,

		nextWorkerId: 0,
		workerStatus: make(map[uint32]WorkerStatus),

		unstartedTasks: make(map[uint32]*Task),
		runningTasks:   make(map[uint32]*Task),
		doneTasks:      make(map[uint32]*Task),
	}

	// Your code here.
	go c.coordinate()

	c.server()
	return &c
}
