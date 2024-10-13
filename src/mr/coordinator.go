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
	filenames []string
	nReduce   int
	// for map
	counter       int
	mu            sync.Mutex
	mapTimes      []time.Time
	mapFinished   []bool
	mapUnFinished []int
	mapFinishedMu sync.Mutex
	mapUnMu       sync.Mutex
	timeMu        sync.Mutex

	// for reduce
	reduceMu         sync.Mutex
	reduceCount      int
	reduceTimes      []time.Time
	reduceFinished   []bool
	reduceUnFinished []int
	reduceFinishedMu sync.Mutex
	reduceUnMu       sync.Mutex
	reduceTimeMu     sync.Mutex

	stop   bool
	stopMu sync.Mutex
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

func (c *Coordinator) SendFileName(args *SendArgs, reply *SendReply) error {
	c.mu.Lock()
	//fmt.Println("handler SendFileName run")
	count := c.counter
	c.counter += 1
	c.mu.Unlock()
	if count >= len(c.filenames) {
		c.mapUnMu.Lock()
		for len(c.mapUnFinished) != 0 {
			for _, v := range c.mapUnFinished {
				c.timeMu.Lock()
				timeDifference := time.Now().Sub(c.mapTimes[v])
				timeDifferenceInMs := timeDifference.Milliseconds()
				if timeDifferenceInMs >= 10000 {
					reply.Filename = c.filenames[v]
					reply.Id = v
					c.mapTimes[v] = time.Now()
					reply.NReduce = c.nReduce
					c.timeMu.Unlock()
					c.mapUnMu.Unlock()
					return nil
				}
				c.timeMu.Unlock()
			}
			c.mapUnMu.Unlock()
			time.Sleep(time.Second)
			c.mapUnMu.Lock()
		}
		c.mapUnMu.Unlock()
		reply.Finish_mapf = true
		return nil
	}

	reply.Filename = c.filenames[count]
	reply.Id = count
	startTime := time.Now()
	c.timeMu.Lock()
	c.mapTimes[count] = startTime
	c.timeMu.Unlock()
	c.mapUnMu.Lock()
	c.mapUnFinished = append(c.mapUnFinished, count)
	c.mapUnMu.Unlock()
	// fmt.Println(reply)
	reply.NReduce = c.nReduce

	return nil
}

func (c *Coordinator) FinishedMap(args *FinishedMapArgs, reply *FinishedMapReply) error {
	c.mapFinishedMu.Lock()
	if c.mapFinished[args.Id] == true {
		c.mapFinishedMu.Unlock()
		for i := 0; i < c.nReduce; i++ {
			fileName := args.TempFileNames[i]
			err := os.Remove(fileName)
			if err != nil {
				log.Fatalf("Error removing file %v: %v", fileName, err)
			}
		}
		return nil
	}
	c.mapFinished[args.Id] = true
	c.mapFinishedMu.Unlock()
	for i := 0; i < c.nReduce; i++ {
		oldFileName := args.TempFileNames[i]
		newFileName := fmt.Sprintf("mr-%v-%v", args.Id, i)
		err := os.Rename(oldFileName, newFileName)
		if err != nil {
			log.Fatalf("os.Rename(%v, %v) failed: %v", oldFileName, newFileName, err)
		}

		c.mapUnMu.Lock()
		j := 0
		for _, v := range c.mapUnFinished {
			if v != args.Id {
				c.mapUnFinished[j] = v
				j++
			}
		}
		c.mapUnFinished = c.mapUnFinished[:j]
		c.mapUnMu.Unlock()

	}
	return nil
}

func (c *Coordinator) SendReduceTask(args *SendReduceArgs, reply *SendReduceReply) error {
	c.reduceMu.Lock()
	count := c.reduceCount
	c.reduceCount++
	c.reduceMu.Unlock()
	if count >= c.nReduce {
		c.reduceUnMu.Lock()
		for len(c.reduceUnFinished) != 0 {
			// fmt.Println("wait for finish", len(c.reduceUnFinished))
			for _, v := range c.reduceUnFinished {
				// fmt.Println("unfinished task", v)
				c.reduceTimeMu.Lock()
				// fmt.Println("time", time.Now().Sub(c.reduceTimes[v]))
				timeDifference := time.Now().Sub(c.reduceTimes[v])
				timeDifferenceInMs := timeDifference.Milliseconds()
				if timeDifferenceInMs >= 10000 {
					reply.TaskId = v
					reply.FileLen = len(c.filenames)
					c.reduceTimes[v] = time.Now()
					c.reduceTimeMu.Unlock()
					c.reduceUnMu.Unlock()
					return nil
				}
				c.reduceTimeMu.Unlock()
			}

			c.reduceUnMu.Unlock()

			time.Sleep(time.Second)
			c.reduceUnMu.Lock()
		}
		c.reduceUnMu.Unlock()
		reply.Finished = true
		// fmt.Println("finished", len(c.reduceUnFinished))
		c.stopMu.Lock()
		c.stop = true
		c.stopMu.Unlock()
		return nil
	}

	reply.TaskId = count
	reply.FileLen = len(c.filenames)

	c.reduceUnMu.Lock()
	c.reduceUnFinished = append(c.reduceUnFinished, count)
	c.reduceUnMu.Unlock()

	c.reduceTimeMu.Lock()
	c.reduceTimes[count] = time.Now()
	c.reduceTimeMu.Unlock()

	return nil
}

func (c *Coordinator) FinishedReduce(args *FinishedReduceArgs, reply *FinishedReduceReply) error {
	c.reduceFinishedMu.Lock()
	// fmt.Println(args.Id)
	if c.reduceFinished[args.Id] == true {
		err := os.Remove(args.FileName)
		if err != nil {
			log.Fatalf("Error removing file %v: %v", args.FileName, err)
		}
		c.reduceFinishedMu.Unlock()
		return nil
	}

	c.reduceFinished[args.Id] = true
	c.reduceFinishedMu.Unlock()

	newFileName := fmt.Sprintf("mr-out-%v", args.Id)
	err := os.Rename(args.FileName, newFileName)
	if err != nil {
		log.Fatalf("Error renaming file %v: %v", newFileName, err)
	}
	c.reduceUnMu.Lock()
	j := 0
	for _, v := range c.reduceUnFinished {
		if v != args.Id {
			c.reduceUnFinished[j] = v
			j++
		}
	}
	c.reduceUnFinished = c.reduceUnFinished[:j]
	c.reduceUnMu.Unlock()
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
	c.stopMu.Lock()
	ret = c.stop
	c.stopMu.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.filenames = files
	c.nReduce = nReduce
	c.counter = 0
	c.mapTimes = make([]time.Time, len(files))
	c.mapFinished = make([]bool, len(files))
	c.mapUnFinished = make([]int, 0, len(files))

	c.reduceFinished = make([]bool, nReduce)
	c.reduceUnFinished = make([]int, 0, nReduce)
	c.reduceTimes = make([]time.Time, nReduce)
	c.reduceCount = 0

	// Your code here
	c.server()
	return &c
}
