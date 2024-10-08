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
	filenames     []string
	nReduce       int
	counter       int
	mu            sync.Mutex
	mapTimes      []time.Time
	mapFinished   []bool
	mapUnFinished []int
	mapFinishedMu sync.Mutex
	mapUnMu       sync.Mutex
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
	if c.counter >= len(c.filenames) {
		c.mu.Unlock()
		c.mapUnMu.Lock()
		for len(c.mapUnFinished) != 0 {
			for _, v := range c.mapUnFinished {

				if (time.Now().Sub(c.mapTimes[v]))*1000000 >= 10 {
					reply.Filename = c.filenames[v]
					reply.Id = v
					c.mapTimes[v] = time.Now()

				}
			}
			c.mapUnMu.Unlock()
			time.Sleep(time.Second)
		}
		c.mapUnMu.Unlock()
		reply.Finish_mapf = true
		return nil
	}
	reply.Filename = c.filenames[c.counter]
	reply.Id = c.counter
	startTime := time.Now()
	c.mapTimes[c.counter] = startTime
	c.mapUnMu.Lock()
	c.mapUnFinished = append(c.mapUnFinished, c.counter)
	c.mapUnMu.Unlock()
	// fmt.Println(reply)
	reply.NReduce = c.nReduce
	c.counter += 1

	c.mu.Unlock()
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
	// Your code here
	c.server()
	return &c
}
