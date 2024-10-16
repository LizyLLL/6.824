package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
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

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	// fmt.Println("Starting worker...")
	for {
		reply := CallSendFileName()
		// fmt.Println("return from call")
		filename := reply.Filename
		mapResolved := reply.Finish_mapf
		if mapResolved == true {
			break
		}
		// fmt.Println("make tempfile")
		// fmt.Println(filename)
		file, err := os.Open(filename)

		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kvs := mapf(filename, string(content))
		tempFiles := make([]*os.File, reply.NReduce)
		tempFileNames := make([]string, reply.NReduce)
		for i := 0; i < reply.NReduce; i++ {
			//fmt.Println(tempFileName)
			file, err := ioutil.TempFile(".", "")
			tempFileNames[i] = file.Name()
			if err != nil {
				log.Fatalf("cannot create temp file")
			}
			tempFiles[i] = file
		}

		for _, kv := range kvs {
			key := kv.Key
			index := ihash(key) % reply.NReduce

			file := tempFiles[index]
			enc := json.NewEncoder(file)
			err = enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode ")
			}

		}
		for i := 0; i < reply.NReduce; i++ {
			file := tempFiles[i]
			file.Close()
		}

		CallFinishedMap(tempFileNames, reply.Id)

	}

	for {
		reply := CallSendReduceTask()
		if reply.Finished == true {
			return
		}
		kva := make([]KeyValue, 0)
		for i := 0; i < reply.FileLen; i++ {
			intermediateFileName := fmt.Sprintf("mr-%v-%v", i, reply.TaskId)
			file, err := os.Open(intermediateFileName)
			if err != nil {
				log.Fatalf("cannot open %v", intermediateFileName)
			}

			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
		}

		sort.Sort(ByKey(kva))

		oname := fmt.Sprintf("mr-%v", reply.TaskId)
		ofile, err := os.Create(oname)
		if err != nil {
			log.Fatalf("cannot create %v", oname)
		}

		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}

			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}

			output := reducef(kva[i].Key, values)
			fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

			i = j
		}
		// fmt.Println(reply.TaskId)
		err = ofile.Close()
		if err != nil {
			log.Fatalf("cannot close %v", oname)
		}
		CallFinishedReduce(oname, reply.TaskId)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func CallSendFileName() SendReply {
	args := SendArgs{}

	reply := SendReply{}

	call("Coordinator.SendFileName", &args, &reply)
	// fmt.Println(reply.Filename)
	return reply
}

func CallSendReduceTask() SendReduceReply {
	args := SendReduceArgs{}

	reply := SendReduceReply{}

	call("Coordinator.SendReduceTask", &args, &reply)
	return reply
}

func CallFinishedMap(tempFileNames []string, id int) {
	args := FinishedMapArgs{tempFileNames, id}
	reply := FinishedMapReply{}
	call("Coordinator.FinishedMap", &args, &reply)
}

func CallFinishedReduce(tempFileName string, id int) {
	args := FinishedReduceArgs{id, tempFileName}
	reply := FinishedReduceReply{}
	call("Coordinator.FinishedReduce", &args, &reply)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	// fmt.Println(reply)
	err = c.Call(rpcname, args, reply)
	// fmt.Println(reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
