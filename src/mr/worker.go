package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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

func generateJson(arg1 string, arg2 string) string {
	return "mr-" + arg1 + "." + arg2 + ".json"
}

func reduceTask(args TaskArgs, reply TaskReply, reducef func(string, []string) string) {
	if reply.Status != REDUCE {
		return
	}

	dest := make(map[string][]string)
	for i := 0; i < args.ServerNum; i++ {
		file, _ := os.OpenFile(generateJson(strconv.Itoa(i), reply.TaskNum), os.O_CREATE|os.O_RDWR, 0644)
		var values []KeyValue
		if err := json.NewDecoder(file).Decode(&values); err == nil {
			for _, keyVal := range values {
				dest[keyVal.Key] = append(dest[keyVal.Key], keyVal.Value)
			}
		}
		file.Close()
	}
	file, _ := os.Create("mr-out-" + reply.TaskNum + ".txt")
	for key, value := range dest {
		fmt.Fprintf(file, "%v %v\n", key, reducef(key, value))
	}
	file.Close()
}

func mapTask(args TaskArgs, reply TaskReply, mapf func(string, string) []KeyValue) {
	if reply.Status != MAP {
		return
	}

	contents, _ := ioutil.ReadFile(reply.Filename)
	values := mapf(reply.Filename, string(contents))
	keyVals := make(map[string][]KeyValue)
	for _, keyVal := range values {
		file := generateJson(reply.TaskNum, strconv.Itoa(ihash(keyVal.Key)%args.ServerNum))
		keyVals[file] = append(keyVals[file], keyVal)
	}
	for file, keyVal := range keyVals {
		f, _ := os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0644)
		enc := json.NewEncoder(f)
		enc.Encode(&keyVal)
		f.Close()
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	args := TaskArgs{}

	call("Master.CreateWorker", &args, &args)
	for {
		reply := TaskReply{}
		call("Master.GetTask", &args, &reply)

		switch reply.Status {
		case DONE:
			return
		case WAIT:
			time.Sleep(time.Second)
			continue
		case MAP:
			mapTask(args, reply, mapf)
			call("Master.MapDone", &reply, &reply)
		case REDUCE:
			reduceTask(args, reply, reducef)
			call("Master.ReduceDone", &reply, &reply)
		}
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
