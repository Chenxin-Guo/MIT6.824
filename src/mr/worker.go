package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		time.Sleep(1 * time.Second)
		if Done() {
			break
		}
		reply := RequestTask()
		fmt.Printf("Ask for task, this reply is %v\n", reply)
		if !reply.HasTask {
			continue
		}
		if reply.IsMapTask {
			mapTask := Task{
				IsMapTask: reply.IsMapTask,
				FilePath:  reply.MapFile,
				Index:     reply.Index,
			}
			Map(mapf, mapTask, reply.NReducer)
			fmt.Print("Finish the mapping task")
		} else {
			reduceTask := Task{
				IsMapTask: reply.IsMapTask,
				Index:     reply.Index,
			}
			Reduce(reducef, reduceTask, reply.NMapper)
			fmt.Print("Finish the reducing task")
		}

		SubmitTask(reply.Index, reply.IsMapTask)
	}

}

// Map is to map key value into a file with mr-%v-%v.json format
func Map(mapf func(string, string) []KeyValue, mapTask Task, NReducer int) {
	encs := make([]*json.Encoder, NReducer)
	fs := make([]*os.File, NReducer)

	for i := 0; i < NReducer; i++ {
		oname := fmt.Sprintf("mr-%v-%v.json", mapTask.Index, i)
		f, err := os.Create(oname)
		if err != nil {
			log.Fatalf("Cannot open file %v", oname)
		}
		enc := json.NewEncoder(f)
		fs[i] = f
		encs[i] = enc
	}

	file, err := os.Open(mapTask.FilePath)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", mapTask.FilePath)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", mapTask.FilePath)
	}

	kva := mapf(mapTask.FilePath, string(content))

	for _, kv := range kva {
		id := ihash(kv.Key) % NReducer
		enc := encs[id]
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("Cannot encode Key-Value pair %v", kv)
		}
	}
	for i := 0; i < NReducer; i++ {
		fs[i].Close()
	}

}

// Reduce function is to sort the key in files and reduce
func Reduce(reducef func(string, []string) string, reduceTask Task, NMap int) {
	kva := make([]KeyValue, 0)
	for i := 0; i < NMap; i++ {
		oname := fmt.Sprintf("mr-%v-%v.json", i, reduceTask.Index)
		f, err := os.Open(oname)
		defer f.Close()
		if err != nil {
			log.Fatalf("cannot open %v", oname)
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)

		}
	}
	sort.Sort(ByKey(kva))
	oname := fmt.Sprintf("mr-out-%v", reduceTask.Index)
	ofile, _ := os.Create(oname)

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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	ofile.Close()
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func RequestTask() RequestTaskReply {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}
	call("Master.RequestTask", &args, &reply)
	return reply
}

//
func SubmitTask(index int, isMap bool) {
	args := SubmitTaskArgs{}
	reply := SubmitTaskReply{}
	args.IsMapTask = isMap
	args.Index = index
	call("Master.SubmitTask", &args, &reply)
	// no reply need
}

//
func Done() bool {
	args := DoneArgs{}
	reply := DoneReply{}
	call("Master.DoneQuery", &args, &reply)
	return reply.IsDone
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
