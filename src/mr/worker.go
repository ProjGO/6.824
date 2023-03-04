package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

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

	// tempFilepath := "./mr-tmp"
	tempFilepath := "./"

	workerId := uint32(0)
	call("Coordinator.RegisterWorker", &workerId, &workerId)

	// Your worker implementation here.
	for {
		task := Task{}
		ok := call("Coordinator.GetTask", &workerId, &task)
		if !ok {
			fmt.Println("Worker:: call Coordinator.GetTask failed")
		}

		switch task.TType {
		case MAP:
			for _, filename := range task.InputFilenames {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("WORKER::MAP failed to open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("WORKER::MAP failed to read %v", filename)
				}
				file.Close()

				kva := mapf(filename, string(content))

				var R int = int(task.R)
				outputFiles := make([]*os.File, R)
				encs := make([]*json.Encoder, R)
				for i := 0; i < R; i++ {
					outputFiles[i], _ = ioutil.TempFile(tempFilepath, "mr-tmp-*")
					encs[i] = json.NewEncoder(outputFiles[i])
				}

				for _, kv := range kva {
					rId := ihash(kv.Key) % int(task.R)
					err := encs[rId].Encode(&kv)
					if err != nil {
						log.Fatalf("Failed to encode Key: %v, Value: %v to %v, err: %v", kv.Key, kv.Value, outputFiles[rId].Name(), err)
					}
				}

				for rId, outputFile := range outputFiles {
					finalOutputFilename := fmt.Sprintf("mr-%d-%d.json", task.Id, rId)
					os.Rename(outputFile.Name(), filepath.Join(tempFilepath, finalOutputFilename))
					outputFile.Close()
				}
			}

			var tmp uint32
			call("Coordinator.NotifyDone", task.GId, &tmp)

		case REDUCE:
			kva := []KeyValue{}
			var M int = int(task.M)

			for mId := 0; mId < M; mId++ {
				inputFilename := fmt.Sprintf("mr-%d-%d.json", mId, task.Id)
				inputFilepathname := filepath.Join(tempFilepath, inputFilename)
				file, err := os.Open(inputFilepathname)
				if err != nil {
					log.Fatalf("Worker::REDUCE failed to open %v, err: %v", inputFilepathname, err)
				}

				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}

					kva = append(kva, kv)
				}

				file.Close()
			}

			// convert kva to type ByKey which implemented 'Interface' required by Sort
			sort.Sort(ByKey(kva))

			outputFile, err := ioutil.TempFile(tempFilepath, "mr-*")
			if err != nil {
				log.Fatalf("Worker::REDUCE failed to create output file")
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

				fmt.Fprintf(outputFile, "%v %v\n", kva[i].Key, output)

				i = j
			}

			finalOutputFilename := fmt.Sprintf("mr-out-%d.txt", task.Id)
			os.Rename(outputFile.Name(), filepath.Join(tempFilepath, finalOutputFilename))
			outputFile.Close()

			var tmp uint32
			call("Coordinator.NotifyDone", task.GId, &tmp)

		case TRYAGAIN:
			// time.Sleep(3 * time.Second)
			time.Sleep(100 * time.Millisecond)
			continue

		case NOMORE:
			return
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
