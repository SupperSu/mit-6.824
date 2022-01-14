package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

type FilesDescriptors struct {
	Key   json.Encoder
	Value string
}

var (
	_, b, _, _ = runtime.Caller(0)
	basepath   = filepath.Dir(b)
)

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

	for {
		reply := getTask()
		switch reply.TaskType {
		case MapPhase:
			filename := reply.FileName
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open in map phase!%v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kvas := mapf(filename, string(content))
			writeKeyVals(kvas, reply.MapTaskId, reply.CntReduceTask)
			call("Coordinator.MarkMapTaskFinish", &FinishMapTaskArgs{TaskId: reply.MapTaskId}, &TaskFinishedReply{})
			time.Sleep(1 * time.Second)
			break
		case ReducePhase:
			// go through intermediate files and find same reduceId files
			matchedFiles, _ := findMatchedFiles(filepath.Join(basepath, "../main"), reply.ReduceTaskId)
			kvalsMap := make(map[string][]string)
			for _, fileName := range matchedFiles {
				fmt.Printf("%s \n", fileName)
				file, err := os.Open(fileName)
				if err != nil {
					log.Fatalf("failed to read %s", fileName)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kvalsMap[kv.Key] = append(kvalsMap[kv.Key], kv.Value)
				}
				file.Close()
			}
			oname := fmt.Sprintf("mr-out-%d", reply.ReduceTaskId)
			ofile, _ := os.Create(oname)
			for k, vals := range kvalsMap {
				output := reducef(k, vals)
				fmt.Fprintf(ofile, "%v %v\n", k, output)
			}
			ofile.Close()
			call("Coordinator.MarkReduceTaskFinish",
				&FinishReduceTaskArgs{TaskId: reply.ReduceTaskId}, &TaskFinishedReply{})
			time.Sleep(1 * time.Second)
			break
		case StopPhase:
			os.Exit(1)
		default:
		}
	}

}

func findMatchedFiles(path string, reduceId int) ([]string, error) {
	var matches []string
	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		if matched, err := filepath.Match(fmt.Sprintf("mr-.*-%d", reduceId), filepath.Base(info.Name())); err != nil {
			return err
		} else if matched {
			matches = append(matches, info.Name())
		}
		return nil
	})
	fmt.Printf("matched file %d", len(matches))
	if err != nil {
		return nil, err
	}
	return matches, nil
}

func getTask() GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	call("Coordinator.GetTask", &args, &reply)
	return reply
}

// write KeyVals to json files, if succeeded return true,
// else return false. Caller needs to handle the failure
func writeKeyVals(kvals []KeyValue, mapTaskNum, cntReduceNum int) bool {
	filesMap := make(map[string]*os.File)
	for _, kval := range kvals {
		reduceId := ihash(kval.Key) % cntReduceNum
		tempname := fmt.Sprintf("__temp_mr-%d-%d", mapTaskNum, reduceId)
		_, ok := filesMap[tempname]
		if !ok {
			tempfile, _ := ioutil.TempFile(basepath, tempname+"#*")
			filesMap[tempname] = tempfile
		}
		tempfile, ok := filesMap[tempname]
		enc := json.NewEncoder(tempfile)
		err := enc.Encode(&kval)
		if err != nil {
			log.Fatalf("cannot encode key: %s val: %s into json format", kval.Key, kval.Value)
			return false
		}
	}
	for _, tempfile := range filesMap {
		filename := filepath.Base(tempfile.Name())
		tempIdx := strings.Index(filename, "#")
		err := os.Rename(tempfile.Name(), filename[7:tempIdx])
		if err != nil {
			log.Fatalf("Can't rename file! with error %s", err.Error())
			return false
		}
	}
	return true
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
