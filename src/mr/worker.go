package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

func handleMap(task TaskInput, mapf func(string, string) []KeyValue) ([]string, []uint, error) {
	if len(task.InputFiles) == 0 {
		return []string{}, []uint{}, nil
	}
	buckettedKeyValues := make(map[uint]map[string][]string)
	var ok bool
	for _, file := range task.InputFiles {
		content, err := os.ReadFile(file)
		if err != nil {
			log.Printf("Worker %d: Error while reading file `%s`: %+v\n", os.Getpid(), file, err)
			return nil, nil, err
		} else {
			kvs := mapf(file, string(content))
			log.Printf("Worker %d: Map for file: `%s` => %d kvs\n", os.Getpid(), file, len(kvs))
			for _, kv := range kvs {
				bucketId := uint(ihash(kv.Key) % int(task.NReduce))
				var bucket map[string][]string
				if bucket, ok = buckettedKeyValues[bucketId]; !ok {
					bucket = make(map[string][]string)
					buckettedKeyValues[bucketId] = bucket
				}
				bucket[kv.Key] = append(bucket[kv.Key], kv.Value)
			}
		}
	}
	var outputFiles []string
	var bucketIds []uint
	for bucketId, bucket := range buckettedKeyValues {
		outputFile := fmt.Sprintf("mr-%d-%d", task.TaskId, bucketId)
		log.Printf("Worker %d: Writing for Bucket Id: %d to %s\n", os.Getpid(), bucketId, outputFile)
		jsonBytes, err := json.Marshal(bucket)
		if err != nil {
			return nil, nil, err
		}
		err = os.WriteFile(outputFile, jsonBytes, 0644)
		if err != nil {
			log.Printf("Worker %d: Error while writing map output to `%s`: %+v\n", os.Getpid(), outputFile, err)
			return nil, nil, err
		}
		outputFiles = append(outputFiles, outputFile)
		bucketIds = append(bucketIds, bucketId)
	}
	return outputFiles, bucketIds, nil
}

func handleReduce(task TaskInput, reducef func(string, []string) string) ([]string, error) {
	if len(task.InputFiles) == 0 {
		return []string{}, nil
	}
	kvs := make(map[string][]string)
	for _, file := range task.InputFiles {
		content, err := os.ReadFile(file)
		if err != nil {
			log.Printf("Worker %d: Error while reading file `%s`: %+v\n", os.Getpid(), file, err)
			return nil, err
		} else {
			bucket := make(map[string][]string)
			err := json.Unmarshal(content, &bucket)
			if err != nil {
				log.Printf("Worker %d: Cannot unmarshal file `%s` into map[string][]string bucket type: %+v\n", os.Getpid(), file, err)
				return nil, err
			}
			for k, vs := range bucket {
				kvs[k] = append(kvs[k], vs...)
			}
		}
	}
	outputFile := fmt.Sprintf("mr-out-%d", task.TaskId)
	outputFD, err := os.Create(outputFile)
	if err != nil {
		log.Printf("Worker %d: Cannot create file `%s` for writing reduce task output: %+v\n", os.Getpid(), outputFile, err)
		return nil, err
	}
	defer outputFD.Close()
	outputFileWriter := bufio.NewWriter(outputFD)
	firstLine := true
	for k, vs := range kvs {
		v := reducef(k, vs)
		if firstLine {
			firstLine = false
		} else {
			_, err := outputFileWriter.WriteString("\n")
			if err != nil {
				log.Printf("Worker %d: Cannot write to file `%s` for key/value: `%v %v`: %+v\n", os.Getpid(), outputFile, k, v, err)
				return nil, err
			}
		}
		_, err := fmt.Fprintf(outputFileWriter, "%v %v", k, v)
		if err != nil {
			log.Printf("Worker %d: Cannot write to file `%s` for key/value: `%v %v`: %+v\n", os.Getpid(), outputFile, k, v, err)
			return nil, err
		}
	}
	err = outputFileWriter.Flush()
	if err != nil {
		log.Printf("Worker %d: Cannot flush file `%s` to disk: %+v\n", os.Getpid(), outputFile, err)
		return nil, err
	}
	return []string{outputFile}, nil
}

// main/mrworker.go calls this function.
func Worker(
	sockname string,
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	for {
		ok, task := getTask()
		if !ok || task.Kind == TaskKindDone {
			break
		}
		log.Printf("Worker %d: Task %+v\n", os.Getpid(), task)
		switch task.Kind {
		case TaskKindMap:
			outputFiles, bucketIds, err := handleMap(task, mapf)
			if err == nil {
				sendTaskOutput(task, outputFiles, bucketIds)
			}
		case TaskKindReduce:
			// log.Printf("Worker %d: Invalid Task Kind: %s\n", os.Getpid(), TaskKindAsString(task.Kind))
			outputFiles, err := handleReduce(task, reducef)
			if err == nil {
				sendTaskOutput(task, outputFiles, nil)
			}
		default:
			log.Printf("Worker %d: Unknown taskKind: %s\n", os.Getpid(), TaskKindAsString(task.Kind))
		}
	}
	log.Printf("Worker %d: Ending Worker\n", os.Getpid())
}

func getTask() (bool, TaskInput) {
	args := GetTaskArgs{}
	reply := TaskInput{}
	ok := call("Coordinator.GetTask", &args, &reply)
	return ok, reply
}

func sendTaskOutput(task TaskInput, outputFiles []string, bucketIds []uint) bool {
	args := TaskOutput{}
	args.Kind = task.Kind
	args.TaskId = task.TaskId
	args.Attempt = task.Attempt
	args.OutputFiles = outputFiles
	args.BucketIds = bucketIds
	reply := SendTaskOutputReply{}
	ok := call("Coordinator.SendTaskOutput", &args, &reply)
	return ok
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args any, reply any) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatalf("Worker %d: dialing: %v\n", os.Getpid(), err)
	}
	defer c.Close()

	log.Printf("Worker %d: call to `%s`, args: %+v\n", os.Getpid(), rpcname, args)
	if err := c.Call(rpcname, args, reply); err == nil {
		// log.Printf("Worker %d: call to `%s`, args: %+v successfull, returning True\n", os.Getpid(), rpcname, args)
		return true
	}
	log.Printf("Worker %d: call to `%s`, args: %+v failed, return False, err: %+v\n", os.Getpid(), rpcname, args, err)

	return false
}
