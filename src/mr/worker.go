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
	bucketIdToTempOutputFd := make(map[uint]*os.File)
	bucketIdToBufioWriter := make(map[uint]*bufio.Writer)
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
				var tempOutputFd *os.File
				var tempBufioWriter *bufio.Writer
				var ok bool
				isFirst := false
				if tempBufioWriter, ok = bucketIdToBufioWriter[bucketId]; !ok {
					isFirst = true
					tempOutputFd, err = os.CreateTemp("", fmt.Sprintf("mr-%d-%d-*", task.TaskId, bucketId))
					if err != nil {
						log.Printf("Worker %d: Error while creating output file `%s`: %+v\n", os.Getpid(), tempOutputFd.Name(), err)
						return nil, nil, err
					}
					log.Printf("Worker %d: Writing for Bucket Id: %d to %s\n", os.Getpid(), bucketId, tempOutputFd.Name())
					tempBufioWriter = bufio.NewWriter(tempOutputFd)
					_, err = tempBufioWriter.WriteString("[")
					if err != nil {
						log.Printf("Worker %d: Error while writing '[' to `%s`: %+v\n", os.Getpid(), tempOutputFd.Name(), err)
						return nil, nil, err
					}
					bucketIdToTempOutputFd[bucketId] = tempOutputFd
					bucketIdToBufioWriter[bucketId] = tempBufioWriter
				}
				if !isFirst {
					_, err = tempBufioWriter.WriteString(",")
					if err != nil {
						log.Printf("Worker %d: Error while writing ',' to `%s`: %+v\n", os.Getpid(), tempOutputFd.Name(), err)
						return nil, nil, err
					}
				}
				jsonBytes, err := json.Marshal(kv)
				if err != nil {
					return nil, nil, err
				}
				_, err = tempBufioWriter.Write(jsonBytes)
				if err != nil {
					log.Printf("Worker %d: Error while writing map output to `%s`: %+v\n", os.Getpid(), tempOutputFd.Name(), err)
					return nil, nil, err
				}
			}
		}
	}
	var outputFiles []string
	var bucketIds []uint
	for bucketId, tempOutputFd := range bucketIdToTempOutputFd {
		tempBufioWriter := bucketIdToBufioWriter[bucketId]
		_, err := tempBufioWriter.WriteString("]")
		if err != nil {
			log.Printf("Worker %d: Error while writing ']' to `%s`: %+v\n", os.Getpid(), tempOutputFd.Name(), err)
			return nil, nil, err
		}
		err = tempBufioWriter.Flush()
		if err != nil {
			log.Printf("Worker %d: Error while flushing to `%s`: %+v\n", os.Getpid(), tempOutputFd.Name(), err)
			return nil, nil, err
		}
		err = tempOutputFd.Close()
		if err != nil {
			log.Printf("Worker %d: Error while closing file `%s`: %+v\n", os.Getpid(), tempOutputFd.Name(), err)
			return nil, nil, err
		}
		outputFile := fmt.Sprintf("mr-%d-%d", task.TaskId, bucketId)
		log.Printf("Worker %d: Renaming `%s` to `%s`\n", os.Getpid(), tempOutputFd.Name(), outputFile)
		err = os.Rename(tempOutputFd.Name(), outputFile)
		if err != nil {
			log.Printf("Worker %d: Error while renaming `%s` to `%s`: %+v\n", os.Getpid(), tempOutputFd.Name(), outputFile, err)
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
			kvArray := []KeyValue{}
			err := json.Unmarshal(content, &kvArray)
			if err != nil {
				log.Printf("Worker %d: Cannot unmarshal file `%s` into []KeyValue: %+v\n", os.Getpid(), file, err)
				return nil, err
			}
			for _, kv := range kvArray {
				kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
			}
		}
	}
	tempOutputFd, err := os.CreateTemp("", fmt.Sprintf("mr-out-%d-*", task.TaskId))
	if err != nil {
		log.Printf("Worker %d: Cannot create file `%s` for writing reduce task output: %+v\n", os.Getpid(), tempOutputFd.Name(), err)
		return nil, err
	}
	defer tempOutputFd.Close()
	tempOutputBufioWriter := bufio.NewWriter(tempOutputFd)
	firstLine := true
	for k, vs := range kvs {
		v := reducef(k, vs)
		if firstLine {
			firstLine = false
		} else {
			_, err := tempOutputBufioWriter.WriteString("\n")
			if err != nil {
				log.Printf("Worker %d: Cannot write to file `%s` for key/value: `%v %v`: %+v\n", os.Getpid(), tempOutputFd.Name(), k, v, err)
				return nil, err
			}
		}
		_, err := fmt.Fprintf(tempOutputBufioWriter, "%v %v", k, v)
		if err != nil {
			log.Printf("Worker %d: Cannot write to file `%s` for key/value: `%v %v`: %+v\n", os.Getpid(), tempOutputFd.Name(), k, v, err)
			return nil, err
		}
	}
	err = tempOutputBufioWriter.Flush()
	if err != nil {
		log.Printf("Worker %d: Cannot flush file `%s` to disk: %+v\n", os.Getpid(), tempOutputFd.Name(), err)
		return nil, err
	}
	outputFile := fmt.Sprintf("mr-out-%d", task.TaskId)
	log.Printf("Worker %d: Renaming `%s` to `%s`\n", os.Getpid(), tempOutputFd.Name(), outputFile)
	err = os.Rename(tempOutputFd.Name(), outputFile)
	if err != nil {
		log.Printf("Worker %d: Error while renaming `%s` to `%s`: %+v\n", os.Getpid(), tempOutputFd.Name(), outputFile, err)
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
