package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"strings"
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
var workerId string      // worker id

func handleMap(task TaskInput, mapf func(string, string) []KeyValue) ([]string, []uint, error) {
	if len(task.InputFiles) == 0 {
		return []string{}, []uint{}, nil
	}
	bucketIdToTempOutputFd := make(map[uint]*os.File)
	bucketIdToBufioWriter := make(map[uint]*bufio.Writer)
	for _, file := range task.InputFiles {
		content, err := os.ReadFile(file)
		if err != nil {
			log.Printf("Worker %s: Error while reading file `%s`: %+v\n", workerId, file, err)
			return nil, nil, err
		} else {
			kvs := mapf(file, string(content))
			log.Printf("Worker %s: Map for file: `%s` => %d kvs\n", workerId, file, len(kvs))
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
						log.Printf("Worker %s: Error while creating output file: %+v\n", workerId, err)
						return nil, nil, err
					}
					log.Printf("Worker %s: Writing for Bucket Id: %d to %s\n", workerId, bucketId, tempOutputFd.Name())
					tempBufioWriter = bufio.NewWriter(tempOutputFd)
					_, err = tempBufioWriter.WriteString("[")
					if err != nil {
						log.Printf("Worker %s: Error while writing '[' to `%s`: %+v\n", workerId, tempOutputFd.Name(), err)
						return nil, nil, err
					}
					bucketIdToTempOutputFd[bucketId] = tempOutputFd
					bucketIdToBufioWriter[bucketId] = tempBufioWriter
				}
				if !isFirst {
					_, err = tempBufioWriter.WriteString(",")
					if err != nil {
						log.Printf("Worker %s: Error while writing ',' to `%s`: %+v\n", workerId, tempOutputFd.Name(), err)
						return nil, nil, err
					}
				}
				jsonBytes, err := json.Marshal(kv)
				if err != nil {
					return nil, nil, err
				}
				_, err = tempBufioWriter.Write(jsonBytes)
				if err != nil {
					log.Printf("Worker %s: Error while writing map output to `%s`: %+v\n", workerId, tempOutputFd.Name(), err)
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
			log.Printf("Worker %s: Error while writing ']' to `%s`: %+v\n", workerId, tempOutputFd.Name(), err)
			return nil, nil, err
		}
		err = tempBufioWriter.Flush()
		if err != nil {
			log.Printf("Worker %s: Error while flushing to `%s`: %+v\n", workerId, tempOutputFd.Name(), err)
			return nil, nil, err
		}
		err = tempOutputFd.Close()
		if err != nil {
			log.Printf("Worker %s: Error while closing file `%s`: %+v\n", workerId, tempOutputFd.Name(), err)
			return nil, nil, err
		}
		outputFile := fmt.Sprintf("mr-%s-%d-%d", workerId, task.TaskId, bucketId)
		log.Printf("Worker %s: Renaming `%s` to `%s`\n", workerId, tempOutputFd.Name(), outputFile)
		err = os.Rename(tempOutputFd.Name(), outputFile)
		if err != nil {
			log.Printf("Worker %s: Error while renaming `%s` to `%s`: %+v\n", workerId, tempOutputFd.Name(), outputFile, err)
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
			log.Printf("Worker %s: Error while reading file `%s`: %+v\n", workerId, file, err)
			return nil, err
		} else {
			kvArray := []KeyValue{}
			err := json.Unmarshal(content, &kvArray)
			if err != nil {
				log.Printf("Worker %s: Cannot unmarshal file `%s` into []KeyValue: %+v\n", workerId, file, err)
				return nil, err
			}
			for _, kv := range kvArray {
				kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
			}
		}
	}
	tempOutputFd, err := os.CreateTemp("", fmt.Sprintf("mr-out-%d-*", task.TaskId))
	if err != nil {
		log.Printf("Worker %s: Cannot create file for writing reduce task output: %+v\n", workerId, err)
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
				log.Printf("Worker %s: Cannot write to file `%s` for key/value: `%v %v`: %+v\n", workerId, tempOutputFd.Name(), k, v, err)
				return nil, err
			}
		}
		_, err := fmt.Fprintf(tempOutputBufioWriter, "%v %v", k, v)
		if err != nil {
			log.Printf("Worker %s: Cannot write to file `%s` for key/value: `%v %v`: %+v\n", workerId, tempOutputFd.Name(), k, v, err)
			return nil, err
		}
	}
	err = tempOutputBufioWriter.Flush()
	if err != nil {
		log.Printf("Worker %s: Cannot flush file `%s` to disk: %+v\n", workerId, tempOutputFd.Name(), err)
		return nil, err
	}
	outputFile := fmt.Sprintf("mr-out-%d", task.TaskId)
	log.Printf("Worker %s: Renaming `%s` to `%s`\n", workerId, tempOutputFd.Name(), outputFile)
	err = os.Rename(tempOutputFd.Name(), outputFile)
	if err != nil {
		log.Printf("Worker %s: Error while renaming `%s` to `%s`: %+v\n", workerId, tempOutputFd.Name(), outputFile, err)
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
	workerHostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("failed to get worker hostname: %v", err)
	}
	workerId = fmt.Sprintf("%s-%d", workerHostname, os.Getpid())

	for {
		ok, task := getTask()
		if !ok || task.Kind == TaskKindDone {
			break
		}
		log.Printf("Worker %s: Task %+v\n", workerId, task)
		switch task.Kind {
		case TaskKindMap:
			outputFiles, bucketIds, err := handleMap(task, mapf)
			if err == nil {
				sendTaskOutput(task, outputFiles, bucketIds)
			}
		case TaskKindReduce:
			// log.Printf("Worker %s: Invalid Task Kind: %s\n", workerId, TaskKindAsString(task.Kind))
			outputFiles, err := handleReduce(task, reducef)
			if err == nil {
				sendTaskOutput(task, outputFiles, nil)
			}
		default:
			log.Printf("Worker %s: Unknown taskKind: %s\n", workerId, TaskKindAsString(task.Kind))
		}
	}
	log.Printf("Worker %s: Ending Worker\n", workerId)
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
	var err error
	var c *rpc.Client
	coordSockName, isTCP := strings.CutPrefix(coordSockName, "tcp://")
	if isTCP {
		// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
		c, err = rpc.DialHTTP("tcp", coordSockName)
	} else {
		c, err = rpc.DialHTTP("unix", coordSockName)
	}
	if err != nil {
		log.Fatalf("Worker %s: dialing: %v\n", workerId, err)
	}
	defer c.Close()

	log.Printf("Worker %s: call to `%s`, args: %+v\n", workerId, rpcname, args)
	if err := c.Call(rpcname, args, reply); err == nil {
		// log.Printf("Worker %s: call to `%s`, args: %+v successfull, returning True\n", workerId, rpcname, args)
		return true
	}
	log.Printf("Worker %s: call to `%s`, args: %+v failed, return False, err: %+v\n", workerId, rpcname, args, err)

	return false
}
