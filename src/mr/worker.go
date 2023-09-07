package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"
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

	taskArgs := PollTaskArgs{}
	for task, ok := GetTask(taskArgs); ; task, ok = GetTask(taskArgs) {
		if !ok {
			fmt.Println("get task failed ")
		} else {
			switch task.TaskType {
			case MapTask:
				doMap(task, mapf)
				callFinish(task)
			case WatiTask:
				time.Sleep(time.Second)
				continue
			case ExitTask:
				fmt.Println("task exit .")
				break
			}
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

// GetTask 通过RPC从Coodinator获取任务
func GetTask(args PollTaskArgs) (Task, bool) {
	reply := Task{}
	ok := call("Coordinator.GetTask", &args, &reply)
	return reply, ok
}

// 执行Map任务
func doMap(task Task, mapf func(string, string) []KeyValue) {
	filename := task.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open file %v", filename)
	} else {
		defer func(file *os.File) {
			err := file.Close()
			if err != nil {
				fmt.Printf("close file failed , %v\n", err)
			}
		}(file)
	}
	filecontent, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatalf("read file failed %v ", err)
	}
	// 执行预定义的Map函数
	values := mapf(filename, string(filecontent))
	// 根据key对数据rehash到每个hash槽里。
	var t = make(map[int][]KeyValue, 8)
	for _, value := range values {
		hashedId := ihash(value.Key) % task.HashMod
		t[hashedId] = append(t[hashedId], value)
	}
	// 根据 hashkey 写入不同的文件
	for i := range t {
		var outputFileName = "mr-map-out-" + strconv.Itoa(task.Id) + "-" + strconv.Itoa(i)
		created, err := os.Create(outputFileName)
		if err != nil {
			log.Fatalf("create file failed %v", err)
		}
		encoder := json.NewEncoder(created)
		for _, value := range t[i] {
			err := encoder.Encode(value)
			if err != nil {
				log.Fatal("encode value failed , value:%v , err:%v", value, err)
			}
		}
		err = created.Close()
		if err != nil {
			log.Fatalf("close file failed , %v", err)
		}
	}

}

func callFinish(t Task) bool {

	ok := call("Coordinator.TaskDone", &t, &t)
	if ok {
		return true
	}
	return false
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
