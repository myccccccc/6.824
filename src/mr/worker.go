package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "io/ioutil"
import "encoding/json"
import "sort"


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


var mf func(string, string) []KeyValue
var rf func(string, []string) string

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	mf = mapf
	rf = reducef
	workercall()
}

func workercall() {
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	for {
		emptyarg := EmptyArg{}
		reply := Task{}
		err = c.Call("Master.AskForTask", &emptyarg, &reply)
		if err != nil {
			fmt.Println(err)
			return
		}
		if reply.TaskType == "map" {
			workmap(&reply)
			err = c.Call("Master.FinishTask", &reply, &emptyarg)
			if err != nil {
				fmt.Println(err)
				return
			}
		} else if reply.TaskType == "reduce" {
			workreduce(&reply)
			err = c.Call("Master.FinishTask", &reply, nil)
			if err != nil {
				fmt.Println(err)
				return
			}
		} else if reply.TaskType == "wait" {
			time.Sleep(time.Second)
		} else {
			break
		}
	}
}

func workmap (mt *Task) {
	buckets := make([][]KeyValue, mt.Nreduce)
	for _, filename := range mt.InputFilenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
			return
		}
		file.Close()
		kva := mf(filename, string(content))
		for _, kv := range kva {
			bucketNum := ihash(kv.Key) % mt.Nreduce
			buckets[bucketNum] = append(buckets[bucketNum], kv)
		}
	}
	for i := 0; i < mt.Nreduce; i++ {
		os.Mkdir("mrtmp", os.ModePerm)
		ofile, err := ioutil.TempFile("mrtmp", mt.OutputFilenames[i])
		if err != nil {
			log.Fatal(err)
		}
		defer os.Remove(ofile.Name())
		enc := json.NewEncoder(ofile)
		for _, kv := range buckets[i] {
			enc.Encode(&kv)
		}
		ofile.Close()
		if err := os.Rename(ofile.Name(), mt.OutputFilenames[i]); err != nil {
			log.Fatal(err)
		}
	}
	mt.TaskState = "completed"
	mt.Pid = os.Getpid()
}

func workreduce (rt *Task) {
	kva := make([]KeyValue, 0)
	for _, filename := range rt.InputFilenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return
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
	os.Mkdir("mrtmp", os.ModePerm)
	ofile, err := ioutil.TempFile("mrtmp", rt.OutputFilenames[0])
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(ofile.Name())
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
		output := rf(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	ofile.Close()
	if err := os.Rename(ofile.Name(), rt.OutputFilenames[0]); err != nil {
		log.Fatal(err)
	}
	rt.TaskState = "completed"
	rt.Pid = os.Getpid()
}