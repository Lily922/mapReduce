package mapreduce

import (
	//	"io/ioutil"
	"encoding/json"
	"log"
	"os"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
// 管理一个reduce工作
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// 处理来自不同的map，属于当前reduce任务的中间结果
	// 处理不同任务可能有对于同一个key值的中间结果，整合中间结果
	keyValues := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTaskNumber)
		file, err := os.Open(fileName)
		defer file.Close()
		if err != nil {
			log.Fatal("reduceJob:", jobName, "error:", err)
			continue
		}
		dec := json.NewDecoder(file)
		// for循环解码,根据key值归类
		for {
			kv := KeyValue{}
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			_, ok := keyValues[kv.Key]
			if !ok {
				keyValues[kv.Key] = make([]string, 0)
			}
			keyValues[kv.Key] = append(keyValues[kv.Key], kv.Value)
		}
	}
	// 将得到的keyValues写进merge文件
	file, err := os.Create(mergeName(jobName, reduceTaskNumber))
	if err != nil {
		log.Fatal("ReduceName:", jobName, "errors", err)
	}
	defer file.Close()
	for k, v := range keyValues {
		enc := json.NewEncoder(file)
		reduceResult := reduceF(k, v)
		enc.Encode(KeyValue{k, reduceResult})
	}

	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
}
