package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	readMap := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		intermediateFile := reduceName(jobName, i, reduceTaskNumber)
		readFromImFile(intermediateFile, readMap)
	}
	var keys []string
	for k, _ := range readMap {
		keys = append(keys, k)
	}
	sort.Sort(Keys(keys))
	var kvs []KeyValue
	for _, k := range keys {
		kvs = append(kvs, KeyValue{k, reduceF(k, readMap[k])})
	}
	fmt.Println("doReduce start write Reduce output")
	emit(outFile, kvs)
}

// Keys for key in readMap
type Keys []string

func (ks Keys) Len() int {
	return len(ks)
}

func (ks Keys) Swap(i, j int) {
	ks[i], ks[j] = ks[j], ks[i]
}

func (ks Keys) Less(i, j int) bool {
	return ks[i] < ks[j]
}

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

// Your doMap() encoded the key/value pairs in the intermediate
// files, so you will need to decode them. If you used JSON, you can
// read and decode by creating a decoder and repeatedly calling
// .Decode(&kv) on it until it returns an error.
//
func readFromImFile(intermediateFiles string, readMap map[string][]string) {
	file, err := os.Open(intermediateFiles)
	checkerr(err)
	dec := json.NewDecoder(file)
	for dec.More() {
		var kv KeyValue
		err := dec.Decode(&kv)
		checkerr(err)
		if _, ok := readMap[kv.Key]; ok {
			readMap[kv.Key] = append(readMap[kv.Key], kv.Value)
		} else {
			readMap[kv.Key] = []string{kv.Value}
		}
	}
	file.Close()
}
