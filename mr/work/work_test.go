package work

import (
	"container/heap"
	"os"
	"strconv"
	"strings"
	"testing"
	"unicode"

	"mapuce/mr/coordinate"
	"mapuce/mr/util"
)

func mapf(filename string, contents string) []KeyValue {
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	kva := []KeyValue{}
	for _, w := range words {
		kv := KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}

func reducef(key string, values []string) string {
	// return the number of occurrences of this word.
	return strconv.Itoa(len(values))
}

func TestHeap(t *testing.T) {
	kvs := make([]KeyValue, 0)
	kvh := KeyValueHeap(kvs)

	for i := 0; i < 10; i++ {
		kv := KeyValue{
			Key:   strconv.Itoa(i),
			Value: strconv.Itoa(i),
		}
		heap.Push(&kvh, kv)
	}

	arr := make([]KeyValue, 0)
	for kvh.Len() != 0 {
		v := heap.Pop(&kvh).(KeyValue)
		arr = append(arr, v)
	}
	for i := 0; i < len(arr)-1; i++ {
		if arr[i].Key >= arr[i+1].Key {
			t.Fatal(arr)
		}
	}
}

func TestWrite(t *testing.T) {
	f1, _ := os.OpenFile("yeah", os.O_CREATE|os.O_RDWR, 0666)
	f2, _ := os.OpenFile("yeah", os.O_CREATE|os.O_RDWR, 0666)
	f1.WriteString("yeah")
	f2.WriteString("u")
	f2.Close()
	f1.Close()
}

func TestMerge(t *testing.T) {
	err := mergeSortedFiles([]string{"sorted1", "sorted2", "sorted3"}, "yeah")
	if err != nil {
		t.Fatal(err)
	}
}

func TestSort(t *testing.T) {
	fileList := []string{"output"}
	for _, file := range fileList {
		err := sortKeyValueFile(file, "task0")
		if err != nil {
			t.Fatal(err)
		}
	}
	// util.RemoveTempFiles()
}

func TestDoMapTask(t *testing.T) {
	err := doMapTask(coordinate.Task{
		InputFiles:  []string{"/home/jean/mapuce/src/mr/coordinate/demo.txt", "/home/jean/mapuce/src/mr/coordinate/demo2.txt"},
		TargetFiles: []string{"target1", "target2", "target3"},
	}, mapf)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMap(t *testing.T) {
	err := MapHandler(coordinate.Task{
		InputFiles:  []string{"/home/jean/mapuce/src/mr/coordinate/demo.txt", "/home/jean/mapuce/src/mr/coordinate/demo2.txt"},
		TargetFiles: []string{"target1", "target2", "target3"},
	}, mapf)
	if err != nil {
		t.Fatal(err)
	}
	util.RemoveTempFiles()
}

func TestReduce(t *testing.T) {
	err := ReduceHandler(coordinate.Task{
		InputFiles:  []string{"target1", "target2", "target3"},
		TargetFiles: []string{"output1"},
	}, reducef)
	if err != nil {
		t.Fatal(err)
	}
	util.RemoveTempFiles()
}

func TestCase(t *testing.T) {
	defer util.RemoveTempFiles()
	inputFiles := []string{"/home/jean/mapuce/src/main/pg-being_ernest.txt"}
	targetFiles := []string{"output"}
	err := MapHandler(coordinate.Task{
		InputFiles:  inputFiles,
		TargetFiles: targetFiles,
	}, mapf)
	if err != nil {
		t.Fatal(err)
	}
}
