package work

import (
	"fmt"
	"hash/fnv"
)

const (
	BLOCK_SIZE_LIMIT = 128 * 1024
)

type KeyValue struct {
	Key   string
	Value string
}

func (kv KeyValue) size() int {
	return len(kv.Key) + len(kv.Value)
}

func (kv KeyValue) fmt() string {
	return fmt.Sprintf("%v %v\r\n", kv.Key, kv.Value)
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (kv *KeyValue) from(ukv []string) {
	kv.Key = ukv[0]
	kv.Value = ukv[1]
}

type KeyValueArray []KeyValue

func (kvs KeyValueArray) Len() int {
	return len(kvs)
}

func (kvs KeyValueArray) Swap(i, j int) {
	kvs[i], kvs[j] = kvs[j], kvs[i]
}
func (kvs KeyValueArray) Less(i, j int) bool {
	return (kvs[i].Key < kvs[j].Key) ||
		(kvs[i].Key == kvs[j].Key && kvs[i].Value < kvs[j].Value)
}

type KeyValueHeap []KeyValue

func (kvs *KeyValueHeap) Push(kv interface{}) {
	*kvs = append(*kvs, kv.(KeyValue))
}

func (kvs *KeyValueHeap) Pop() interface{} {
	v := (*kvs)[len(*kvs)-1]
	*kvs = (*kvs)[:len(*kvs)-1]
	return v
}

func (kvs *KeyValueHeap) Swap(i, j int) {
	(*kvs)[i], (*kvs)[j] = (*kvs)[j], (*kvs)[i]
}
func (kvs *KeyValueHeap) Less(i, j int) bool {
	return ((*kvs)[i].Key < (*kvs)[j].Key) ||
		((*kvs)[i].Key == (*kvs)[j].Key && (*kvs)[i].Value < (*kvs)[j].Value)
}

func (kvs *KeyValueHeap) Len() int {
	return len(*kvs)
}
