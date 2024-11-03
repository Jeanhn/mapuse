package util

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

var logFile *bufio.Writer
var logMutex sync.Mutex = sync.Mutex{}

const LOG_SIZE int = 128 * 1024

func init() {
	f, err := os.OpenFile(fmt.Sprintf("logfile%v.txt", os.Getpid()), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	logFile = bufio.NewWriter(f)
}

func WriteTo(src interface{}, file io.Writer) error {
	enc := json.NewEncoder(file)
	err := enc.Encode(src)
	if err != nil {
		return err
	}
	return nil
}

func ReadFrom(file io.Reader, desc interface{}) error {
	dec := json.NewDecoder(file)

	err := dec.Decode(desc)
	if err != nil {
		return err
	}
	return nil
}

func I64ToString(i int64) string {
	f := false
	if i < 0 {
		f = true
		i = -i
	}

	buf := make([]byte, 0, 64)
	for i != 0 {
		k := i % 10
		buf = append(buf, byte(k+'0'))
		i /= 10
	}
	if f {
		buf = append(buf, '-')
	}

	for i, j := 0, len(buf)-1; i < j; {
		buf[i], buf[j] = buf[j], buf[i]
		i++
		j--
	}

	return *(*string)(unsafe.Pointer(&buf))
}

var tempFiles = make([]string, 0)
var fileRecords = make(map[string]struct{})
var tempFileLock sync.Mutex = sync.Mutex{}

func CollectTempFile(names ...string) {
	tempFileLock.Lock()
	defer tempFileLock.Unlock()
	for _, name := range names {
		_, ok := fileRecords[name]
		if ok {
			log.Default().Printf("CollectTempFile repeated file collected: %v\r\n", name)
			continue
		}
		tempFiles = append(tempFiles, name)
	}
}

func RemoveTempFiles() error {
	tempFileLock.Lock()
	defer tempFileLock.Unlock()
	for _, file := range tempFiles {
		err := os.Remove(file)
		if err != nil {
			panic(err)
		}
		delete(fileRecords, file)
	}
	return nil
}

func RandomTaskId() string {
	return "mr-task-" + I64ToString(time.Now().Unix())
}

func Ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func UnmarshalKeyAndValue(byts []byte) ([]string, error) {
	ans := bytes.Split(byts, []byte{' '})
	if len(ans) != 2 {
		return nil, errors.New("UnmarshalKeyAndValue:wrong src input")
	}
	return []string{string(ans[0]), string(ans[1])}, nil
}

var localId int64 = 0

func LocalIncreaseId() int64 {
	n := atomic.AddInt64(&localId, 1)
	return n - 1
}

func Log(format string, args ...interface{}) error {
	logf := fmt.Sprintf(format, args...)
	logMutex.Lock()
	defer logMutex.Unlock()
	_, err := logFile.WriteString(logf)
	if err != nil {
		return err
	}
	if logFile.Size() > LOG_SIZE {
		err = logFile.Flush()
		if err != nil {
			return err
		}
	}
	return nil
}

func FlushLogs() error {
	logMutex.Lock()
	defer logMutex.Unlock()
	return logFile.Flush()
}

//TODO remove all ByteToString
