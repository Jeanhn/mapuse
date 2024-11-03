package work

import (
	"bufio"
	"container/heap"
	"errors"
	"fmt"
	"os"

	"mapuce/mr/coordinate"
	"mapuce/mr/util"
)

const (
	MergingFileFormat = "mr-merging-file-id%v"
)

func mergeSortedFiles(filenames []string, dest string) error {
	fileReaders := make([]*bufio.Scanner, 0, len(filenames))
	for _, filename := range filenames {
		f, err := os.Open(filename)
		if err != nil {
			return err
		}
		defer f.Close()
		sc := bufio.NewScanner(f)
		sc.Split(bufio.ScanLines)
		fileReaders = append(fileReaders, sc)
	}

	destFile, err := os.OpenFile(dest, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer destFile.Close()
	wr := bufio.NewWriter(destFile)

	kvh := KeyValueHeap(make([]KeyValue, 0))
	heap.Init(&kvh)
	keyValueIndex := make(map[KeyValue][]int, 0)

	for index, reader := range fileReaders {
		if !reader.Scan() {
			continue
		}
		err := reader.Err()
		if err != nil {
			return err
		}

		ukv, err := util.UnmarshalKeyAndValue(reader.Bytes())
		if err != nil {
			return err
		}
		kv := KeyValue{}
		kv.from(ukv)
		heap.Push(&kvh, kv)
		_, ok := keyValueIndex[kv]
		if !ok {
			keyValueIndex[kv] = make([]int, 0)
		}
		keyValueIndex[kv] = append(keyValueIndex[kv], index)
	}

	for kvh.Len() != 0 {
		kv := heap.Pop(&kvh).(KeyValue)
		wr.WriteString(kv.fmt())
		if wr.Size() > BLOCK_SIZE_LIMIT {
			err = wr.Flush()
			if err != nil {
				return err
			}
		}

		index := keyValueIndex[kv][0]
		keyValueIndex[kv] = keyValueIndex[kv][1:]
		if len(keyValueIndex[kv]) == 0 {
			delete(keyValueIndex, kv)
		}

		if fileReaders[index].Scan() {
			err = fileReaders[index].Err()
			if err != nil {
				return err
			}
			nextLine := fileReaders[index].Bytes()
			ukv, err := util.UnmarshalKeyAndValue(nextLine)
			if err != nil {
				return err
			}
			nextKeyValue := KeyValue{}
			nextKeyValue.from(ukv)

			heap.Push(&kvh, nextKeyValue)
			_, ok := keyValueIndex[nextKeyValue]
			if !ok {
				keyValueIndex[nextKeyValue] = make([]int, 0)
			}
			keyValueIndex[nextKeyValue] = append(keyValueIndex[nextKeyValue], index)
		}
	}
	err = wr.Flush()
	if err != nil {
		return err
	}
	return nil
}

func ReduceHandler(task coordinate.Task, reducef func(string, []string) string) error {
	if len(task.TargetFiles) != 1 {
		return errors.New("target output files count wrong")
	}
	targetFile, err := os.OpenFile(task.TargetFiles[0], os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	wr := bufio.NewWriter(targetFile)

	tempFileName := fmt.Sprintf(MergingFileFormat, task.Id)
	mergeSortedFiles(task.InputFiles, tempFileName)

	tempFile, err := os.Open(tempFileName)
	if err != nil {
		return err
	}
	defer tempFile.Close()
	util.CollectTempFile(tempFileName)
	defer tempFile.Close()
	sc := bufio.NewScanner(tempFile)
	sc.Split(bufio.ScanLines)

	currentKeyValue := make([]KeyValue, 0)

	flushCurrent := func() error {
		values := make([]string, 0)
		for _, v := range currentKeyValue {
			values = append(values, v.Value)
		}
		reduceValue := reducef(currentKeyValue[0].Key, values)
		text := fmt.Sprintf("%v %v\r\n", currentKeyValue[0].Key, reduceValue)
		_, err = wr.WriteString(text)
		if err != nil {
			return err
		}
		if wr.Size() > BLOCK_SIZE_LIMIT {
			err = wr.Flush()
			if err != nil {
				return err
			}
		}
		return nil
	}
	for sc.Scan() {
		err = sc.Err()
		if err != nil {
			return err
		}
		line := sc.Bytes()
		ukv, err := util.UnmarshalKeyAndValue(line)
		if err != nil {
			return err
		}
		kv := KeyValue{}
		kv.from(ukv)
		if len(currentKeyValue) != 0 {
			ckv := currentKeyValue[0]
			if ckv.Key != kv.Key {
				err = flushCurrent()
				if err != nil {
					return err
				}
				currentKeyValue = make([]KeyValue, 0)
			}
		}
		currentKeyValue = append(currentKeyValue, kv)
	}

	if len(currentKeyValue) != 0 {
		err = flushCurrent()
		if err != nil {
			return err
		}
	}

	err = wr.Flush()
	if err != nil {
		return err
	}
	return nil
}
