package work

import (
	"bufio"
	"fmt"
	"os"
	"sort"

	"mapuce/mr/coordinate"
	"mapuce/mr/util"
)

const (
	SortFileFormat = "mr-sort-file-prefix-task%v-id%v"
)

func flushKeyValues(kvs []KeyValue, taskId string) (string, error) {
	tempName := fmt.Sprintf(SortFileFormat, taskId, util.LocalIncreaseId())
	f, err := os.OpenFile(tempName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return "", err
	}
	defer f.Close()
	wr := bufio.NewWriter(f)
	sort.Sort(KeyValueArray(kvs))
	for _, kv := range kvs {
		_, err = wr.WriteString(kv.fmt())
		if err != nil {
			return "", err
		}
	}
	err = wr.Flush()
	if err != nil {
		return "", err
	}
	return tempName, nil
}

func sortKeyValueFile(filename, taskId string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Split(bufio.ScanLines)

	kvs := make([]KeyValue, 0)
	kvSize := 0
	tempSortedFiles := make([]string, 0)

	for sc.Scan() {
		err = sc.Err()
		if err != nil {
			return err
		}

		byts := sc.Bytes()
		kvSize += len(byts)
		ukv, err := util.UnmarshalKeyAndValue(byts)
		if err != nil {
			return err
		}
		kv := KeyValue{}
		kv.from(ukv)
		kvs = append(kvs, kv)

		if kvSize > BLOCK_SIZE_LIMIT {
			tempFile, err := flushKeyValues(kvs, taskId)
			if err != nil {
				return err
			}
			tempSortedFiles = append(tempSortedFiles, tempFile)
			util.CollectTempFile(tempFile)
			kvs = make([]KeyValue, 0)
			kvSize = 0
		}

	}

	if len(kvs) != 0 {
		tempFile, err := flushKeyValues(kvs, taskId)
		if err != nil {
			return err
		}
		tempSortedFiles = append(tempSortedFiles, tempFile)
		util.CollectTempFile(tempFile)
	}

	err = mergeSortedFiles(tempSortedFiles, filename)
	if err != nil {
		return err
	}
	return nil
}

func doMapTask(task coordinate.Task, mapf func(string, string) []KeyValue) error {
	targetFiles := make([]*bufio.Writer, 0)
	for _, targetFile := range task.TargetFiles {
		f, err := os.OpenFile(targetFile, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return err
		}
		defer f.Close()
		wr := bufio.NewWriter(f)
		targetFiles = append(targetFiles, wr)
	}

	for _, filename := range task.InputFiles {
		f, err := os.Open(filename)
		if err != nil {
			return err
		}
		defer f.Close()

		sc := bufio.NewScanner(f)
		sc.Split(bufio.ScanLines)

		for sc.Scan() {
			err = sc.Err()
			if err != nil {
				return err
			}
			byts := sc.Bytes()
			kvs := mapf(filename, string(byts))

			for _, kv := range kvs {
				n := ihash(kv.Key) % len(task.TargetFiles)
				_, err = targetFiles[n].WriteString(kv.fmt())
				if err != nil {
					return err
				}
				if targetFiles[n].Size() > BLOCK_SIZE_LIMIT {
					err = targetFiles[n].Flush()
					if err != nil {
						return err
					}
				}
			}
		}
	}

	for _, fileWriter := range targetFiles {
		err := fileWriter.Flush()
		if err != nil {
			return err
		}
	}
	return nil
}

func MapHandler(task coordinate.Task, mapf func(string, string) []KeyValue) error {
	err := doMapTask(task, mapf)
	if err != nil {
		return err
	}
	for _, filename := range task.TargetFiles {
		err = sortKeyValueFile(filename, util.I64ToString(task.Id))
		if err != nil {
			return err
		}
	}
	return nil
}
