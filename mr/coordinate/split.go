package coordinate

import (
	"bufio"
	"fmt"
	"os"

	"mapuce/mr/util"
)

const (
	SPLIT_SIZE = 128 * 1024

	BUFFER_FLUSH_SIZE = 128 * 1024

	MergeTempFormat = "mr-merge-temp-task%v"

	SplitTempFormat = "mr-split-temp-task%v-shard%v"

	SplitCounterNamePrefix = "mr-split-counter-"
)

type SplitExecutor struct {
	mergeFile  *os.File
	sc         *bufio.Scanner
	blockSize  int
	taskId     string
	splitFiles []string
}

func merge(files []string, taskId string) (string, error) {
	name := fmt.Sprintf(MergeTempFormat, taskId)

	dest, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return "", err
	}
	defer dest.Close()

	wr := bufio.NewWriter(dest)

	for _, file := range files {
		f, err := os.Open(file)
		defer f.Close()
		if err != nil {
			return "", err
		}

		sc := bufio.NewScanner(f)
		sc.Split(bufio.ScanLines)
		err = sc.Err()
		if err != nil {
			return "", err
		}
		for sc.Scan() {
			err = sc.Err()
			if err != nil {
				return "", err
			}
			line := sc.Bytes()
			_, err = wr.Write(line)
			if err != nil {
				return "", err
			}
			_, err = wr.WriteString("\r\n")
			if err != nil {
				return "", err
			}
			if wr.Size() > BUFFER_FLUSH_SIZE {
				err = wr.Flush()
				if err != nil {
					return "", err
				}
			}
		}
	}

	err = wr.Flush()
	if err != nil {
		return "", err
	}

	return name, nil
}

func NewSplitExecutor(files []string, blockSize int, taskId string) (*SplitExecutor, error) {
	mergeFile, err := merge(files, taskId)
	if err != nil {
		return nil, err
	}
	util.CollectTempFile(mergeFile)

	mf, err := os.Open(mergeFile)
	if err != nil {
		return nil, err
	}

	sc := bufio.NewScanner(mf)
	sc.Split(bufio.ScanLines)

	se := SplitExecutor{
		mergeFile:  mf,
		blockSize:  blockSize,
		sc:         sc,
		taskId:     taskId,
		splitFiles: make([]string, 0),
	}
	return &se, nil
}

func (se *SplitExecutor) iterate() ([]string, error) {
	cacheSize := 0
	cache := make([]string, 0)

	err := se.sc.Err()
	if err != nil {
		return nil, err
	}

	for se.sc.Scan() {
		err := se.sc.Err()
		if err != nil {
			return nil, err
		}

		line := se.sc.Bytes()
		line = append(line, "\r\n"...)
		cacheSize += len(line)
		cache = append(cache, string(line))

		if cacheSize > se.blockSize {
			break
		}
	}

	if cacheSize == 0 {
		err := se.mergeFile.Close()
		if err != nil {
			return nil, err
		}
		return nil, nil
	}
	return cache, nil
}

func (se *SplitExecutor) Iterate() (bool, error) {
	text, err := se.iterate()
	if err != nil {
		return true, err
	}

	if len(text) == 0 {
		return false, nil
	}

	i := util.LocalIncreaseId()
	filename := fmt.Sprintf(SplitTempFormat, se.taskId, i)
	util.CollectTempFile(filename)
	se.splitFiles = append(se.splitFiles, filename)

	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return false, err
	}
	defer f.Close()

	for _, line := range text {
		_, err = f.WriteString(line)
		if err != nil {
			return false, err
		}
	}

	return true, nil
}

func (se *SplitExecutor) GetSplitFiles() []string {
	return se.splitFiles
}
