package coordinate

import (
	"errors"
	"fmt"
	"sync"

	"mapuce/mr/data"
	"mapuce/mr/util"
)

const (
	MAP_TASK_TYPE    = 1
	REDUCE_TASK_TYPE = 2

	MapResultFormat = "mr-map-result-task%v-id%v-shard%v"

	ReduceResultFormat = "mr-out-%v"
)

type Task struct {
	Id          int64
	ProjectId   string
	InputFiles  []string
	Type        int
	TargetFiles []string
}

func getMappedFileName(projectId string, id int64, nReduce int) []string {
	ret := make([]string, 0, nReduce)
	for i := 0; i < nReduce; i++ {
		name := fmt.Sprintf(MapResultFormat, projectId, id, i)
		ret = append(ret, name)
	}
	return ret
}

func newMapTask(taskId string, inputFiles []string, nReduce int) (Task, error) {
	id, err := data.Default().IdGenerate()
	if err != nil {
		return Task{}, err
	}
	return Task{
		Id:          id,
		ProjectId:   taskId,
		InputFiles:  inputFiles,
		Type:        MAP_TASK_TYPE,
		TargetFiles: getMappedFileName(taskId, id, nReduce),
	}, nil
}

type TaskManager struct {
	taskId    string
	initQueue *util.Queue
	waitMap   map[int64]Task
	doneQueue []Task
	lock      sync.Mutex
	nReduce   int
}

func NewTaskManager(splitFiles []string, taskId string, nWorker, nReduce int) (*TaskManager, error) {
	q := util.NewQueue()

	limit := len(splitFiles) / nWorker
	taskFiles := make([]string, 0, limit)
	for _, file := range splitFiles {
		taskFiles = append(taskFiles, file)
		if len(taskFiles) == limit {
			t, err := newMapTask(taskId, taskFiles, nReduce)
			if err != nil {
				return nil, err
			}
			q.Push(t)
			taskFiles = make([]string, 0, limit)
		}
	}

	if len(taskFiles) != 0 {
		t, err := newMapTask(taskId, taskFiles, nReduce)
		if err != nil {
			return nil, err
		}
		q.Push(t)
	}

	return &TaskManager{
		taskId:    taskId,
		initQueue: &q,
		waitMap:   make(map[int64]Task),
		doneQueue: make([]Task, 0),
		nReduce:   nReduce,
	}, nil
}

func (tm *TaskManager) initReduceTasks() error {
	mapFiles := make([][]string, tm.nReduce)
	for _, doneTask := range tm.doneQueue {
		if len(doneTask.TargetFiles) != tm.nReduce {
			return errors.New("wrong mapped result count")
		}
		for i := 0; i < tm.nReduce; i++ {
			mapFiles[i] = append(mapFiles[i], doneTask.TargetFiles[i])
		}
	}

	for idx, group := range mapFiles {
		id, err := data.Default().IdGenerate()
		if err != nil {
			return err
		}
		task := Task{
			Id:          id,
			ProjectId:   tm.taskId,
			InputFiles:  group,
			Type:        REDUCE_TASK_TYPE,
			TargetFiles: []string{fmt.Sprintf(ReduceResultFormat, idx)},
		}
		tm.initQueue.Push(task)
	}
	return nil
}

func (tm *TaskManager) Finish(id int64) error {
	tm.lock.Lock()
	defer tm.lock.Unlock()
	t, ok := tm.waitMap[id]
	if !ok {
		return errors.New("task is not waitting")
	}
	delete(tm.waitMap, id)
	tm.doneQueue = append(tm.doneQueue, t)

	if t.Type == MAP_TASK_TYPE && len(tm.waitMap) == 0 && tm.initQueue.Empty() {
		tm.initReduceTasks()
	}
	return nil
}

func (tm *TaskManager) Acquire() (*Task, error) {
	tm.lock.Lock()
	defer tm.lock.Unlock()
	if tm.initQueue.Empty() {
		return nil, nil
	}
	e := tm.initQueue.Pop()
	t, ok := e.(Task)
	if !ok {
		return nil, errors.New("wrong type in init q")
	}

	tm.waitMap[t.Id] = t
	return &t, nil
}

func (tm *TaskManager) Timeout(id int64) error {
	tm.lock.Lock()
	defer tm.lock.Unlock()
	t, ok := tm.waitMap[id]
	if !ok {
		return errors.New("task is not waitting")
	}
	delete(tm.waitMap, id)
	tm.initQueue.Push(t)
	return nil
}

func (tm *TaskManager) Done() bool {
	tm.lock.Lock()
	defer tm.lock.Unlock()
	return tm.initQueue.Empty() && len(tm.waitMap) == 0
}
