package coordinate

import "testing"

func TestTaskManager(t *testing.T) {
	// 3map task 3*2 reduce task
	tm, err := NewTaskManager([]string{"1", "2", "3"}, randomTaskId(), 3, 2)
	if err != nil {
		t.Fatal(err)
	}

	mapTasks := make([]*Task, 0)
	for {
		task, err := tm.Acquire()
		if err != nil {
			t.Fatal(err)
		}
		if task == nil {
			break
		}
		mapTasks = append(mapTasks, task)
	}

	task := mapTasks[2]
	mapTasks = mapTasks[0:2]
	tm.Timeout(task.Id)

	task, err = tm.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	if task == nil {
		t.Fatal("should have a init which is timout before")
	}

	mapTasks = append(mapTasks, task)

	for i := range mapTasks {
		err := tm.Finish(mapTasks[i].Id)
		if err != nil {
			t.Fatal(err)
		}
	}

	reduceTaks := make([]Task, 0)
	for {
		tsk, err := tm.Acquire()
		if err != nil {
			t.Fatal(err)
		}
		if tsk == nil {
			break
		}
		reduceTaks = append(reduceTaks, *tsk)
	}

	if len(reduceTaks) != 6 {
		t.Fatal("wrong reduce num")
	}

	for _, tsk := range reduceTaks {
		err := tm.Finish(tsk.Id)
		if err != nil {
			ids := make([]int, 0)
			for _, tk := range reduceTaks {
				ids = append(ids, int(tk.Id))
			}
			t.Error(ids)
			t.Fatal(err)
		}
	}

	if !tm.Done() {
		t.Fatal("should finish")
	}
}
