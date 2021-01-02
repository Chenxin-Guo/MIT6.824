package mr

import (
	"fmt"
	"sync"
	"time"
)

// Task represents the status of a single map/reduce task.
type Task struct {
	// True if this task is a map task, else it is a reduce task.
	IsMapTask bool
	// True if this task has been already assigned to a worker at least once.
	IsAssigned bool
	// The latest timestamp of this task was assigned to a worker. Only valid if IsAssigned is True.
	AssignTime time.Time
	// The file to be mapped, only valid if IsMapTask is True.
	FilePath string
	// The index of the task
	Index int
}

// TaskManager manages unfinished map/reduce tasks
type TaskManager struct {
	Tasks map[int]*Task

	mu sync.Mutex
}

// AddMapTask adds a new map task to the task manager.
func (t *TaskManager) AddMapTask(index int, mapFile string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.Tasks[index]
	if ok {
		return fmt.Errorf("Index %v is already added", index)
	}
	t.Tasks[index] = &Task{
		IsMapTask:  true,
		IsAssigned: false,
		FilePath:   mapFile,
		Index:      index,
	}
	return nil
}

// AddReduceTask adds a new reduce task to the task manager.
func (t *TaskManager) AddReduceTask(index int) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.Tasks[index]; ok {
		return fmt.Errorf("Index %v is already added", index)
	}
	t.Tasks[index] = &Task{
		IsMapTask:  false,
		IsAssigned: false,
		Index:      index,
	}
	return nil
}

// GetAssignment returns a pending task.
func (t *TaskManager) GetAssignment() *Task {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, v := range t.Tasks {
		if !v.IsAssigned || time.Now().Sub(v.AssignTime) > 10*time.Second {
			v.AssignTime = time.Now()
			v.IsAssigned = true
			return v
		}
	}
	return nil
}

// RemoveTask removes a task from the task manager.
func (t *TaskManager) RemoveTask(index int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.Tasks[index]; ok {
		delete(t.Tasks, index)
	}
}

// Done returns true if there is no more pending task.
func (t *TaskManager) Done() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.Tasks) == 0 {
		return true
	}
	return false
}
