package mr

import "time"

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
}

// TaskManager manages unfinished map/reduce tasks
type TaskManager struct {
	Tasks map[int]Task
}

// AddMapTask adds a new map task to the task manager.
func (t *TaskManager) AddMapTask(index int, mapFile string) {

}

// AddReduceTask adds a new map task to the task manager.
func (t *TaskManager) AddReduceTask(index int, mapFile string) {

}

// GetAssignment returns a pending task.
func (t *TaskManager) GetAssignment() Task {

}

// RemoveTask removes a task from the task manager.
func (t *TaskManager) RemoveTask(index int) Task {

}

// Done returns true if there is no more pending task.
func (t *TaskManager) Done() bool {

}
