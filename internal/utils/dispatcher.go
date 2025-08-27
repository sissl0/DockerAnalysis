package utils

import (
	"sync"
)

type Dispatcher struct {
	tasks map[int]Task
	mu    sync.Mutex
}

// NewDispatcher erstellt einen neuen Dispatcher
func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		tasks: make(map[int]Task),
	}
}

// AddTask fügt eine Aufgabe mit einer eindeutigen ID hinzu
func (d *Dispatcher) AddTask(id int, task Task) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.tasks[id] = task
}

// GetTask gibt eine Aufgabe basierend auf ihrer ID zurück
func (d *Dispatcher) GetTask(id int) (Task, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	task, exists := d.tasks[id]
	return task, exists
}

// RemoveTask entfernt eine Aufgabe basierend auf ihrer ID
func (d *Dispatcher) RemoveTask(id int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.tasks, id)
}
