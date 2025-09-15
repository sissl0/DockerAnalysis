/*
Georg Heindl
Hilfsfunktionen zum Verwalten von Platz im RAM.
Nutzt ein blockierenden Semaphore für Acquire und Release.
*/
package database

import (
	"context"

	"golang.org/x/sync/semaphore"
)

type StorageHandler struct {
	MaxStorage int64
	memSem     *semaphore.Weighted
	rootCTX    context.Context
}

/*
NewStorageHandler erstellt einen neuen StorageHandler mit der angegebenen maximalen Speichernutzung in Bytes.
rootCTX, für Abbruch bei Deadlocks.
*/
func NewStorageHandler(maxStorage int64) *StorageHandler {
	return &StorageHandler{
		MaxStorage: maxStorage,
		memSem:     semaphore.NewWeighted(maxStorage),
		rootCTX:    context.Background(), // sicherer Default
	}
}

func (r *StorageHandler) WithContext(ctx context.Context) {
	if ctx != nil {
		r.rootCTX = ctx
	}
}

func (r *StorageHandler) Acquire(bytes int64) bool {
	if bytes > r.MaxStorage {
		return false
	}
	if err := r.memSem.Acquire(r.rootCTX, bytes); err != nil {
		return false
	}
	return true
}

func (r *StorageHandler) Release(bytes int64) {
	if bytes <= 0 {
		return
	}
	r.memSem.Release(bytes)
}
