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
	// Kontext-abbrechbar: verhindert echten Deadlock
	if err := r.memSem.Acquire(r.rootCTX, bytes); err != nil {
		return false
	}
	return true
}

// Speicher freigeben
func (r *StorageHandler) Release(bytes int64) {
	if bytes <= 0 {
		return
	}
	r.memSem.Release(bytes)
}
