package inmemory

import "sync"

type Storage struct {
	mu    sync.RWMutex
	store map[string]string
}

func NewStorage() *Storage {
	return &Storage{
		store: make(map[string]string),
	}
}

func (storage *Storage) Put(key, value string) {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	storage.store[key] = value
}

func (storage *Storage) Get(key string) (string, bool) {
	storage.mu.RLock()
	defer storage.mu.RUnlock()
	value, exists := storage.store[key]
	return value, exists
}

func (storage *Storage) Dump() map[string]string {
	storage.mu.RLock()
	defer storage.mu.RUnlock()

	storageCopy := make(map[string]string, len(storage.store))
	for k, v := range storage.store {
		storageCopy[k] = v
	}
	return storageCopy
}
