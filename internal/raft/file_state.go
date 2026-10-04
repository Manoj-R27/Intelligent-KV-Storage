package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type FileStateStore struct {
	mu   sync.Mutex
	path string
}

func NewFileStateStore(path string) *FileStateStore { return &FileStateStore{path: path} }

func (store *FileStateStore) Load() (PersistentState, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	data, err := os.ReadFile(store.path)
	if os.IsNotExist(err) {
		return PersistentState{}, nil
	}
	if err != nil {
		return PersistentState{}, fmt.Errorf("load raft state: %w", err)
	}
	var state PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return PersistentState{}, fmt.Errorf("decode raft state: %w", err)
	}
	return state, nil
}

func (store *FileStateStore) Save(state PersistentState) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(store.path), 0o700); err != nil {
		return err
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	temporary := store.path + ".tmp"
	if err := os.WriteFile(temporary, data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(temporary, store.path); err != nil {
		_ = os.Remove(temporary)
		return fmt.Errorf("persist raft state: %w", err)
	}
	return nil
}
