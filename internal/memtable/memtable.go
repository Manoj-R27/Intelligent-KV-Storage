package memtable

import (
	"sort"
	"sync"
)

type Entry struct {
	Key    string
	Value  string
	Delete bool
}

type Table struct {
	mu      sync.RWMutex
	entries map[string]Entry
}

func New() *Table { return &Table{entries: make(map[string]Entry)} }

func (table *Table) Put(key, value string) {
	table.mu.Lock()
	defer table.mu.Unlock()
	table.entries[key] = Entry{Key: key, Value: value}
}

func (table *Table) Delete(key string) {
	table.mu.Lock()
	defer table.mu.Unlock()
	table.entries[key] = Entry{Key: key, Delete: true}
}

func (table *Table) Get(key string) (string, bool) {
	entry, ok := table.Lookup(key)
	if !ok || entry.Delete {
		return "", false
	}
	return entry.Value, true
}

func (table *Table) Lookup(key string) (Entry, bool) {
	table.mu.RLock()
	defer table.mu.RUnlock()
	entry, ok := table.entries[key]
	return entry, ok
}

func (table *Table) Snapshot() []Entry {
	table.mu.RLock()
	defer table.mu.RUnlock()
	entries := make([]Entry, 0, len(table.entries))
	for _, entry := range table.entries {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	return entries
}

func (table *Table) Len() int {
	table.mu.RLock()
	defer table.mu.RUnlock()
	return len(table.entries)
}
