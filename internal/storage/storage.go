package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/Manoj-R27/Intelligent-KV-Storage/internal/bloom"
	"github.com/Manoj-R27/Intelligent-KV-Storage/internal/memtable"
	"github.com/Manoj-R27/Intelligent-KV-Storage/internal/wal"
)

var ErrNotFound = errors.New("storage: key not found")

type Config struct {
	Directory       string
	MemTableEntries int
}

type Engine struct {
	mu        sync.RWMutex
	directory string
	threshold int
	wal       *wal.Log
	mem       *memtable.Table
	stable    []*table
	nextTable uint64
}

type table struct {
	path    string
	entries []memtable.Entry
	index   map[string]int
	filter  *bloom.Filter
}

func Open(config Config) (*Engine, error) {
	if config.Directory == "" {
		return nil, errors.New("storage: directory is required")
	}
	if config.MemTableEntries <= 0 {
		config.MemTableEntries = 1024
	}
	if err := os.MkdirAll(config.Directory, 0o700); err != nil {
		return nil, fmt.Errorf("create storage directory: %w", err)
	}
	log, err := wal.Open(filepath.Join(config.Directory, "store.wal"))
	if err != nil {
		return nil, err
	}
	engine := &Engine{directory: config.Directory, threshold: config.MemTableEntries, wal: log, mem: memtable.New()}
	if err := engine.loadTables(); err != nil {
		log.Close()
		return nil, err
	}
	records, err := log.Replay()
	if err != nil {
		log.Close()
		return nil, err
	}
	for _, record := range records {
		if record.Delete {
			engine.mem.Delete(record.Key)
		} else {
			engine.mem.Put(record.Key, record.Value)
		}
	}
	return engine, nil
}

func (engine *Engine) Put(key, value string) error {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if err := engine.wal.Append(wal.Record{Key: key, Value: value}); err != nil {
		return err
	}
	engine.mem.Put(key, value)
	return engine.flushIfNeeded()
}

func (engine *Engine) Delete(key string) error {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if err := engine.wal.Append(wal.Record{Key: key, Delete: true}); err != nil {
		return err
	}
	engine.mem.Delete(key)
	return engine.flushIfNeeded()
}

func (engine *Engine) Get(key string) (string, error) {
	engine.mu.RLock()
	defer engine.mu.RUnlock()
	if entry, ok := engine.mem.Lookup(key); ok {
		if entry.Delete {
			return "", ErrNotFound
		}
		return entry.Value, nil
	}
	for index := len(engine.stable) - 1; index >= 0; index-- {
		if entry, ok := engine.stable[index].lookup(key); ok {
			if entry.Delete {
				return "", ErrNotFound
			}
			return entry.Value, nil
		}
	}
	return "", ErrNotFound
}

func (engine *Engine) Flush() error {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	return engine.flush()
}

func (engine *Engine) Compact() error {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if len(engine.stable) < 2 {
		return nil
	}
	latest := make(map[string]memtable.Entry)
	for index := len(engine.stable) - 1; index >= 0; index-- {
		for _, entry := range engine.stable[index].entries {
			if _, exists := latest[entry.Key]; !exists {
				latest[entry.Key] = entry
			}
		}
	}
	entries := make([]memtable.Entry, 0, len(latest))
	for _, entry := range latest {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	newTable, err := engine.writeTable(entries)
	if err != nil {
		return err
	}
	for _, old := range engine.stable {
		if err := os.Remove(old.path); err != nil {
			return fmt.Errorf("remove compacted table: %w", err)
		}
	}
	engine.stable = []*table{newTable}
	return nil
}

func (engine *Engine) Close() error {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if err := engine.flush(); err != nil {
		return err
	}
	return engine.wal.Close()
}

func (engine *Engine) flushIfNeeded() error {
	if engine.mem.Len() < engine.threshold {
		return nil
	}
	return engine.flush()
}

func (engine *Engine) flush() error {
	if engine.mem.Len() == 0 {
		return nil
	}
	entries := engine.mem.Snapshot()
	newTable, err := engine.writeTable(entries)
	if err != nil {
		return err
	}
	engine.stable = append(engine.stable, newTable)
	engine.mem = memtable.New()
	return nil
}

func (engine *Engine) writeTable(entries []memtable.Entry) (*table, error) {
	engine.nextTable++
	path := filepath.Join(engine.directory, fmt.Sprintf("sstable-%020d.sst", engine.nextTable))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create sstable: %w", err)
	}
	if err := writeEntries(file, entries); err != nil {
		file.Close()
		os.Remove(path)
		return nil, err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(path)
		return nil, fmt.Errorf("sync sstable: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("close sstable: %w", err)
	}
	return newTable(path, entries), nil
}

func (engine *Engine) loadTables() error {
	paths, err := filepath.Glob(filepath.Join(engine.directory, "sstable-*.sst"))
	if err != nil {
		return err
	}
	sort.Strings(paths)
	for _, path := range paths {
		entries, err := readEntries(path)
		if err != nil {
			return err
		}
		engine.stable = append(engine.stable, newTable(path, entries))
		name := strings.TrimSuffix(filepath.Base(path), ".sst")
		if number, err := strconv.ParseUint(strings.TrimPrefix(name, "sstable-"), 10, 64); err == nil && number >= engine.nextTable {
			engine.nextTable = number
		}
	}
	return nil
}

func newTable(path string, entries []memtable.Entry) *table {
	index := make(map[string]int, len(entries))
	filter := bloom.New(len(entries), 10, 3)
	for position, entry := range entries {
		index[entry.Key] = position
		filter.Add(entry.Key)
	}
	return &table{path: path, entries: entries, index: index, filter: filter}
}

func (current *table) lookup(key string) (memtable.Entry, bool) {
	if !current.filter.MayContain(key) {
		return memtable.Entry{}, false
	}
	position, ok := current.index[key]
	if !ok {
		return memtable.Entry{}, false
	}
	return current.entries[position], true
}

func writeEntries(file *os.File, entries []memtable.Entry) error {
	if _, err := file.Write([]byte("IKV1")); err != nil {
		return err
	}
	var number [4]byte
	binary.BigEndian.PutUint32(number[:], uint32(len(entries)))
	if _, err := file.Write(number[:]); err != nil {
		return err
	}
	for _, entry := range entries {
		if len(entry.Key) > int(^uint32(0)) || len(entry.Value) > int(^uint32(0)) {
			return errors.New("storage: entry too large")
		}
		binary.BigEndian.PutUint32(number[:], uint32(len(entry.Key)))
		if _, err := file.Write(number[:]); err != nil {
			return err
		}
		binary.BigEndian.PutUint32(number[:], uint32(len(entry.Value)))
		if _, err := file.Write(number[:]); err != nil {
			return err
		}
		if entry.Delete {
			if _, err := file.Write([]byte{1}); err != nil {
				return err
			}
		} else if _, err := file.Write([]byte{0}); err != nil {
			return err
		}
		if _, err := io.WriteString(file, entry.Key); err != nil {
			return err
		}
		if _, err := io.WriteString(file, entry.Value); err != nil {
			return err
		}
	}
	return nil
}

func readEntries(path string) ([]memtable.Entry, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	magic := make([]byte, 4)
	if _, err := io.ReadFull(file, magic); err != nil || string(magic) != "IKV1" {
		return nil, errors.New("storage: corrupt sstable header")
	}
	var number [4]byte
	if _, err := io.ReadFull(file, number[:]); err != nil {
		return nil, errors.New("storage: corrupt sstable count")
	}
	entries := make([]memtable.Entry, 0, binary.BigEndian.Uint32(number[:]))
	for count := uint32(0); count < binary.BigEndian.Uint32(number[:]); count++ {
		var fields [9]byte
		if _, err := io.ReadFull(file, fields[:]); err != nil {
			return nil, errors.New("storage: corrupt sstable entry")
		}
		key := make([]byte, binary.BigEndian.Uint32(fields[0:4]))
		value := make([]byte, binary.BigEndian.Uint32(fields[4:8]))
		if fields[8] > 1 || len(key) == 0 {
			return nil, errors.New("storage: invalid sstable entry")
		}
		if _, err := io.ReadFull(file, key); err != nil {
			return nil, errors.New("storage: corrupt sstable key")
		}
		if _, err := io.ReadFull(file, value); err != nil {
			return nil, errors.New("storage: corrupt sstable value")
		}
		entries = append(entries, memtable.Entry{Key: string(key), Value: string(value), Delete: fields[8] == 1})
	}
	return entries, nil
}
