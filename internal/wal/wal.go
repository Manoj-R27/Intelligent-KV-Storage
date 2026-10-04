package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

const (
	putRecord     byte = 1
	deleteRecord  byte = 2
	headerSize         = 8
	maxRecordSize      = 64 << 20
)

var ErrCorrupt = errors.New("wal: corrupt record")

type Record struct {
	Key    string
	Value  string
	Delete bool
}

type Log struct {
	mu   sync.Mutex
	file *os.File
}

func Open(path string) (*Log, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	return &Log{file: file}, nil
}

func (log *Log) Append(record Record) error {
	log.mu.Lock()
	defer log.mu.Unlock()
	payload, err := encode(record)
	if err != nil {
		return err
	}
	frame := make([]byte, headerSize+len(payload))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[headerSize:], payload)
	if _, err := log.file.Write(frame); err != nil {
		return fmt.Errorf("append wal record: %w", err)
	}
	if err := log.file.Sync(); err != nil {
		return fmt.Errorf("sync wal: %w", err)
	}
	return nil
}

func (log *Log) Replay() ([]Record, error) {
	log.mu.Lock()
	defer log.mu.Unlock()
	if _, err := log.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek wal: %w", err)
	}
	reader := bufio.NewReader(log.file)
	var records []Record
	for {
		header := make([]byte, headerSize)
		_, err := io.ReadFull(reader, header)
		if errors.Is(err, io.EOF) {
			return records, nil
		}
		if err != nil {
			return nil, fmt.Errorf("%w: incomplete header", ErrCorrupt)
		}
		length := binary.BigEndian.Uint32(header[0:4])
		if length == 0 || length > maxRecordSize {
			return nil, fmt.Errorf("%w: invalid payload length %d", ErrCorrupt, length)
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return nil, fmt.Errorf("%w: incomplete payload", ErrCorrupt)
		}
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(header[4:8]) {
			return nil, fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
		}
		record, err := decode(payload)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
}

func (log *Log) Close() error {
	log.mu.Lock()
	defer log.mu.Unlock()
	return log.file.Close()
}

func encode(record Record) ([]byte, error) {
	if record.Key == "" {
		return nil, errors.New("wal: key cannot be empty")
	}
	if record.Delete && record.Value != "" {
		return nil, errors.New("wal: delete record cannot contain a value")
	}
	key := []byte(record.Key)
	value := []byte(record.Value)
	payload := make([]byte, 9+len(key)+len(value))
	if record.Delete {
		payload[0] = deleteRecord
	} else {
		payload[0] = putRecord
	}
	binary.BigEndian.PutUint32(payload[1:5], uint32(len(key)))
	binary.BigEndian.PutUint32(payload[5:9], uint32(len(value)))
	copy(payload[9:], key)
	copy(payload[9+len(key):], value)
	return payload, nil
}

func decode(payload []byte) (Record, error) {
	if len(payload) < 9 {
		return Record{}, fmt.Errorf("%w: short payload", ErrCorrupt)
	}
	keyLength := binary.BigEndian.Uint32(payload[1:5])
	valueLength := binary.BigEndian.Uint32(payload[5:9])
	if uint64(9)+uint64(keyLength)+uint64(valueLength) != uint64(len(payload)) {
		return Record{}, fmt.Errorf("%w: invalid field lengths", ErrCorrupt)
	}
	if keyLength == 0 || (payload[0] == deleteRecord && valueLength != 0) {
		return Record{}, fmt.Errorf("%w: invalid record fields", ErrCorrupt)
	}
	if payload[0] != putRecord && payload[0] != deleteRecord {
		return Record{}, fmt.Errorf("%w: unknown record type", ErrCorrupt)
	}
	keyEnd := 9 + int(keyLength)
	return Record{Key: string(payload[9:keyEnd]), Value: string(payload[keyEnd:]), Delete: payload[0] == deleteRecord}, nil
}
