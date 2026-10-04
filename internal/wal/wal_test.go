package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestReplayRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	log, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []Record{{Key: "user:101", Value: "Manoj"}, {Key: "user:101", Delete: true}}
	for _, record := range want {
		if err := log.Append(record); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	log, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	got, err := log.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("replayed records = %#v, want %#v", got, want)
	}
}

func TestReplayRejectsCorruptionAndIncompleteRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	log, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Append(Record{Key: "a", Value: "b"}); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{0, 0, 0, 4, 0, 0, 0, 0, 1}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	log, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	_, err = log.Replay()
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("replay error = %v, want ErrCorrupt", err)
	}
}

func TestReplayRejectsChecksumMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	log, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Append(Record{Key: "a", Value: "b"}); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	contents[len(contents)-1] ^= 1
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	log, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	_, err = log.Replay()
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("replay error = %v, want ErrCorrupt", err)
	}
}
