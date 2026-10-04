package storage

import (
	"errors"
	"testing"
)

func TestEngineRecoversFromWALAndSSTable(t *testing.T) {
	directory := t.TempDir()
	engine, err := Open(Config{Directory: directory, MemTableEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Put("a", "one"); err != nil {
		t.Fatal(err)
	}
	if err := engine.Put("b", "two"); err != nil {
		t.Fatal(err)
	}
	if err := engine.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	engine, err = Open(Config{Directory: directory, MemTableEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	if _, err := engine.Get("a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(a) = %v", err)
	}
	value, err := engine.Get("b")
	if err != nil || value != "two" {
		t.Fatalf("Get(b) = %q, %v", value, err)
	}
}

func TestEngineCompactsNewestValues(t *testing.T) {
	engine, err := Open(Config{Directory: t.TempDir(), MemTableEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	if err := engine.Put("key", "old"); err != nil {
		t.Fatal(err)
	}
	if err := engine.Put("key", "new"); err != nil {
		t.Fatal(err)
	}
	if err := engine.Compact(); err != nil {
		t.Fatal(err)
	}
	value, err := engine.Get("key")
	if err != nil || value != "new" {
		t.Fatalf("Get(key) = %q, %v", value, err)
	}
}
