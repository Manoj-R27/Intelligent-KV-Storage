package memtable

import (
	"sync"
	"testing"
)

func TestTableSupportsUpdatesAndTombstones(t *testing.T) {
	table := New()
	table.Put("b", "two")
	table.Put("a", "one")
	table.Put("b", "updated")
	if value, ok := table.Get("b"); !ok || value != "updated" {
		t.Fatalf("Get(b) = %q, %v", value, ok)
	}
	table.Delete("b")
	if _, ok := table.Get("b"); ok {
		t.Fatal("deleted key was returned")
	}
	entries := table.Snapshot()
	if len(entries) != 2 || entries[0].Key != "a" || entries[1].Key != "b" || !entries[1].Delete {
		t.Fatalf("snapshot = %#v", entries)
	}
}

func TestTableSupportsConcurrentAccess(t *testing.T) {
	table := New()
	var group sync.WaitGroup
	for i := 0; i < 16; i++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			key := string(rune('a' + index))
			table.Put(key, "value")
			_, _ = table.Get(key)
		}(i)
	}
	group.Wait()
	if table.Len() != 16 {
		t.Fatalf("table length = %d, want 16", table.Len())
	}
}
