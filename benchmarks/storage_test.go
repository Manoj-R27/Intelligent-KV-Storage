package benchmarks

import (
	"testing"

	"github.com/Manoj-R27/Intelligent-KV-Storage/internal/storage"
)

func BenchmarkSingleNodePut(b *testing.B) {
	engine, err := storage.Open(storage.Config{Directory: b.TempDir(), MemTableEntries: b.N + 1})
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := engine.Put("key", "value"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSingleNodeGet(b *testing.B) {
	engine, err := storage.Open(storage.Config{Directory: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()
	if err := engine.Put("key", "value"); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := engine.Get("key"); err != nil {
			b.Fatal(err)
		}
	}
}
