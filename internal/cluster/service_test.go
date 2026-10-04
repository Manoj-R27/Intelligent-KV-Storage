package cluster

import (
	"testing"

	"github.com/Manoj-R27/Intelligent-KV-Storage/internal/raft"
	"github.com/Manoj-R27/Intelligent-KV-Storage/internal/storage"
)

func TestSingleNodeServiceAppliesCommittedCommands(t *testing.T) {
	engine, err := storage.Open(storage.Config{Directory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	transport := raft.NewMemoryTransport()
	service := NewService("n1", nil, transport, engine, 2, 1)
	transport.Register(service.Node())
	for index := 0; index < 2; index++ {
		service.Node().Tick()
	}
	if service.Node().Role() != raft.Leader {
		t.Fatalf("role = %v", service.Node().Role())
	}
	if err := service.Put("key", "value"); err != nil {
		t.Fatal(err)
	}
	value, err := service.Get("key")
	if err != nil || value != "value" {
		t.Fatalf("Get(key) = %q, %v", value, err)
	}
}
