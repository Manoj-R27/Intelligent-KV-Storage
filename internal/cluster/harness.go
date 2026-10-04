package cluster

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Manoj-R27/Intelligent-KV-Storage/internal/raft"
	"github.com/Manoj-R27/Intelligent-KV-Storage/internal/storage"
)

type Harness struct {
	transport *raft.MemoryTransport
	root      string
	ids       []string
	services  map[string]*Service
	engines   map[string]*storage.Engine
}

func NewHarness() (*Harness, error) {
	root, err := os.MkdirTemp("", "intelligent-kv-cluster-")
	if err != nil {
		return nil, err
	}
	harness := &Harness{transport: raft.NewMemoryTransport(), root: root, ids: []string{"node-1", "node-2", "node-3"}, services: make(map[string]*Service), engines: make(map[string]*storage.Engine)}
	for _, id := range harness.ids {
		if err := harness.startNode(id); err != nil {
			harness.Close()
			return nil, err
		}
	}
	return harness, nil
}

func (harness *Harness) Tick() {
	for _, service := range harness.services {
		service.Node().Tick()
	}
}

func (harness *Harness) CurrentLeader() (*Service, error) {
	for _, service := range harness.services {
		if service.Node().Role() == raft.Leader {
			return service, nil
		}
	}
	return nil, errors.New("cluster: no leader")
}

func (harness *Harness) Put(key, value string) error {
	leader, err := harness.CurrentLeader()
	if err != nil {
		return err
	}
	return leader.Put(key, value)
}
func (harness *Harness) Get(key string) (string, error) {
	leader, err := harness.CurrentLeader()
	if err != nil {
		return "", err
	}
	return leader.Get(key)
}

func (harness *Harness) StopNode(id string) error {
	service, ok := harness.services[id]
	if !ok {
		return fmt.Errorf("cluster: unknown node %s", id)
	}
	harness.transport.Unregister(id)
	if err := harness.engines[id].Close(); err != nil {
		return err
	}
	delete(harness.services, id)
	delete(harness.engines, id)
	_ = service
	return nil
}

func (harness *Harness) RestartNode(id string) error    { return harness.startNode(id) }
func (harness *Harness) Partition(left, right []string) { harness.transport.Partition(left, right) }
func (harness *Harness) HealPartition()                 { harness.transport.Heal() }

func (harness *Harness) Close() error {
	for id := range harness.engines {
		_ = harness.engines[id].Close()
	}
	return os.RemoveAll(harness.root)
}

func (harness *Harness) startNode(id string) error {
	if old, ok := harness.engines[id]; ok {
		_ = old.Close()
		harness.transport.Unregister(id)
	}
	engine, err := storage.Open(storage.Config{Directory: filepath.Join(harness.root, id), MemTableEntries: 16})
	if err != nil {
		return err
	}
	peers := make([]string, 0, len(harness.ids)-1)
	for _, peer := range harness.ids {
		if peer != id {
			peers = append(peers, peer)
		}
	}
	service := NewService(id, peers, harness.transport, engine, 3, 1)
	harness.engines[id] = engine
	harness.services[id] = service
	harness.transport.Register(service.Node())
	return nil
}
