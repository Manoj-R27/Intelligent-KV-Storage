package cluster

import (
	"encoding/json"
	"errors"

	"github.com/Manoj-R27/Intelligent-KV-Storage/internal/raft"
	"github.com/Manoj-R27/Intelligent-KV-Storage/internal/storage"
)

type command struct {
	Operation string `json:"operation"`
	Key       string `json:"key"`
	Value     string `json:"value,omitempty"`
}

type Service struct {
	engine *storage.Engine
	node   *raft.Node
}

func NewService(id string, peers []string, transport raft.Transport, engine *storage.Engine, electionSpan, heartbeatSpan int) *Service {
	return NewServiceWithState(id, peers, transport, engine, electionSpan, heartbeatSpan, nil)
}

func NewServiceWithState(id string, peers []string, transport raft.Transport, engine *storage.Engine, electionSpan, heartbeatSpan int, stateStore raft.StateStore) *Service {
	service := &Service{engine: engine}
	service.node = raft.NewWithState(id, peers, transport, service.apply, electionSpan, heartbeatSpan, stateStore)
	return service
}

func (service *Service) Node() *raft.Node { return service.node }

func (service *Service) Put(key, value string) error {
	data, err := json.Marshal(command{Operation: "put", Key: key, Value: value})
	if err != nil {
		return err
	}
	return service.node.Propose(data)
}

func (service *Service) Delete(key string) error {
	data, err := json.Marshal(command{Operation: "delete", Key: key})
	if err != nil {
		return err
	}
	return service.node.Propose(data)
}

func (service *Service) Get(key string) (string, error) { return service.engine.Get(key) }

func (service *Service) apply(entry raft.Entry) error {
	var value command
	if err := json.Unmarshal(entry.Command, &value); err != nil {
		return err
	}
	switch value.Operation {
	case "put":
		return service.engine.Put(value.Key, value.Value)
	case "delete":
		return service.engine.Delete(value.Key)
	default:
		return errors.New("cluster: unknown command")
	}
}
