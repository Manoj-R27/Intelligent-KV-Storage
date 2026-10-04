package kv

import (
	"errors"

	"github.com/Manoj-R27/Intelligent-KV-Storage/internal/storage"
)

type Service struct {
	engine *storage.Engine
}

func New(engine *storage.Engine) *Service { return &Service{engine: engine} }

func (service *Service) Put(key, value string) error {
	return service.engine.Put(key, value)
}

func (service *Service) Get(key string) (string, error) {
	return service.engine.Get(key)
}

func (service *Service) Delete(key string) error {
	return service.engine.Delete(key)
}

func IsNotFound(err error) bool { return errors.Is(err, storage.ErrNotFound) }
