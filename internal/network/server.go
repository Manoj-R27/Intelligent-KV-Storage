package network

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/Manoj-R27/Intelligent-KV-Storage/internal/storage"
	"github.com/Manoj-R27/Intelligent-KV-Storage/internal/telemetry"
)

type Service interface {
	Put(key, value string) error
	Get(key string) (string, error)
	Delete(key string) error
}

type Server struct {
	listener net.Listener
	service  Service
	metrics  *telemetry.Metrics
	clients  sync.WaitGroup
	stop     sync.Once
}

func Listen(address string, service Service) (*Server, error) {
	return listen(address, service, nil)
}

func ListenWithMetrics(address string, service Service, metrics *telemetry.Metrics) (*Server, error) {
	return listen(address, service, metrics)
}

func listen(address string, service Service, metrics *telemetry.Metrics) (*Server, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", address, err)
	}
	return &Server{listener: listener, service: service, metrics: metrics}, nil
}

func (server *Server) Address() string { return server.listener.Addr().String() }

func (server *Server) Serve(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		server.Close()
	}()
	for {
		connection, err := server.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
				server.clients.Wait()
				return nil
			}
			return fmt.Errorf("accept connection: %w", err)
		}
		server.clients.Add(1)
		go func() {
			defer server.clients.Done()
			_ = server.handle(connection)
		}()
	}
}

func (server *Server) Close() error {
	var err error
	server.stop.Do(func() { err = server.listener.Close() })
	return err
}

func (server *Server) handle(connection net.Conn) error {
	defer connection.Close()
	reader := bufio.NewScanner(connection)
	reader.Buffer(make([]byte, 1024), 16<<20)
	writer := bufio.NewWriter(connection)
	for reader.Scan() {
		started := time.Now()
		request := strings.TrimSpace(reader.Text())
		response := server.execute(request)
		server.record(request, response, time.Since(started))
		if _, err := writer.WriteString(response + "\n"); err != nil {
			return err
		}
		if err := writer.Flush(); err != nil {
			return err
		}
	}
	if err := reader.Err(); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func (server *Server) record(request, response string, duration time.Duration) {
	if server.metrics == nil {
		return
	}
	parts := strings.SplitN(request, " ", 2)
	operation := "unknown"
	if len(parts) > 0 && parts[0] != "" {
		operation = strings.ToLower(parts[0])
	}
	server.metrics.Inc("kv_requests_total")
	server.metrics.Inc("kv_" + operation + "_total")
	if strings.HasPrefix(response, "ERR") {
		server.metrics.Inc("kv_errors_total")
	}
	server.metrics.ObserveRequest(duration)
}

func (server *Server) execute(line string) string {
	parts := strings.SplitN(line, " ", 3)
	if len(parts) == 0 || parts[0] == "" {
		return "ERR empty command"
	}
	switch strings.ToUpper(parts[0]) {
	case "PUT":
		if len(parts) != 3 || parts[1] == "" {
			return "ERR usage: PUT key value"
		}
		if err := server.service.Put(parts[1], parts[2]); err != nil {
			return "ERR " + err.Error()
		}
		return "OK"
	case "GET":
		if len(parts) != 2 || parts[1] == "" {
			return "ERR usage: GET key"
		}
		value, err := server.service.Get(parts[1])
		if errors.Is(err, storage.ErrNotFound) {
			return "NOT_FOUND"
		}
		if err != nil {
			return "ERR " + err.Error()
		}
		return "VALUE " + value
	case "DELETE":
		if len(parts) != 2 || parts[1] == "" {
			return "ERR usage: DELETE key"
		}
		if err := server.service.Delete(parts[1]); err != nil {
			return "ERR " + err.Error()
		}
		return "OK"
	default:
		return "ERR unknown command"
	}
}
