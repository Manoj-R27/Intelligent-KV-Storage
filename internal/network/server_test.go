package network

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"

	"github.com/Manoj-R27/Intelligent-KV-Storage/internal/cluster"
	"github.com/Manoj-R27/Intelligent-KV-Storage/internal/raft"
	"github.com/Manoj-R27/Intelligent-KV-Storage/internal/storage"
)

func TestServerHandlesKVCommands(t *testing.T) {
	engine, err := storage.Open(storage.Config{Directory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	transport := raft.NewMemoryTransport()
	service := cluster.NewService("node-1", nil, transport, engine, 2, 1)
	transport.Register(service.Node())
	service.Node().Tick()
	service.Node().Tick()
	server, err := Listen("127.0.0.1:0", service)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(ctx) }()
	connection, err := net.DialTimeout("tcp", server.Address(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	writer := bufio.NewWriter(connection)
	reader := bufio.NewReader(connection)
	command := func(request, expected string) {
		t.Helper()
		if _, err := writer.WriteString(request + "\n"); err != nil {
			t.Fatal(err)
		}
		if err := writer.Flush(); err != nil {
			t.Fatal(err)
		}
		response, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if response != expected+"\n" {
			t.Fatalf("response = %q, want %q", response, expected)
		}
	}
	command("PUT user:101 Manoj", "OK")
	command("GET user:101", "VALUE Manoj")
	command("DELETE user:101", "OK")
	command("GET user:101", "NOT_FOUND")
	connection.Close()
	cancel()
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not shut down")
	}
}
