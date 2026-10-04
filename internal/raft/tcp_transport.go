package raft

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
)

type TCPTransport struct {
	mu        sync.RWMutex
	addresses map[string]string
	timeout   int
}

func NewTCPTransport(addresses map[string]string) *TCPTransport {
	copyAddresses := make(map[string]string, len(addresses))
	for id, address := range addresses {
		copyAddresses[id] = address
	}
	return &TCPTransport{addresses: copyAddresses}
}

func (transport *TCPTransport) Send(envelope Envelope) {
	transport.mu.RLock()
	address := transport.addresses[envelope.To]
	transport.mu.RUnlock()
	if address == "" {
		return
	}
	connection, err := net.DialTimeout("tcp", address, 2e9)
	if err != nil {
		return
	}
	defer connection.Close()
	_ = json.NewEncoder(connection).Encode(envelope)
}

func (transport *TCPTransport) ListenAndServe(ctx context.Context, address string, node *Node) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen raft transport: %w", err)
	}
	defer listener.Close()
	go func() { <-ctx.Done(); _ = listener.Close() }()
	for {
		connection, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept raft connection: %w", err)
		}
		go transport.handle(connection, node)
	}
}

func (transport *TCPTransport) handle(connection net.Conn, node *Node) {
	defer connection.Close()
	decoder := json.NewDecoder(bufio.NewReader(connection))
	var envelope Envelope
	if err := decoder.Decode(&envelope); err == nil {
		node.Receive(envelope)
	}
}
