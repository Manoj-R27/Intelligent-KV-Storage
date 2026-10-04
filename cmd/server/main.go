package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Manoj-R27/Intelligent-KV-Storage/internal/cluster"
	"github.com/Manoj-R27/Intelligent-KV-Storage/internal/network"
	"github.com/Manoj-R27/Intelligent-KV-Storage/internal/raft"
	"github.com/Manoj-R27/Intelligent-KV-Storage/internal/storage"
	"github.com/Manoj-R27/Intelligent-KV-Storage/internal/telemetry"
)

func main() {
	address := flag.String("address", "127.0.0.1:7000", "TCP listen address")
	nodeID := flag.String("node-id", "node-1", "Raft node ID")
	raftAddress := flag.String("raft-address", "127.0.0.1:7100", "Raft transport listen address")
	peerSpec := flag.String("peers", "", "comma-separated Raft peers: node-2=host:port,node-3=host:port")
	dataDirectory := flag.String("data", "./data", "storage directory")
	memTableEntries := flag.Int("memtable-entries", 1024, "entries per MemTable flush")
	electionTicks := flag.Int("election-ticks", 10, "ticks before a node starts an election")
	heartbeatTicks := flag.Int("heartbeat-ticks", 2, "ticks between leader heartbeats")
	metricsAddress := flag.String("metrics-address", "127.0.0.1:9090", "Prometheus metrics address")
	flag.Parse()

	engine, err := storage.Open(storage.Config{Directory: *dataDirectory, MemTableEntries: *memTableEntries})
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()
	peers, addresses := parsePeers(*peerSpec)
	transport := raft.NewTCPTransport(addresses)
	stateStore := raft.NewFileStateStore(filepath.Join(*dataDirectory, "raft-state.json"))
	service := cluster.NewServiceWithState(*nodeID, peers, transport, engine, *electionTicks, *heartbeatTicks, stateStore)
	metrics := telemetry.NewMetrics()
	server, err := network.ListenWithMetrics(*address, service, metrics)
	if err != nil {
		log.Fatal(err)
	}
	defer server.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go tickRaft(ctx, service.Node(), metrics)
	go func() {
		if err := transport.ListenAndServe(ctx, *raftAddress, service.Node()); err != nil {
			log.Printf("raft transport stopped: %v", err)
		}
	}()
	metricsServer := &http.Server{Addr: *metricsAddress, Handler: metrics.Handler()}
	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics server stopped: %v", err)
		}
	}()
	defer metricsServer.Close()
	log.Printf("listening on %s with data directory %s", server.Address(), *dataDirectory)
	if err := server.Serve(ctx); err != nil {
		log.Fatal(err)
	}
}

func parsePeers(specification string) ([]string, map[string]string) {
	addresses := make(map[string]string)
	var peers []string
	for _, item := range strings.Split(specification, ",") {
		parts := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			continue
		}
		peers = append(peers, parts[0])
		addresses[parts[0]] = parts[1]
	}
	return peers, addresses
}

func tickRaft(ctx context.Context, node *raft.Node, metrics *telemetry.Metrics) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			node.Tick()
			metrics.Set("raft_current_term", float64(node.Term()))
			metrics.Set("raft_commit_index", float64(node.CommitIndex()))
			metrics.Set("raft_applied_index", float64(node.LastApplied()))
			role := node.Role()
			metrics.Set("raft_is_leader", boolGauge(role == raft.Leader))
			metrics.Set("raft_is_candidate", boolGauge(role == raft.Candidate))
			metrics.Set("raft_is_follower", boolGauge(role == raft.Follower))
			metrics.Set("node_up", 1)
		}
	}
}

func boolGauge(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
