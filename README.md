# Intelligent Distributed KV Store & Reliability Platform

This repository is being built incrementally as a real Go distributed key-value store. The core implementation will use TCP, a custom LSM-style storage engine, and a Raft replicated state machine. Reliability analysis and the dashboard will be added only after the deterministic database is working.

## Current implementation

The repository currently contains:

- Checksummed, length-prefixed WAL records.
- `fsync` after every mutation append.
- WAL replay with corruption and incomplete-record detection.
- Concurrent MemTable with PUT, GET, DELETE tombstones, and sorted snapshots.
- Persistent SSTables with recovery, point lookup, flush thresholds, and compaction.
- A concurrent line-oriented TCP API and runnable single-node server.
- A message-driven Raft core with elections, replication, majority commit, and partitionable in-memory transport.
- Prometheus-compatible metrics formatting and rolling z-score anomaly detection.

The TCP server now routes mutations through the Raft-backed cluster service. The default command starts a single-node Raft cluster; multi-process TCP Raft transport and persistent Raft metadata are the next distributed-production steps.

Run the tests after installing Go:

```powershell
go test ./...
go test -race ./...
```

Run a single-node server and use the client:

```powershell
go run ./cmd/server -address 127.0.0.1:7000 -data ./data
go run ./cmd/client PUT user:101 Manoj
go run ./cmd/client GET user:101
go run ./cmd/client DELETE user:101
```

Start three Raft nodes on separate ports:

```powershell
go run ./cmd/server -node-id node-1 -address 127.0.0.1:7001 -raft-address 127.0.0.1:7101 -peers "node-2=127.0.0.1:7102,node-3=127.0.0.1:7103" -data ./data/node-1
go run ./cmd/server -node-id node-2 -address 127.0.0.1:7002 -raft-address 127.0.0.1:7102 -peers "node-1=127.0.0.1:7101,node-3=127.0.0.1:7103" -data ./data/node-2
go run ./cmd/server -node-id node-3 -address 127.0.0.1:7003 -raft-address 127.0.0.1:7103 -peers "node-1=127.0.0.1:7101,node-2=127.0.0.1:7102" -data ./data/node-3
```

Mutation responses are returned only after the Raft log entry reaches a majority and is applied to the local state machine. A leader without a majority returns a commit timeout instead of acknowledging an uncommitted write.

Open `dashboard/index.html` in a browser while the server is running to view live request counters and latency telemetry. Use `dashboard/index.html?metrics=http://127.0.0.1:9090/` when the metrics endpoint is on another address.

## Planned order

1. Integrate Raft commit application with the KV service.
2. Add Bloom filters, persistent Raft state, and TCP Raft transport.
3. Build the reusable cluster fault-injection harness and recovery scenarios.
4. Wire metrics into all request, Raft, and storage paths.
5. Add benchmark runs, incident correlation, recommendations, and dashboard.

See [docs/architecture.md](docs/architecture.md) and [docs/storage-engine.md](docs/storage-engine.md) for design notes.