# Raft

Each node tracks a current term, vote, replicated log, commit index, and applied index. Elections use RequestVote messages; leaders use AppendEntries for heartbeats and log replication. A log entry is acknowledged only after a majority reports the matching index and the leader applies the committed command to its state machine.

The production transport uses one JSON envelope per TCP connection on the configured Raft address. The deterministic test transport can drop or partition links without involving the network.

Term, voted-for, and log state are written atomically to `raft-state.json` in the node data directory. The KV storage engine remains responsible for its own WAL and recovery. On restart, Raft restores its log before elections resume; committed commands are replayed by the storage recovery path and subsequent committed entries are applied by Raft.

The current command server supports multi-process Raft transport, but leader discovery and client forwarding are not automatic: clients must connect to the current leader. A production follow-up is a redirect response containing the leader address and a client retry policy.