# Architecture

The project is intentionally built in milestones. The long-term request path is:

`TCP client -> request parser -> KV service -> Raft -> state machine -> storage engine`

The storage engine is independent of networking and consensus so it can be tested in isolation. Raft will submit committed commands to the state machine; it will not let an AI component participate in correctness decisions.

Current implementation includes the storage engine, TCP single-node service, Raft core, metrics formatter, and anomaly detector. The TCP path does not yet route mutations through Raft, and no distributed consistency claim is made until that integration and its failure tests are complete.