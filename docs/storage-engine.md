# Storage Engine

## Milestone 1

The WAL uses a length-prefixed binary frame. Each frame contains a record type, key length, value length, key, and value, followed by a CRC-32 checksum in the frame header. Appends are followed by `File.Sync`, so a successful append has reached the operating system's durable-write boundary exposed by Go.

Replay rejects short headers, short payloads, invalid field lengths, unknown record types, and checksum mismatches. A partial final record is therefore reported as corruption instead of being silently accepted.

The MemTable uses a mutex-protected map for point lookup and update, then sorts a copy when a snapshot is requested. This is a deliberate first-milestone tradeoff: map operations are simple and fast for point reads, while sorting is deferred to flush time. A production LSM MemTable may use a skip list or balanced tree to keep ordered iteration cheap, which will matter when SSTables are added.

Deletes are represented as tombstones. They must be retained through flush and compaction until it is safe to remove them, otherwise an older SSTable could resurrect deleted data.