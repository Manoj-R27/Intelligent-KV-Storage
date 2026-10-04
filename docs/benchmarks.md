# Benchmarks

The benchmark package measures storage point PUT and GET operations using Go's benchmark harness. It intentionally contains no checked-in performance claims. Generate measurements locally with:

```powershell
go test ./benchmarks -bench=. -benchmem -count=5 | Tee-Object benchmarks/results/storage.txt
```

Distributed and concurrent benchmarks will be added after TCP Raft transport is integrated. Results in `benchmarks/results/` must come from an actual run on the machine being described.