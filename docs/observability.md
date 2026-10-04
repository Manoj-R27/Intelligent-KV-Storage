# Observability and Reliability

The telemetry package exposes counters, gauges, and request latency quantiles in Prometheus text format without uncontrolled key labels. The reliability package starts with a rolling-window z-score detector. An anomaly is evidence, not a diagnosis: future correlation code will combine network, storage, Raft, and process signals before producing a likely cause and confidence value.

The deterministic database remains authoritative. Reliability analysis cannot commit writes, elect leaders, or execute destructive operations.