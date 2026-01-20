# fault-tolerant-telemetry

Distributed telemetry pipeline with Raft consensus and fault injection, written in Rust.

5-node cluster replicates a stream of telemetry frames. The Raft layer handles leader election, log replication, and recovery from node failures. A simulated fault-injection network drops and delays packets to verify correctness under partition.

```
cargo run --release
```

Zero checksum failures across all fault scenarios. Each frame carries CRC-32 and is rejected on mismatch.
