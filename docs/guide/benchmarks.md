# Benchmark baseline

Measured with Criterion on 2026-06-21 on a local macOS development machine.

Command:

```bash
cargo bench -p pharos-benches --all-features
```

## Results

| Benchmark | Mean time | Derived throughput |
| --- | ---: | ---: |
| `event_bus_publish_no_handlers` | 19.004 ns | 52.6M events/s |
| `event_bus_publish_one_handler` | 90.119 ns | 11.1M events/s |
| `dispatch_pending_100_messages_in_memory` | 220.45 us per 100 msgs | 453.6k msgs/s |

Throughput formulas:

- event bus: `1 / mean_time_seconds`
- outbox batch: `100 / mean_time_seconds`

## Scope and caveats

- These are in-memory baselines, useful for regression tracking.
- They do not include network/broker/database latency.
- Re-run on CI hardware before using as an external SLO commitment.
