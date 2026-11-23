# Benchmark results

Measured on 2026-11-23 15:19:10 against the real Binance `aggTrade` and
`depth@100ms` streams for `BTCUSDT`.

| Field | Value |
|---|---|
| Date (UTC) | 2026-11-23 15:19:10 |
| Duration | 600.0 s |
| CPU | AMD Ryzen 9 5900X 12-Core Processor |
| Cores / threads | 12 / 24 |
| RAM | 31.9 GiB |
| OS | Windows-11-10.0.26300-SP0 |
| Python | 3.12.10 |
| Clock offset applied (Binance - host) | +1.352 s |
| Command | `python scripts/benchmark.py --duration 600 --symbol BTCUSDT --report docs\benchmark.md` |

## Throughput

| Metric | Measured |
|---|---|
| WebSocket messages | 26,168 (43.61/s) |
| Trade messages | 20,247 (33.74/s) |
| Depth messages | 5,921 (9.87/s) |
| Trades ingested | 20,247 |
| Trades persisted (whole run) | 21,340 |
| Gaps detected | 0 |
| Trades backfilled | 0 |
| Trades missing | 0 |

## Latency

| Metric | p50 | p95 | Mean |
|---|---|---|---|
| Exchange timestamp to WebSocket ingest | 188.8 ms | 785.3 ms | 279.2 ms |
| Exchange timestamp to Parquet write | 3120.2 ms | 8594.6 ms | 3203.1 ms |

## Storage

| Metric | Measured |
|---|---|
| Trade data | 1.00 MiB |
| Book data | 539.82 KiB |
| Trade bytes per row | 49.2 B |
| Disk per million trades | 46.92 MiB |
| Total disk growth | 8.61 MiB/h |
| Trade files | 117 |
| Book files | 105 |
| Process uptime at final read | 639.2 s |

## Process

| Metric | Measured |
|---|---|
| RSS average | 106.1 MiB |
| RSS peak | 111.2 MiB |
| CPU average | 0.016 cores (1.6% of one core) |
| CPU seconds consumed | 9.7 s |

## Method

- Throughput comes from the service's Prometheus counters, sampled at the start
  and at the end of the window. Samples are not interpolated.
- Latency percentiles come from the `mds_message_latency_seconds` and
  `mds_write_lag_seconds` histograms exported by the process itself. They are
  bucket-interpolated, so their resolution is the bucket width. The mean is
  `_sum / _count`.
- The host clock was compared with `GET /api/v3/time` five times and the
  median offset was passed to the service as `MDS_CLOCK_OFFSET_S`. The
  histograms are therefore already expressed in exchange time. If the offset
  is `n/a`, the host could not reach Binance and the service ran with 0.
- The service clamps negative histogram observations at zero; the benchmark
  reports the offset separately so clock skew stays visible.
- Disk numbers are the file sizes reported by `/api/v1/status` after a
  seven-second drain that lets buffered rows reach Parquet. They cover every
  file written by the process, including the warmup. The per-million figure
  divides the measured trade bytes by the measured persisted trade count.
- The hourly disk figure multiplies the measured growth by `3600 / uptime`,
  where uptime comes from the same status payload. It is a unit conversion of
  the measured rate, not an independent estimate.
- Memory and CPU come from `psutil` sampling the service process. RSS is the
  process working set. CPU is the delta of `user + system` CPU time divided by
  the window.
- Exchange timestamps are compared after applying the measured host offset;
  residual skew and network latency remain part of the reported values.

