# Data model

All records are normalized before they touch disk. The lake is immutable: files
are written once, never updated. Every timestamp is UTC and every price is an
exact decimal.

## Storage layout

```text
data/
├── trades/
│   └── symbol=BTCUSDT/
│       └── date=2026-11-23/
│           ├── part-20261123T132750123456Z-4f2a9c1e.parquet
│           └── part-20261123T132755234567Z-90b1d7aa.parquet
└── book/
    └── symbol=BTCUSDT/
        └── date=2026-11-23/
            └── part-20261123T132750123456Z-0d5e8c31.parquet
```

* `date=YYYY-MM-DD` is the UTC calendar date of `event_time`.
* A new file is started when the in-memory buffer reaches 50 000 rows, 64 MiB
  or 5 seconds. All three thresholds are configurable.
* Files are written to a hidden temporary name in the destination directory and
  moved with `os.replace`, so a reader never sees a half-written Parquet file.
* Data is compressed with zstd.

The dataset layout is Hive-style for interoperability, but the service queries
it with `hive_partitioning = false`: `symbol` and `date` are also real columns
and physical partition paths, so automatic partition inference would duplicate
them.

## `trades` dataset

One row per Binance aggregate trade (`<symbol>@aggTrade`).

| Column | Arrow type | Unit | Notes |
|---|---|---|---|
| `symbol` | `string` | | Uppercase, e.g. `BTCUSDT` |
| `trade_id` | `uint64` | | Aggregate trade id, monotonic per symbol |
| `price` | `decimal128(38, 12)` | quote currency | Exact, as published |
| `quantity` | `decimal128(38, 12)` | base asset | Exact, as published |
| `quote_quantity` | `decimal128(38, 12)` | quote currency | `price * quantity`, rounded to 12 dp |
| `event_time` | `timestamp[us, UTC]` | | Exchange event time (`E`) |
| `trade_time` | `timestamp[us, UTC]` | | Exchange trade time (`T`) |
| `first_trade_id` | `uint64` | | First raw trade id in the aggregate (`f`) |
| `last_trade_id` | `uint64` | | Last raw trade id in the aggregate (`l`) |
| `is_buyer_maker` | `bool` | | `true` means the aggressor was a seller |
| `ingested_at` | `timestamp[us, UTC]` | | Local receive time |

Why aggregate trades: their ids are contiguous and `GET /api/v3/aggTrades`
accepts `fromId`, which makes an exact gap repair possible without an API key.
The raw trade stream has a different id space and no public `fromId` backfill.

### Invariants

* `trade_id` is strictly increasing in the order data is written.
* There are no duplicate ids.
* The sequence is contiguous except for ids reported as missing on
  `mds_trades_missing_total`. A missing id is a visible hole, never a silent one.
* `price > 0`, `quantity > 0`, `quote_quantity >= 0`.
* `date` equals `event_time` truncated to the UTC day.

## `book` dataset

Long format, one row per price level per sampled snapshot. Snapshots are taken
from the synchronized local book once per second by default.

| Column | Arrow type | Unit | Notes |
|---|---|---|---|
| `symbol` | `string` | | Uppercase |
| `last_update_id` | `uint64` | | Binance `u` of the last applied diff |
| `event_time` | `timestamp[us, UTC]` | | Event time of that last diff |
| `ingested_at` | `timestamp[us, UTC]` | | Snapshot capture time |
| `side` | `string` | | `bid` or `ask` |
| `level` | `uint16` | | `0` is the best price on that side |
| `price` | `decimal128(38, 12)` | quote currency | Exact |
| `quantity` | `decimal128(38, 12)` | base asset | Exact, always `> 0` |

Zero-quantity updates are book removals and are not persisted. The top 20
levels per side are captured (`MDS_DEPTH_LEVELS`). The local book itself is
maintained at 100 ms cadence from the diff stream.

## Why the Parquet schema is explicit

PyArrow infers types from Python objects. That is convenient and dangerous:
`0.1 + 0.2` becomes a float and a 12-decimal price silently loses digits. This
project declares the schema up front, stores prices and quantities as
`decimal128`, and never converts them to `float64`.

## Querying with DuckDB

```sql
SELECT trade_id, price, quantity, trade_time
FROM read_parquet('data/trades/symbol=BTCUSDT/date=*/*.parquet', hive_partitioning = false)
WHERE symbol = 'BTCUSDT'
ORDER BY trade_id DESC
LIMIT 10;
```

Candles are derived from trades at query time, not stored twice:

```sql
SELECT CAST(floor(epoch(trade_time) / 60) * 60 AS BIGINT) AS bucket,
       first(price ORDER BY trade_time, trade_id) AS open,
       max(price) AS high,
       min(price) AS low,
       last(price ORDER BY trade_time, trade_id) AS close,
       sum(quantity) AS volume,
       count(*) AS trades
FROM read_parquet('data/trades/symbol=BTCUSDT/date=*/*.parquet', hive_partitioning = false)
WHERE symbol = 'BTCUSDT'
GROUP BY bucket
ORDER BY bucket;
```

## Time semantics

* Exchange timestamps are milliseconds since the Unix epoch. They are converted
  with `datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=ms)`; no float
  division is involved.
* `event_time` is when the exchange produced the event. `ingested_at` is when
  this process received it. `lag_seconds = ingested_at - event_time` is exported
  as `mds_ingest_lag_seconds` and as a histogram.
* Candles cover `[open_time, close_time)`. `close_time` in the API is exclusive.
