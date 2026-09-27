CREATE TABLE IF NOT EXISTS market_bars
(
    symbol      String,
    interval    LowCardinality(String),
    timestamp   DateTime,
    open        Float64,
    high        Float64,
    low         Float64,
    close       Float64,
    volume      UInt64,
    sma_5       Nullable(Float64),
    sma_20      Nullable(Float64),
    rsi_14      Nullable(Float64),
    vwap        Nullable(Float64),
    ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, interval, timestamp);

CREATE MATERIALIZED VIEW IF NOT EXISTS market_bars_daily_mv
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (symbol, day)
AS
SELECT
    symbol,
    toDate(timestamp) AS day,
    argMinState(open, timestamp)  AS open_state,
    maxState(high)                AS high_state,
    minState(low)                 AS low_state,
    argMaxState(close, timestamp) AS close_state,
    sumState(volume)               AS volume_state
FROM market_bars
GROUP BY symbol, day;
