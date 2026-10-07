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


DROP VIEW IF EXISTS market_bars_daily_mv;

CREATE VIEW IF NOT EXISTS market_bars_current AS
SELECT *
FROM market_bars FINAL;

CREATE VIEW IF NOT EXISTS market_bars_daily AS
SELECT
    symbol,
    interval,
    toDate(timestamp) AS day,
    argMin(open, timestamp) AS open,
    max(high) AS high,
    min(low) AS low,
    argMax(close, timestamp) AS close,
    sum(volume) AS volume
FROM market_bars_current
GROUP BY symbol, interval, day;
