-- Tick data: 30-day retention
CREATE TABLE IF NOT EXISTS findata.trades
(
    symbol LowCardinality(String),
    price Float64 CODEC(Gorilla, ZSTD(1)),
    volume Float64 CODEC(Gorilla, ZSTD(1)),
    timestamp DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- Time bars (no TTL)
CREATE TABLE IF NOT EXISTS findata.bars_1m
(
    symbol LowCardinality(String),
    window_start DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    open Float64 CODEC(Gorilla, ZSTD(1)),
    high Float64 CODEC(Gorilla, ZSTD(1)),
    low Float64 CODEC(Gorilla, ZSTD(1)),
    close Float64 CODEC(Gorilla, ZSTD(1)),
    volume Float64 CODEC(Gorilla, ZSTD(1)),
    trade_count UInt32,
    vwap Float64 CODEC(Gorilla, ZSTD(1))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (symbol, window_start)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS findata.bars_5m
(
    symbol LowCardinality(String),
    window_start DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    open Float64 CODEC(Gorilla, ZSTD(1)),
    high Float64 CODEC(Gorilla, ZSTD(1)),
    low Float64 CODEC(Gorilla, ZSTD(1)),
    close Float64 CODEC(Gorilla, ZSTD(1)),
    volume Float64 CODEC(Gorilla, ZSTD(1)),
    trade_count UInt32,
    vwap Float64 CODEC(Gorilla, ZSTD(1))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (symbol, window_start)
SETTINGS index_granularity = 8192;

-- Volume bars: ~50,000 shares per bar (no TTL)
CREATE TABLE IF NOT EXISTS findata.bars_volume
(
    symbol LowCardinality(String),
    window_start DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    open Float64 CODEC(Gorilla, ZSTD(1)),
    high Float64 CODEC(Gorilla, ZSTD(1)),
    low Float64 CODEC(Gorilla, ZSTD(1)),
    close Float64 CODEC(Gorilla, ZSTD(1)),
    volume Float64 CODEC(Gorilla, ZSTD(1)),
    trade_count UInt32,
    vwap Float64 CODEC(Gorilla, ZSTD(1)),
    notional Float64 CODEC(Gorilla, ZSTD(1))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (symbol, window_start)
SETTINGS index_granularity = 8192;

-- Dollar bars: ~$5,000,000 notional per bar (no TTL)
CREATE TABLE IF NOT EXISTS findata.bars_dollar
(
    symbol LowCardinality(String),
    window_start DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    open Float64 CODEC(Gorilla, ZSTD(1)),
    high Float64 CODEC(Gorilla, ZSTD(1)),
    low Float64 CODEC(Gorilla, ZSTD(1)),
    close Float64 CODEC(Gorilla, ZSTD(1)),
    volume Float64 CODEC(Gorilla, ZSTD(1)),
    trade_count UInt32,
    vwap Float64 CODEC(Gorilla, ZSTD(1)),
    notional Float64 CODEC(Gorilla, ZSTD(1))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (symbol, window_start)
SETTINGS index_granularity = 8192;
