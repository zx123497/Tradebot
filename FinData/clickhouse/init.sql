CREATE TABLE IF NOT EXISTS findata.trades
(
    symbol String,
    price Float64,
    volume Float64,
    timestamp_ms UInt64,
    trade_time DateTime64(3, 'UTC'),
    received_at DateTime64(3, 'UTC'),
    conditions Array(String) DEFAULT []
)
ENGINE = MergeTree()
ORDER BY (symbol, timestamp_ms);

CREATE TABLE IF NOT EXISTS findata.bars_1m
(
    symbol String,
    window_start DateTime64(3, 'UTC'),
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    trade_count UInt32,
    vwap Float64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (symbol, window_start);
