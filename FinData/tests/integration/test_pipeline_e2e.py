"""End-to-end pipeline integration test using mock Finnhub + ClickHouse fixtures.

Requires infra already running (Kafka, Flink, ClickHouse, consumer):
  make infra
  cd FinData && docker compose up -d consumer

Then:
  make test-integration
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import clickhouse_connect
import pytest

from findata.config import FIN_DATA_DIR, load_settings
from findata.fixtures import FIXTURE_SYMBOL, expected_bar, seed_trades

SRC_DIR = FIN_DATA_DIR / "src"
WAIT_BARS_SEC = int(os.getenv("INTEGRATION_WAIT_BARS_SEC", "90"))


def _client():
    settings = load_settings()
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
    )


def fetch_bar(symbol: str, window_start_ms: int) -> dict | None:
    client = _client()
    window_start = datetime.fromtimestamp(window_start_ms / 1000, tz=timezone.utc)
    result = client.query(
        """
        SELECT symbol, window_start, open, high, low, close, volume, trade_count, vwap
        FROM bars_1m
        WHERE symbol = {symbol:String}
          AND window_start = {window_start:DateTime64(3, 'UTC')}
        ORDER BY window_start DESC
        LIMIT 1
        """,
        parameters={"symbol": symbol, "window_start": window_start},
    )
    if not result.result_rows:
        return None
    row = result.result_rows[0]
    return {
        "symbol": row[0],
        "window_start": row[1],
        "open": float(row[2]),
        "high": float(row[3]),
        "low": float(row[4]),
        "close": float(row[5]),
        "volume": float(row[6]),
        "trade_count": int(row[7]),
        "vwap": float(row[8]),
    }


def assert_bar(actual: dict, expected: dict, tol: float = 1e-6) -> None:
    for key in ("symbol", "trade_count"):
        assert actual[key] == expected[key], f"{key}: {actual[key]} != {expected[key]}"
    for key in ("open", "high", "low", "close", "volume", "vwap"):
        assert abs(actual[key] - expected[key]) <= tol, (
            f"{key}: {actual[key]} != {expected[key]}"
        )


def _kill_pids(pids: list[int]) -> None:
    for pid in pids:
        try:
            os.kill(pid, 15)
        except OSError:
            pass


def _pids_on_port(port: int) -> list[int]:
    try:
        out = subprocess.check_output(["lsof", "-ti", f"tcp:{port}"], text=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        return []
    return [int(pid) for pid in out.split() if pid.strip()]


def _pids_matching(*patterns: str) -> list[int]:
    try:
        out = subprocess.check_output(["pgrep", "-f", "|".join(patterns)], text=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        return []
    return [int(pid) for pid in out.split() if pid.strip()]


def _cleanup_leftovers(port: int = 8765) -> None:
    pids = set(_pids_on_port(port))
    pids.update(_pids_matching("mock_finnhub_ws.py", "FinData/src/main.py"))
    pids.discard(os.getpid())
    _kill_pids(sorted(pids))
    time.sleep(0.5)


@pytest.mark.integration
def test_pipeline_e2e():
    load_settings()
    _cleanup_leftovers()

    print("==> Seeding ClickHouse trades fixture", flush=True)
    window_start_ms = seed_trades(_client(), clear_existing=True)
    expected = expected_bar()
    print("Expected:", expected)

    mock_env = os.environ.copy()
    mock_env["PYTHONUNBUFFERED"] = "1"
    mock_env["PYTHONPATH"] = str(SRC_DIR)
    mock_env.setdefault("CLICKHOUSE_HOST", "localhost")
    mock_proc = subprocess.Popen(
        [sys.executable, "-u", str(SRC_DIR / "mock_finnhub_ws.py")],
        cwd=str(SRC_DIR),
        env=mock_env,
    )

    producer_env = os.environ.copy()
    producer_env.update(
        {
            "PYTHONUNBUFFERED": "1",
            "FINNHUB_WS_URL": os.getenv("FINNHUB_WS_URL", "ws://localhost:8765"),
            "FINNHUB_API_KEY": "test-token",
            "SUBSCRIBE_SYMBOLS": FIXTURE_SYMBOL,
            "KAFKA_BOOTSTRAP_SERVERS": os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"
            ),
            "OPENLINEAGE_ENABLED": "false",
            "PYTHONPATH": str(SRC_DIR),
        }
    )

    time.sleep(1.0)
    print("==> Starting producer against mock Finnhub")
    producer_proc = subprocess.Popen(
        [sys.executable, "-u", str(SRC_DIR / "main.py")],
        cwd=str(SRC_DIR),
        env=producer_env,
    )

    try:
        deadline = time.time() + WAIT_BARS_SEC
        actual = None
        while time.time() < deadline:
            actual = fetch_bar(FIXTURE_SYMBOL, window_start_ms)
            if actual:
                break
            print(
                f"Waiting for bars_1m {FIXTURE_SYMBOL} "
                f"window_start_ms={window_start_ms} ..."
            )
            time.sleep(5)

        assert actual is not None, "timed out waiting for aggregated bar in ClickHouse"
        print("Got bar:", actual)
        assert_bar(actual, expected)
    finally:
        producer_proc.terminate()
        mock_proc.terminate()
        try:
            producer_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            producer_proc.kill()
        try:
            mock_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            mock_proc.kill()
