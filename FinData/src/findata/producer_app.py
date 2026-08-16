"""Finnhub WebSocket → Kafka producer application."""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from datetime import datetime
from typing import Any
from urllib.parse import urlsplit

import websocket

from findata.config import Settings, load_settings
from findata.kafka_producer import TradeKafkaProducer
from findata.lineage import OpenLineageEmitter, finnhub_dataset, kafka_dataset
from findata.models import parse_finnhub_trade_message
from findata.ports import TradePublisher
from findata.symbols import get_sp500_symbols

WsFactory = Callable[..., Any]


def resolve_symbols(settings: Settings) -> list[str]:
    if settings.subscribe_symbols:
        return [
            s.strip()
            for s in settings.subscribe_symbols.split(",")
            if s.strip()
        ]

    symbols = get_sp500_symbols(gics_sector=settings.gics_sector)
    print(f"Found {len(symbols)} symbols in {settings.gics_sector}")
    if settings.symbol_limit is not None:
        symbols = symbols[: settings.symbol_limit]
    return symbols


def handle_trade_message(raw: str | dict, publisher: TradePublisher) -> int:
    """Parse a Finnhub WS message and publish trades. Returns count published."""
    trades = parse_finnhub_trade_message(raw)
    for trade in trades:
        publisher.publish_trade(trade)
        print(
            f"{trade['s']} @ {trade['p']} "
            f"(vol={trade['v']}, ts={datetime.fromtimestamp(trade['t'] / 1000)})"
        )
    if trades:
        publisher.flush()
    return len(trades)


def run_producer(
    settings: Settings,
    publisher: TradePublisher,
    symbols: Sequence[str],
    *,
    lineage: OpenLineageEmitter | None = None,
    ws_factory: WsFactory = websocket.WebSocketApp,
    run_forever: bool = True,
) -> Any:
    """Wire Finnhub WS callbacks to ``publisher``. Returns the WS app instance."""
    if not settings.using_mock_finnhub and not settings.finnhub_api_key:
        raise ValueError(
            "FINNHUB_API_KEY is required (GitHub secret, secret manager, "
            "or gitignored FinData/.env)"
        )

    ws_url = settings.resolve_finnhub_ws_url()
    topic = settings.kafka_topic
    bootstrap = settings.kafka_bootstrap_servers
    lineage = lineage or OpenLineageEmitter(
        "finnhub_producer", enabled=settings.openlineage_enabled
    )
    lineage.start(
        inputs=[finnhub_dataset()],
        outputs=[kafka_dataset(topic, bootstrap)],
    )

    def on_open(ws: websocket.WebSocketApp) -> None:
        for symbol in symbols:
            ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
        print(
            f"Subscribed to {len(symbols)} symbols "
            f"({settings.gics_sector}) on topic '{topic}'"
        )

    def on_message(_ws: websocket.WebSocketApp, message: str) -> None:
        handle_trade_message(message, publisher)

    def on_error(_ws: websocket.WebSocketApp, error: Exception) -> None:
        print(f"WebSocket error: {error}")
        lineage.fail(
            error=str(error),
            inputs=[finnhub_dataset()],
            outputs=[kafka_dataset(topic, bootstrap)],
        )

    def on_close(_ws: websocket.WebSocketApp, *_args) -> None:
        print("WebSocket closed, flushing Kafka producer")
        publisher.close()
        lineage.complete(
            inputs=[finnhub_dataset()],
            outputs=[kafka_dataset(topic, bootstrap)],
        )

    ws = ws_factory(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    parsed_ws_url = urlsplit(ws_url)
    host = parsed_ws_url.hostname or ""
    port = f":{parsed_ws_url.port}" if parsed_ws_url.port is not None else ""
    path = parsed_ws_url.path or ""
    safe_ws_url = f"{parsed_ws_url.scheme}://{host}{port}{path}"
    print(f"Connecting to {safe_ws_url}")
    if run_forever:
        ws.run_forever()
    return ws


def main() -> None:
    settings = load_settings()
    symbols = resolve_symbols(settings)
    publisher = TradeKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_topic,
    )
    run_producer(settings, publisher, symbols)
